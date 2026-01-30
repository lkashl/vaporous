const fs = require('fs')

const fileHandler = {}

const closeCheckpointFiles = async () => {
    const closePromises = Object.keys(fileHandler).map(key => {
        return new Promise((resolve, reject) => {
            const handler = fileHandler[key]
            handler.end(() => {
                delete fileHandler[key]
                resolve()
            })
            handler.on('error', reject)
        })
    })

    return Promise.all(closePromises)
}


module.exports = {
    _checkpoint(operation, name, data, { disableCloning }) {
        const operations = {
            create: () => {
                if (this.activeCheckpointRestore && this.checkpoints[name]) data = this.checkpoints[name].concat(data)
                this.checkpoints[name] = disableCloning ? data : structuredClone(data)
            },
            retrieve: () => {
                if (typeof name === 'string') {
                    this.events = disableCloning ? this.checkpoints[name] : structuredClone(this.checkpoints[name])
                } else if (name instanceof Array) {
                    const events = [];

                    name.forEach(name => {
                        const target = disableCloning ? this.checkpoints[name] : structuredClone(this.checkpoints[name])
                        events.push(...target)
                    })

                    this.events = events
                } else {
                    throw new Error('Name argument not recognised ' + name)
                }
            },
            delete: () => delete this.checkpoints[name]
        }

        operations[operation]()
        return this
    },

    checkpoint(operation, name, { disableCloning } = {}) {

        this._checkpoint(operation, name, this.events, { disableCloning })
        return this;
    },

    filterIntoCheckpoint(checkpointName, funct, { disableCloning = false, destroy = true } = {}) {

        const dataCheckpoint = this.events.filter(funct)
        this._checkpoint('create', checkpointName, dataCheckpoint, { disableCloning })
        if (destroy) this.events = this.events.filter(event => !funct(event))
        return this;
    },

    async storedCheckpoint(operation, name, partitionBy) {


        if (operation == 'create') {
            if (this.activeCheckpointRestore && name !== this.activeCheckpointRestore) throw new Error('Only one checkpoint restoration can be active at a time')

            const checkpointKeys = Object.keys(this.checkpoints)


            for (const checkpoint of checkpointKeys) {
                const checkpointEvents = this.checkpoints[checkpoint]

                for (const event in checkpointEvents) {
                    const thisEvent = checkpointEvents[event]
                    const handlerKey = `${checkpoint}.${name}.${thisEvent[partitionBy]}.vpck`

                    if (!fileHandler[handlerKey]) fileHandler[handlerKey] = fs.createWriteStream(`./${handlerKey}`)
                    const thisFileHandler = fileHandler[handlerKey]

                    function writeData(data) {
                        return new Promise(resolve => {
                            const status = thisFileHandler.write(data)
                            if (!status) {
                                thisFileHandler.once('drain', resolve)
                            } else {
                                resolve()
                            }
                        })
                    }

                    thisEvent._checkpoint = checkpoint;
                    await writeData(JSON.stringify(thisEvent) + '\n')
                }

            }
            await closeCheckpointFiles()
            this.activeCheckpointRestore = null;
        } else if (operation == 'retrieve') {
            this.activeCheckpointRestore = name;

            const files = this._fileScan('./').filter(event => event._fileInput.includes('.vpck'))
            let filesToIntrospect = []
            const sessionsDiscovered = []

            this.events.forEach(event => {
                const entitiesFound = files.filter(file => file._fileInput.includes(name + "." + event.session + ".vpck"))
                if (entitiesFound.length > 0) sessionsDiscovered.push(event.session)
                filesToIntrospect = filesToIntrospect.concat(entitiesFound)
            })

            if (filesToIntrospect.length === 0) return this;

            let fileContents = await this._fileLoad(filesToIntrospect, '\n', event => JSON.parse(event))
            fileContents = fileContents.flat(1)

            fileContents.forEach(item => {
                if (!this.checkpoints[item._checkpoint]) this.checkpoints[item._checkpoint] = []
                this.checkpoints[item._checkpoint].push(item)
            })

            this.events = this.events.filter(event => !sessionsDiscovered.includes(event.session))

        }

        return this;
    }
}
