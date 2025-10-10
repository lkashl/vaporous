const fs = require('fs')

const fileHandler = {}

module.exports = {
    _checkpoint(operation, name, data, { disableCloning }) {
        const operations = {
            create: () => {
                if (this.activeCheckpointRestore && this.checkpoints[name]) data = this.checkpoints[name].concat(data)
                this.checkpoints[name] = disableCloning ? data : structuredClone(data)
            },
            retrieve: () => this.events = disableCloning ? this.checkpoints[name] : structuredClone(this.checkpoints[name]),
            delete: () => delete this.checkpoints[name]
        }

        operations[operation]()
        return this
    },

    checkpoint(operation, name, { disableCloning } = {}) {
        this.manageEntry()
        this._checkpoint(operation, name, this.events, { disableCloning })
        return this.manageExit()
    },

    filterIntoCheckpoint(checkpointName, funct, { disableCloning = false, destroy = true } = {}) {
        this.manageEntry()
        const dataCheckpoint = this.events.filter(funct)
        this._checkpoint('create', checkpointName, dataCheckpoint, { disableCloning })
        if (destroy) this.events = this.events.filter(event => !funct(event))
        return this.manageExit()
    },


    async storedCheckpoint(operation, name, partitionBy) {
        this.manageEntry()

        if (operation == 'create') {
            if (this.activeCheckpointRestore && name !== this.activeCheckpointRestore) throw new Error('Only one checkpoint restoration can be active at a time')

            Object.keys(this.checkpoints).forEach(async (checkpoint) => {
                const checkpointEvents = this.checkpoints[checkpoint]

                for (const event in checkpointEvents) {
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
                                    thisFileHandler.once('drain', () => resolve(writeData(data)))
                                } else {
                                    resolve()
                                }
                            })
                        }

                        thisEvent._checkpoint = checkpoint;
                        await writeData(JSON.stringify(thisEvent) + '\n')
                    }
                }
            })

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


        return this.manageExit()
    }
}
