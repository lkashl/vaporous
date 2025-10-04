const fs = require('fs');
const path = require('path');
const split2 = require('split2');
const Papa = require('papaparse');

module.exports = {
    fileScan(directory) {
        this.manageEntry()
        const items = fs.readdirSync(directory)
        this.events = items.map(item => {
            return {
                _fileInput: path.resolve(directory, item)
            }
        })
        return this.manageExit()
    },

    async csvLoad(parser) {
        this.manageEntry()
        const tasks = this.events.map(obj => {
            const content = []

            return new Promise((resolve, reject) => {
                const thisStream = fs.createReadStream(obj._fileInput)

                Papa.parse(thisStream, {
                    header: true,
                    skipEmptyLines: true,
                    step: (row) => {
                        try {
                            const event = parser(row)
                            if (event !== null) {
                                if (event instanceof Array) {
                                    event.forEach(item => {
                                        item._fileInput = obj._fileInput
                                        content.push(item)
                                    })
                                } else {
                                    event._fileInput = obj._fileInput
                                    content.push(event)
                                }
                            }
                        } catch (err) {
                            reject(err)
                        }
                    },
                    complete: () => {
                        resolve(content)
                    }
                })
            })
        })

        const payloads = await Promise.all(tasks)
        this.events = payloads
        return this.manageExit()
    },

    async fileLoad(delim, parser) {
        this.manageEntry()
        const tasks = this.events.map(obj => {
            const content = []

            return new Promise((resolve, reject) => {
                fs.createReadStream(obj._fileInput)
                    .pipe(split2(delim))
                    .on('data', line => {
                        try {
                            const event = parser(line)
                            if (!event) return;

                            if (event instanceof Array) {
                                event.forEach(item => {
                                    item._fileInput = obj._fileInput
                                    content.push(item)
                                })
                            } else {
                                if (!event._fileInput) event._fileInput = obj._fileInput
                                content.push(event)
                            }
                        } catch (err) {
                            throw err;
                        }

                    })
                    .on('end', () => {
                        resolve(content)
                    })
            })
        })

        this.events = await Promise.all(tasks)
        return this.manageExit()
    },

    writeFile(title) {
        this.manageEntry()
        fs.writeFileSync('./' + title, JSON.stringify(this.events))
        return this.manageExit()
    },

    output(...args) {
        this.manageEntry()
        if (args.length) {
            console.log(this.events.map(event => {
                return args.map(item => event[item])
            }))
        } else {
            console.log(this.events)
        }

        return this.manageExit()
    }
}
