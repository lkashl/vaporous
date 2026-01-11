module.exports = {
    method(operation, name, options) {
        const operations = {
            create: () => {
                this.savedMethods[name] = options
            },
            retrieve: () => {
                this.savedMethods[name](this, options)
            },
            delete: () => {
                delete this.savedMethods[name]
            }
        }

        operations[operation]()
        if (operation !== 'retrieve') return this;
        return this;
    },

    filter(...args) {
        this.events = this.events.filter(...args)
        return this;
    },

    append(entities) {
        this.events = this.events.concat(entities)
        return this;
    },

    async begin(stageName = '') {
        if (stageName) stageName = `[${stageName}] `
        this._isExecuting = true


        for (let taskNum in this.processingQueue) {
            const [method, params] = this.processingQueue[taskNum]
            const start = new Date()
            await this[method](...params)
            if (this.loggers?.perf) this.loggers.perf(1, `${stageName} OP: ${taskNum} - ${method} - took ${new Date() - start + ' ms'}`)
        }

        this.processingQueue = []
        this._isExecuting = false
        return this;
    },

    clone({ deep } = {}) {
        const Vaporous = require('../Vaporous').Vaporous;
        const cloneInstance = new Vaporous()

        const excludeStructualClone = ['loggers', 'intervals']
        const excludeCompletely = ['processingQueue', '_isExecuting']

        Object.keys(this).forEach(key => {
            if (excludeCompletely.includes(key)) return;
            cloneInstance[key] = (deep && !excludeStructualClone.includes(key)) ? structuredClone(this[key]) : this[key]
        })

        return cloneInstance
    },

    serialise() {
        return {
            events: this.events,
            processingQueue: this.processingQueue,
        }
    },

    destroy() {
        return this;
    }
}
