module.exports = {
    async debug(callback) {
        await callback(this)
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

        const initiationTime = new Date()
        let taskNum = 0

        while (this.processingQueue.length > 0) {
            const [method, params, { stack }] = this.processingQueue.splice(0, 1)[0]

            const opAlias = `${stageName}OP: ${taskNum} [${stack}] - ${method}`
            const start = new Date()

            try {
                await this[method](...params)
                if (this.loggers?.perf) console.log(`${opAlias} - took ${new Date() - start + ' ms'}`)
            } catch (err) {
                throw new Error(`${opAlias} not resolvable after ${new Date() - start + ' ms'} - ${err.message} - ${err.stack}`)
            }
            taskNum++
        }

        if (!stageName) {
            const nearestName = new Error().stack.toString().match(/Proxy\.(?!begin\b)[^(]+/)?.[0]
            if (nearestName) stageName = nearestName
        }

        console.log(`${stageName} - total time - ${new Date() - initiationTime} ms`)


        this._isExecuting = false
        return this;
    },

    clone({ deep } = {}) {
        const Vaporous = require('../Vaporous').Vaporous;
        const cloneInstance = new Vaporous()

        const excludeStructualClone = ['loggers', 'intervals']
        const excludeCompletely = ['_isExecuting']

        Object.keys(this).forEach(key => {
            if (excludeCompletely.includes(key)) return;
            cloneInstance[key] = (deep && !excludeStructualClone.includes(key)) ? structuredClone(this[key]) : this[key]
        })

        const purge = ['checkpoints', 'events']

        purge.forEach(purgeItem => {
            cloneInstance[purgeItem] = []
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
