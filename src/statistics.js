const By = require('../types/By');
const Aggregation = require('../types/Aggregation');
const Window = require('../types/Window');
const { keyFromEvent } = require('./utils');

module.exports = {
    _stats(args, events) {
        const by = args.filter(arg => arg instanceof By)
        const aggregations = args.filter(arg => arg instanceof Aggregation);
        const targetFields = [... new Set(aggregations.map(i => i.field))]

        const map = {}

        events.forEach(item => {
            const key = keyFromEvent(item, by)

            if (!map[key]) {
                map[key] = {
                    _statsRaw: {},
                }

                // Add key fields
                by.forEach(i => {
                    map[key][i.bySplit] = item[i.bySplit]
                })
            }

            targetFields.forEach(field => {
                if (!map[key]._statsRaw[field]) map[key]._statsRaw[field] = [];
                const _values = map[key]._statsRaw[field];
                _values.push(item[field])
            })
        })

        const arr = Object.keys(map).map(key => {
            const result = map[key]

            let sortedCache = {}
            aggregations.forEach(aggregation => {
                const outputField = aggregation.outputField
                const reference = map[key]._statsRaw[aggregation.field]

                if (aggregation.sortable) {
                    sortedCache[aggregation.field] = reference.slice().sort((a, b) => a - b)
                    result[outputField] = aggregation.calculate(sortedCache[aggregation.field])
                } else {
                    result[outputField] = aggregation.calculate(reference)
                }


            })

            delete map[key]._statsRaw
            return map[key]
        })

        return { arr, map, by, aggregations }
    },

    stats(...args) {
        this.manageEntry()
        this.events = this._stats(args, this.events).arr
        return this.manageExit()
    },

    eventstats(...args) {
        this.manageEntry()
        const stats = this._stats(args, this.events)

        this.events.forEach(event => {
            const key = keyFromEvent(event, stats.by)

            Object.assign(event, stats.map[key])
        })

        return this.manageExit()
    },

    _streamstats(...args) {
        const backwardIterate = (event, i, by, maxBoundary = 0) => {
            let backwardIndex = 0
            const thisKey = keyFromEvent(event, by)
            const byKey = thisKey

            while (true) {
                const target = i - backwardIndex

                if (target < 0 || target < maxBoundary) break

                const newKey = keyFromEvent(this.events[target], by)
                if (thisKey !== newKey) break
                backwardIndex++
            }

            return { byKey, start: i - backwardIndex + 1 }
        }


        const window = args.filter(i => i instanceof Window)
        const by = args.filter(i => i instanceof By)

        // Perform some validation
        if (window.length > 1) throw new Error('Only one window allowed in streamstats')

        this.events.forEach((event, i) => {
            let start, byKey = "";

            // Refine to window size 
            if (window.length > 0) start = Math.max(i - window[0].size + 1, 0)
            if (by.length !== 0) ({ start, byKey } = backwardIterate(event, i, by, start))

            const eventRange = this.events.slice(start, i + 1)
            const embed = this._stats(args, eventRange).map[byKey]
            Object.assign(event, {
                _streamstats: embed
            })
        })

        // We need to assign to a separate streamstats object to avoid collusions
        // As streamstats iteratively updates the data but rlies on previous samples
        // Modifying data in place corrupts the results of the query
        this.events.forEach(event => {
            Object.assign(event, event._streamstats)
            delete event._streamstats
        })

        return this
    },

    streamstats(...args) {
        this.manageEntry()
        this._streamstats(...args)
        return this.manageExit()
    },

    delta(field, remapField, ...bys) {
        this.manageEntry()
        this._streamstats(new Aggregation(field, 'range', remapField), new Window(2), ...bys)
        return this.manageExit()
    }
}
