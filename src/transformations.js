const dayjs = require('dayjs');

module.exports = {
    eval(modifier, discard) {

        this.events.forEach((event, i) => {
            const vals = modifier(event)
            if (discard && vals) {
                this.events[i] = vals
            } else if (vals) {
                Object.assign(event, vals)
            }
        })
        return this;
    },

    _table(modifier) {
        this.events = this.events.map(event => {
            const vals = modifier(event)
            return vals;
        })
    },

    table(modifier) {

        this._table(modifier)
        return this;
    },

    rename(...entities) {

        this.events.forEach(event => {
            entities.forEach(([from, to]) => {
                event[to] = event[from]
                delete event[from]
            })
        })
        return this;
    },

    parseTime(value, customFormat) {

        this.events.forEach(event => {
            event[value] = dayjs(event[value], customFormat).valueOf()
        })
        return this;
    },

    bin(value, span) {

        this.events.forEach(event => {
            event[value] = Math.floor(event[value] / span) * span
        })
        return this;
    },

    flatten(depth = 1) {

        this.events = this.events.flat(depth)
        return this;
    },

    mvexpand(targets) {

        const arr = []
        this.events.forEach(event => {
            if (event instanceof Array) {
                if (targets.length !== 0) throw new Error('Cannot mvexpand on a target value when source data is array')

                event.forEach((item, i) => {
                    item._mvExpand = i
                    arr.push(item)
                })
            } else {
                // Identify max iterations
                const max = targets.reduce((prev, curr) => Math.max(prev, event[curr].length, 0))

                for (let i = 0; i < max; i++) {
                    const obj = { ...event, _mvExpand: i }
                    targets.forEach(target => {
                        obj[target] = event[target][i]
                    })
                    arr.push(obj)
                }
            }
        })

        this.events = arr
        return this;
    }
}

