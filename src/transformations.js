const dayjs = require('dayjs');

module.exports = {
    eval(modifier) {
        this.manageEntry()
        this.events.forEach(event => {
            const vals = modifier(event)
            if (vals) Object.assign(event, vals)
        })
        return this.manageExit()
    },

    _table(modifier) {
        this.events = this.events.map(event => {
            const vals = modifier(event)
            return vals;
        })
    },

    table(modifier) {
        this.manageEntry()
        this._table(modifier)
        return this.manageExit()
    },

    rename(...entities) {
        this.manageEntry()
        this.events.forEach(event => {
            entities.forEach(([from, to]) => {
                event[to] = event[from]
                delete event[from]
            })
        })
        return this.manageExit()
    },

    parseTime(value, customFormat) {
        this.manageEntry()
        this.events.forEach(event => {
            event[value] = dayjs(event[value], customFormat).valueOf()
        })
        return this.manageExit()
    },

    bin(value, span) {
        this.manageEntry()
        this.events.forEach(event => {
            event[value] = Math.floor(event[value] / span) * span
        })
        return this.manageExit()
    },

    flatten(depth = 1) {
        this.manageEntry()
        this.events = this.events.flat(depth)
        return this.manageExit()
    },

    mvexpand(target) {
        this.manageEntry()
        const arr = []
        this.events.forEach(event => {
            if (!event[target]) return arr.push(event)
            event[target].forEach((item, i) => {
                arr.push({
                    ...event,
                    [target]: item,
                    [`_mvExpand_${target}`]: i
                })
            })
        })

        this.events = arr
        return this.manageExit()
    }
}
