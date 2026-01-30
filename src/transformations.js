const dayjs = require('dayjs');
const { events } = require('./processing');

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

    mvexpand(target) {

        const arr = []
        this.events.forEach(event => {
            if (event instanceof Array) {
                if (!!target) throw new Error('Cannot mvexpand an array to a target')
                event.forEach((item, i) => {
                    item.i = i
                    arr.push(item)
                })
            } else {


                if (!event[target]) return arr.push(event)
                event[target].forEach((item, i) => {
                    arr.push({
                        ...event,
                        [target]: item,
                        [`_mvExpand_${target}`]: i
                    })
                })
            }
        })

        this.events = arr
        return this;
    }
}

