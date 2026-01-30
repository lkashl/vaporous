const keyFromEvent = (event, bys) => bys.map(i => event[i.bySplit]).join('|')

const _sort = (order, data, ...keys) => {
    return data.sort((a, b) => {
        let directive = 0;

        keys.some(key => {
            directive = typeof a[key] === 'number' ? a[key] - b[key] : a[key].localeCompare(b[key])
            if (order === 'dsc') directive = directive * -1
            if (directive !== 0) return true;
        })

        return directive;
    })
}

module.exports = {
    keyFromEvent,
    _sort,
    sort(order, ...keys) {

        this.events = _sort(order, this.events, ...keys)
        return this;
    },

    assert(funct) {

        const expect = (funct) => { if (!funct) throw new Error('Assertion failed') }
        this.events.forEach((event, i) => {
            funct(event, i, { expect, events: this.events })
        })
        return this;
    }
}
