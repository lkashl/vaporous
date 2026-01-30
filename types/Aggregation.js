class Aggregation {
    constructor(field, type, outputField = field, options) {
        this.type = type;
        this.field = field;
        this.outputField = outputField
        this.options = options;
        this.sortable = ['max', 'min', 'percentile', 'median', 'range'].includes(type)
    }

    count(values) {
        return values.length
    }

    distinctCount(values) {
        return new Set(values).size
    }

    list(values) {
        return values;
    }

    values(values) {
        return [...new Set(values).filter(item => item !== undefined)];
    }

    calculate(val) {
        if (!this[this.type]) throw new Error('The aggregation method "' + this.type + '" is not valid')
        return this[this.type](val)
    }

    max(values) {
        return values.at(-1)
    }

    min(values) {
        return values[0]
    }

    range(values) {
        return values.at(-1) - values[0]
    }

    percentile(values) {
        const index = Math.round(this.options / 100 * (values.length - 1));
        return values[index]
    }

    median(values) {
        const index = Math.floor((values.length - 1) / 2);
        return values[index]
    }

    sum(values) {
        return values.reduce((a, b) => a + b, 0)
    }

    average(values) {
        return this.sum(values) / this.count(values)
    }

    avg(values) {
        return this.average(values)
    }
}

module.exports = Aggregation