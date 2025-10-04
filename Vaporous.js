#!/usr/bin/env node

const By = require('./types/By');
const Aggregation = require('./types/Aggregation');
const Window = require('./types/Window')

// Import mixins
const utilsMixin = require('./src/utils');
const transformationsMixin = require('./src/transformations');
const fileOperationsMixin = require('./src/fileOperations');
const statisticsMixin = require('./src/statistics');
const checkpointsMixin = require('./src/checkpoints');
const visualizationMixin = require('./src/visualization');

class Vaporous {

    constructor({ loggers } = {}) {
        this.events = [];
        this.visualisations = [];
        this.visualisationData = []
        this.graphFlags = []
        this.tabs = []

        this.savedMethods = {}
        this.checkpoints = {}

        this.loggers = loggers
        this.perf = null
        this.totalTime = 0
    }

    manageEntry() {
        if (this.loggers?.perf) {
            const [, , method, ...origination] = new Error().stack.split('\n')
            const invokedMethod = method.match(/Vaporous.(.+?) /)

            let orig = origination.find(orig => {
                const originator = orig.split("/").at(-1)
                return !originator.includes("Vaporous")
            })

            orig = orig.split("/").at(-1)
            const logLine = "(" + orig + " BEGIN " + invokedMethod[1]
            this.loggers.perf('info', logLine)
            this.perf = { time: new Date().valueOf(), logLine }
        }
    }

    manageExit() {
        if (this.loggers?.perf) {
            let { logLine, time } = this.perf;
            const executionTime = new Date() - time
            this.totalTime += executionTime

            const match = logLine.match(/^.*?BEGIN/);
            const prepend = "END"
            if (match) {
                const toReplace = match[0]; // the matched substring
                const spaces = " ".repeat(toReplace.length - prepend.length); // same length, all spaces
                logLine = spaces + prepend + logLine.replace(toReplace, "");
            }

            this.loggers.perf('info', logLine + " (" + executionTime + "ms)")
        }
        return this
    }

    method(operation, name, options) {
        if (operation != 'retrieve') this.manageEntry()
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
        if (operation !== 'retrieve') return this.manageExit()
        return this;
    }

    filter(...args) {
        this.manageEntry()
        this.events = this.events.filter(...args)
        return this.manageExit()
    }

    append(entities) {
        this.manageEntry()
        this.events = this.events.concat(entities)
        return this.manageExit()
    }
}

// Apply mixins to Vaporous prototype
Object.assign(Vaporous.prototype, utilsMixin);
Object.assign(Vaporous.prototype, transformationsMixin);
Object.assign(Vaporous.prototype, fileOperationsMixin);
Object.assign(Vaporous.prototype, statisticsMixin);
Object.assign(Vaporous.prototype, checkpointsMixin);
Object.assign(Vaporous.prototype, visualizationMixin);

module.exports = { Vaporous, Aggregation, By, Window }
