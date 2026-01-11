#!/usr/bin/env node

const By = require('./types/By');
const Aggregation = require('./types/Aggregation');
const Window = require('./types/Window')

// Import mixin implementations
const core = require('./src/core');
const utils = require('./src/utils');
const transformations = require('./src/transformations');
const fileOperations = require('./src/fileOperations');
const statistics = require('./src/statistics');
const checkpoints = require('./src/checkpoints');
const visualization = require('./src/visualization');
const http = require('./src/http')
const processing = require('./src/processing')


class Vaporous {

    constructor({ loggers } = {}) {
        this.events = [];
        this.visualisations = [];
        this.visualisationData = []
        this.graphFlags = []
        this.tabs = []

        this.savedMethods = {}
        this.checkpoints = {}
        this.activeCheckpointRestore = null

        this.loggers = loggers
        this.perf = null
        this.totalTime = 0

        this.intervals = []
        this.processingQueue = []

        this._isExecuting = false

        // Return a proxy that intercepts method calls
        return new Proxy(this, {
            get(target, prop, receiver) {

                // If this is not a function then return actual value
                if (typeof target[prop] !== 'function') return target[prop];

                const value = target[prop]

                // If it's a function  we should queue it
                if (typeof value === 'function'
                    && !target._isExecuting
                    && target._shouldQueue(prop)) {
                    return function (...args) {
                        target.processingQueue.push([prop, args])
                        return receiver  // Return the proxy, not the target!
                    }
                }

                return target[prop]
            }
        })
    }

    _shouldQueue(methodName) {
        const nonQueueable = ['begin', 'clone', 'serialise', 'destroy', '_shouldQueue', 'valueOf', 'toString']
        return !nonQueueable.includes(methodName)
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

    // ========================================
    // Core methods
    // ========================================

    /**
     * Create, retrieve, or delete saved methods
     * @param {string} operation - 'create', 'retrieve', or 'delete'
     * @param {string} name - Method name
     * @param {*} options - Method options or function
     * @returns {Vaporous} - Returns this instance for chaining
     */
    method(operation, name, options) {
        return core.method.call(this, operation, name, options);
    }

    /**
     * Filter events using a predicate function
     * @param {...*} args - Arguments to pass to Array.filter
     * @returns {Vaporous} - Returns this instance for chaining
     */
    filter(...args) {
        return core.filter.call(this, ...args);
    }

    /**
     * Append new entities to events
     * @param {Array} entities - Array of entities to append
     * @returns {Vaporous} - Returns this instance for chaining
     */
    append(entities) {
        return core.append.call(this, entities);
    }

    /**
     * Execute all queued operations
     * @returns {Promise<Vaporous>} - Returns this instance for chaining
     */
    async begin() {
        return await core.begin.call(this);
    }

    serialise({ } = {}) {
        return core.serialise.call(this);
    }

    /**
     * Clone the Vaporous instance
     * @param {Object} [options] - Clone options
     * @param {boolean} [options.deep] - Whether to perform a deep clone
     * @returns {Vaporous} - Returns cloned instance
     */
    clone({ deep } = {}) {
        return core.clone.call(this, { deep });
    }

    /**
     * Destroy the Vaporous instance and clean up resources
     * @returns {Vaporous} - Returns this instance for chaining
     */
    destroy() {
        return core.destroy.call(this);
    }

    // ========================================
    // Utils methods
    // ========================================

    /**
     * Sort events by specified keys
     * @param {string} order - 'asc' or 'dsc'
     * @param {...string} keys - Keys to sort by
     * @returns {Vaporous} - Returns this instance for chaining
     */
    sort(order, ...keys) {
        return utils.sort.call(this, order, ...keys);
    }

    /**
     * Assert conditions on each event
     * @param {Function} funct - Function that receives (event, index, {expect}) and performs assertions
     * @returns {Vaporous} - Returns this instance for chaining
     */
    assert(funct) {
        return utils.assert.call(this, funct);
    }

    // ========================================
    // Transformation methods
    // ========================================

    /**
     * Evaluate and modify each event
     * @param {Function} modifier - Function that receives an event and returns modifications to apply
     * @returns {Vaporous} - Returns this instance for chaining
     */
    eval(modifier) {
        return transformations.eval.call(this, modifier);
    }

    /**
     * Internal table transformation helper
     * @private
     */
    _table(modifier) {
        return transformations._table.call(this, modifier);
    }

    /**
     * Transform events into a table format
     * @param {Function} modifier - Function that receives an event and returns the transformed row
     * @returns {Vaporous} - Returns this instance for chaining
     */
    table(modifier) {
        return transformations.table.call(this, modifier);
    }

    /**
     * Rename fields in events
     * @param {...Array} entities - Arrays of [from, to] field name pairs
     * @returns {Vaporous} - Returns this instance for chaining
     */
    rename(...entities) {
        return transformations.rename.call(this, ...entities);
    }

    /**
     * Parse time fields into timestamps
     * @param {string} value - Field name containing time value
     * @param {string} [customFormat] - Optional custom time format
     * @returns {Vaporous} - Returns this instance for chaining
     */
    parseTime(value, customFormat) {
        return transformations.parseTime.call(this, value, customFormat);
    }

    /**
     * Bin numeric values into intervals
     * @param {string} value - Field name to bin
     * @param {number} span - Bin size
     * @returns {Vaporous} - Returns this instance for chaining
     */
    bin(value, span) {
        return transformations.bin.call(this, value, span);
    }

    /**
     * Flatten nested arrays in events
     * @param {number} [depth=1] - Depth to flatten
     * @returns {Vaporous} - Returns this instance for chaining
     */
    flatten(depth = 1) {
        return transformations.flatten.call(this, depth);
    }

    /**
     * Expand array field into multiple events
     * @param {string} target - Field name containing array to expand
     * @returns {Vaporous} - Returns this instance for chaining
     */
    mvexpand(target) {
        return transformations.mvexpand.call(this, target);
    }

    // ========================================
    // File operations methods
    // ========================================

    /**
     * Internal file scan helper
     * @private
     */
    _fileScan(directory) {
        return fileOperations._fileScan?.call(this, directory);
    }

    /**
     * Internal file load helper
     * @private
     */
    async _fileLoad(events, delim, parser) {
        return await fileOperations._fileLoad?.call(this, events, delim, parser);
    }

    /**
     * Scan a directory for files
     * @param {string} directory - Directory path to scan
     * @returns {Vaporous} - Returns this instance for chaining
     */
    fileScan(directory) {
        return fileOperations.fileScan?.call(this, directory) || this;
    }

    /**
     * Load CSV files
     * @param {Function} parser - Parser function for CSV rows
     * @returns {Vaporous} - Returns this instance for chaining
     */
    async csvLoad(parser) {
        return await fileOperations.csvLoad?.call(this, parser) || this;
    }

    /**
     * Load files with custom delimiter and parser
     * @param {string} delim - Delimiter for splitting file content
     * @param {Function} parser - Parser function for lines
     * @returns {Vaporous} - Returns this instance for chaining
     */
    async fileLoad(delim, parser) {
        return await fileOperations.fileLoad?.call(this, delim, parser) || this;
    }

    /**
     * Write events to file
     * @param {string} title - File name to write to
     * @returns {Vaporous} - Returns this instance for chaining
     */
    writeFile(title) {
        return fileOperations.writeFile?.call(this, title) || this;
    }

    /**
     * Output events to console or file
     * @param {...string} [args] - Optional field names to output
     * @returns {Vaporous} - Returns this instance for chaining
     */
    output(...args) {
        return fileOperations.output?.call(this, ...args) || this;
    }

    // ========================================
    // Statistics methods
    // ========================================

    /**
     * Internal statistics calculation helper
     * @private
     */
    _stats(args, events) {
        return statistics._stats.call(this, args, events);
    }

    /**
     * Calculate statistics with aggregations
     * @param {...(Aggregation|By)} args - Aggregation and By objects for statistical operations
     * @returns {Vaporous} - Returns this instance for chaining
     */
    stats(...args) {
        return statistics.stats?.call(this, ...args) || this;
    }

    /**
     * Add statistics to each event based on grouping
     * @param {...(Aggregation|By)} args - Aggregation and By objects for statistical operations
     * @returns {Vaporous} - Returns this instance for chaining
     */
    eventstats(...args) {
        return statistics.eventstats?.call(this, ...args) || this;
    }

    /**
     * Internal streaming statistics helper
     * @private
     */
    _streamstats(...args) {
        return statistics._streamstats?.call(this, ...args) || this;
    }

    /**
     * Calculate cumulative statistics over a window
     * @param {...(Aggregation|By|Window)} args - Aggregation, By, and Window objects
     * @returns {Vaporous} - Returns this instance for chaining
     */
    streamstats(...args) {
        return statistics.streamstats?.call(this, ...args) || this;
    }

    /**
     * Calculate delta (range) between consecutive values
     * @param {string} field - Field to calculate delta for
     * @param {string} remapField - Output field name
     * @param {...By} bys - Optional By objects for grouping
     * @returns {Vaporous} - Returns this instance for chaining
     */
    delta(field, remapField, ...bys) {
        return statistics.delta?.call(this, field, remapField, ...bys) || this;
    }

    // ========================================
    // Checkpoint methods
    // ========================================

    /**
     * Internal checkpoint operation helper
     * @private
     */
    _checkpoint(operation, name, data, options) {
        return checkpoints._checkpoint?.call(this, operation, name, data, options) || this;
    }

    /**
     * Create, retrieve, or delete checkpoints
     * @param {string} operation - 'create', 'retrieve', or 'delete'
     * @param {string} name - Checkpoint name
     * @param {Object} [options] - Optional configuration
     * @returns {Vaporous} - Returns this instance for chaining
     */
    checkpoint(operation, name, options) {
        return checkpoints.checkpoint?.call(this, operation, name, options) || this;
    }

    /**
     * Filter events into a checkpoint
     * @param {string} checkpointName - Name for the checkpoint
     * @param {Function} funct - Filter function
     * @param {Object} [options] - Optional configuration
     * @returns {Vaporous} - Returns this instance for chaining
     */
    filterIntoCheckpoint(checkpointName, funct, options) {
        return checkpoints.filterIntoCheckpoint?.call(this, checkpointName, funct, options) || this;
    }

    /**
     * Store or retrieve checkpoints from disk
     * @param {string} operation - 'create' or 'retrieve'
     * @param {string} name - Checkpoint name
     * @param {string} partitionBy - Field to partition by
     * @returns {Vaporous} - Returns this instance for chaining
     */
    async storedCheckpoint(operation, name, partitionBy) {
        return await checkpoints.storedCheckpoint?.call(this, operation, name, partitionBy) || this;
    }

    // ========================================
    // Visualization methods
    // ========================================

    /**
     * Prepare data for graph visualization
     * @param {...string} keys - Keys to use for graph data
     * @returns {Vaporous} - Returns this instance for chaining
     */
    toGraph(...keys) {
        return visualization.toGraph?.call(this, ...keys) || this;
    }

    /**
     * Build visualization with specified configuration
     * @param {string} title - Visualization title
     * @param {string} type - Visualization type
     * @param {Object} [options] - Optional configuration
     * @returns {Vaporous} - Returns this instance for chaining
     */
    build(title, type, options) {
        return visualization.build?.call(this, title, type, options) || this;
    }

    /**
     * Render visualizations
     * @param {Object} [options] - Optional render options
     * @returns {Vaporous} - Returns this instance for chaining
     */
    render(options) {
        return visualization.render?.call(this, options) || this;
    }

    // ========================================
    // HTTP methods
    // ========================================

    /**
     * Load data from HTTP requests
     * @param {Object} [options] - Optional HTTP configuration
     * @returns {Vaporous} - Returns this instance for chaining
     */
    load_http(options) {
        return http.load_http?.call(this, options) || this;
    }

    // ========================================
    // Processing methods
    // ========================================

    /**
     * Process events at regular intervals
     * @param {Function} callback - Function to call on each interval
     * @param {number} ms - Interval in milliseconds
     * @returns {Vaporous} - Returns this instance for chaining
     */
    interval(callback, ms) {
        return processing.interval?.call(this, callback, ms) || this;
    }

    /**
     * Process events in parallel
     * @param {number} concurrency - Number of parallel operations
     * @param {Function} callback - Function to execute for each batch
     * @param {Object} [options] - Optional configuration
     * @returns {Vaporous} - Returns this instance for chaining
     */
    parallel(concurrency, callback, options) {
        return processing.parallel?.call(this, concurrency, callback, options) || this;
    }
}

module.exports = { Vaporous, Aggregation, By, Window }
