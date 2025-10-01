#!/usr/bin/env node

const dayjs = require('dayjs');
const fs = require('fs');
const split2 = require('split2');

const By = require('./types/By');
const Aggregation = require('./types/Aggregation');
const Window = require('./types/Window')
const path = require('path')

const styles = fs.readFileSync(__dirname + '/styles.css')

const Papa = require('papaparse')

// These globals allow us to write functions from the HTML page directly without needing to stringify
const document = {}

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

    filterIntoCheckpoint(checkpointName, funct, destroy) {
        this.manageEntry()
        const dataCheckpoint = this.events.filter(funct)
        this._checkpoint('create', checkpointName, dataCheckpoint)
        if (destroy) this.events = this.events.filter(event => !funct(event))
        return this.manageExit()
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

    eval(modifier) {
        this.manageEntry()
        this.events.forEach(event => {
            const vals = modifier(event)
            if (vals) Object.assign(event, vals)
        })
        return this.manageExit()
    }

    _table(modifier) {
        this.events = this.events.map(event => {
            const vals = modifier(event)
            return vals;
        })
    }
    table(modifier) {
        this.manageEntry()
        this._table(modifier)
        return this.manageExit()
    }

    rename(...entities) {
        this.manageEntry()
        this.events.forEach(event => {
            entities.forEach(([from, to]) => {
                event[to] = event[from]
                delete event[from]
            })
        })
        return this.manageExit()
    }

    parseTime(value, customFormat) {
        this.manageEntry()
        this.events.forEach(event => {
            event[value] = dayjs(event[value], customFormat).valueOf()
        })
        return this.manageExit()
    }

    bin(value, span) {
        this.manageEntry()
        this.events.forEach(event => {
            event[value] = Math.floor(event[value] / span) * span
        })
        return this.manageExit()
    }

    fileScan(directory) {
        this.manageEntry()
        const items = fs.readdirSync(directory)
        this.events = items.map(item => {
            return {
                _fileInput: path.resolve(directory, item)
            }
        })
        return this.manageExit()
    }

    async csvLoad(parser) {
        this.manageEntry()
        const tasks = this.events.map(obj => {
            const content = []

            return new Promise((resolve, reject) => {
                const thisStream = fs.createReadStream(obj._fileInput)

                Papa.parse(thisStream, {
                    header: true,
                    skipEmptyLines: true,
                    step: (row) => {
                        try {
                            const event = parser(row)
                            if (event !== null) content.push(event)
                        } catch (err) {
                            reject(err)
                        }
                    },
                    complete: () => {
                        obj._raw = content
                        resolve(this)
                    }
                })
            })
        })

        await Promise.all(tasks)
        return this.manageExit()
    }

    async fileLoad(delim, parser) {
        this.manageEntry()
        const tasks = this.events.map(obj => {
            const content = []

            return new Promise((resolve, reject) => {
                fs.createReadStream(obj._fileInput)
                    .pipe(split2(delim))
                    .on('data', line => {
                        try {
                            const event = parser(line)
                            if (event !== null) content.push(event)
                        } catch (err) {
                            throw err;
                        }

                    })
                    .on('end', () => {
                        obj._raw = content;
                        resolve(this)
                    })
            })
        })

        await Promise.all(tasks)
        return this.manageExit()
    }

    output(...args) {
        this.manageEntry()
        if (args.length) {
            console.log(this.events.map(event => {
                return args.map(item => event[item])
            }))
        } else {
            console.log(this.events)
        }

        return this.manageExit()
    }

    flatten() {
        this.manageEntry()
        const arraySize = this.events.reduce((acc, obj) => acc + obj._raw.length, 0)
        let flattened = new Array(arraySize)
        let i = 0

        this.events.forEach(obj => {
            const raws = obj._raw
            delete obj._raw

            raws.forEach(event => {
                flattened[i++] = {
                    ...obj,
                    _raw: event,
                }
            })

        })
        this.events = flattened;
        return this.manageExit()
    }

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
    }

    stats(...args) {
        this.manageEntry()
        this.events = this._stats(args, this.events).arr
        return this.manageExit()
    }

    eventstats(...args) {
        this.manageEntry()
        const stats = this._stats(args, this.events)

        this.events.forEach(event => {
            const key = keyFromEvent(event, stats.by)

            Object.assign(event, stats.map[key])
        })

        return this
    }

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
    }

    streamstats(...args) {
        this.manageEntry()
        this._streamstats(...args)
        return this.manageExit()
    }

    delta(field, remapField, ...bys) {
        this.manageEntry()
        this._streamstats(new Aggregation(field, 'range', remapField), new Window(2), ...bys)
        return this.manageExit()
    }

    sort(order, ...keys) {
        this.manageEntry()
        this.events = _sort(order, this.events, ...keys)
        return this.manageExit()
    }

    assert(funct) {
        this.manageEntry()
        const expect = (funct) => { if (!funct) throw new Error('Assertion failed') }
        this.events.forEach((event, i) => {
            funct(event, i, { expect })
        })
        return this.manageExit()
    }

    build(name, type, { tab = 'Default', columns = 2, y2, y1Type, y2Type, y1Stacked, y2Stacked, sortX = 'asc', xTicks = false, trellisAxis = "shared", legend } = {}) {
        this.manageEntry()

        const visualisationOptions = { tab, columns }


        let bounds = {}

        const isY2 = (data) => {
            let y2Mapped = false;

            if (y2 instanceof Array) {
                y2Mapped = y2.includes(data)
            }
            else if (y2 instanceof RegExp) {
                y2Mapped = y2.test(data)
            }

            return y2Mapped
        }
        const graphData = this.events.map((trellis, i) => {
            if (type === 'Table') {
                return trellis;
            }

            const dataOptions = {}

            // For every event in this trellis restructure to chart.js
            if (sortX) trellis = _sort(sortX, trellis, '_time')

            const trellisName = this.graphFlags.at(-1).trellisName?.[i] || ""
            const columnDefinitions = this.graphFlags.at(-1).columnDefinitions[i]

            trellis.forEach(event => {
                columnDefinitions.forEach(prop => {
                    if (!dataOptions[prop]) dataOptions[prop] = []
                    const val = event[prop]
                    dataOptions[prop].push(val)

                    if (!bounds[prop]) bounds[prop] = {
                        min: val,
                        max: val
                    }

                    if (val < bounds[prop].min) bounds[prop].min = val;
                    if (val > bounds[prop].max) bounds[prop].max = val

                })
            })


            const _time = dataOptions._time
            delete dataOptions._time

            let y2WasMapped = false
            const data = {
                labels: _time,
                datasets: Object.keys(dataOptions).map(data => {
                    const y2Mapped = isY2(data)
                    if (y2Mapped) y2WasMapped = y2Mapped

                    const base = {
                        label: data,
                        yAxisID: y2Mapped ? 'y2' : undefined,
                        data: dataOptions[data],
                        type: y2Mapped ? y2Type : y1Type,
                        // borderColor: 'red',
                        // backgroundColor: 'red',
                    }

                    if (type === 'Scatter') {
                        base.showLine = false
                        base.pointRadius = 8
                        base.pointStyle = 'rect'
                    } else if (type === 'Area') {
                        base.fill = 'origin'
                    } else if (type === 'Line') {
                        base.pointRadius = 0;
                    }
                    return base
                })
            };

            const scales = {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    stacked: y1Stacked
                },
                x: {
                    type: 'linear',
                    ticks: {
                        display: xTicks
                    }
                }
            }

            if (y2WasMapped) scales.y2 = {
                type: 'linear',
                display: true,
                position: 'right',
                grid: {
                    drawOnChartArea: false
                },
                stacked: y2Stacked
            }

            return {
                type: 'line',
                data: data,
                options: {
                    scales,
                    responsive: true,
                    plugins: {
                        legend: {
                            display: legend || true,
                            position: 'bottom',
                        },
                        title: {
                            display: true,
                            text: name + trellisName
                        }
                    }
                }
            }
        })

        if (trellisAxis === 'shared') {
            // Do a second iteration to implement bounds
            graphData.forEach(trellisGraph => {
                Object.keys(bounds).forEach(bound => {
                    let axis = isY2(bound) ? 'y2' : 'y'
                    if (bound === '_time') axis = 'x';

                    const thisAxis = trellisGraph.options.scales[axis]
                    const { min, max } = bounds[bound]
                    if (!thisAxis.min) {
                        thisAxis.min = min
                        thisAxis.max = max
                    }
                    if (min < thisAxis.min) thisAxis.min = min
                    if (max > thisAxis.max) thisAxis.max = max

                })

            })
        }

        const data = JSON.stringify(graphData)
        const lastData = this.visualisationData.at(-1)

        if (lastData !== data) this.visualisationData.push(data)
        this.visualisations.push([name, type, visualisationOptions, this.visualisationData.length - 1, this.graphFlags[this.graphFlags.length - 1]])

        if (visualisationOptions.tab && !this.tabs.includes(visualisationOptions.tab)) {
            this.tabs.push(visualisationOptions.tab)
            this.tabs = this.tabs.sort((a, b) => a.localeCompare(b))
        }

        return this.manageExit()
    }

    _checkpoint(operation, name, data) {

        const operations = {
            create: () => this.checkpoints[name] = structuredClone(data),
            retrieve: () => this.events = structuredClone(this.checkpoints[name]),
            delete: () => delete this.checkpoints[name]
        }

        operations[operation]()
        return this
    }

    checkpoint(operation, name) {
        this.manageEntry()
        this._checkpoint(operation, name, this.events)
        return this.manageExit()
    }

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

    writeFile(title) {
        this.manageEntry()
        fs.writeFileSync('./' + title, JSON.stringify(this.events))
        return this.manageExit()
    }

    toGraph(x, y, series, trellis = false) {
        this.manageEntry()
        if (!(y instanceof Array)) y = [y]

        const yAggregations = y.map(item => [
            new Aggregation(item, 'list', item),
        ]).flat()

        this.events = this._stats([
            ...yAggregations,
            new Aggregation(series, 'list', series),
            new Aggregation(trellis, 'values', 'trellis'),
            new By(x), trellis ? new By(trellis) : null], this.events
        ).arr

        const trellisMap = {}, columnDefinitions = {}

        this._table(event => {
            const _time = event[x]
            if (_time === null || _time === undefined) throw new Error(`To graph operation with params ${x}, ${y.join(',')} looks corrupt. x value resolves to null - the graph will not render`)
            const obj = {
                _time
            }

            event[series].forEach((series, i) => {
                y.forEach(item => {
                    let name;
                    if (y.length === 1) {
                        if (series === undefined) name = item
                        else name = series
                    } else {
                        if (series !== undefined) name = `${series}_${item}`
                        else name = item
                    }
                    obj[name] = event[item][i]
                })
            })

            if (trellis) {
                const tval = event.trellis[0]
                if (!trellisMap[tval]) {
                    trellisMap[tval] = []
                    columnDefinitions[tval] = {}
                }
                trellisMap[tval].push(obj)
                Object.keys(obj).forEach(key => {
                    columnDefinitions[tval][key] = true
                })
            } else {
                Object.keys(obj).forEach(key => {
                    columnDefinitions[key] = true;
                })
            }

            return obj
        })

        const graphFlags = {}

        if (trellis) {
            graphFlags.trellis = true;
            graphFlags.trellisName = Object.keys(trellisMap)
            graphFlags.columnDefinitions = Object.keys(trellisMap).map(tval => {
                const adjColumns = ['_time']
                Object.keys(columnDefinitions[tval]).forEach(col => (col !== '_time') ? adjColumns.push(col) : null)
                return adjColumns
            })
            this.events = Object.keys(trellisMap).map(tval => trellisMap[tval])
        } else {
            const adjColumns = ['_time']
            Object.keys(columnDefinitions).forEach(col => (col !== '_time') ? adjColumns.push(col) : null)

            this.events = [this.events]
            graphFlags.columnDefinitions = [adjColumns]
        }

        this.graphFlags.push(graphFlags)
        return this.manageExit()
    }

    render(location = './Vaporous_generation.html') {
        this.manageEntry()
        const classSafe = (name) => name.replace(/[^a-zA-Z0-9]/g, "_")

        const createElement = (name, type, visualisationOptions, eventData, { trellis, trellisName = "" }) => {

            if (classSafe(visualisationOptions.tab) !== selectedTab) return;

            eventData = visualisationData[eventData]

            // TODO: migrate trellis functionality from here to tograph
            if (trellis) {
                let pairs = trellisName.map((name, i) => [name, eventData[i]]);
                pairs = pairs.sort((a, b) => a[0].localeCompare(b[0]))

                // Unzip back into separate arrays
                trellisName = pairs.map(p => p[0]);
                eventData = pairs.map(p => p[1]);
            }

            const columnCount = visualisationOptions.columns || 2

            eventData.forEach((trellisData, i) => {
                const parentHolder = document.createElement('div')



                document.getElementById('content').appendChild(parentHolder)

                parentHolder.style = `flex: 0 0 calc(${100 / columnCount}% - 8px); max-width: calc(${100 / columnCount}% - 8px);`
                if (type === 'Table') {
                    new Tabulator(parentHolder, { data: trellisData, autoColumns: 'full', layout: "fitDataStretch", })
                } else {
                    const graphEntity = document.createElement('canvas')
                    parentHolder.appendChild(graphEntity)
                    new Chart(graphEntity, trellisData)
                }

            })
        }

        const filePath = location
        fs.writeFileSync(filePath, `
<html>
        <head>
        <meta name="viewport" content="width=device-width, initial-scale=0.5">
            <style>
                ${styles}
            </style>
    
            <link href="https://unpkg.com/tabulator-tables@6.3.1/dist/css/tabulator.min.css" rel="stylesheet">
            <script type="text/javascript" src="https://unpkg.com/tabulator-tables@6.3.1/dist/js/tabulator.min.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    
    <script type="text/javascript">

      const classSafe = ${classSafe.toString()}

      var selectedTab = classSafe("${this.tabs[0]}")
      const tokens = {}
      const visualisationData = [${this.visualisationData.join(',')}]

      const _sort = ${_sort.toString()}
      const createElement = ${createElement.toString()}
      
      function drawVis(tab) {
            if (tab) {
                if (selectedTab) document.getElementById(selectedTab).classList.remove('selectedTab')
                selectedTab = tab
            } else if (${this.tabs.length > 0}) {
                selectedTab = classSafe('${this.tabs[0]}')
            }

            if (selectedTab) document.getElementById(selectedTab).classList.add('selectedTab')
            document.getElementById('content').innerHTML = ''
            ${this.visualisations.map(([name, type, visualisationOptions, dataIndex, graphFlags]) => {
            return `createElement('${name}', '${type}', ${JSON.stringify(visualisationOptions)} ,${dataIndex}, ${JSON.stringify(graphFlags)})`
        })}
      }

    </script>
  </head>
  <body>
        ${this.tabs.length > 0 ? `<div class='tabBar'>
            ${this.tabs.map(tab => `<div id=${classSafe(tab)} class='tabs' onclick="drawVis('${classSafe(tab)}')">${tab}</div>`).join("\n")}
        </div>` : ''}

    <div id='content'>
        </div>
  </body>
</html>
        `)
        console.log('File ouput created ', path.resolve(filePath))
        if (this.totalTime) console.log("File completed in " + this.totalTime)
        return this.manageExit()
    }
}

module.exports = { Vaporous, Aggregation, By, Window }
