const fs = require('fs');
const path = require('path');
const By = require('../types/By');
const Aggregation = require('../types/Aggregation');
const { _sort } = require('./utils');

const styles = fs.readFileSync(__dirname + '/../styles.css')

// These globals allow us to write functions from the HTML page directly without needing to stringify
const document = {}

module.exports = {
    toGraph(x, y, series, trellis = false) {


        if (!(y instanceof Array)) y = [y]
        if (!(x instanceof Array)) x = [x]

        const yAggregations = y.map(item => [
            new Aggregation(item, 'list', item),
        ]).flat()

        const xBy = x.map(x => new By(x))

        this.events = this._stats([
            ...yAggregations,
            new Aggregation(series, 'list', series),
            new Aggregation(trellis, 'values', 'trellis'),
            ...xBy, trellis ? new By(trellis) : null], this.events
        ).arr

        const trellisMap = {}, columnDefinitions = {}

        this._table(event => {
            const obj = {
                [x[0]]: event[x[0]]
            }

            x.forEach(item => {
                obj[item] = event[item]
            })

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

            obj._trellis = event.trellis[0]

            return obj
        })

        const graphFlags = {
            xPrimary: x[0]
        }

        if (trellis) {
            graphFlags.trellis = true;
            graphFlags.trellisName = Object.keys(trellisMap)
            graphFlags.columnDefinitions = Object.keys(trellisMap).map(tval => {
                const adjColumns = [x[0]]
                Object.keys(columnDefinitions[tval]).forEach(col => (col !== x[0]) ? adjColumns.push(col) : null)
                return adjColumns
            })
            this.events = Object.keys(trellisMap).map(tval => trellisMap[tval])
        } else {
            const adjColumns = [x[0]]
            Object.keys(columnDefinitions).forEach(col => (col !== x[0]) ? adjColumns.push(col) : null)

            this.events = [this.events]
            graphFlags.columnDefinitions = [adjColumns]
        }

        this.graphFlags.push(graphFlags)
        return this;
    },

    build(name, type, { tab = 'Default', columns = 2, y2, y1Type, y2Type, y1Stacked, y2Stacked, sortX = 'asc', xTicks, trellisAxis = "shared", legend, extendedDescription } = {}) {


        const visualisationOptions = { tab, columns, extendedDescription }


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

        const xPrimary = this.graphFlags.at(-1).xPrimary

        // Check whether x is categorical
        // Ideally this should be explicitly defined, but we will use inference by the first data type in the event it is not
        let xCategorical = typeof this.events[0][xPrimary] !== "number"
        if (xTicks === undefined && xCategorical) xTicks = true

        const graphData = this.events.map((trellis, i) => {

            const trellisName = this.graphFlags.at(-1).trellisName?.[i] || ""
            const columnDefinitions = this.graphFlags.at(-1).columnDefinitions[i]

            const titleText = name + trellisName

            if (type === 'Table') {
                return {
                    columnDefinitions: columnDefinitions.map(field => ({ field })),
                    rowData: trellis,
                    title: titleText
                };
            }

            const dataOptions = {}

            // For every event in this trellis restructure to chart.js
            if (sortX) trellis = _sort(sortX, trellis, xPrimary)


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


            const primary = dataOptions[xPrimary]
            delete dataOptions[xPrimary]

            let y2WasMapped = false
            const data = {
                labels: primary,
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
                    } else {
                        throw new Error('Visualisation of "' + type + '" is not supported')
                    }
                    return base
                })
            };

            const isSharedAxis = trellisAxis === 'shared'
            const sharedAxisArr = () => isSharedAxis ? [] : undefined

            const scales = {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    stacked: y1Stacked,
                    min: sharedAxisArr(),
                    max: sharedAxisArr(),
                }
            }

            if (!xCategorical) {
                scales.x = {
                    type: 'linear',
                    stacked: y1Stacked,
                    ticks: {
                        display: xTicks
                    },
                    min: sharedAxisArr(),
                    max: sharedAxisArr(),
                }
            } else {
                scales.x = {
                    type: 'category',
                    ticks: {
                        display: xTicks
                    },

                }
            }


            if (y2WasMapped) {
                scales.y2 = {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    grid: {
                        drawOnChartArea: false
                    },
                    stacked: y2Stacked,
                    min: sharedAxisArr(),
                    max: sharedAxisArr(),
                }
                // If y2 stacking is enabled, also enable x-axis stacking
                if (y2Stacked) scales.x.stacked = true
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
                            display: false,
                            text: titleText
                        }
                    }
                }
            }
        })

        // Do a second iteration to implement bounds on axis if the trellis is shared
        // We need to keep in mind that stacking might be required and sum accordingly
        if (trellisAxis === 'shared' && type != "Table") {
            const stacked = { y1Stacked, y2Stacked }


            graphData.forEach(trellisGraph => {

                Object.keys(bounds).forEach(bound => {
                    let axis = isY2(bound) ? 'y2' : 'y'
                    if (bound === xPrimary) {
                        if (xCategorical) return;
                        axis = 'x';
                    }

                    const thisAxis = trellisGraph.options.scales[axis]

                    const { min, max } = bounds[bound]
                    thisAxis.min.push(min)
                    thisAxis.max.push(max)
                })

                if (trellisGraph.options) Object.keys(trellisGraph.options.scales).forEach(axis => {

                    if (axis === "x" && xCategorical) return;

                    const scale = trellisGraph.options.scales[axis]
                    // Sort our axis
                    scale.min = scale.min.sort((a, b) => a - b)
                    scale.max = scale.max.sort((a, b) => a - b)

                    const highestMax = scale.max.at(-1)
                    const lowestMin = scale.min.at(-1)

                    // Determine if stacking is enabled and take action min
                    if (stacked[`${axis}Stacked`] && lowestMin < 0) {
                        scale.min.reduce((prev, curr) => {
                            if (curr < 0) return prev + curr
                            return curr
                        }, 0)
                    } else {
                        scale.min = lowestMin
                    }

                    // Determine if stacking is enabled and take action max
                    if (stacked[`${axis}Stacked`] && highestMax > 0) {
                        scale.max.reduce((prev, curr) => {
                            if (curr > 0) return prev + curr
                            return curr
                        }, 0)
                    } else {
                        scale.min = lowestMin
                    }

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

        return this;
    },

    render(location = './Vaporous_generation.html', { tabOrder } = {}) {


        const classSafe = (name) => name.replace(/[^a-zA-Z0-9]/g, "_")

        if (tabOrder) this.tabs = tabOrder

        const createElement = (name, type, visualisationOptions, eventData, { trellis, trellisName = "", columnDefinitions }) => {

            if (classSafe(visualisationOptions.tab) !== selectedTab) return;

            if (visualisationOptions.extendedDescription) {
                const descriptions = document.getElementById('extendedDescription')
                const thisDescription = document.createElement('div')
                thisDescription.innerHTML = visualisationOptions.extendedDescription
                descriptions.appendChild(thisDescription)
            }

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

                const titleDiv = document.createElement('div')
                titleDiv.classList.add('graphTitle')
                parentHolder.appendChild(titleDiv)

                if (trellisData.options) {
                    titleDiv.textContent = trellisData.options.plugins.title.text
                } else {
                    titleDiv.textContent = trellisData.title
                }

                document.getElementById('content').appendChild(parentHolder)

                parentHolder.style = `flex: 0 0 calc(${100 / columnCount}% - 8px); max-width: calc(${100 / columnCount}% - 8px);`
                if (type === 'Table') {
                    const tableDiv = document.createElement('div')
                    tableDiv.classList.add('tableHolder')
                    document.documentElement.style.setProperty("--ag-spacing", `4px`);


                    // Need to do column defintiions here
                    parentHolder.appendChild(tableDiv)
                    new agGrid.createGrid(tableDiv, {
                        rowData: trellisData.rowData,
                        // Columns to be displayed (Should match rowData properties)
                        columnDefs: trellisData.columnDefinitions,
                        defaultColDef: {
                            flex: 1,
                            resizable: true,
                            sortable: true,
                            filter: true
                        },
                        domLayout: 'autoHeight'
                        // suppressHorizontalScroll: false,
                        // autoSizeStrategy: {
                        //     type: 'fitGridWidth',
                        //     defaultMinWidth: 100
                        // }
                    });
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
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            <script src="https://cdn.jsdelivr.net/npm/ag-grid-community@34.2.0/dist/ag-grid-community.min.js"></script>

    
    <script type="text/javascript">

      const classSafe = ${classSafe.toString()}

      var selectedTab = classSafe("${this.tabs[0]}")
      var previousTab = null
      const tokens = {}
      const visualisationData = [${this.visualisationData.join(',')}]
      const tabOrder = [${this.tabs.map(tab => `"${classSafe(tab)}"`).join(',')}]

      const _sort = ${_sort.toString()}
      const createElement = ${createElement.toString()}
      
      function drawVis(tab) {
            if (tab) {
                if (selectedTab) {
                    document.getElementById(selectedTab).classList.remove('selectedTab')
                    document.getElementById(selectedTab).classList.remove('slideFromRight')
                    previousTab = selectedTab
                }
                selectedTab = tab
            } else if (${this.tabs.length > 0}) {
                selectedTab = classSafe('${this.tabs[0]}')
            }

            if (selectedTab) {
                const selectedElement = document.getElementById(selectedTab)
                selectedElement.classList.add('selectedTab')
                
                // Determine slide direction based on tab positions
                if (previousTab) {
                    const previousIndex = tabOrder.indexOf(previousTab)
                    const currentIndex = tabOrder.indexOf(selectedTab)
                    
                    if (currentIndex < previousIndex) {
                        // Moving to left tab, slide from right
                        selectedElement.classList.add('slideFromRight')
                    }
                    // For right movement or first load, use default slideFromLeft animation
                }
            }
            document.getElementById('content').innerHTML = ''
            document.getElementById('extendedDescription').innerHTML = ''
            ${this.visualisations.map(([name, type, visualisationOptions, dataIndex, graphFlags]) => {
            return `createElement('${name}', '${type}', ${JSON.stringify(visualisationOptions)} ,${dataIndex}, ${JSON.stringify(graphFlags)})`
        })}
      }

        document.addEventListener("DOMContentLoaded", function(event) {
            drawVis()
        });
      
    </script>
  </head>
  <body>
        ${this.tabs.length > 0 ? `<div class='tabBar'>
            ${this.tabs.map(tab => `<div id=${classSafe(tab)} class='tabs' onclick="drawVis('${classSafe(tab)}')">${tab}</div>`).join("\n")}
        </div>` : ''}

    <div id='extendedDescription'></div>
    <div id='content'></div>
  </body>
</html>
        `)
        console.log('File ouput created ', path.resolve(filePath))
        if (this.totalTime) console.log("File completed in " + this.totalTime)
        return this;
    }
}
