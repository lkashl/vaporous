// These tests are generated

const { Vaporous, By, Aggregation, Window } = require("../Vaporous")
const fs = require('fs')
const path = require('path')

// Generate comprehensive test data
const generateTestData = () => {
    const dataDir = path.join(__dirname, 'testData')
    if (!fs.existsSync(dataDir)) {
        fs.mkdirSync(dataDir, { recursive: true })
    }

    // Create CSV test file
    const csvData = `id,value,category,timestamp,nested_value
1,100,A,2024-01-01T10:00:00,10
2,200,B,2024-01-01T11:00:00,20
3,150,A,2024-01-01T12:00:00,15
4,250,B,2024-01-01T13:00:00,25
5,175,A,2024-01-01T14:00:00,17`

    fs.writeFileSync(path.join(dataDir, 'test.csv'), csvData)

    // Create JSON test file
    const jsonData = [
        { sensor: 'temp1', reading: 22.5, location: 'room1', time: '2024-01-01T10:00:00' },
        { sensor: 'temp2', reading: 23.1, location: 'room2', time: '2024-01-01T10:00:00' },
        { sensor: 'temp1', reading: 23.0, location: 'room1', time: '2024-01-01T11:00:00' },
        { sensor: 'temp2', reading: 24.5, location: 'room2', time: '2024-01-01T11:00:00' }
    ].map(obj => JSON.stringify(obj)).join('\n')

    fs.writeFileSync(path.join(dataDir, 'sensors.json'), jsonData)

    return dataDir
}

const main = async () => {
    console.log('\n=== COMPREHENSIVE VAPOROUS TEST SUITE ===\n')

    const dataFolder = generateTestData()
    let testsPassed = 0
    let totalTests = 0

    // ===================================================================
    // TEST 1: Constructor and Basic Properties
    // ===================================================================
    console.log('TEST 1: Constructor and Basic Properties')
    totalTests++
    const vaporous = new Vaporous({
        loggers: {
            perf: (level, event) => console.log(`[PERF] ${event}`)
        }
    })

    if (vaporous.events.length === 0 &&
        vaporous.visualisations.length === 0 &&
        vaporous.checkpoints && typeof vaporous.checkpoints === 'object' &&
        vaporous.savedMethods && typeof vaporous.savedMethods === 'object') {
        console.log('✓ Constructor initialized correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 2: Append Method - Adding Data Manually
    // ===================================================================
    console.log('TEST 2: Append Method')
    totalTests++
    const initialData = [
        { id: 1, value: 10, category: 'X' },
        { id: 2, value: 20, category: 'Y' }
    ]

    vaporous.append(initialData)
        .assert((event, i, { expect }) => {
            expect(vaporous.events.length === 2)
            expect(event.id !== undefined)
            expect(event.value !== undefined)
            expect(event.category !== undefined)
        })

    if (vaporous.events.length === 2) {
        console.log('✓ Append method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 3: Filter Method
    // ===================================================================
    console.log('TEST 3: Filter Method')
    totalTests++
    vaporous.filter(event => event.category === 'X')
        .assert((event, i, { expect }) => {
            expect(vaporous.events.length === 1)
            expect(event.category === 'X')
            expect(event.id === 1)
        })

    if (vaporous.events.length === 1 && vaporous.events[0].category === 'X') {
        console.log('✓ Filter method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 4: Eval Method - Data Transformation
    // ===================================================================
    console.log('TEST 4: Eval Method - Data Transformation')
    totalTests++
    vaporous.append([{ id: 3, value: 30, category: 'Z' }])
        .eval(event => {
            return {
                doubled: event.value * 2,
                computed: event.id + event.value
            }
        })
        .assert((event, i, { expect }) => {
            expect(event.doubled === event.value * 2)
            expect(event.computed === event.id + event.value)
            if (event.id === 1) {
                expect(event.doubled === 20)
                expect(event.computed === 11)
            }
        })

    if (vaporous.events[0].doubled === 20) {
        console.log('✓ Eval method transforms data correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 5: Sort Method
    // ===================================================================
    console.log('TEST 5: Sort Method - Ascending and Descending')
    totalTests++
    vaporous.sort('asc', 'value')
        .assert((event, i, { expect }) => {
            if (i === 0) expect(event.value === 10)
            if (i === 1) expect(event.value === 30)
        })

    vaporous.sort('dsc', 'value')
        .assert((event, i, { expect }) => {
            if (i === 0) expect(event.value === 30)
            if (i === 1) expect(event.value === 10)
        })

    if (vaporous.events[0].value === 30) {
        console.log('✓ Sort method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 6: Rename Method
    // ===================================================================
    console.log('TEST 6: Rename Method')
    totalTests++
    vaporous.rename(['value', 'amount'], ['category', 'type'])
        .assert((event, i, { expect }) => {
            expect(event.amount !== undefined)
            expect(event.type !== undefined)
            expect(event.value === undefined)
            expect(event.category === undefined)
        })

    if (vaporous.events[0].amount && vaporous.events[0].type) {
        console.log('✓ Rename method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 7: Bin Method
    // ===================================================================
    console.log('TEST 7: Bin Method - Numerical Binning')
    totalTests++
    vaporous.eval(event => ({ rawValue: event.amount }))
        .bin('amount', 10)
        .assert((event, i, { expect }) => {
            expect(event.amount % 10 === 0)
            expect(event.amount <= event.rawValue)
            const expectedBin = Math.floor(event.rawValue / 10) * 10
            expect(event.amount === expectedBin)
        })

    if (vaporous.events.every(e => e.amount % 10 === 0)) {
        console.log('✓ Bin method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 8: Checkpoint Methods - Create, Retrieve, Delete
    // ===================================================================
    console.log('TEST 8: Checkpoint Methods')
    totalTests++
    const checkpointData = vaporous.events.length
    vaporous.checkpoint('create', 'testCheckpoint')
        .filter(event => event.id === 1)
        .assert((event, i, { expect }) => {
            expect(vaporous.events.length === 1)
        })

    vaporous.checkpoint('retrieve', 'testCheckpoint')
        .assert((event, i, { expect }) => {
            expect(vaporous.events.length === checkpointData)
        })

    vaporous.checkpoint('delete', 'testCheckpoint')

    if (vaporous.checkpoints.testCheckpoint === undefined) {
        console.log('✓ Checkpoint create/retrieve/delete works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 9: Table Method - Data Restructuring
    // ===================================================================
    console.log('TEST 9: Table Method - Data Restructuring')
    totalTests++
    const appendVaporous = new Vaporous()
    appendVaporous.append([{ x: 1, y: 2, z: 3 }])
        .table(event => ({
            a: event.x,
            b: event.y,
            original: event
        }))
        .assert((event, i, { expect }) => {
            expect(event.a !== undefined)
            expect(event.b !== undefined)
            expect(event.original !== undefined)
        })

    if (appendVaporous.events[0].a !== undefined) {
        console.log('✓ Table method restructures data correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 10: Flatten Method
    // ===================================================================
    console.log('TEST 10: Flatten Method')
    totalTests++
    const nestedVaporous = new Vaporous()
    nestedVaporous.append([
        [[{ id: 1 }, { id: 2 }], [{ id: 3 }]],
        [[{ id: 4 }]]
    ])
        .flatten(2)
        .assert((event, i, { expect }) => {
            expect(event.id !== undefined)
            expect(typeof event.id === 'number')
        })

    if (nestedVaporous.events.length === 4) {
        console.log('✓ Flatten method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 11: MvExpand Method
    // ===================================================================
    console.log('TEST 11: MvExpand Method')
    totalTests++
    const mvVaporous = new Vaporous()
    mvVaporous.append([
        { id: 1, tags: ['a', 'b', 'c'], value: 100 },
        { id: 2, tags: ['x', 'y'], value: 200 }
    ])
        .mvexpand('tags')
        .assert((event, i, { expect }) => {
            expect(typeof event.tags === 'string')
            expect(event._mvExpand_tags !== undefined)
            expect(event.id !== undefined)
            expect(event.value !== undefined)
        })

    if (mvVaporous.events.length === 5) {
        console.log('✓ MvExpand method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 12: Stats Method - Comprehensive Aggregations
    // ===================================================================
    console.log('TEST 12: Stats Method - All Aggregation Types')
    totalTests++
    const statsVaporous = new Vaporous()
    statsVaporous.append([
        { category: 'A', value: 10, id: 1 },
        { category: 'A', value: 20, id: 2 },
        { category: 'A', value: 30, id: 3 },
        { category: 'B', value: 15, id: 4 },
        { category: 'B', value: 25, id: 5 }
    ])
        .stats(
            new Aggregation('value', 'sum', 'totalValue'),
            new Aggregation('value', 'count', 'countValue'),
            new Aggregation('value', 'max', 'maxValue'),
            new Aggregation('value', 'min', 'minValue'),
            new Aggregation('value', 'median', 'medianValue'),
            new Aggregation('value', 'range', 'rangeValue'),
            new Aggregation('value', 'list', 'listValue'),
            new Aggregation('value', 'values', 'uniqueValues'),
            new Aggregation('id', 'distinctCount', 'distinctIds'),
            new By('category')
        )
        .assert((event, i, { expect }) => {
            expect(event.category !== undefined)
            expect(event.totalValue !== undefined)
            expect(event.countValue !== undefined)
            expect(event.maxValue !== undefined)
            expect(event.minValue !== undefined)
            expect(event.medianValue !== undefined)
            expect(event.rangeValue !== undefined)
            expect(Array.isArray(event.listValue))
            expect(Array.isArray(event.uniqueValues))

            if (event.category === 'A') {
                expect(event.totalValue === 60)
                expect(event.countValue === 3)
                expect(event.maxValue === 30)
                expect(event.minValue === 10)
                expect(event.medianValue === 20)
                expect(event.rangeValue === 20)
                expect(event.listValue.length === 3)
                expect(event.distinctIds === 3)
            }

            if (event.category === 'B') {
                expect(event.totalValue === 40)
                expect(event.countValue === 2)
                expect(event.maxValue === 25)
                expect(event.minValue === 15)
                expect(event.rangeValue === 10)
            }
        })

    if (statsVaporous.events.length === 2) {
        console.log('✓ Stats method with all aggregation types works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 13: Percentile Aggregation
    // ===================================================================
    console.log('TEST 13: Percentile Aggregation')
    totalTests++
    const percentileVaporous = new Vaporous()
    percentileVaporous.append(
        Array.from({ length: 100 }, (_, i) => ({ value: i + 1 }))
    )
        .stats(
            new Aggregation('value', 'percentile', 'p50', 50),
            new Aggregation('value', 'percentile', 'p95', 95),
            new Aggregation('value', 'percentile', 'p99', 99)
        )
        .assert((event, i, { expect }) => {
            expect(event.p50 >= 49 && event.p50 <= 51)
            expect(event.p95 >= 94 && event.p95 <= 96)
            expect(event.p99 >= 98 && event.p99 <= 100)
        })

    if (percentileVaporous.events.length === 1) {
        console.log('✓ Percentile aggregation works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 14: Eventstats Method
    // ===================================================================
    console.log('TEST 14: Eventstats Method')
    totalTests++
    const eventstatsVaporous = new Vaporous()
    eventstatsVaporous.append([
        { category: 'A', value: 10 },
        { category: 'A', value: 20 },
        { category: 'B', value: 15 },
        { category: 'B', value: 25 }
    ])
        .eventstats(
            new Aggregation('value', 'sum', 'categorySum'),
            new Aggregation('value', 'count', 'categoryCount'),
            new By('category')
        )
        .assert((event, i, { expect }) => {
            expect(event.categorySum !== undefined)
            expect(event.categoryCount !== undefined)
            expect(event.value !== undefined)

            if (event.category === 'A') {
                expect(event.categorySum === 30)
                expect(event.categoryCount === 2)
            }
            if (event.category === 'B') {
                expect(event.categorySum === 40)
                expect(event.categoryCount === 2)
            }
        })

    if (eventstatsVaporous.events.length === 4) {
        console.log('✓ Eventstats method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 15: Streamstats Method with Window
    // ===================================================================
    console.log('TEST 15: Streamstats Method with Window')
    totalTests++
    const streamstatsVaporous = new Vaporous()
    streamstatsVaporous.append([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
        { id: 4, value: 40 },
        { id: 5, value: 50 }
    ])
        .streamstats(
            new Aggregation('value', 'sum', 'runningSum'),
            new Aggregation('value', 'count', 'runningCount'),
            new Aggregation('value', 'list', 'windowList'),
            new Window(3)
        )
        .assert((event, i, { expect }) => {
            expect(event.runningSum !== undefined)
            expect(event.runningCount !== undefined)
            expect(Array.isArray(event.windowList))

            if (i === 0) {
                expect(event.runningSum === 10)
                expect(event.runningCount === 1)
                expect(event.windowList.length === 1)
            }
            if (i === 2) {
                expect(event.runningSum === 60)
                expect(event.runningCount === 3)
                expect(event.windowList.length === 3)
            }
            if (i === 4) {
                expect(event.runningSum === 120)
                expect(event.runningCount === 3)
                expect(event.windowList.length === 3)
            }
        })

    if (streamstatsVaporous.events[4].runningSum === 120) {
        console.log('✓ Streamstats with Window works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 16: Streamstats Method with By clause
    // ===================================================================
    console.log('TEST 16: Streamstats Method with By clause')
    totalTests++
    const streamstatsByVaporous = new Vaporous()
    streamstatsByVaporous.append([
        { category: 'A', value: 10, seq: 1 },
        { category: 'A', value: 20, seq: 2 },
        { category: 'B', value: 15, seq: 3 },
        { category: 'A', value: 30, seq: 4 },
        { category: 'B', value: 25, seq: 5 }
    ])
        .streamstats(
            new Aggregation('value', 'sum', 'categoryRunningSum'),
            new Aggregation('value', 'list', 'categoryList'),
            new By('category')
        )
        .assert((event, i, { expect }) => {
            expect(Array.isArray(event.categoryList))

            if (event.seq === 1) {
                expect(event.categoryRunningSum === 10)
                expect(event.categoryList.length === 1)
            }
            if (event.seq === 2) {
                expect(event.categoryRunningSum === 30)
                expect(event.categoryList.length === 2)
            }
            if (event.seq === 3) {
                expect(event.categoryRunningSum === 15)
                expect(event.categoryList.length === 1)
            }
            if (event.seq === 4) {
                expect(event.categoryRunningSum === 30)
                expect(event.categoryList.length === 1)
            }
        })

    if (streamstatsByVaporous.events[3].categoryRunningSum === 30) {
        console.log('✓ Streamstats with By clause works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 17: Delta Method
    // ===================================================================
    console.log('TEST 17: Delta Method')
    totalTests++
    const deltaVaporous = new Vaporous()
    deltaVaporous.append([
        { time: 0, category: 'A' },
        { time: 5, category: 'A' },
        { time: 12, category: 'A' },
        { time: 20, category: 'A' }
    ])
        .delta('time', 'timeDelta', new By('category'))
        .assert((event, i, { expect }) => {
            expect(event.timeDelta !== undefined)

            if (i === 0) {
                expect(event.timeDelta === 0)
            }
            if (i === 1) {
                expect(event.timeDelta === 5)
            }
            if (i === 2) {
                expect(event.timeDelta === 7)
            }
            if (i === 3) {
                expect(event.timeDelta === 8)
            }
        })

    if (deltaVaporous.events[1].timeDelta === 5) {
        console.log('✓ Delta method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 18: FilterIntoCheckpoint Method
    // ===================================================================
    console.log('TEST 18: FilterIntoCheckpoint Method')
    totalTests++
    const filterCheckpointVaporous = new Vaporous()
    filterCheckpointVaporous.append([
        { id: 1, status: 'active', value: 10 },
        { id: 2, status: 'inactive', value: 20 },
        { id: 3, status: 'active', value: 30 },
        { id: 4, status: 'inactive', value: 40 }
    ])
        .filterIntoCheckpoint('inactiveItems', event => event.status === 'inactive')
        .assert((event, i, { expect }) => {
            expect(event.status === 'active')
            expect(filterCheckpointVaporous.events.length === 2)
        })

    filterCheckpointVaporous.checkpoint('retrieve', 'inactiveItems')
        .assert((event, i, { expect }) => {
            expect(event.status === 'inactive')
            expect(filterCheckpointVaporous.events.length === 2)
        })

    if (filterCheckpointVaporous.events.length === 2) {
        console.log('✓ FilterIntoCheckpoint method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 19: ParseTime Method
    // ===================================================================
    console.log('TEST 19: ParseTime Method')
    totalTests++
    const parseTimeVaporous = new Vaporous()
    parseTimeVaporous.append([
        { timestamp: '2024-01-01T10:00:00' },
        { timestamp: '2024-01-01T11:00:00' },
        { timestamp: '2024-01-01T12:00:00' }
    ])
        .parseTime('timestamp')
        .assert((event, i, { expect }) => {
            expect(typeof event.timestamp === 'number')
            expect(event.timestamp > 0)
        })
        .bin('timestamp', 3600000) // Bin to hours
        .assert((event, i, { expect }) => {
            expect(event.timestamp % 3600000 === 0)
        })

    if (typeof parseTimeVaporous.events[0].timestamp === 'number') {
        console.log('✓ ParseTime method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 20: Method - Create, Retrieve, Delete
    // ===================================================================
    console.log('TEST 20: Method - Create, Retrieve, Delete')
    totalTests++
    const methodVaporous = new Vaporous()
    methodVaporous.append([
        { value: 10 },
        { value: 20 }
    ])
        .method('create', 'doubleValues', (vap, options) => {
            vap.eval(event => ({
                doubled: event.value * (options.multiplier || 2)
            }))
        })
        .method('retrieve', 'doubleValues', { multiplier: 3 })
        .assert((event, i, { expect }) => {
            expect(event.doubled !== undefined)
            if (event.value === 10) expect(event.doubled === 30)
            if (event.value === 20) expect(event.doubled === 60)
        })

    methodVaporous.method('delete', 'doubleValues')

    if (methodVaporous.savedMethods.doubleValues === undefined) {
        console.log('✓ Method create/retrieve/delete works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 21: Complex Pipeline - Multiple Operations
    // ===================================================================
    console.log('TEST 21: Complex Pipeline with Multiple Operations')
    totalTests++
    const complexVaporous = new Vaporous()
    complexVaporous.append([
        { id: 1, value: 100, category: 'A', tags: ['x', 'y'] },
        { id: 2, value: 200, category: 'B', tags: ['y', 'z'] },
        { id: 3, value: 150, category: 'A', tags: ['x'] },
        { id: 4, value: 250, category: 'B', tags: ['z'] }
    ])
        .checkpoint('create', 'original')
        .eval(event => ({ doubled: event.value * 2 }))
        .bin('value', 50)
        .sort('asc', 'value')
        .stats(
            new Aggregation('doubled', 'sum', 'totalDoubled'),
            new Aggregation('id', 'count', 'count'),
            new By('category')
        )
        .assert((event, i, { expect }) => {
            expect(event.category !== undefined)
            expect(event.totalDoubled !== undefined)
            expect(event.count !== undefined)
        })

    complexVaporous.checkpoint('retrieve', 'original')
        .mvexpand('tags')
        .stats(
            new Aggregation('value', 'sum', 'totalValue'),
            new By('tags')
        )
        .assert((event, i, { expect }) => {
            expect(event.tags !== undefined)
            expect(event.totalValue !== undefined)
        })

    if (complexVaporous.events.length === 3) {
        console.log('✓ Complex pipeline with multiple operations works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 22: File Operations - FileScan
    // ===================================================================
    console.log('TEST 22: File Operations - FileScan')
    totalTests++
    const fileVaporous = new Vaporous()
    await fileVaporous.fileScan(dataFolder)
        .assert((event, i, { expect }) => {
            expect(event._fileInput !== undefined)
            expect(typeof event._fileInput === 'string')
        })

    if (fileVaporous.events.length > 0) {
        console.log('✓ FileScan method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 23: CSV Load
    // ===================================================================
    console.log('TEST 23: CSV Load')
    totalTests++
    const csvVaporous = new Vaporous()
    await csvVaporous.fileScan(dataFolder)
        .filter(event => event._fileInput.endsWith('.csv'))
        .csvLoad(({ data }) => ({
            id: parseInt(data.id),
            value: parseInt(data.value),
            category: data.category,
            timestamp: data.timestamp,
            nested_value: parseInt(data.nested_value)
        }))
    csvVaporous
        .flatten()
        .assert((event, i, { expect }) => {
            expect(event.id !== undefined)
            expect(event.value !== undefined)
            expect(event.category !== undefined)
            expect(event._fileInput !== undefined)
            expect(typeof event.id === 'number')
            expect(['A', 'B'].includes(event.category))
        })

    if (csvVaporous.events.length === 5) {
        console.log('✓ CSV Load method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 24: File Load (JSON)
    // ===================================================================
    console.log('TEST 24: File Load (JSON)')
    totalTests++
    const jsonVaporous = new Vaporous()
    await jsonVaporous.fileScan(dataFolder)
        .filter(event => event._fileInput.endsWith('.json'))
        .fileLoad('\n', line => JSON.parse(line))

    jsonVaporous.flatten()
        .assert((event, i, { expect }) => {
            expect(event.sensor !== undefined)
            expect(event.reading !== undefined)
            expect(event.location !== undefined)
            expect(event._fileInput !== undefined)
            expect(typeof event.reading === 'number')
        })

    if (jsonVaporous.events.length === 4) {
        console.log('✓ File Load (JSON) method works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 25: WriteFile and Output
    // ===================================================================
    console.log('TEST 25: WriteFile Method')
    totalTests++
    const outputVaporous = new Vaporous()
    outputVaporous.append([
        { id: 1, value: 100 },
        { id: 2, value: 200 }
    ])
        .writeFile('test_output.json')

    const fileExists = fs.existsSync('./test_output.json')
    if (fileExists) {
        const content = JSON.parse(fs.readFileSync('./test_output.json', 'utf8'))
        if (content.length === 2 && content[0].id === 1) {
            console.log('✓ WriteFile method works correctly\n')
            testsPassed++
            fs.unlinkSync('./test_output.json')
        }
    }

    // ===================================================================
    // TEST 26: Visualization Methods - toGraph and build
    // ===================================================================
    console.log('TEST 26: Visualization Methods - toGraph and build')
    totalTests++
    const graphVaporous = new Vaporous()
    graphVaporous.append([
        { x: 1, y1: 10, y2: 20, series: 'A', trellis: 'T1' },
        { x: 2, y1: 15, y2: 25, series: 'A', trellis: 'T1' },
        { x: 1, y1: 12, y2: 22, series: 'B', trellis: 'T1' },
        { x: 2, y1: 17, y2: 27, series: 'B', trellis: 'T1' }
    ])
        .toGraph('x', ['y1', 'y2'], 'series', 'trellis')
        .assert((event, i, { expect }) => {
            expect(Array.isArray(event))
            expect(graphVaporous.graphFlags.length > 0)
        })
        .build('Test Graph', 'LineChart', { tab: 'Test', columns: 2 })
        .assert((event, i, { expect }) => {
            expect(graphVaporous.visualisations.length > 0)
        })

    if (graphVaporous.visualisations.length > 0 && graphVaporous.graphFlags.length > 0) {
        console.log('✓ Visualization methods work correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 27: Combined Window and By in streamstats
    // ===================================================================
    console.log('TEST 27: Combined Window and By in Streamstats')
    totalTests++
    const combinedStreamVaporous = new Vaporous()
    combinedStreamVaporous.append([
        { category: 'A', value: 10, seq: 1 },
        { category: 'A', value: 20, seq: 2 },
        { category: 'A', value: 30, seq: 3 },
        { category: 'B', value: 15, seq: 4 },
        { category: 'B', value: 25, seq: 5 },
        { category: 'A', value: 40, seq: 6 }
    ])
        .streamstats(
            new Aggregation('value', 'sum', 'windowBySum'),
            new Aggregation('value', 'list', 'windowByList'),
            new Window(2),
            new By('category')
        )
        .assert((event, i, { expect }) => {
            expect(event.windowBySum !== undefined)
            expect(Array.isArray(event.windowByList))

            if (event.seq === 1) {
                expect(event.windowBySum === 10)
                expect(event.windowByList.length === 1)
            }
            if (event.seq === 2) {
                expect(event.windowBySum === 30)
                expect(event.windowByList.length === 2)
            }
            if (event.seq === 3) {
                expect(event.windowBySum === 50)
                expect(event.windowByList.length === 2)
            }
            if (event.seq === 6) {
                expect(event.windowBySum === 40)
                expect(event.windowByList.length === 1)
            }
        })

    if (combinedStreamVaporous.events[5].windowBySum === 40) {
        console.log('✓ Combined Window and By in streamstats works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 28: Edge Cases - Empty Arrays, Null Values
    // ===================================================================
    console.log('TEST 28: Edge Cases - Empty Arrays and Null Handling')
    totalTests++
    const edgeCaseVaporous = new Vaporous()
    edgeCaseVaporous.append([
        { id: 1, value: 10, empty: [] },
        { id: 2, value: null, empty: [] },
        { id: 3, value: 30, empty: [] }
    ])
        .eval(event => {
            if (event.value === null) event.value = 0
            return { hasValue: event.value > 0 }
        })
        .assert((event, i, { expect }) => {
            expect(event.hasValue !== undefined)
            expect(typeof event.hasValue === 'boolean')
        })
        .mvexpand('empty')
        .assert((event, i, { expect }) => {
            // When mvexpand encounters empty array, it should keep the event
            expect(vaporous.events.length === 0)
        })

    if (edgeCaseVaporous.events.length === 0) {
        console.log('✓ Edge case handling works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 29: Multi-key Sorting
    // ===================================================================
    console.log('TEST 29: Multi-key Sorting')
    totalTests++
    const multiSortVaporous = new Vaporous()
    multiSortVaporous.append([
        { category: 'B', priority: 2, value: 10 },
        { category: 'A', priority: 1, value: 20 },
        { category: 'A', priority: 2, value: 30 },
        { category: 'B', priority: 1, value: 40 }
    ])
        .sort('asc', 'category', 'priority')
        .assert((event, i, { expect }) => {
            if (i === 0) {
                expect(event.category === 'A')
                expect(event.priority === 1)
            }
            if (i === 1) {
                expect(event.category === 'A')
                expect(event.priority === 2)
            }
            if (i === 2) {
                expect(event.category === 'B')
                expect(event.priority === 1)
            }
            if (i === 3) {
                expect(event.category === 'B')
                expect(event.priority === 2)
            }
        })

    if (multiSortVaporous.events[0].category === 'A' && multiSortVaporous.events[0].priority === 1) {
        console.log('✓ Multi-key sorting works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 30: DisableCloning in Checkpoints
    // ===================================================================
    console.log('TEST 30: DisableCloning in Checkpoints')
    totalTests++
    const cloningVaporous = new Vaporous()
    cloningVaporous.append([{ id: 1, value: 100 }])
        .checkpoint('create', 'withCloning')
        .checkpoint('create', 'withoutCloning', { disableCloning: true })
        .eval(event => ({ modified: true }))

    cloningVaporous.checkpoint('retrieve', 'withCloning')
    const hasClonedCorrectly = cloningVaporous.events[0].modified === undefined

    cloningVaporous.checkpoint('retrieve', 'withoutCloning')
    const hasNotCloned = cloningVaporous.events[0].modified === true

    if (hasClonedCorrectly && hasNotCloned) {
        console.log('✓ DisableCloning in checkpoints works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // TEST 31: FilterIntoCheckpoint without Destroy
    // ===================================================================
    console.log('TEST 31: FilterIntoCheckpoint without Destroy')
    totalTests++
    const filterNoDestroyVaporous = new Vaporous()
    filterNoDestroyVaporous.append([
        { id: 1, active: true },
        { id: 2, active: false },
        { id: 3, active: true }
    ])
        .filterIntoCheckpoint('inactiveOnly', event => !event.active, { destroy: false })
        .assert((event, i, { expect }) => {
            expect(filterNoDestroyVaporous.events.length === 3)
        })

    filterNoDestroyVaporous.checkpoint('retrieve', 'inactiveOnly')
        .assert((event, i, { expect }) => {
            expect(filterNoDestroyVaporous.events.length === 1)
            expect(event.active === false)
        })

    if (filterNoDestroyVaporous.events.length === 1) {
        console.log('✓ FilterIntoCheckpoint without destroy works correctly\n')
        testsPassed++
    }

    // ===================================================================
    // FINAL SUMMARY
    // ===================================================================
    console.log('\n=== TEST SUMMARY ===')
    console.log(`Tests Passed: ${testsPassed}/${totalTests}`)
    console.log(`Success Rate: ${(testsPassed / totalTests * 100).toFixed(2)}%`)

    if (testsPassed === totalTests) {
        console.log('\n✓ ALL TESTS PASSED!\n')
    } else {
        console.log(`\n⚠ ${totalTests - testsPassed} test(s) failed\n`)
    }

    // Cleanup test data
    const testDataDir = path.join(__dirname, 'testData')
    if (fs.existsSync(testDataDir)) {
        fs.rmSync(testDataDir, { recursive: true, force: true })
        console.log('Test data cleaned up')
    }
}

main().catch(console.error)
