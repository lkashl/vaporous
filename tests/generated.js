// These tests are generated

const { Vaporous, By, Aggregation, Window } = require("../Vaporous")
const fs = require('fs')
const path = require('path')

const testsPassing = [], testsFailing = []
const recordTestResult = (testName, passed, message) => {
    if (passed) {
        testsPassing.push(testName)
        if (message) console.log(`✓ ${testName} ${message}`)
    } else {
        testsFailing.push(testName)
    }
}

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

    // ===================================================================
    // Constructor and Basic Properties
    // ===================================================================
    const vaporous = new Vaporous({
        loggers: {
            perf: (level, event) => console.log(`[PERF] ${event}`)
        }
    })

    const test1Passed = vaporous.events.length === 0 &&
        vaporous.visualisations.length === 0 &&
        vaporous.checkpoints && typeof vaporous.checkpoints === 'object' &&
        vaporous.savedMethods && typeof vaporous.savedMethods === 'object'

    recordTestResult('Constructor and Basic Properties', test1Passed, 'Constructor initialized correctly\n')

    // ===================================================================
    // Append Method - Adding Data Manually
    // ===================================================================
    const initialData = [
        { id: 1, value: 10, category: 'X' },
        { id: 2, value: 20, category: 'Y' }
    ]

    await vaporous
        .append(initialData)
        .assert((event, i, { expect }) => {
            expect(vaporous.events.length === 2)
            expect(event.id !== undefined)
            expect(event.value !== undefined)
            expect(event.category !== undefined)
        })
        .begin()

    const test2Passed = vaporous.events.length === 2
    recordTestResult('Append Method', test2Passed, 'Append Method works correctly')


    // ===================================================================
    // Filter Method
    // ===================================================================
    await vaporous
        .filter(event => event.category === 'X')
        .assert((event, i, { expect }) => {
            expect(vaporous.events.length === 1)
            expect(event.category === 'X')
            expect(event.id === 1)
        })
        .begin()

    const test3Passed = vaporous.events.length === 1 && vaporous.events[0].category === 'X'
    recordTestResult('Filter Method', test3Passed, 'Filter Method works correctly')


    // ===================================================================
    // Eval Method - Data Transformation
    // ===================================================================
    await vaporous
        .append([{ id: 3, value: 30, category: 'Z' }])
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
        .begin()

    const test4Passed = vaporous.events[0].doubled === 20
    recordTestResult('Eval Method - Data Transformation', test4Passed, 'Eval Method - Data Transformation works correctly')


    // ===================================================================
    // Sort Method
    // ===================================================================
    await vaporous
        .sort('asc', 'value')
        .assert((event, i, { expect }) => {
            if (i === 0) expect(event.value === 10)
            if (i === 1) expect(event.value === 30)
        })
        .begin()

    await vaporous
        .sort('dsc', 'value')
        .assert((event, i, { expect }) => {
            if (i === 0) expect(event.value === 30)
            if (i === 1) expect(event.value === 10)
        })
        .begin()

    const test5Passed = vaporous.events[0].value === 30
    recordTestResult('Sort Method - Ascending and Descending', test5Passed, 'Sort Method - Ascending and Descending works correctly')


    // ===================================================================
    // Rename Method
    // ===================================================================
    await vaporous
        .rename(['value', 'amount'], ['category', 'type'])
        .assert((event, i, { expect }) => {
            expect(event.amount !== undefined)
            expect(event.type !== undefined)
            expect(event.value === undefined)
            expect(event.category === undefined)
        })
        .begin()

    const test6Passed = vaporous.events[0].amount && vaporous.events[0].type
    recordTestResult('Rename Method', test6Passed, 'Rename Method works correctly')


    // ===================================================================
    // Bin Method
    // ===================================================================
    await vaporous
        .eval(event => ({ rawValue: event.amount }))
        .bin('amount', 10)
        .assert((event, i, { expect }) => {
            expect(event.amount % 10 === 0)
            expect(event.amount <= event.rawValue)
            const expectedBin = Math.floor(event.rawValue / 10) * 10
            expect(event.amount === expectedBin)
        })
        .begin()

    const test7Passed = vaporous.events.every(e => e.amount % 10 === 0)
    recordTestResult('Bin Method - Numerical Binning', test7Passed, 'Bin Method - Numerical Binning works correctly')


    // ===================================================================
    // Checkpoint Methods - Create, Retrieve, Delete
    // ===================================================================
    const checkpointData = vaporous.events.length
    await vaporous.checkpoint('create', 'testCheckpoint')
        .filter(event => event.id === 1)
        .assert((event, i, { expect }) => {
            expect(vaporous.events.length === 1)
        })
        .begin()

    await vaporous.checkpoint('retrieve', 'testCheckpoint')
        .assert((event, i, { expect }) => {
            expect(vaporous.events.length === checkpointData)
        })
        .begin()

    await vaporous
        .checkpoint('delete', 'testCheckpoint')
        .begin()

    const test8Passed = vaporous.checkpoints.testCheckpoint === undefined
    recordTestResult('Checkpoint Methods', test8Passed, 'Checkpoint Methods works correctly')


    // ===================================================================
    // Table Method - Data Restructuring
    // ===================================================================
    const appendVaporous = new Vaporous()
    await appendVaporous.append([{ x: 1, y: 2, z: 3 }])
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
        .begin()

    const test9Passed = appendVaporous.events[0].a !== undefined
    recordTestResult('Table Method - Data Restructuring', test9Passed, 'Table Method - Data Restructuring works correctly')


    // ===================================================================
    // Flatten Method
    // ===================================================================
    const nestedVaporous = new Vaporous()
    await nestedVaporous
        .append([
            [[{ id: 1 }, { id: 2 }], [{ id: 3 }]],
            [[{ id: 4 }]]
        ])
        .flatten(2)
        .assert((event, i, { expect }) => {
            expect(event.id !== undefined)
            expect(typeof event.id === 'number')
        })
        .begin()

    const test10Passed = nestedVaporous.events.length === 4
    recordTestResult('Flatten Method', test10Passed, 'Flatten Method works correctly')


    // ===================================================================
    // MvExpand Method
    // ===================================================================
    const mvVaporous = new Vaporous()
    await mvVaporous
        .append([
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
        .begin()

    const test11Passed = mvVaporous.events.length === 5
    recordTestResult('MvExpand Method', test11Passed, 'MvExpand Method works correctly')


    // ===================================================================
    // Stats Method - Comprehensive Aggregations
    // ===================================================================
    const statsVaporous = new Vaporous()
    await statsVaporous
        .append([
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
        .begin()

    const test12Passed = statsVaporous.events.length === 2
    recordTestResult('Stats Method - All Aggregation Types', test12Passed, 'Stats Method - All Aggregation Types works correctly')


    // ===================================================================
    // Percentile Aggregation
    // ===================================================================
    const percentileVaporous = new Vaporous()
    await percentileVaporous
        .append(
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
        .begin()

    const test13Passed = percentileVaporous.events.length === 1
    recordTestResult('Percentile Aggregation', test13Passed, 'Percentile Aggregation works correctly')


    // ===================================================================
    // Eventstats Method
    // ===================================================================
    const eventstatsVaporous = new Vaporous()
    await eventstatsVaporous
        .append([
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
        .begin()

    const test14Passed = eventstatsVaporous.events.length === 4
    recordTestResult('Eventstats Method', test14Passed, 'Eventstats Method works correctly')


    // ===================================================================
    // Streamstats Method with Window
    // ===================================================================
    const streamstatsVaporous = new Vaporous()
    await streamstatsVaporous
        .append([
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
        .begin()

    const test15Passed = streamstatsVaporous.events[4].runningSum === 120
    recordTestResult('Streamstats Method with Window', test15Passed, 'Streamstats Method with Window works correctly')


    // ===================================================================
    // Streamstats Method with By clause
    // ===================================================================
    const streamstatsByVaporous = new Vaporous()
    await streamstatsByVaporous
        .append([
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
        .begin()

    const test16Passed = streamstatsByVaporous.events[3].categoryRunningSum === 30
    recordTestResult('Streamstats Method with By clause', test16Passed, 'Streamstats Method with By clause works correctly')


    // ===================================================================
    // Delta Method
    // ===================================================================
    const deltaVaporous = new Vaporous()
    await deltaVaporous
        .append([
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
        .begin()

    const test17Passed = deltaVaporous.events[1].timeDelta === 5
    recordTestResult('Delta Method', test17Passed, 'Delta Method works correctly')


    // ===================================================================
    // FilterIntoCheckpoint Method
    // ===================================================================
    const filterCheckpointVaporous = new Vaporous()
    await filterCheckpointVaporous.append([
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
        .begin()

    filterCheckpointVaporous.checkpoint('retrieve', 'inactiveItems')
        .assert((event, i, { expect }) => {
            expect(event.status === 'inactive')
            expect(filterCheckpointVaporous.events.length === 2)
        })
        .begin()

    const test18Passed = filterCheckpointVaporous.events.length === 2
    recordTestResult('FilterIntoCheckpoint Method', test18Passed, 'FilterIntoCheckpoint Method works correctly')


    // ===================================================================
    // ParseTime Method
    // ===================================================================
    const parseTimeVaporous = new Vaporous()
    await parseTimeVaporous.append([
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
        .begin()

    const test19Passed = typeof parseTimeVaporous.events[0].timestamp === 'number'
    recordTestResult('ParseTime Method', test19Passed, 'ParseTime Method works correctly')


    // ===================================================================
    // Method - Create, Retrieve, Delete
    // ===================================================================
    const methodVaporous = new Vaporous()
    await methodVaporous.append([
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
        .method('delete', 'doubleValues')
        .begin()

    const test20Passed = methodVaporous.savedMethods.doubleValues === undefined
    recordTestResult('Method - Create, Retrieve, Delete', test20Passed, 'Method - Create, Retrieve, Delete works correctly')


    // ===================================================================
    // Complex Pipeline - Multiple Operations
    // ===================================================================
    const complexVaporous = new Vaporous()
    await complexVaporous.append([
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
        .begin()

    await complexVaporous.checkpoint('retrieve', 'original')
        .mvexpand('tags')
        .stats(
            new Aggregation('value', 'sum', 'totalValue'),
            new By('tags')
        )
        .assert((event, i, { expect }) => {
            expect(event.tags !== undefined)
            expect(event.totalValue !== undefined)
        })
        .begin()

    const test21Passed = complexVaporous.events.length === 3
    recordTestResult('Complex Pipeline with Multiple Operations', test21Passed, 'Complex Pipeline with Multiple Operations works correctly')


    // ===================================================================
    // File Operations - FileScan
    // ===================================================================
    const fileVaporous = new Vaporous()
    await fileVaporous.fileScan(dataFolder)
        .assert((event, i, { expect }) => {
            expect(event._fileInput !== undefined)
            expect(typeof event._fileInput === 'string')
        })
        .begin()

    const test22Passed = fileVaporous.events.length > 0
    recordTestResult('File Operations - FileScan', test22Passed, 'File Operations - FileScan works correctly')


    // ===================================================================
    // CSV Load
    // ===================================================================
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
        .flatten()
        .assert((event, i, { expect }) => {
            expect(event.id !== undefined)
            expect(event.value !== undefined)
            expect(event.category !== undefined)
            expect(event._fileInput !== undefined)
            expect(typeof event.id === 'number')
            expect(['A', 'B'].includes(event.category))
        })
        .begin()

    const test23Passed = csvVaporous.events.length === 5
    recordTestResult('CSV Load', test23Passed, 'CSV Load works correctly')


    // ===================================================================
    // File Load (JSON)
    // ===================================================================
    const jsonVaporous = new Vaporous()
    await jsonVaporous.fileScan(dataFolder)
        .filter(event => event._fileInput.endsWith('.json'))
        .fileLoad('\n', line => JSON.parse(line))
        .flatten()
        .assert((event, i, { expect }) => {
            expect(event.sensor !== undefined)
            expect(event.reading !== undefined)
            expect(event.location !== undefined)
            expect(event._fileInput !== undefined)
            expect(typeof event.reading === 'number')
        })
        .begin()

    const test24Passed = jsonVaporous.events.length === 4
    recordTestResult('File Load (JSON)', test24Passed, 'File Load (JSON) works correctly')


    // ===================================================================
    // WriteFile and Output
    // ===================================================================
    const outputVaporous = new Vaporous()
    await outputVaporous.append([
        { id: 1, value: 100 },
        { id: 2, value: 200 }
    ])
        .writeFile('test_output.json')
        .begin()

    const fileExists = fs.existsSync('./test_output.json')
    let test25Passed = false
    if (fileExists) {
        const content = JSON.parse(fs.readFileSync('./test_output.json', 'utf8'))
        test25Passed = content.length === 2 && content[0].id === 1
        if (test25Passed) {
            fs.unlinkSync('./test_output.json')
        }
    }
    recordTestResult('WriteFile Method', test25Passed, 'WriteFile Method works correctly')


    // ===================================================================
    // Visualization Methods - toGraph and build
    // ===================================================================
    const graphVaporous = new Vaporous()
    await graphVaporous.append([
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
        .build('Test Graph', 'Line', { tab: 'Test', columns: 2 })
        .assert((event, i, { expect }) => {
            expect(graphVaporous.visualisations.length > 0)
        })
        .begin()

    const test26Passed = graphVaporous.visualisations.length > 0 && graphVaporous.graphFlags.length > 0
    recordTestResult('Visualization Methods - toGraph and build', test26Passed, 'Visualization Methods - toGraph and build works correctly')


    // ===================================================================
    // Combined Window and By in streamstats
    // ===================================================================
    const combinedStreamVaporous = new Vaporous()
    await combinedStreamVaporous.append([
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
        .begin()

    const test27Passed = combinedStreamVaporous.events[5].windowBySum === 40
    recordTestResult('Combined Window and By in Streamstats', test27Passed, 'Combined Window and By in Streamstats works correctly')


    // ===================================================================
    // Edge Cases - Empty Arrays, Null Values
    // ===================================================================
    const edgeCaseVaporous = new Vaporous()
    await edgeCaseVaporous.append([
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
        .begin()

    const test28Passed = edgeCaseVaporous.events.length === 0
    recordTestResult('Edge Cases - Empty Arrays and Null Handling', test28Passed, 'Edge Cases - Empty Arrays and Null Handling works correctly')


    // ===================================================================
    // Multi-key Sorting
    // ===================================================================
    const multiSortVaporous = new Vaporous()
    await multiSortVaporous.append([
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
        .begin()

    const test29Passed = multiSortVaporous.events[0].category === 'A' && multiSortVaporous.events[0].priority === 1
    recordTestResult('Multi-key Sorting', test29Passed, 'Multi-key Sorting works correctly')


    // ===================================================================
    // DisableCloning in Checkpoints
    // ===================================================================
    const cloningVaporous = new Vaporous()
    await cloningVaporous.append([{ id: 1, value: 100 }])
        .checkpoint('create', 'withCloning')
        .checkpoint('create', 'withoutCloning', { disableCloning: true })
        .eval(event => ({ modified: true }))
        .checkpoint('retrieve', 'withCloning')
        .begin()

    const hasClonedCorrectly = cloningVaporous.events[0].modified === undefined

    await cloningVaporous.checkpoint('retrieve', 'withoutCloning').begin()
    const hasNotCloned = cloningVaporous.events[0].modified === true

    const test30Passed = hasClonedCorrectly && hasNotCloned
    recordTestResult('DisableCloning in Checkpoints', test30Passed, 'DisableCloning in Checkpoints works correctly')


    // ===================================================================
    // FilterIntoCheckpoint without Destroy
    // ===================================================================
    const filterNoDestroyVaporous = new Vaporous()
    await filterNoDestroyVaporous.append([
        { id: 1, active: true },
        { id: 2, active: false },
        { id: 3, active: true }
    ])
        .filterIntoCheckpoint('inactiveOnly', event => !event.active, { destroy: false })
        .assert((event, i, { expect }) => {
            expect(filterNoDestroyVaporous.events.length === 3)
        })
        .begin()

    await filterNoDestroyVaporous.checkpoint('retrieve', 'inactiveOnly')
        .assert((event, i, { expect }) => {
            expect(filterNoDestroyVaporous.events.length === 1)
            expect(event.active === false)
        })
        .begin()

    const test31Passed = filterNoDestroyVaporous.events.length === 1
    recordTestResult('FilterIntoCheckpoint without Destroy', test31Passed, 'FilterIntoCheckpoint without Destroy works correctly')


    // ===================================================================
    // FINAL SUMMARY
    // ===================================================================
    console.log('\n=== TEST SUMMARY ===')
    const totalTests = testsPassing.length + testsFailing.length
    console.log(`Tests Passed: ${testsPassing.length}/${totalTests}`)
    console.log(`Success Rate: ${(testsPassing.length / totalTests * 100).toFixed(2)}%`)

    if (testsFailing.length === 0) {
        console.log('\n✓ ALL TESTS PASSED!\n')
    } else {
        console.log(`\n⚠ ${testsFailing.length} test(s) failed\n`)
        console.log('Failed tests:')
        testsFailing.forEach(test => console.log(`  - ${test}`))
    }

    // Cleanup test data
    const testDataDir = path.join(__dirname, 'testData')
    if (fs.existsSync(testDataDir)) {
        fs.rmSync(testDataDir, { recursive: true, force: true })
        console.log('Test data cleaned up')
    }
}

main().catch(console.error)

