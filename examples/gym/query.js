const dayjs = require("dayjs")
const { Vaporous, By, Aggregation, Window } = require("../../Vaporous")


const dataFolder = __dirname + '/exampleData'
const main = async () => {
    await new Vaporous({
        loggers: {
            perf: (level, event) => console[level](event)
        }
    })
        // Load folder and files
        .fileScan(dataFolder)
        .filter(event => event._fileInput.endsWith('.git'))
        .csvLoad(({ data }) => {
            const event = {
                'seconds': Number.parseFloat(data.seconds),
                'left_kgf': Number.parseFloat(data.left_kgf),
                'left_cm': Number.parseFloat(data.left_cm),
                'left_cm_per_s': Number.parseFloat(data.left_cm_per_s),
                'right_kgf': Number.parseFloat(data.right_kgf),
                'right_cm': Number.parseFloat(data.right_cm),
                'right_cm_per_s': Number.parseFloat(data.right_cm_per_s),
                'phase': Number.parseFloat(data.phase),

                temps: [
                    Number.parseFloat(data.temp0_C),
                    Number.parseFloat(data.temp1_C),
                    Number.parseFloat(data.temp2_C),
                    Number.parseFloat(data.temp3_C),
                    Number.parseFloat(data.temp4_C),
                    Number.parseFloat(data.temp5_C),
                    Number.parseFloat(data.temp6_C),
                    Number.parseFloat(data.temp7_C)
                ]
            }

            return event;
        })
        .flatten()
        .eval(event => event._fileInput = event._fileInput.split('/').at(-1).split('.')[0])
        .assert((event, i, { expect }) => {
            expect(event._fileInput !== undefined)
        })
        .eval(event => ({
            daysAgo: Math.floor((new Date() - dayjs(event._fileInput, 'YYYYMMDD')) / 86400000)
        }))
        .sort('asc', 'daysAgo', 'seconds')
        .delta('seconds', 'timeHeld', new By('daysAgo'))
        .checkpoint('create', 'mainDataSeries')

        // Create polling interval graph
        .delta('seconds', 'pollingInterval', new By('daysAgo'))
        .bin('seconds', 1)
        .stats(new Aggregation('pollingInterval', 'percentile', 'pollingInterval', 95), new By('seconds'), new By('daysAgo'))
        .toGraph('seconds', 'pollingInterval', 'daysAgo')
        .build('Machine polling', 'Line', {
            columns: 2,
            tab: 'Machine Diagnostics'
        })

        // Create a second infomration density chart
        .checkpoint('retrieve', 'mainDataSeries')
        .bin('seconds', 1)
        .stats(new Aggregation('seconds', 'count', 'count'), new By('daysAgo'), new By('seconds'))
        .toGraph('seconds', 'count', 'daysAgo')
        .build('Second test', 'Line', { columns: 2, tab: "Machine Diagnostics" })

        // Create temperature graph
        .checkpoint('retrieve', 'mainDataSeries')
        .bin('seconds', 1)
        .mvexpand('temps')
        .stats(new Aggregation('temps', 'max', 'maxTemp'),
            new By('seconds'), new By('daysAgo'), new By('_mvExpand_temps')
        )
        .toGraph('seconds', 'maxTemp', 'daysAgo', '_mvExpand_temps')
        .build('Temp sensor - ', 'Line', {
            columns: 3,
            tab: "Machine Diagnostics"
        })

        .method('create', 'aggregateKG', (vaporous, { field }) => {
            vaporous
                .checkpoint('retrieve', 'mainDataSeries')
                .bin(field + '_kgf', 1)
                .sort('dsc', 'daysAgo', field + "_kgf")
                .streamstats(
                    new Aggregation('timeHeld', 'sum', 'timeHeld'),
                    new By('daysAgo'),
                )
                .filter(event => event[field + '_kgf'] > 30)
                .toGraph(field + '_kgf', 'timeHeld', 'daysAgo')
                .build('Cumulative time weight held for - ' + field.toUpperCase(), 'Area', {
                    tab: 'Cumulative',
                    columns: 2
                })
        })
        .method('retrieve', 'aggregateKG', { field: 'right' })
        .method('retrieve', 'aggregateKG', { field: 'left' })

        // Duration weight held for
        .method('create', 'weightHeld', (vaporous, { field }) => {
            vaporous
                .checkpoint('retrieve', 'mainDataSeries')
                .bin(field + '_kgf', 2)
                .stats(new Aggregation('timeHeld', 'sum', 'timeHeld'), new By('daysAgo'), new By(field + '_kgf'))
                .filter(event => event[field + '_kgf'] > 30)
                .toGraph(field + '_kgf', 'timeHeld', 'daysAgo')
                .build('Time weight held - ' + field.toUpperCase(), 'Scatter', {
                    tab: 'Cumulative',
                    columns: 2,
                })
        })
        .method('retrieve', 'weightHeld', { field: 'right' })
        .method('retrieve', 'weightHeld', { field: 'left' })



        // Weight by phase
        .method('create', 'weightByPhase', (vaporous, { field }) => {
            vaporous
                .checkpoint('retrieve', 'mainDataSeries')
                .stats(new Aggregation(field + '_kgf', 'max', field + '_kgf'), new By('phase'), new By('daysAgo'))
                .toGraph('phase', field + '_kgf', 'daysAgo')
                .build('Power at location - ' + field.toUpperCase(), 'Line', {
                    tab: 'Instant',
                    columns: 2
                })
        })
        .method('retrieve', 'weightByPhase', { field: 'right' })
        .method('retrieve', 'weightByPhase', { field: 'left' })


        .checkpoint('retrieve', 'mainDataSeries')
        .bin('seconds', 0.25)
        .toGraph('seconds', ['left_cm', 'right_cm', 'left_kgf', 'right_kgf'], null, 'daysAgo')
        .build('Arm position over time - ', 'Line', {
            y2: /_cm/,

            y1Type: 'line',
            y2Type: 'line',

            // y1Stacked: true,
            // y2Stacked: true,

            tab: 'Balance and power',
            columns: 2,

            sortX: 'asc',
            trellisAxis: 'shared',
            legend: false
        })

        .checkpoint('retrieve', 'mainDataSeries')
        .method('create', 'weightByLocation', (vaporous, { field }) => {
            vaporous
                .checkpoint('retrieve', 'mainDataSeries')
                .filter(event => event.phase < 5)
                .bin(field + "_cm", 2)
                .stats(new Aggregation(field + '_kgf', 'median', field + '_kgf'), new By(field + "_cm"), new By('phase'), new By('daysAgo'))
                .toGraph(field + "_cm", field + '_kgf', 'phase', 'daysAgo')
                .build('Power over rep - ' + field.toUpperCase(), 'Line', {
                    tab: 'Power over rep',
                    columns: 2,
                    sortX: 'asc'
                })
        })
        .method('retrieve', 'weightByLocation', { field: 'left' })
        .method('retrieve', 'weightByLocation', { field: 'right' })
        .render('gym.html')
        .begin()

}

main()
