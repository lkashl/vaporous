const dayjs = require("dayjs")
const { Vaporous, By, Aggregation, Window } = require("../../Vaporous")


const dataFolder = __dirname + '/exampleData'
const main = async () => {

    console.log('Starting')
    const vaporous = await new Vaporous()
        // Load folder and files
        .fileScan(dataFolder)
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

    vaporous
        .flatten()
        .eval(event => ({
            ...event._raw,
            _fileInput: event._fileInput.split('/').at(-1).split('.')[0]
        }))
        .assert((event, i, { expect }) => {
            expect(event._fileInput !== undefined)
        })
        .eval(event => ({
            daysAgo: Math.floor((new Date() - dayjs(event._fileInput, 'YYYYMMDD')) / 86400000)
        }))
        .output()
        .sort('asc', 'seconds')
        .checkpoint('create', 'mainDataSeries')

        // Create polling interval graph
        .delta('seconds', 'pollingInterval')
        .bin('seconds', 1)
        .stats(new Aggregation('pollingInterval', 'percentile', 'pollingInterval', 95), new By('seconds'), new By('daysAgo'))
        .toGraph('seconds', 'pollingInterval', 'daysAgo')
        .build('Machine polling', 'LineChart', {
            columns: 1,
            tab: 'Diagnostics'
        })

        // Create temperature graph
        .checkpoint('retrieve', 'mainDataSeries')
        .bin('seconds', 1)
        .mvexpand('temps')
        .stats(new Aggregation('temps', 'max', 'maxTemp'),
            new By('seconds'), new By('daysAgo'), new By('_mvExpand_temps')
        )
        .toGraph('seconds', 'maxTemp', 'daysAgo', '_mvExpand_temps')
        .build('Temp sensor - ', 'LineChart', {
            columns: 3,
            tab: 'Diagnostics'
        })

        .bin('seconds', 1)
        .stats(new Aggregation('seconds', 'count', 'count'), new By('daysAgo'), new By('seconds'))
        .toGraph('seconds', 'count', 'daysAgo')
        .writeFile('test1')
        .build('Second test', 'LineChart', { columns: 1 })


        .checkpoint('retrieve', 'mainDataSeries')
        .method('create', 'aggregateKG', (vaporous, { field }) => {
            vaporous
                .sort('asc', 'daysAgo', 'seconds')
                .delta('seconds', 'timeHeld', new By('daysAgo'))
                .bin(field + '_kgf', 1)
                .sort('dsc', 'daysAgo', field + "_kgf")
                .streamstats(
                    new Aggregation('timeHeld', 'sum', 'cumTimeHeld'),
                    new By('daysAgo'),
                )
                .filter(event => event[field + '_kgf'] > 30)
                .toGraph(field + '_kgf', 'cumTimeHeld', 'daysAgo')
                .output()
                .build('Cumulative weight held', 'LineChart', {
                    tab: 'Cumulative weight',
                    columns: 1
                })
        })
        .method('retrieve', 'aggregateKG', { field: 'right' })
        .render()
}

main()



//TODO: Stream modifying functions - eventstats, streamstats and delta need to enforce a check for variable uniqueness, otherwis ethey will backwards permute a variable that is arleady in use 