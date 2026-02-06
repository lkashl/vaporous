const { Worker } = require('worker_threads');
const path = require('path');

async function parallel(target, { multiThread = false } = {}, callbackPath) {

    let workers = []

    let progress, funct;

    if (typeof callbackPath === 'object' && !multiThread) {
        funct = require(callbackPath[0])
        callbackPath.splice(0, 1)
        callbackPath.forEach(item => {
            funct = funct[item]
        })
    } else if ((typeof callbackPath === 'function' && !multiThread)
        || (typeof callbackPath === 'object' && multiThread)) {
        funct = callbackPath
    } else if (typeof callbackPath !== 'object' && multiThread) {
        throw new Error('Parallel processing requires a path if multithread is being used')
    }

    const tasks = []
    const eventList = this.events.slice()
    const loggers = this.loggers

    const processSingleThread = async (event) => {
        if (eventList.length === 0) return;
        const thisEvent = eventList.splice(0, 1)
        const { Vaporous } = require('../Vaporous');
        const instance = new Vaporous({ loggers })
        instance.events = thisEvent

        const task = await funct(instance)
        tasks.push(task.begin())

        await processSingleThread()
    }

    const processMultiThread = async (worker) => {
        if (eventList.length === 0) return;
        const thisEvent = eventList.splice(0, 1)

        const task = await new Promise((resolve, reject) => {
            worker.on('message', (result) => {
                if (result instanceof Error) {
                    return reject(result)
                }
                resolve(result.events);
            });

            worker.on('error', (error) => {
                worker.terminate();
                reject(error);
            });

            worker.on('exit', (code) => {
                if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
            });

            worker.postMessage({ callbackPath: funct, events: thisEvent, loggers, workerId: workers.length });
        })

        tasks.push(task)

        await processMultiThread(worker);
    }

    const streams = []
    target = Math.min(target, this.events.length)

    for (let i = 0; i < target; i++) {
        if (multiThread) {

            const workerPath = path.join(__dirname, 'processing.worker.js');

            if (!workers[i]) workers[i] = new Worker(workerPath);
            const worker = workers[i];

            streams.push(processMultiThread(worker))
        } else {
            streams.push(processSingleThread())
        }
    }

    await Promise.all(streams)
    progress = tasks
    progress = await Promise.all(progress)

    workers.forEach(worker => worker.terminate())

    const eventsCumulative = []
    progress.forEach(events => {
        if (events instanceof require('../Vaporous').Vaporous) events = events.serialise().events
        eventsCumulative.push(...events)
    })

    this.events = eventsCumulative
    return this;
}


const sleep = (interval) => {
    return new Promise(resolve => {
        setTimeout(() => resolve(), interval)
    })
}

async function interval(funct, intervalTiming, options) {
    const { Vaporous } = require('../Vaporous')
    const ref = this;
    // this.intervals.push(this)
    // const reference = this.intervals.at(-1)

    const loop = async () => {
        const target = new Vaporous({ loggers: ref.loggers })

        target.events = structuredClone(ref.events);

        await funct(target)
        await target.begin()

        await sleep(intervalTiming)
        await loop()
    }

    await loop()
    return this;
}

async function recurse(funct) {
    const { Vaporous } = require('../Vaporous')
    const events = []

    for (let event of this.events) {
        const target = new Vaporous({ loggers: this.loggers })
        target.events = [event]

        const localRecursion = async (target) => {
            const val = await funct(target)
            if (val.events[0]._recursion) return localRecursion(target)
            return val
        }

        const val = await localRecursion(target)
        events.push(...val.events)
    }

    this.events = events
    return this;
}

async function doIf(condition, callback) {
    const proceed = await condition(this)
    if (proceed) {
        const clone = this.clone()
        const task = await callback(clone)
        task.begin()
        this.events = clone.events
    }
    return this;
}

async function method(operation, name, options) {
    const operations = {
        create: () => {
            this.savedMethods[name] = options
        },
        retrieve: async () => {
            if (!this.savedMethods[name]) throw new Error('Method not found ' + name)
            const clone = this.clone()
            const task = await clone.savedMethods[name](clone, options)
            await task.begin()
            this.events = clone.events
        },
        delete: () => {
            delete this.savedMethods[name]
        }
    }

    await operations[operation]()
    return this;
}

module.exports = {
    parallel,
    interval,
    recurse,
    doIf,
    method
}
