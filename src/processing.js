const { Worker } = require('worker_threads');
const path = require('path');

let workers = []

async function parallel(target, funct, { mode = "dynamic", multiThread = false } = {}) {

    let progress;

    if (mode === "preallocated") {


        const additionalMembers = this.events.length % target
        const normalQueueSize = Math.floor(this.events.length / target)

        const vaporousTasks = []

        let offset = 0;
        for (let i = 0; i < target; i++) {
            const size = i < additionalMembers ? normalQueueSize + 1 : normalQueueSize
            const current = this.events.slice(offset, offset + size)
            const instance = this.clone()
            instance.events = current
            vaporousTasks.push(instance)
            offset += size
        }

        progress = vaporousTasks.map(vaporousTask => funct(vaporousTask))


    } else if (mode === "dynamic") {
        const tasks = []
        const eventList = this.events.slice();

        const processSingleThread = async (event) => {
            if (eventList.length === 0) return;
            const thisEvent = eventList.splice(0, 1)
            const instance = this.clone()
            instance.events = thisEvent

            const task = await funct(instance)
            tasks.push(task)
            await processSingleThread()
        }

        const processMultiThread = async (worker) => {
            if (eventList.length === 0) return;
            const thisEvent = eventList.splice(0, 1)
            const instance = this.clone({ deep: true })
            instance.events = thisEvent
            const message = instance.serialise()

            const task = await new Promise((resolve, reject) => {
                worker.on('message', (result) => {
                    instance.events = result.events
                    resolve(result);
                });

                worker.on('error', (error) => {
                    worker.terminate();
                    reject(error);
                });

                worker.on('exit', (code) => {
                    if (code !== 0) {
                        reject(new Error(`Worker stopped with exit code ${code}`));
                    }
                });

                worker.postMessage(message);
            })

            tasks.push(task)

            await processMultiThread(worker);
        }

        const streams = []
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
    } else {
        throw new Error('Parallel processing mode "' + mode + '" not recognised')
    }

    progress = await Promise.all(progress)

    // Collate all events from the processed instances
    const events = []
    progress.forEach(instance => {
        if (instance && instance.events) {
            events.push(...instance.events)
        }
    })

    // Update this instance's events with the collated results
    this.events = events
    return this;
}

const sleep = (interval) => {
    return new Promise(resolve => {
        setTimeout(() => resolve(), interval)
    })
}

async function interval(funct, intervalTiming, options) {
    this.intervals.push(this)
    const reference = this.intervals.at(-1)

    const loop = async () => {
        const cloned = reference.clone({ deep: true })

        await funct(cloned)
        await sleep(intervalTiming)
        await loop()
    }

    await loop()
    return this;
}


module.exports = {
    parallel,
    interval
}
