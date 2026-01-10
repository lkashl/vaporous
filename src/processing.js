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

        const processMultiThread = async (event) => {

        }

        const streams = []
        for (let i = 0; i < target; i++) {
            streams.push(processSingleThread())
        }

        await Promise.all(streams)
        progress = tasks
    } else {
        throw new Error('Parallel processing mode "' + mode + '" not recognised')
    }

    progress = await Promise.all(progress)

    const newLength = progress.reduce((prev, curr) => curr.events.length + prev, 0)

    const events = new Array(newLength)

    let curr = 0;
    progress.forEach(instance => {
        events[curr] = instance.events[curr]
        curr++
    })

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

        console.log(cloned)
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
