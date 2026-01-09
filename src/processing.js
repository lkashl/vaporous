async function parallel(target, funct, { mode = "dynamic" } = {}) {
    this.manageEntry()
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

        progress = await Promise.all(progress)
    } else if (mode === "dynamic") {
        const tasks = []
        const eventList = this.events.slice();

        const process = async (event) => {
            if (eventList.length === 0) return;
            const thisEvent = eventList.splice(0, 1)
            const instance = this.clone()
            instance.events = thisEvent
            const task = await funct(instance)
            tasks.push(task)
            await process()
        }

        const streams = []
        for (let i = 0; i < target; i++) {
            streams.push(process())
        }

        await Promise.all(streams)
        progress = tasks
    } else {
        throw new Error('Parallel processing mode "' + mode + '" not recognised')
    }


    const newLength = progress.reduce((prev, curr) => curr.events.length + prev, 0)

    const events = new Array(newLength)

    let curr = 0;
    progress.forEach(instance => {
        events[curr] = instance.events[curr]
        curr++
    })

    this.manageExit()
}

async function sequential() {
    this.manageEntry()
    this.manageExit()
}

async function batch() {
    this.manageEntry()
    this.manageExit()
}


module.exports = {
    parallel,
    sequential
}