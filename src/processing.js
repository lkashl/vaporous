async function parallel(target) {
    this.manageEntry()

    const additionalMembers = this.events.length % target
    const normalQueueSize = Math.floor(this.events.length / target)

    const vaporousTasks = []

    let offset = 0;
    for (let i = 0; i < target; i++) {
        const size = i < additionalMembers ? normalQueueSize + 1 : normalQueueSize
        const current = this.events.slice(offset, offset + size)
        vaporousTasks.push(current)
        offset += size
    }

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