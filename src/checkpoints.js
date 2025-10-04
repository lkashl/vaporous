module.exports = {
    _checkpoint(operation, name, data, { disableCloning }) {
        const operations = {
            create: () => this.checkpoints[name] = disableCloning ? data : structuredClone(data),
            retrieve: () => this.events = disableCloning ? this.checkpoints[name] : structuredClone(this.checkpoints[name]),
            delete: () => delete this.checkpoints[name]
        }

        operations[operation]()
        return this
    },

    checkpoint(operation, name, { disableCloning } = {}) {
        this.manageEntry()
        this._checkpoint(operation, name, this.events, { disableCloning })
        return this.manageExit()
    },

    filterIntoCheckpoint(checkpointName, funct, { disableCloning = false, destroy = true } = {}) {
        this.manageEntry()
        const dataCheckpoint = this.events.filter(funct)
        this._checkpoint('create', checkpointName, dataCheckpoint, { disableCloning })
        if (destroy) this.events = this.events.filter(event => !funct(event))
        return this.manageExit()
    }
}
