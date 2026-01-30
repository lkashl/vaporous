const { parentPort, workerData } = require('worker_threads');
const { Vaporous } = require('../Vaporous');

// Listen for messages from the main thread
parentPort.on('message', async (message) => {
    try {
        let { callbackPath, events, loggers, workerId } = message;

        let funct = require(callbackPath[0])
        callbackPath.splice(0, 1)
        callbackPath.forEach(item => {
            funct = funct[item]
        })

        let instance = new Vaporous();
        instance.events = events;
        await funct(instance).begin('w', workerId)
        parentPort
            .postMessage(instance.serialise());
    } catch (error) {
        parentPort.postMessage(error)
    }
});
