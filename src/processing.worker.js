const { parentPort, workerData } = require('worker_threads');
const { Vaporous } = require('../Vaporous');

// Helper function to deserialize functions from strings
function deserializeFunction(serializedFunc) {
    if (serializedFunc && serializedFunc.__isSerialized) {
        // Reconstruct the function from its string representation
        return eval(`(${serializedFunc.funcString})`);
    }
    return serializedFunc;
}

// Listen for messages from the main thread
parentPort.on('message', async (message) => {
    try {
        const { processingQueue, events } = message;

        console.log("RECEIVING", events[0].username)

        // Create a new Vaporous instance
        const instance = new Vaporous();
        instance.processingQueue = processingQueue
        instance.events = events;

        await instance.begin()

        console.log("EMITTING", events[0].username)
        // Return the processed result to the main thread
        parentPort.postMessage(instance.serialise());
    } catch (error) {
        // Return error to the main thread
        parentPort.postMessage({
            success: false,
            error: {
                message: error.message,
                stack: error.stack
            }
        });
    }
});
