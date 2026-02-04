const { parentPort } = require('worker_threads');
const baseUrl = process.env.BACKEND_URL ?? "http://localhost:4000"

const createTransaction = async () => {
    try {
        await fetch(`${baseUrl}/transaction-generation/generate`, {
            method: "POST"
        })
        parentPort.postMessage({ status: "created" })
    }
    catch(error) {
        console.error(`Error creating transaction: ${error}`)
        parentPort.postMessage({ status: 'error' })
    }
}

parentPort.on("message", (data) => {
    const { start, stop, rate } = data;
    let task = null
    if(start) {
        task = setInterval(async () => await createTransaction(), (1000/rate))
        parentPort.postMessage({ status: "running" });
    }
    if(stop) {
        if(task) clearInterval(task)
        parentPort.postMessage({ status: "stopped" });
    }
})
