const express = require("express");
const cors = require("cors");
const http = require("http");
const { Worker } = require("worker_threads");

const app = express();
app.use(express.json());
app.use(cors({origin: "*"}));
const server = http.createServer(app);

const workers = []
const baseUrl = process.env.BACKEND_URL ?? "http://localhost:4000"

let maxRate = 200
let currentRate = 0
let running = false
let total = 0
let errors = 0
let startTime = null

const startWorkers = async (rate, start) => {
    startTime = start
    running = true
    total = 0
    errors = 0
    currentRate = rate
    
    const numWorkers = Math.ceil(rate / 100)
    const remainder = rate % 100
    
    for(let i = 0; i < numWorkers; i++) {
        workers.push(new Worker('./worker.js'))
        workers[i]?.on("message", listenToWorker)
        if(remainder && i === numWorkers - 1) {
            workers[i].postMessage({ start: true, rate: remainder })
        }
        else workers[i].postMessage({ start: true, rate: 100 })
    }
}

const stopWorkers = async () => {
    try {
        while(workers.length > 0) {
            workers[0]?.terminate();
            workers.shift();
        }
        running = false
        total = 0
        errors = 0
        return
    }
    catch (e) {
        console.error(`Error stopping workers: {e}`)
        return
    }
}

const listenToWorker = async (data) => {
    const { status } = data
    if(status === 'created') total++
    if(status === 'error') {
        errors++
        if(errors > 100) {
            await stopWorkers()
        }
    }
}

app.post("/generate/start", async (req, res) => {
    if(running) {
        res.send({ error: "Generation already running" })
        return
    }
    
    const { rate, start } = req.body
    if(rate > maxRate) {
        res.send({ error: "Rate is greater than maximum rate" })
        return
    }
    
    try {
        const response  = await fetch(`${baseUrl}/performance/reset`, { method: 'POST' })
        if(response.ok) {
            await startWorkers(rate, start)
            res.send({ status: "running" })
            return
        }
        else throw new Error(`Error reseting performance monitor: ${response.statusText}`)
    }
    catch(e) {
        console.error(e.message)
        res.send({ error: e.message })
    }
})

app.post("/generate/stop", async (_, res) => {
    if(!running) {
        res.send({ error: "Generation not running" })
        return
    }
    stopWorkers()
    .then(() => {
        res.send({ status: "stopped" })
    })
    .catch(error => res.send({ error }))
})

app.get("/generate/status", async (_, res) => {
    res.send({
        running,
        total,
        startTime,
        currentRate,
        maxRate,
        errors
    })
})

server.listen(4001, () => console.log("Listening on port 4001"))