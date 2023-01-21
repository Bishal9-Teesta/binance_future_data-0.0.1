const { Kafka, logLevel } = require('kafkajs')
const { v4: uuidv4 } = require('uuid')

const topic = process.env.TOPIC

const kafka = new Kafka({
    clientId: 'binance_future_depth',
    // logLevel: logLevel.ERROR,
    brokers: [`localhost:9092`]
})

const consumer = kafka.consumer({ groupId: uuidv4() })
const consume = async () => {

    await consumer.connect()
    await consumer.subscribe({ topic: "Future_Depth", fromBeginning: true })
    await consumer.run({
        eachMessage: async ({ message }) => {
            const consumeTime = Date.now()
            const produceTime = Number(message.timestamp.toString())
            console.log("\nConsume Time: ", consumeTime)
            console.log("Produce Time: ", produceTime)
            console.log("Time Difference: ", consumeTime - produceTime)
            // console.log("Data: ", JSON.parse(message.value.toString()))
        },
    })
}

consume()
