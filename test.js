const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092'],
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'test-group' })

const run = async () => {
    // Producing
    await producer.connect()
    setInterval(async () => {
        await producer.send({
            topic: 'test-topic',
            messages: [
                { value: Date.now().toString(), partition: 0 },
            ],
        })
    }, 3000)

    // Consuming
    await consumer.connect()
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

    await consumer.run({
        eachMessage: async ({ _, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                timestamp: Date.now(),
                value: message.value.toString(),
            })
            console.log("Time Difference: ", Number(Date.now()) - Number(message.value.toString()))
        },
    })
}

run().catch(console.error)
