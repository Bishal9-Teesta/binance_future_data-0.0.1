const { Kafka, logLevel } = require('kafkajs')

const topic = process.env.TOPIC

const createTopic = async () => {
    try {
        const kafka = new Kafka({
            clientId: "binance_future_depth",
            brokers: ["localhost:9092"],
            retry: {
                initialRetryTime: 5000,
                restartOnFailure: true,
            },
            logLevel: logLevel.ERROR
        })

        const admin = kafka.admin()

        // ! Connecting to Kafka Server
        console.info("Connecting to Kafka Server...")
        await admin.connect()
        console.info("Kafka connected!")

        // ! Check if Topic already exist
        const topicExist = await admin.listTopics()
        if (topicExist.includes(topic)) {
            console.info("Topic already exist.")
        } else {
            // ! Creating Topic
            await admin.createTopics({
                topics: [
                    { topic: topic, numPartitions: 1 }
                ]
            })
            console.info("Topic is created successfully!")
        }


        // ! Disconnecting from Kafka Server
        await admin.disconnect()
        console.info("Kafka disconnected successfully!")
    } catch (error) {
        console.error("Kafka Topic creation Error: ", error?.message)
        // console.error(error, "\n\n")
    }
}

module.exports = createTopic
