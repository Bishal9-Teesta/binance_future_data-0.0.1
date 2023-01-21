// ! Config for Accessing Environment Variables from .env
require('dotenv').config()

const createSocketConnection = require("./binanceSocket")
const createTopic = require("./kafka_topic")

const main = async () => {
    // ! Creating Kafka Topic
    await createTopic()
    
    // ! Creating Future Depth Socket
    createSocketConnection()
}

main()
