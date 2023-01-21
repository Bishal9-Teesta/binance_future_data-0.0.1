const WebSocket = require('ws')
const { Kafka, logLevel } = require('kafkajs')

const topic = process.env.TOPIC

const MyPartitioner = () => {
    return ({ topic, partitionMetadata, message }) => {
        // select a partition based on some logic
        // return the partition number
        return 0
    }
}

const producer = new Kafka({
    clientId: 'binance_future_depth',
    brokers: ['localhost:9092'],
    // logLevel: logLevel.ERROR
}).producer({
    allowAutoTopicCreation: false,
    // transactionTimeout: 30000,
    createPartitioner: MyPartitioner
})

const createSocketConnection = async () => {

    await producer.connect()

    // setInterval(async () => {
    //     await producer.send({
    //         topic: topic,
    //         // acks: 1,
    //         messages: [{
    //             // key: Date.now(),
    //
    //             // partition: 1,
    //             value: Date.now().toString(),
    //
    //         }],
    //     })
    // }, 100)

    const pairList = process.env.PAIR_LIST.split(",")
    let socketUrl = "wss://fstream.binance.com/stream?streams="

    // ! Setting URL 
    for (const pair in pairList) {
        socketUrl = socketUrl + pairList[pair].toLowerCase() + "@depth@100ms/";
    }
    socketUrl = socketUrl.substring(0, socketUrl.length - 1);
    // console.log("URL: ", socketUrl)

    // ! Future Socket connection instance
    const futureSocket = new WebSocket(socketUrl)

    // ! Future Socket opening connection
    futureSocket.on("open", () => {
        console.info("Futures Socket connected!");
    });

    // ! Future Socket received message
    futureSocket.on("message", async (raw) => {
        const bufferData = Buffer.from(raw);
        const data = bufferData.toString();


        await producer.send({
            topic: topic,
            messages: [{
                partition: 0,
                value: data,
            }],
        })

        // console.clear()
        // console.info(JSON.parse(data), "\n")
    })

    // ! Future Socket received unexpected response
    futureSocket.on("unexpected-response", (_, request, response) => {
        console.info("Future Socket has received some unexpected response!")
        console.log("Unexpected Response: ", response)
    })

    // ! Future Socket connection close
    futureSocket.on("close", (code, reason) => {
        socketConnectionStatus.future = futureSocket.CLOSED;
        console.info("Future Socket has been closed!");
        console.info("Future Socket Close Code: ", code);

        const bufferData = Buffer.from(reason);
        console.info("Reason: ", bufferData.toString());

        // ! Reconnecting Web Socket
        futureSocket.on("open", () => {
            console.info("Futures Socket reconnected!");
        })
    });

    // ! Future Socket error connection
    futureSocket.on("error", (error) => {
        socketConnectionStatus.future = futureSocket.CLOSED;
        console.info("Future Socket Error: ", error?.name);
        console.info(error);

        // ! Close connection on error
        futureSocket.close();
    });
}

module.exports = createSocketConnection
