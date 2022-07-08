const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['kafka1:9092', 'kafka2:9092']
})
const producer = kafka.producer()



export const sendToKafka = async (payload) => {
    const response = await producer.send(payload);
    console.log(response);
}