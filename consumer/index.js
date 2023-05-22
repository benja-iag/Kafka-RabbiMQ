// This in the future will be make it in go for make "n" consumers in parallel
import { Kafka } from "kafkajs";
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

await sleep(25000)
const kafka = new Kafka({
    clientId: "1",
    brokers: ["kafka:9092"]
})
const consumer = kafka.consumer({ groupId: "test-group" });
await consumer.connect();
await consumer.subscribe({topic: "test-topic", fromBeginning: true})
await consumer.run({
    eachMessage: async ({topic, partition, message}) => {
        const value = JSON.parse(message.value.toString())
        console.log(value)

    }
})