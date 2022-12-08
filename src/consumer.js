'use strict';

const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'kafka-app',
    brokers: ['localhost:9092'],
});

consume();

async function consume() {
    try {
        const consumer = kafka.consumer({ groupId: 'my-group' });
        await consumer.connect();
        await consumer.subscribe({ topics: ['users'] });

        await consumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
                console.log({
                    ...(message.key && { key: message.key.toString() }),
                    value: message.value.toString(),
                    headers: message.headers,
                })
            },
        });
    } catch (err) {
        console.error(err);
    }
}
