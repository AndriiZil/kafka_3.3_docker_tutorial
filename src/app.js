'use strict';

const express = require('express');
const logger = require('morgan');
const app = express();
const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'kafka-app',
    brokers: ['localhost:9092'],
});

app.use(express.json());
app.use(logger('dev'));

app.get('/topics', async (req, res) => {
    try {
        const admin = kafka.admin();
        await admin.connect();

        const data = await admin.listTopics();
        await admin.disconnect();

        res.send({ message: 'success', data });
    } catch (err) {
        console.error(err);
    }
});

app.post('/create-topic', async (req, res) => {
    try {
        const { topic, numPartitions = 1, replicationFactor = 1 } = req.body;

        const admin = kafka.admin();
        await admin.connect();

        await admin.createTopics({
            topics: [
                { topic, numPartitions, replicationFactor },
            ]
        });

        await admin.disconnect();

        res.send({ message: 'success' });
    } catch (err) {
        console.error(err);
    }
});

app.post('/message', async (req, res) => {
    try {
        const { message, topic } = req.body;

        const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

        await producer.connect();
        await producer.send({
            topic,
            messages: [
                // { key: 'message', value: message },
                { value: message, partition: 0 },
                { value: 'hey hey!', partition: 1 },
                // { key: 'message', value: message },
                // { key: 'test', value: 'hey hey!' },
                // { value: message },
            ],
        })

        res.send({ message: 'success' });
    } catch (err) {
        console.error(err);
    }
});

app.post('/metadata', async (req, res) => {
    try {
        const { topic } = req.body;

        const admin = kafka.admin();
        await admin.connect();

        const [offsets, metadata] = await Promise.all([
            admin.fetchTopicOffsets(topic),
            admin.fetchTopicMetadata({ topics: [topic] })
        ]);

        await admin.disconnect();

        res.send({ message: 'success', result: { offsets, metadata }});
    } catch (err) {
        console.error(err);
    }
})

app.delete('/delete-topic', async (req, res) => {
    try {
        const { topic } = req.body;

        const admin = kafka.admin();
        await admin.connect();

        await admin.deleteTopics({
            topics: [topic],
        });

        await admin.disconnect();

        res.send({ message: 'success' });
    } catch (err) {
        console.error(err);
    }
});

app.listen(3000, () => console.info(`Start listening PORT: 3000`));
