import { Kafka } from 'kafkajs';
import {
    getModifiedDate,
    getModifiedRootFolders,
} from './ldata-submodule-util';

process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

async function run(targetDataset: string, producerName: string) {
    const messageData = {
        dataset_id: targetDataset,
        source: producerName,
        timestamp: getModifiedDate(targetDataset),
        update_type: 'modification',
        affected_fields: getModifiedRootFolders(targetDataset),
        change_summary: `Script ran at: ${new Date().toString()}`,
    };

    // Create a new Kafka instance
    const kafka = new Kafka({
        clientId: producerName,
        brokers: ['leap-relay1:9092'],
    });

    // Create a producer
    const producer = kafka.producer();

    // Connect the producer
    await producer.connect();

    // Send a message
    await producer.send({
        topic: 'ldata-update-log',
        messages: [{ value: JSON.stringify(messageData) }],
    });

    // Disconnect the producer
    await producer.disconnect();
}

export function sendNotification(targetDataset: string, producerName: string) {
    run(targetDataset, producerName).catch(console.error);
}
