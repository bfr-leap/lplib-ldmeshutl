import { Kafka, LogEntry, Consumer, EachMessagePayload } from 'kafkajs';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';

const TOPIC = 'kmsvc-reqres';
const BROKERS = ['leap-relay1:9092']

const ajv = new Ajv();
addFormats(ajv);


const svcResponseSchema = {
    $schema: 'http://json-schema.org/draft-07/schema#',
    type: 'object',
    properties: {
        target: {
            type: 'string',
        },
        source: {
            type: 'string',
        },
        timestamp: {
            type: 'number',
        },
        key: {
            type: 'string',
        },
        response: {

        }
    },
    required: [
        'target',
        'source',
        'timestamp',
        'key',
        'response'
    ],
    additionalProperties: true,
};

export interface KSvcResponse {
    target: string;
    source: string;
    timestamp: number;
    key: string;
    response: any;
}

export interface KSvcRequest {
    target: string;
    source: string;
    timestamp: number;
    key: string;
    request: any;
}

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export class KSvcConsumer {

    private _onResponse: (entry: KSvcResponse) => void;
    private _onConnect: () => void;
    private _clientName: string = '';
    private _key: string = '';

    private _done: boolean = false;

    private _kafka: Kafka | null = null;
    private _consumer: Consumer | null = null;
    private _msgValidator = ajv.compile(svcResponseSchema);

    constructor(
        clientName: string,
        key: string,
        onConnect: () => void,
        onResponse: (entry: KSvcResponse) => void
    ) {
        this._clientName = clientName;
        this._key = key;
        this._onResponse = onResponse;
        this._onConnect = onConnect
    }

    async onMessage(payload: EachMessagePayload) {
        let message = payload.message;
        let msg = null;

        try {
            msg = JSON.parse(message?.value?.toString() || '{}');
        } catch (e) {
            return;
        }

        if (!this._msgValidator(msg)) {
            return;
        }

        const lmsg: KSvcResponse = <KSvcResponse>(<any>msg);

        if (lmsg.target === this._clientName && lmsg.key === this._key) {
            this._onResponse(lmsg);
        }
    }

    noLogging(): (entry: LogEntry) => void {
        return (entry: LogEntry) => {
            // No-op function for logging
        };
    }

    public async run() {
        // Create a new Kafka instance
        this._kafka = new Kafka({
            clientId: this._clientName,
            brokers: BROKERS,
            logCreator: this.noLogging, // comment out this line to re-enable logging
        });

        // Create a consumer
        this._consumer = this._kafka.consumer({
            groupId: this._clientName + "_" + Date.now(),
        });

        // Connect the consumer
        await this._consumer.connect();

        // Subscribe to the topic
        await this._consumer.subscribe({
            topic: TOPIC,
            fromBeginning: false,
        });

        // Handle each incoming message
        await this._consumer.run({
            eachMessage: this.onMessage.bind(this),
        });

        await this._onConnect();

        let delay = 1;

        while (!this._done) {
            await sleep(delay);
            delay = Math.min(delay * 2, 1000);
        }
    }

    public async stop() {
        if (this._consumer) {
            await this._consumer.disconnect();
        }

        this._done = true;
    }
}

process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

async function sendRequest(messageData: KSvcRequest) {
    // Create a new Kafka instance
    const kafka = new Kafka({
        clientId: messageData.source,
        brokers: BROKERS,
    });

    // Create a producer
    const producer = kafka.producer();

    // Connect the producer
    await producer.connect();

    // Send a message
    await producer.send({
        topic: TOPIC,
        messages: [{ value: JSON.stringify(messageData) }],
    });

    // Disconnect the producer
    await producer.disconnect();
}


export async function kSvcRequest(req: KSvcRequest): Promise<any> {

    let resp: any = null;

    let consumer = new KSvcConsumer(
        req.source,
        req.key,
        async () => {
            await sendRequest(req);
        },
        (response: KSvcResponse) => {
            resp = response;
            consumer.stop();
        }
    )

    await consumer.run();

    return resp;
}
