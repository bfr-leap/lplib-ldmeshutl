import { Kafka, LogEntry, Consumer, EachMessagePayload } from 'kafkajs';
import Ajv from 'ajv';
import addFormats from 'ajv-formats';

const ajv = new Ajv();
addFormats(ajv);

export interface LdataUpdateLogEntry {
    dataset_id: string;
    source: string;
    timestamp: number;
    update_type: string;
    affected_fields: string[];
    change_summary: string;
}

const ldataUpdateLogEntrySchema = {
    $schema: 'http://json-schema.org/draft-07/schema#',
    type: 'object',
    properties: {
        dataset_id: {
            type: 'string',
        },
        source: {
            type: 'string',
        },
        timestamp: {
            type: 'number',
        },
        update_type: {
            type: 'string',
        },
        affected_fields: {
            type: 'array',
            items: {
                type: 'string',
            },
        },
        change_summary: {
            type: 'string',
        },
    },
    required: [
        'dataset_id',
        'source',
        'timestamp',
        'update_type',
        'affected_fields',
        'change_summary',
    ],
    additionalProperties: false,
};

export class LdataUpdateLogClient {
    private _sourceNames: string[];
    private _callback: (entry: LdataUpdateLogEntry) => void;
    private _kafka: Kafka | null = null;
    private _consumer: Consumer | null = null;
    private _msgValidator = ajv.compile(ldataUpdateLogEntrySchema);
    private _clientName: string = '';

    constructor(
        clientName: string,
        sourceNames: string[],
        callback: (entry: LdataUpdateLogEntry) => void
    ) {
        this._clientName = clientName;
        this._sourceNames = JSON.parse(JSON.stringify(sourceNames));
        this._sourceNames.push(`${clientName}:exrq`);
        this._callback = callback;
    }

    async onMessage(payload: EachMessagePayload) {
        let message = payload.message;
        let msg = null;

        try {
            msg = JSON.parse(message?.value?.toString() || '{}');
        } catch (e) {
            console.log(`Error parsing incoming kafka message: ${e}`);
            return;
        }

        if (!this._msgValidator(msg)) {
            console.log(`Invalid message: ${this._msgValidator.errors}`);
            return;
        }

        const lmsg: LdataUpdateLogEntry = <LdataUpdateLogEntry>(<any>msg);

        if (-1 === this._sourceNames.indexOf(lmsg.dataset_id)) {
            console.log(
                `Ignoring message; [${lmsg.dataset_id}] not found in [${this._sourceNames}]`
            );
            return;
        }

        this._callback(lmsg);
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
            brokers: ['leap-relay1:9092'],
            logCreator: this.noLogging, // comment out this line to re-enable logging
        });

        // Create a consumer
        this._consumer = this._kafka.consumer({
            groupId: this._clientName,
        });

        // Connect the consumer
        await this._consumer.connect();

        // Subscribe to the topic
        await this._consumer.subscribe({
            topic: 'ldata-update-log',
            fromBeginning: true,
        });

        // Handle each incoming message
        await this._consumer.run({
            eachMessage: this.onMessage.bind(this),
        });
    }

    public async stop() {
        if (this._consumer) {
            await this._consumer.disconnect();
        }
    }
}
