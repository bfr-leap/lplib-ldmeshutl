import { sendNotification } from './ldata-update-log-producer';
import {
    LdataUpdateLogClient,
    LdataUpdateLogEntry,
} from './ldata-update-log-client';
import { getModifiedDate } from './ldata-submodule-util';

async function run(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[]
) {
    let timer: NodeJS.Timeout | null = null;
    const interval = 1000 * 5;

    let outputDate = getModifiedDate(targetDataset);
    let done = false;

    const client = new LdataUpdateLogClient(
        clientName,
        dependencyDatasets,
        (entry: LdataUpdateLogEntry) => {
            console.log(
                `Data Update for ${entry.dataset_id} on ${entry.timestamp}`
            );
            if (
                entry.dataset_id === `${clientName}:exrq` ||
                entry.timestamp > outputDate
            ) {
                if (timer) {
                    console.log(`Clearing old timer`);
                    clearTimeout(timer);
                }

                console.log(`Starting new timer ${interval}ms`);
                timer = setTimeout(() => {
                    console.log(`Ready to update ${targetDataset}`);
                    client.stop();

                    sendNotification(`${clientName}:exst`, clientName);
                    done = true;
                }, interval);
            }
        }
    );

    console.log(`${targetDataset} last modified: ${outputDate}`);
    console.log(`Waiting for ${dependencyDatasets} to be updated...`);
    await client.run();

    while (!done) {
        await delay(1000);
    }
}

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export function popcornAwait(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[]
) {
    run(clientName, targetDataset, dependencyDatasets).catch(console.error);
}

export async function popcornAwaitAsync(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[]
) {
    await run(clientName, targetDataset, dependencyDatasets);
}

