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

    const client = new LdataUpdateLogClient(
        clientName,
        dependencyDatasets,
        (entry: LdataUpdateLogEntry) => {
            console.log(
                `Data Update for ${entry.dataset_id} on ${entry.timestamp}`
            );
            if (entry.timestamp > outputDate) {
                if (timer) {
                    console.log(`Clearing old timer`);
                    clearTimeout(timer);
                }

                console.log(`Starting new timer ${interval}ms`);
                timer = setTimeout(() => {
                    console.log(`Ready to update ${targetDataset}`);
                    client.stop();
                }, interval);
            }
        }
    );

    console.log(`${targetDataset} last modified: ${outputDate}`);
    console.log(`Waiting for ${dependencyDatasets} to be updated...`);
    await client.run();
}

export function popcornAwait(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[]
) {
    run(clientName, targetDataset, dependencyDatasets).catch(console.error);
}
