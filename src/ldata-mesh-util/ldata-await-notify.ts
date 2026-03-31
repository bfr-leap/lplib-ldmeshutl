import { sendNotification } from './ldata-update-log-producer';
import {
    LdataUpdateLogClient,
    LdataUpdateLogEntry,
} from './ldata-update-log-client';
import { getModifiedDate } from './ldata-submodule-util';
import { loadScheduleTimes, getNextScheduledTime } from './schedule-file';

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function run(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[]
) {
    let timer: NodeJS.Timeout | null = null;
    const interval = 1000 * 5;

    let outputDate = getModifiedDate(targetDataset);
    let done = false;

    // Load schedule times.  If the file exists, the loop can also unblock
    // when the nearest future timestamp is reached – even without a
    // dependency message.
    const scheduleTimes = loadScheduleTimes();
    const nextTime = getNextScheduledTime(scheduleTimes);

    if (nextTime !== null) {
        const msUntil = nextTime - Date.now();
        console.log(
            `[${clientName}] Next scheduled time: ${new Date(
                nextTime
            ).toISOString()} (${msUntil}ms from now)`
        );

        // Set a timer that fires at the scheduled time.
        // If a dependency message arrives first the timer is cleared.
        const scheduleMs = Math.max(msUntil, 0);
        timer = setTimeout(() => {
            console.log(`[${clientName}] Scheduled time reached – unblocking`);
            client.stop();
            sendNotification(`${clientName}:exst`, clientName);
            done = true;
        }, scheduleMs);
    }

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
                // Cancel a pending schedule timer – dependency wins.
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
        await sleep(1000);
    }
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
