import { sendNotification } from './ldata-update-log-producer';
import {
    LdataUpdateLogClient,
    LdataUpdateLogEntry,
} from './ldata-update-log-client';
import { getModifiedDate } from './ldata-submodule-util';
import {
    ReadinessCondition,
    ReadinessState,
    evaluateReadiness,
    buildLegacyCondition,
    buildTimedCondition,
} from './readiness';
import { loadScheduleFile, computeEarliestRunTime } from './delay-config';

type WakeReason = 'kafka_event' | 'debounce_timeout' | 'time_wait' | 'startup';

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function logReadiness(
    clientName: string,
    targetDataset: string,
    condition: ReadinessCondition,
    satisfiedContexts: Set<string>,
    reason: WakeReason,
    dependenciesSatisfied: boolean,
    timeConditionSatisfied: boolean,
    msUntilTime: number
) {
    const depStatus = dependenciesSatisfied ? 'YES' : 'NO';
    const timeStatus = timeConditionSatisfied ? 'YES' : 'NO';
    const pendingDeps = condition.requiredDataContexts.filter(
        (d) => !satisfiedContexts.has(d)
    );
    const earliestStr =
        condition.earliestRunTime !== null
            ? new Date(condition.earliestRunTime).toISOString()
            : 'none (immediate)';

    console.log(
        [
            `[${clientName}] readiness check`,
            `  target=${targetDataset}`,
            `  wake_reason=${reason}`,
            `  dependencies_satisfied=${depStatus}`,
            `  time_condition_satisfied=${timeStatus}`,
            `  earliest_run_time=${earliestStr}`,
            `  ms_until_time_condition=${msUntilTime}`,
            pendingDeps.length > 0
                ? `  pending_deps=[${pendingDeps.join(', ')}]`
                : `  pending_deps=[]`,
        ].join('\n')
    );
}

// ---------------------------------------------------------------------------
// Core loop
// ---------------------------------------------------------------------------

const DEBOUNCE_MS = 5_000;
const MAX_POLL_INTERVAL_MS = 30_000;

async function run(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[]
) {
    // Try to load a schedule file.  If it exists and has an entry for this
    // clientName, build a timed readiness condition.  Otherwise fall back to
    // the legacy dependency-only condition (zero integration cost for repos
    // that don't use delayed scheduling).
    const schedule = loadScheduleFile();

    const outputDate = getModifiedDate(targetDataset);

    // Mutable state --------------------------------------------------------
    const satisfiedContexts = new Set<string>();
    let debounceTimer: NodeJS.Timeout | null = null;
    let dependencyDebounceSettled = false;
    let done = false;

    // We track the latest dependency timestamp so we can compute
    // earliest_run_time once the debounce settles.
    let latestDependencyTimestamp = 0;

    // The readiness condition starts as dependency-only.  Once the debounce
    // settles we may upgrade it to include a time gate (if the schedule file
    // dictates a delay for this clientName).
    let condition: ReadinessCondition =
        buildLegacyCondition(dependencyDatasets);

    // Helpers --------------------------------------------------------------

    function tryFinish(reason: WakeReason): boolean {
        const state: ReadinessState = {
            satisfiedContexts,
            currentTime: Date.now(),
        };
        const result = evaluateReadiness(condition, state);

        logReadiness(
            clientName,
            targetDataset,
            condition,
            satisfiedContexts,
            reason,
            result.dependenciesSatisfied,
            result.timeConditionSatisfied,
            result.msUntilTimeCondition
        );

        if (result.ready) {
            console.log(`[${clientName}] Ready to update ${targetDataset}`);
            return true;
        }
        return false;
    }

    // Kafka message handler -----------------------------------------------
    const client = new LdataUpdateLogClient(
        clientName,
        dependencyDatasets,
        (entry: LdataUpdateLogEntry) => {
            console.log(
                `[${clientName}] Data Update for ${entry.dataset_id} on ${entry.timestamp}`
            );

            const isExplicitRequest = entry.dataset_id === `${clientName}:exrq`;

            if (isExplicitRequest || entry.timestamp > outputDate) {
                satisfiedContexts.add(entry.dataset_id);

                if (entry.timestamp > latestDependencyTimestamp) {
                    latestDependencyTimestamp = entry.timestamp;
                }

                if (debounceTimer) {
                    console.log(`[${clientName}] Clearing debounce timer`);
                    clearTimeout(debounceTimer);
                }

                console.log(
                    `[${clientName}] Starting debounce timer ${DEBOUNCE_MS}ms`
                );
                debounceTimer = setTimeout(() => {
                    dependencyDebounceSettled = true;

                    // Now that dependencies have settled, compute the time
                    // gate from the schedule file (if applicable).
                    if (schedule) {
                        const leagueId = schedule.league ?? null;
                        const earliest = computeEarliestRunTime(
                            latestDependencyTimestamp,
                            schedule,
                            clientName,
                            leagueId
                        );
                        if (earliest !== null) {
                            condition = buildTimedCondition(
                                dependencyDatasets,
                                earliest
                            );
                            console.log(
                                `[${clientName}] Schedule file applied: earliest_run_time=${new Date(
                                    earliest
                                ).toISOString()}`
                            );
                        }
                    }

                    // Fast path: if time condition is already met, finish now.
                    if (tryFinish('debounce_timeout')) {
                        client.stop();
                        sendNotification(`${clientName}:exst`, clientName);
                        done = true;
                    }
                }, DEBOUNCE_MS);
            }
        }
    );

    console.log(
        `[${clientName}] ${targetDataset} last modified: ${outputDate}`
    );
    console.log(
        `[${clientName}] Waiting for dependencies [${dependencyDatasets}]...`
    );
    if (schedule) {
        console.log(
            `[${clientName}] Schedule file loaded; delay will be evaluated after dependencies settle`
        );
    }

    await client.run();

    // Main wait loop -------------------------------------------------------
    while (!done) {
        if (dependencyDebounceSettled && !done) {
            const state: ReadinessState = {
                satisfiedContexts,
                currentTime: Date.now(),
            };
            const result = evaluateReadiness(condition, state);

            if (result.ready) {
                logReadiness(
                    clientName,
                    targetDataset,
                    condition,
                    satisfiedContexts,
                    'time_wait',
                    result.dependenciesSatisfied,
                    result.timeConditionSatisfied,
                    result.msUntilTimeCondition
                );
                console.log(`[${clientName}] Ready to update ${targetDataset}`);
                client.stop();
                sendNotification(`${clientName}:exst`, clientName);
                done = true;
                break;
            }

            const sleepMs = Math.min(
                result.msUntilTimeCondition > 0
                    ? result.msUntilTimeCondition
                    : MAX_POLL_INTERVAL_MS,
                MAX_POLL_INTERVAL_MS
            );
            console.log(
                `[${clientName}] Time condition not met; sleeping ${sleepMs}ms`
            );
            await sleep(sleepMs);
        } else {
            await sleep(1_000);
        }
    }
}

// ---------------------------------------------------------------------------
// Public API – signatures unchanged from original
// ---------------------------------------------------------------------------

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
