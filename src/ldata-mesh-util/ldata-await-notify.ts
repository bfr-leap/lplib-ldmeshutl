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
} from './readiness';

/** Options for the generalized await loop. */
export interface AwaitOptions {
    /**
     * Readiness condition. When omitted the loop falls back to legacy
     * behavior (dependency-only, no time gate).
     */
    condition?: ReadinessCondition;

    /**
     * Debounce interval in ms applied after the last dependency message
     * before declaring dependencies satisfied.  Defaults to 5 000 ms.
     */
    debounceMs?: number;

    /**
     * Maximum time in ms to sleep between readiness re-evaluations when
     * waiting for a time condition.  Keeps the loop from busy-polling while
     * still waking reasonably close to the target time.  Defaults to 30 000 ms.
     */
    maxPollIntervalMs?: number;
}

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
// Core loop – supports both dependency-only and dependency+time readiness
// ---------------------------------------------------------------------------

async function run(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[],
    options: AwaitOptions = {}
) {
    const debounceMs = options.debounceMs ?? 5_000;
    const maxPollIntervalMs = options.maxPollIntervalMs ?? 30_000;

    // Build condition: either caller-supplied or legacy dependency-only.
    const condition: ReadinessCondition =
        options.condition ?? buildLegacyCondition(dependencyDatasets);

    const outputDate = getModifiedDate(targetDataset);

    // Mutable state --------------------------------------------------------
    const satisfiedContexts = new Set<string>();
    let debounceTimer: NodeJS.Timeout | null = null;
    let dependencyDebounceSettled = false; // true once debounce fires
    let done = false;

    // Helpers --------------------------------------------------------------

    /** Evaluate and optionally finish. Returns true when ready. */
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
                // Mark this dependency as satisfied.
                satisfiedContexts.add(entry.dataset_id);

                // Reset debounce timer – we wait for a quiet period before
                // considering dependencies settled.
                if (debounceTimer) {
                    console.log(`[${clientName}] Clearing debounce timer`);
                    clearTimeout(debounceTimer);
                }

                console.log(
                    `[${clientName}] Starting debounce timer ${debounceMs}ms`
                );
                debounceTimer = setTimeout(() => {
                    dependencyDebounceSettled = true;

                    // If time condition is already met we can finish right
                    // inside the debounce callback (fast path).
                    if (tryFinish('debounce_timeout')) {
                        client.stop();
                        sendNotification(`${clientName}:exst`, clientName);
                        done = true;
                    }
                }, debounceMs);
            }
        }
    );

    console.log(
        `[${clientName}] ${targetDataset} last modified: ${outputDate}`
    );
    console.log(
        `[${clientName}] Waiting for dependencies [${dependencyDatasets}]...`
    );
    if (condition.earliestRunTime !== null) {
        console.log(
            `[${clientName}] Earliest run time: ${new Date(
                condition.earliestRunTime
            ).toISOString()}`
        );
    }

    // Start consuming Kafka messages (non-blocking after setup).
    await client.run();

    // Main wait loop – wakes on debounce settling or time boundary ---------
    while (!done) {
        // If dependencies have debounce-settled but time hasn't arrived yet,
        // sleep until the time boundary (bounded by maxPollIntervalMs).
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

            // Sleep until the next meaningful wake time.
            const sleepMs = Math.min(
                result.msUntilTimeCondition > 0
                    ? result.msUntilTimeCondition
                    : maxPollIntervalMs,
                maxPollIntervalMs
            );
            console.log(
                `[${clientName}] Time condition not met; sleeping ${sleepMs}ms`
            );
            await sleep(sleepMs);
        } else {
            // Dependencies not yet settled – light polling.
            await sleep(1_000);
        }
    }
}

// ---------------------------------------------------------------------------
// Public API – backward-compatible signatures
// ---------------------------------------------------------------------------

/**
 * Legacy fire-and-forget await (dependency-only, no time gate).
 */
export function popcornAwait(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[]
) {
    run(clientName, targetDataset, dependencyDatasets).catch(console.error);
}

/**
 * Legacy async await (dependency-only, no time gate).
 */
export async function popcornAwaitAsync(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[]
) {
    await run(clientName, targetDataset, dependencyDatasets);
}

/**
 * Generalized fire-and-forget await supporting both dependency and time-based
 * readiness.
 */
export function popcornAwaitTimed(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[],
    options: AwaitOptions
) {
    run(clientName, targetDataset, dependencyDatasets, options).catch(
        console.error
    );
}

/**
 * Generalized async await supporting both dependency and time-based readiness.
 */
export async function popcornAwaitTimedAsync(
    clientName: string,
    targetDataset: string,
    dependencyDatasets: string[],
    options: AwaitOptions
) {
    await run(clientName, targetDataset, dependencyDatasets, options);
}
