/**
 * Generalized readiness model for the Kafka await loop.
 *
 * Readiness is the conjunction of:
 *   1. Dependency satisfaction – all required data contexts have been observed.
 *   2. Temporal condition    – current time >= earliest_run_time (if defined).
 *
 * When no earliest_run_time is set the behavior is identical to the legacy
 * dependency-only mode.
 */

export interface ReadinessCondition {
    /** Dataset IDs that must be observed before the job is eligible. */
    requiredDataContexts: string[];

    /**
     * Unix-epoch millisecond timestamp before which the job must not run.
     * `null` means there is no time gate (immediate execution).
     */
    earliestRunTime: number | null;
}

export interface ReadinessState {
    /** Set of dataset IDs that have been observed so far. */
    satisfiedContexts: Set<string>;

    /** Current wall-clock time in epoch milliseconds. */
    currentTime: number;
}

export interface ReadinessResult {
    /** True when all conditions are met and the job may execute. */
    ready: boolean;

    /** True when every required data context has been observed. */
    dependenciesSatisfied: boolean;

    /**
     * True when no time gate exists, or current time is at or past the
     * earliest run time.
     */
    timeConditionSatisfied: boolean;

    /**
     * Milliseconds until the time condition will be satisfied.
     * 0 when already satisfied or no time gate exists.
     */
    msUntilTimeCondition: number;
}

/**
 * Pure function – evaluates readiness from a condition and the current state.
 */
export function evaluateReadiness(
    condition: ReadinessCondition,
    state: ReadinessState
): ReadinessResult {
    const dependenciesSatisfied = condition.requiredDataContexts.every((ctx) =>
        state.satisfiedContexts.has(ctx)
    );

    let timeConditionSatisfied: boolean;
    let msUntilTimeCondition: number;

    if (condition.earliestRunTime === null) {
        timeConditionSatisfied = true;
        msUntilTimeCondition = 0;
    } else {
        timeConditionSatisfied = state.currentTime >= condition.earliestRunTime;
        msUntilTimeCondition = timeConditionSatisfied
            ? 0
            : condition.earliestRunTime - state.currentTime;
    }

    return {
        ready: dependenciesSatisfied && timeConditionSatisfied,
        dependenciesSatisfied,
        timeConditionSatisfied,
        msUntilTimeCondition,
    };
}

/**
 * Build a ReadinessCondition from the legacy parameters (dependency-only,
 * no time gate).  Preserves backward compatibility.
 */
export function buildLegacyCondition(
    dependencyDatasets: string[]
): ReadinessCondition {
    return {
        requiredDataContexts: [...dependencyDatasets],
        earliestRunTime: null,
    };
}

/**
 * Build a ReadinessCondition with both dependency and temporal constraints.
 */
export function buildTimedCondition(
    dependencyDatasets: string[],
    earliestRunTime: number
): ReadinessCondition {
    return {
        requiredDataContexts: [...dependencyDatasets],
        earliestRunTime,
    };
}
