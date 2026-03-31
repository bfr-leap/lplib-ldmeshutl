/**
 * Delay configuration for content scheduling.
 *
 * Allows content types to declare a delay before becoming eligible to run,
 * with optional per-league overrides.
 */

export interface ContentSchedule {
    /** Delay string, e.g. "0d", "1d", "2d", "7d". */
    delay: string;
}

export interface DelayConfig {
    /** Default schedules keyed by content type name. */
    contentSchedules: Record<string, ContentSchedule>;

    /** Per-league overrides keyed by league ID, then content type. */
    leagueOverrides?: Record<string, Record<string, ContentSchedule>>;
}

const DELAY_PATTERN = /^(\d+)(ms|s|m|h|d)$/;

const UNIT_TO_MS: Record<string, number> = {
    ms: 1,
    s: 1_000,
    m: 60_000,
    h: 3_600_000,
    d: 86_400_000,
};

/**
 * Parse a delay string like "2d", "12h", "30m", "0d" into milliseconds.
 * Returns 0 for unrecognized formats.
 */
export function parseDelay(delay: string): number {
    const match = delay.match(DELAY_PATTERN);
    if (!match) {
        return 0;
    }
    const value = parseInt(match[1], 10);
    const unit = match[2];
    return value * UNIT_TO_MS[unit];
}

/**
 * Resolve the effective delay for a content type, considering league overrides.
 *
 * Resolution order:
 *   1. League-specific override (if leagueId provided and override exists)
 *   2. Default content schedule
 *   3. Zero delay (if content type not found in config)
 */
export function resolveDelay(
    config: DelayConfig,
    contentType: string,
    leagueId?: string | null
): number {
    // Check league override first
    if (leagueId && config.leagueOverrides) {
        const leagueConfig = config.leagueOverrides[leagueId];
        if (leagueConfig && leagueConfig[contentType]) {
            return parseDelay(leagueConfig[contentType].delay);
        }
    }

    // Fall back to default content schedule
    const defaultSchedule = config.contentSchedules[contentType];
    if (defaultSchedule) {
        return parseDelay(defaultSchedule.delay);
    }

    // No config found – immediate execution
    return 0;
}

/**
 * Compute the earliest run time for a job given a source event timestamp
 * and delay configuration.
 *
 * Returns `null` when delay is zero (immediate execution).
 */
export function computeEarliestRunTime(
    sourceEventTime: number,
    config: DelayConfig,
    contentType: string,
    leagueId?: string | null
): number | null {
    const delayMs = resolveDelay(config, contentType, leagueId);
    if (delayMs === 0) {
        return null;
    }
    return sourceEventTime + delayMs;
}
