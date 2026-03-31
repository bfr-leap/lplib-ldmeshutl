/**
 * Reads a schedule file containing ISO-8601 timestamps, one per line.
 * Returns the parsed timestamps as epoch milliseconds, sorted ascending.
 *
 * Blank lines and unparseable lines are silently skipped.
 * Returns an empty array when the file does not exist.
 */

import * as fs from 'fs';

const SCHEDULE_FILE = './content-schedule';

export function loadScheduleTimes(path: string = SCHEDULE_FILE): number[] {
    let raw: string;
    try {
        raw = fs.readFileSync(path, 'utf-8');
    } catch {
        return [];
    }

    const times: number[] = [];
    for (const line of raw.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const ms = Date.parse(trimmed);
        if (!isNaN(ms)) {
            times.push(ms);
        }
    }

    times.sort((a, b) => a - b);
    return times;
}

/**
 * Returns the nearest future timestamp from the list, or `null` if
 * every entry is in the past (or the list is empty).
 */
export function getNextScheduledTime(
    times: number[],
    now: number = Date.now()
): number | null {
    for (const t of times) {
        if (t > now) return t;
    }
    return null;
}
