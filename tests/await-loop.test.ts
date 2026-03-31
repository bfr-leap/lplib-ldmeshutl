/**
 * Integration-style tests for the await loop readiness evaluation.
 *
 * These tests exercise the readiness model in scenarios that mirror the
 * await loop's decision logic without requiring a live Kafka broker.
 * They validate the state machine transitions the loop would go through.
 */
import * as assert from 'assert';
import {
    evaluateReadiness,
    ReadinessCondition,
    ReadinessState,
} from '../src/ldata-mesh-util/readiness';
import {
    computeEarliestRunTime,
    DelayConfig,
} from '../src/ldata-mesh-util/delay-config';

let passed = 0;
let failed = 0;

function test(name: string, fn: () => void) {
    try {
        fn();
        passed++;
        console.log(`  ✓ ${name}`);
    } catch (e: any) {
        failed++;
        console.log(`  ✗ ${name}`);
        console.log(`    ${e.message}`);
    }
}

console.log('\n=== Await Loop Integration Tests ===\n');

// Simulate the config a real worker would use.
const config: DelayConfig = {
    contentSchedules: {
        race_summary: { delay: '0d' },
        driver_of_the_day: { delay: '2d' },
    },
    leagueOverrides: {
        league_fast: { driver_of_the_day: { delay: '1d' } },
    },
};

// ---------------------------------------------------------------------------
// Scenario: immediate job – runs as soon as dependencies arrive
// ---------------------------------------------------------------------------

test('immediate job: wakes and is ready on dependency satisfaction', () => {
    const deps = ['telemetry', 'results'];
    const earliest = computeEarliestRunTime(Date.now(), config, 'race_summary');
    assert.strictEqual(earliest, null); // zero delay

    const condition: ReadinessCondition = {
        requiredDataContexts: deps,
        earliestRunTime: earliest,
    };

    // Before any messages
    let state: ReadinessState = {
        satisfiedContexts: new Set(),
        currentTime: Date.now(),
    };
    assert.strictEqual(evaluateReadiness(condition, state).ready, false);

    // After first dependency
    state.satisfiedContexts.add('telemetry');
    assert.strictEqual(evaluateReadiness(condition, state).ready, false);

    // After all dependencies
    state.satisfiedContexts.add('results');
    assert.strictEqual(evaluateReadiness(condition, state).ready, true);
});

// ---------------------------------------------------------------------------
// Scenario: delayed job – does not run early
// ---------------------------------------------------------------------------

test('delayed job: not ready even when deps satisfied before time', () => {
    const sourceEventTime = Date.now();
    const deps = ['telemetry', 'results'];
    const earliest = computeEarliestRunTime(
        sourceEventTime,
        config,
        'driver_of_the_day'
    );
    assert.ok(earliest !== null);
    assert.ok(earliest > Date.now()); // 2 days from now

    const condition: ReadinessCondition = {
        requiredDataContexts: deps,
        earliestRunTime: earliest,
    };

    // Dependencies satisfied immediately
    const state: ReadinessState = {
        satisfiedContexts: new Set(['telemetry', 'results']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, false);
    assert.strictEqual(result.dependenciesSatisfied, true);
    assert.strictEqual(result.timeConditionSatisfied, false);
    assert.ok(result.msUntilTimeCondition > 0);
});

// ---------------------------------------------------------------------------
// Scenario: delayed job runs after scheduled time + deps
// ---------------------------------------------------------------------------

test('delayed job: ready once both deps and time are satisfied', () => {
    const sourceEventTime = Date.now() - 3 * 86_400_000; // 3 days ago
    const deps = ['telemetry', 'results'];
    const earliest = computeEarliestRunTime(
        sourceEventTime,
        config,
        'driver_of_the_day'
    );
    assert.ok(earliest !== null);
    // earliest = 3 days ago + 2 days delay = 1 day ago -> should be past
    assert.ok(earliest < Date.now());

    const condition: ReadinessCondition = {
        requiredDataContexts: deps,
        earliestRunTime: earliest,
    };

    const state: ReadinessState = {
        satisfiedContexts: new Set(['telemetry', 'results']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, true);
});

// ---------------------------------------------------------------------------
// Scenario: time passes but dependencies still missing
// ---------------------------------------------------------------------------

test('delayed job: time reached but deps missing -> not ready', () => {
    const sourceEventTime = Date.now() - 10 * 86_400_000; // 10 days ago
    const earliest = computeEarliestRunTime(
        sourceEventTime,
        config,
        'driver_of_the_day'
    );
    assert.ok(earliest !== null);
    assert.ok(earliest < Date.now()); // well past

    const condition: ReadinessCondition = {
        requiredDataContexts: ['telemetry', 'results'],
        earliestRunTime: earliest,
    };

    const state: ReadinessState = {
        satisfiedContexts: new Set(['telemetry']), // missing 'results'
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, false);
    assert.strictEqual(result.dependenciesSatisfied, false);
    assert.strictEqual(result.timeConditionSatisfied, true);
});

// ---------------------------------------------------------------------------
// Scenario: deps satisfied but time hasn't passed
// ---------------------------------------------------------------------------

test('delayed job: deps satisfied but time not passed -> not ready', () => {
    const sourceEventTime = Date.now(); // just happened
    const earliest = computeEarliestRunTime(
        sourceEventTime,
        config,
        'driver_of_the_day'
    );

    const condition: ReadinessCondition = {
        requiredDataContexts: ['telemetry'],
        earliestRunTime: earliest,
    };

    const state: ReadinessState = {
        satisfiedContexts: new Set(['telemetry']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, false);
    assert.strictEqual(result.dependenciesSatisfied, true);
    assert.strictEqual(result.timeConditionSatisfied, false);
});

// ---------------------------------------------------------------------------
// Scenario: multiple wake cycles are safe (idempotent evaluation)
// ---------------------------------------------------------------------------

test('multiple evaluations are idempotent', () => {
    const condition: ReadinessCondition = {
        requiredDataContexts: ['ds:a'],
        earliestRunTime: Date.now() + 60_000,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(['ds:a']),
        currentTime: Date.now(),
    };

    // Evaluate multiple times – should always return the same answer
    const r1 = evaluateReadiness(condition, state);
    const r2 = evaluateReadiness(condition, state);
    const r3 = evaluateReadiness(condition, state);
    assert.deepStrictEqual(r1, r2);
    assert.deepStrictEqual(r2, r3);
});

// ---------------------------------------------------------------------------
// Scenario: league-specific override changes the delay
// ---------------------------------------------------------------------------

test('league override produces different earliest_run_time', () => {
    const sourceTime = 1_000_000;

    const defaultEarliest = computeEarliestRunTime(
        sourceTime,
        config,
        'driver_of_the_day'
    );
    const leagueEarliest = computeEarliestRunTime(
        sourceTime,
        config,
        'driver_of_the_day',
        'league_fast'
    );

    assert.ok(defaultEarliest !== null);
    assert.ok(leagueEarliest !== null);
    // league_fast has 1d delay vs default 2d
    assert.ok(leagueEarliest < defaultEarliest);
    assert.strictEqual(defaultEarliest - leagueEarliest, 86_400_000); // 1 day diff
});

// ---------------------------------------------------------------------------
// Scenario: process restart – recomputed readiness from persisted data
// ---------------------------------------------------------------------------

test('recomputed readiness after restart: past-due job is immediately ready', () => {
    // Simulate: source event was 5 days ago, delay is 2d, deps were satisfied.
    const sourceEventTime = Date.now() - 5 * 86_400_000;
    const earliest = computeEarliestRunTime(
        sourceEventTime,
        config,
        'driver_of_the_day'
    );

    const condition: ReadinessCondition = {
        requiredDataContexts: ['telemetry'],
        earliestRunTime: earliest,
    };

    // On restart, deps re-observed from Kafka replay
    const state: ReadinessState = {
        satisfiedContexts: new Set(['telemetry']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, true);
    assert.strictEqual(result.msUntilTimeCondition, 0);
});

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) {
    process.exit(1);
}
