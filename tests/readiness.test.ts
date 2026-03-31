import * as assert from 'assert';
import {
    evaluateReadiness,
    buildLegacyCondition,
    buildTimedCondition,
    ReadinessCondition,
    ReadinessState,
} from '../src/ldata-mesh-util/readiness';

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

console.log('\n=== Readiness Evaluator Tests ===\n');

// --- evaluateReadiness ---

test('dependency satisfied, no delay -> ready', () => {
    const condition: ReadinessCondition = {
        requiredDataContexts: ['ds:a', 'ds:b'],
        earliestRunTime: null,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(['ds:a', 'ds:b']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, true);
    assert.strictEqual(result.dependenciesSatisfied, true);
    assert.strictEqual(result.timeConditionSatisfied, true);
    assert.strictEqual(result.msUntilTimeCondition, 0);
});

test('dependency not satisfied, no delay -> not ready', () => {
    const condition: ReadinessCondition = {
        requiredDataContexts: ['ds:a', 'ds:b'],
        earliestRunTime: null,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(['ds:a']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, false);
    assert.strictEqual(result.dependenciesSatisfied, false);
    assert.strictEqual(result.timeConditionSatisfied, true);
});

test('dependency satisfied, time not reached -> not ready', () => {
    const futureTime = Date.now() + 86_400_000; // +1 day
    const condition: ReadinessCondition = {
        requiredDataContexts: ['ds:a'],
        earliestRunTime: futureTime,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(['ds:a']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, false);
    assert.strictEqual(result.dependenciesSatisfied, true);
    assert.strictEqual(result.timeConditionSatisfied, false);
    assert.ok(result.msUntilTimeCondition > 0);
});

test('dependency satisfied, time reached -> ready', () => {
    const pastTime = Date.now() - 1000;
    const condition: ReadinessCondition = {
        requiredDataContexts: ['ds:a'],
        earliestRunTime: pastTime,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(['ds:a']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, true);
    assert.strictEqual(result.dependenciesSatisfied, true);
    assert.strictEqual(result.timeConditionSatisfied, true);
    assert.strictEqual(result.msUntilTimeCondition, 0);
});

test('dependency not satisfied, time reached -> not ready', () => {
    const pastTime = Date.now() - 1000;
    const condition: ReadinessCondition = {
        requiredDataContexts: ['ds:a', 'ds:b'],
        earliestRunTime: pastTime,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(['ds:a']),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, false);
    assert.strictEqual(result.dependenciesSatisfied, false);
    assert.strictEqual(result.timeConditionSatisfied, true);
});

test('no dependencies, no delay -> ready', () => {
    const condition: ReadinessCondition = {
        requiredDataContexts: [],
        earliestRunTime: null,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, true);
});

test('no dependencies, time not reached -> not ready', () => {
    const futureTime = Date.now() + 60_000;
    const condition: ReadinessCondition = {
        requiredDataContexts: [],
        earliestRunTime: futureTime,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(),
        currentTime: Date.now(),
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.ready, false);
    assert.strictEqual(result.dependenciesSatisfied, true);
    assert.strictEqual(result.timeConditionSatisfied, false);
});

test('msUntilTimeCondition is accurate', () => {
    const now = 1_000_000;
    const target = 1_050_000;
    const condition: ReadinessCondition = {
        requiredDataContexts: [],
        earliestRunTime: target,
    };
    const state: ReadinessState = {
        satisfiedContexts: new Set(),
        currentTime: now,
    };
    const result = evaluateReadiness(condition, state);
    assert.strictEqual(result.msUntilTimeCondition, 50_000);
});

// --- buildLegacyCondition ---

test('buildLegacyCondition creates dependency-only condition', () => {
    const condition = buildLegacyCondition(['ds:x', 'ds:y']);
    assert.deepStrictEqual(condition.requiredDataContexts, ['ds:x', 'ds:y']);
    assert.strictEqual(condition.earliestRunTime, null);
});

// --- buildTimedCondition ---

test('buildTimedCondition creates timed condition', () => {
    const condition = buildTimedCondition(['ds:x'], 999_999);
    assert.deepStrictEqual(condition.requiredDataContexts, ['ds:x']);
    assert.strictEqual(condition.earliestRunTime, 999_999);
});

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) {
    process.exit(1);
}
