import * as assert from 'assert';
import {
    parseDelay,
    resolveDelay,
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

console.log('\n=== Delay Config Tests ===\n');

// --- parseDelay ---

test('parseDelay: "0d" -> 0', () => {
    assert.strictEqual(parseDelay('0d'), 0);
});

test('parseDelay: "1d" -> 86400000', () => {
    assert.strictEqual(parseDelay('1d'), 86_400_000);
});

test('parseDelay: "2d" -> 172800000', () => {
    assert.strictEqual(parseDelay('2d'), 172_800_000);
});

test('parseDelay: "7d" -> 604800000', () => {
    assert.strictEqual(parseDelay('7d'), 604_800_000);
});

test('parseDelay: "12h" -> 43200000', () => {
    assert.strictEqual(parseDelay('12h'), 43_200_000);
});

test('parseDelay: "30m" -> 1800000', () => {
    assert.strictEqual(parseDelay('30m'), 1_800_000);
});

test('parseDelay: "5000ms" -> 5000', () => {
    assert.strictEqual(parseDelay('5000ms'), 5_000);
});

test('parseDelay: "10s" -> 10000', () => {
    assert.strictEqual(parseDelay('10s'), 10_000);
});

test('parseDelay: invalid string -> 0', () => {
    assert.strictEqual(parseDelay('abc'), 0);
    assert.strictEqual(parseDelay(''), 0);
    assert.strictEqual(parseDelay('2x'), 0);
});

// --- resolveDelay ---

const testConfig: DelayConfig = {
    contentSchedules: {
        race_summary: { delay: '0d' },
        driver_of_the_day: { delay: '2d' },
        fastest_lap_feature: { delay: '1d' },
        driver_of_the_week: { delay: '7d' },
    },
    leagueOverrides: {
        league_a: {
            driver_of_the_day: { delay: '1d' },
        },
        league_b: {
            driver_of_the_day: { delay: '3d' },
        },
    },
};

test('resolveDelay: zero delay content type', () => {
    assert.strictEqual(resolveDelay(testConfig, 'race_summary'), 0);
});

test('resolveDelay: positive delay content type', () => {
    assert.strictEqual(
        resolveDelay(testConfig, 'driver_of_the_day'),
        172_800_000
    ); // 2d
});

test('resolveDelay: league override overrides default', () => {
    assert.strictEqual(
        resolveDelay(testConfig, 'driver_of_the_day', 'league_a'),
        86_400_000
    ); // 1d
});

test('resolveDelay: different league override', () => {
    assert.strictEqual(
        resolveDelay(testConfig, 'driver_of_the_day', 'league_b'),
        259_200_000
    ); // 3d
});

test('resolveDelay: league with no override falls back to default', () => {
    assert.strictEqual(
        resolveDelay(testConfig, 'driver_of_the_day', 'league_c'),
        172_800_000
    ); // 2d default
});

test('resolveDelay: league override exists but not for this content type', () => {
    assert.strictEqual(
        resolveDelay(testConfig, 'fastest_lap_feature', 'league_a'),
        86_400_000
    ); // 1d default
});

test('resolveDelay: unknown content type -> 0', () => {
    assert.strictEqual(resolveDelay(testConfig, 'unknown_type'), 0);
});

test('resolveDelay: null leagueId uses default', () => {
    assert.strictEqual(
        resolveDelay(testConfig, 'driver_of_the_day', null),
        172_800_000
    );
});

test('resolveDelay: config without leagueOverrides', () => {
    const simpleConfig: DelayConfig = {
        contentSchedules: { foo: { delay: '3d' } },
    };
    assert.strictEqual(
        resolveDelay(simpleConfig, 'foo', 'any_league'),
        259_200_000
    );
});

// --- computeEarliestRunTime ---

test('computeEarliestRunTime: zero delay -> null', () => {
    const result = computeEarliestRunTime(
        1_000_000,
        testConfig,
        'race_summary'
    );
    assert.strictEqual(result, null);
});

test('computeEarliestRunTime: positive delay -> source + delay', () => {
    const sourceTime = 1_000_000;
    const result = computeEarliestRunTime(
        sourceTime,
        testConfig,
        'driver_of_the_day'
    );
    assert.strictEqual(result, sourceTime + 172_800_000); // +2d
});

test('computeEarliestRunTime: league override delay', () => {
    const sourceTime = 1_000_000;
    const result = computeEarliestRunTime(
        sourceTime,
        testConfig,
        'driver_of_the_day',
        'league_b'
    );
    assert.strictEqual(result, sourceTime + 259_200_000); // +3d
});

test('computeEarliestRunTime: missing config fallback -> null', () => {
    const result = computeEarliestRunTime(1_000_000, testConfig, 'nonexistent');
    assert.strictEqual(result, null);
});

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) {
    process.exit(1);
}
