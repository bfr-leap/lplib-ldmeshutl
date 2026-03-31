import * as assert from 'assert';
import * as fs from 'fs';
import * as path from 'path';
import {
    loadScheduleTimes,
    getNextScheduledTime,
} from '../src/ldata-mesh-util/schedule-file';

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

const tmpDir = path.join(__dirname, '.tmp-test-schedule');

function withTmpFile(content: string, fn: (filePath: string) => void): void {
    if (!fs.existsSync(tmpDir)) {
        fs.mkdirSync(tmpDir, { recursive: true });
    }
    const filePath = path.join(tmpDir, `sched-${Date.now()}.txt`);
    try {
        fs.writeFileSync(filePath, content);
        fn(filePath);
    } finally {
        try {
            fs.unlinkSync(filePath);
        } catch {}
    }
}

console.log('\n=== Schedule File Tests ===\n');

// --- loadScheduleTimes ---

test('parses ISO timestamps from file', () => {
    const content = [
        '2026-04-01T00:00:00Z',
        '2026-04-03T12:00:00Z',
        '2026-04-07T00:00:00Z',
    ].join('\n');

    withTmpFile(content, (p) => {
        const times = loadScheduleTimes(p);
        assert.strictEqual(times.length, 3);
        assert.strictEqual(times[0], Date.parse('2026-04-01T00:00:00Z'));
        assert.strictEqual(times[1], Date.parse('2026-04-03T12:00:00Z'));
        assert.strictEqual(times[2], Date.parse('2026-04-07T00:00:00Z'));
    });
});

test('returns sorted ascending', () => {
    const content = [
        '2026-04-07T00:00:00Z',
        '2026-04-01T00:00:00Z',
        '2026-04-03T00:00:00Z',
    ].join('\n');

    withTmpFile(content, (p) => {
        const times = loadScheduleTimes(p);
        assert.ok(times[0] < times[1]);
        assert.ok(times[1] < times[2]);
    });
});

test('skips blank lines', () => {
    const content = '2026-04-01T00:00:00Z\n\n\n2026-04-02T00:00:00Z\n';
    withTmpFile(content, (p) => {
        assert.strictEqual(loadScheduleTimes(p).length, 2);
    });
});

test('skips unparseable lines', () => {
    const content = '2026-04-01T00:00:00Z\nnot-a-date\n2026-04-02T00:00:00Z';
    withTmpFile(content, (p) => {
        assert.strictEqual(loadScheduleTimes(p).length, 2);
    });
});

test('missing file returns empty array', () => {
    const times = loadScheduleTimes('/nonexistent/path');
    assert.deepStrictEqual(times, []);
});

test('empty file returns empty array', () => {
    withTmpFile('', (p) => {
        assert.deepStrictEqual(loadScheduleTimes(p), []);
    });
});

// --- getNextScheduledTime ---

test('returns nearest future time', () => {
    const now = Date.parse('2026-04-02T00:00:00Z');
    const times = [
        Date.parse('2026-04-01T00:00:00Z'), // past
        Date.parse('2026-04-03T00:00:00Z'), // future
        Date.parse('2026-04-05T00:00:00Z'), // future
    ];
    assert.strictEqual(
        getNextScheduledTime(times, now),
        Date.parse('2026-04-03T00:00:00Z')
    );
});

test('returns null when all times are past', () => {
    const now = Date.parse('2026-04-10T00:00:00Z');
    const times = [
        Date.parse('2026-04-01T00:00:00Z'),
        Date.parse('2026-04-03T00:00:00Z'),
    ];
    assert.strictEqual(getNextScheduledTime(times, now), null);
});

test('returns null for empty list', () => {
    assert.strictEqual(getNextScheduledTime([], Date.now()), null);
});

test('returns first entry when all are future', () => {
    const now = Date.parse('2026-03-01T00:00:00Z');
    const times = [
        Date.parse('2026-04-01T00:00:00Z'),
        Date.parse('2026-04-05T00:00:00Z'),
    ];
    assert.strictEqual(
        getNextScheduledTime(times, now),
        Date.parse('2026-04-01T00:00:00Z')
    );
});

// Cleanup
try {
    fs.rmdirSync(tmpDir);
} catch {}

// --- Summary ---
console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) {
    process.exit(1);
}
