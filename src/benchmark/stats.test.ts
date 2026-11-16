import { expect, test } from 'bun:test';
import { latencyStats, responseOutcome } from './stats';

test('nearest-rank latency percentiles preserve samples and exact boundaries', () => {
  const samples = [100, ...Array.from({ length: 99 }, (_, index) => index + 1).reverse()];
  const original = [...samples];
  expect(latencyStats(samples)).toEqual({ count: 100, mean: 50.5, min: 1, max: 100, p50: 50, p95: 95, p99: 99 });
  expect(samples).toEqual(original);
  expect(latencyStats([7])).toEqual({ count: 1, mean: 7, min: 7, max: 7, p50: 7, p95: 7, p99: 7 });
  expect(latencyStats([4, 1])).toEqual({ count: 2, mean: 2.5, min: 1, max: 4, p50: 1, p95: 4, p99: 4 });
  expect(latencyStats([])).toEqual({ count: 0, mean: null, min: null, max: null, p50: null, p95: null, p99: null });
  for (const invalid of [NaN, Infinity, -Infinity, -1]) expect(() => latencyStats([invalid])).toThrow();
});

test('replays are successful responses, not new commits, and errors remain explicit', () => {
  const body = {
    transactionId: '00000000-0000-4000-8000-000000000001', status: 'PROCESSED',
    balance: { amount: '999999.99', currency: 'BRL' }, idempotentReplay: false,
  };
  expect(responseOutcome(200, body)).toBe('committed');
  expect(responseOutcome(200, { ...body, idempotentReplay: true })).toBe('replay');
  for (const status of [202, 422, 500, 503]) expect(responseOutcome(status, body)).toBe('http-error');
  for (const malformed of [null, 'text', {}, { ...body, idempotentReplay: 'true' }, { ...body, status: 'REJECTED' },
    { ...body, transactionId: '' }, { ...body, transactionId: `${body.transactionId}\n` },
    { ...body, balance: { amount: '1.00\n', currency: 'BRL' } }, { ...body, balance: { amount: 1, currency: 'BRL' } }]) {
    expect(responseOutcome(200, malformed)).toBe('protocol-error');
  }
});
