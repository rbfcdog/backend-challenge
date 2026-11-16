import { expect, test } from 'bun:test';
import { loadOptions, localUrl } from './options';

test('load controls have bounded defaults and reject invalid values before setup', () => {
  const defaults = loadOptions({});
  expect([defaults.instances, defaults.concurrency, defaults.requests, defaults.wallets, defaults.warmup]).toEqual([3, 24, 1000, 32, 100]);
  expect([defaults.requestTimeoutMs, defaults.drainTimeoutMs, defaults.sampleIntervalMs, defaults.report]).toEqual([10000, 60000, 250, 'load-results.json']);
  expect(loadOptions({ LOAD_WARMUP: '0' }).warmup).toBe(0);
  for (const value of ['0', '-1', 'NaN', '1.5', '257', ' 2', '2\n']) {
    expect(() => loadOptions({ LOAD_CONCURRENCY: value })).toThrow();
  }
  expect(() => loadOptions({ LOAD_INSTANCES: '9' })).toThrow();
  expect(() => loadOptions({ LOAD_WALLETS: '1' })).toThrow();
  expect(() => loadOptions({ LOAD_REQUESTS: '100001' })).toThrow();
  expect(() => loadOptions({ LOAD_REPORT: ' ' })).toThrow();
});

test('only explicit loopback infrastructure is permitted and PostgreSQL overrides are forbidden', () => {
  for (const host of ['localhost', '127.0.0.1', '[::1]']) {
    expect(localUrl(`postgresql://user:password@${host}:5432/main`, 'DATABASE_URL', ['postgresql:']).hostname).toBe(host);
  }
  for (const url of ['postgresql://example.com/db', 'postgresql://192.168.1.1/db',
    'postgresql://localhost.example.com/db', 'postgresql://localhost/db?host=example.com',
    'postgresql://localhost/db?dbname=shared', 'http://localhost/db']) {
    expect(() => localUrl(url, 'DATABASE_URL', ['postgres:', 'postgresql:'])).toThrow();
  }
  for (const url of ['http://example.com:4566', 'https://user:pass@localhost:4566', 'http://localhost:4566?endpoint=remote']) {
    expect(() => localUrl(url, 'SQS_ENDPOINT', ['http:', 'https:'])).toThrow();
  }
});
