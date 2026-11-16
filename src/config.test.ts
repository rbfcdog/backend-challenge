import { expect, test } from 'bun:test';
import { loadConfig } from './config';

const valid = {
  DATABASE_URL: 'postgresql://wagering:wagering@localhost:5432/wagering',
  SQS_QUEUE_URL: 'http://localhost:4566/queue/us-east-1/000000000000/wager-transactions.fifo',
  AWS_REGION: 'us-east-1',
};

test('startup rejects missing dependencies and malformed endpoints or ports', () => {
  expect(() => loadConfig({ ...valid, DATABASE_URL: '' })).toThrow('DATABASE_URL');
  expect(() => loadConfig({ ...valid, DATABASE_URL: 'https://localhost/db' })).toThrow('DATABASE_URL');
  expect(() => loadConfig({ ...valid, SQS_QUEUE_URL: 'not-a-url' })).toThrow();
  expect(() => loadConfig({ ...valid, SQS_ENDPOINT: 'file:///tmp/queue' })).toThrow('SQS_ENDPOINT');
  for (const port of ['0', '65536', '3000.5', '3e3', ' 3000']) {
    expect(() => loadConfig({ ...valid, PORT: port })).toThrow('PORT');
  }
  for (const host of ['', 'https://example.org', '127.0.0.1\n']) {
    expect(() => loadConfig({ ...valid, HOST: host })).toThrow('HOST');
  }
  expect(loadConfig({ ...valid, PORT: '4000' })).toMatchObject({ port: 4000 });
});
