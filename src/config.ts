import { isIP } from 'node:net';

export interface AppConfig {
  readonly port: number;
  readonly host: string;
  readonly databaseUrl: string;
  readonly sqsEndpoint: string | undefined;
  readonly sqsQueueUrl: string;
  readonly sqsDlqUrl: string;
  readonly sqsEventsQueueUrl: string;
  readonly awsRegion: string;
  readonly workersEnabled: boolean;
  readonly workerIntervalMs: number;
  readonly referenceMaxAttempts: number;
  readonly referenceRetryBaseMs: number;
  readonly referenceTtlMs: number;
  readonly sqsVisibilityTimeout: number;
  readonly sqsMaxAttempts: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const required = (key: string): string => {
    const value = env[key];
    if (!value?.trim()) throw new Error(`Missing environment variable: ${key}`);
    return value;
  };
  const urlValue = (key: string, value: string, protocols: readonly string[]): string => {
    if (!protocols.includes(new URL(value).protocol)) throw new Error(`Invalid protocol for ${key}`);
    return value;
  };
  const integer = (key: string, fallback: number, max: number): number => {
    const value = env[key] ?? String(fallback);
    if (!/^[0-9]+$(?![\s\S])/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) < 1 || Number(value) > max) {
      throw new Error(`${key} must be an integer between 1 and ${max}`);
    }
    return Number(value);
  };
  const inputQueue = urlValue('SQS_QUEUE_URL', required('SQS_QUEUE_URL'), ['http:', 'https:']);
  const siblingQueue = (key: string, name: string): string => {
    const value = env[key] ?? inputQueue.replace(/[^/]+$/, name);
    return urlValue(key, value, ['http:', 'https:']);
  };
  const workers = env.WORKERS_ENABLED ?? 'true';
  if (workers !== 'true' && workers !== 'false') throw new Error('WORKERS_ENABLED must be true or false');
  const host = env.HOST ?? '0.0.0.0';
  if (host !== 'localhost' && !isIP(host)) throw new Error('HOST must be an IP address or localhost');
  const dlq = siblingQueue('SQS_DLQ_URL', 'wager-transactions-dlq.fifo');
  const events = siblingQueue('SQS_EVENTS_QUEUE_URL', 'wager-events.fifo');
  if (new Set([inputQueue, dlq, events]).size !== 3) throw new Error('Input, DLQ, and events queues must be distinct');
  return Object.freeze({
    port: integer('PORT', 3000, 65535),
    host,
    databaseUrl: urlValue('DATABASE_URL', required('DATABASE_URL'), ['postgres:', 'postgresql:']),
    sqsEndpoint: env.SQS_ENDPOINT ? urlValue('SQS_ENDPOINT', env.SQS_ENDPOINT, ['http:', 'https:']) : undefined,
    sqsQueueUrl: inputQueue, sqsDlqUrl: dlq, sqsEventsQueueUrl: events,
    awsRegion: required('AWS_REGION'), workersEnabled: workers === 'true',
    workerIntervalMs: integer('WORKER_INTERVAL_MS', 250, 60_000),
    referenceMaxAttempts: integer('REFERENCE_MAX_ATTEMPTS', 10, 100),
    referenceRetryBaseMs: integer('REFERENCE_RETRY_BASE_MS', 1000, 300_000),
    referenceTtlMs: integer('REFERENCE_TTL_MS', 86_400_000, 604_800_000),
    sqsVisibilityTimeout: integer('SQS_VISIBILITY_TIMEOUT', 30, 43_200),
    sqsMaxAttempts: integer('SQS_MAX_ATTEMPTS', 5, 100),
  });
}
