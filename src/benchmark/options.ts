export function localUrl(value: string, name: string, protocols: readonly string[]): URL {
  const url = new URL(value);
  if (!protocols.includes(url.protocol) || !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)
    || url.hash || (name === 'SQS_ENDPOINT' && (url.username || url.password || url.search))) {
    throw new Error(`${name} must use a loopback host (localhost, 127.0.0.1 or ::1) and ${protocols.join('/')}`);
  }
  // PostgreSQL URL options can otherwise override the checked host or database.
  if (name === 'DATABASE_URL' && url.search) throw new Error('DATABASE_URL query overrides are not allowed by the load benchmark');
  return url;
}

export function loadOptions(env: NodeJS.ProcessEnv = process.env) {
  const integer = (key: string, fallback: number, min: number, max: number) => {
    const text = env[key] ?? String(fallback);
    const value = Number(text);
    if (!/^\d+$(?![\s\S])/.test(text) || !Number.isSafeInteger(value) || value < min || value > max) {
      throw new Error(`${key} must be an integer between ${min} and ${max}`);
    }
    return value;
  };
  const report = env.LOAD_REPORT ?? 'load-results.json';
  if (!report.trim() || report.includes('\0')) throw new Error('LOAD_REPORT must be a nonempty file path');
  return {
    instances: integer('LOAD_INSTANCES', 3, 1, 8),
    concurrency: integer('LOAD_CONCURRENCY', 24, 1, 256),
    requests: integer('LOAD_REQUESTS', 1000, 1, 100000),
    wallets: integer('LOAD_WALLETS', 32, 2, 1000),
    warmup: integer('LOAD_WARMUP', 100, 0, 10000),
    requestTimeoutMs: integer('LOAD_REQUEST_TIMEOUT_MS', 10000, 100, 60000),
    drainTimeoutMs: integer('LOAD_DRAIN_TIMEOUT_MS', 60000, 1000, 300000),
    sampleIntervalMs: integer('LOAD_SAMPLE_INTERVAL_MS', 250, 50, 10000),
    report,
    database: localUrl(env.DATABASE_URL ?? 'postgresql://wagering:wagering@localhost:5432/wagering', 'DATABASE_URL', ['postgres:', 'postgresql:']),
    sqs: localUrl(env.SQS_ENDPOINT ?? 'http://localhost:4566', 'SQS_ENDPOINT', ['http:', 'https:']),
  };
}
