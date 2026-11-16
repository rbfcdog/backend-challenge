export function latencyStats(samples: readonly number[]) {
  if (samples.some(value => !Number.isFinite(value) || value < 0)) {
    throw new Error('Latency samples must be finite nonnegative milliseconds');
  }
  const sorted = [...samples].sort((left, right) => left - right);
  const percentile = (fraction: number) => sorted[Math.ceil(sorted.length * fraction) - 1] ?? null;
  return {
    count: sorted.length,
    mean: sorted.length ? sorted.reduce((sum, value) => sum + value, 0) / sorted.length : null,
    min: sorted[0] ?? null,
    max: sorted[sorted.length - 1] ?? null,
    p50: percentile(0.5), p95: percentile(0.95), p99: percentile(0.99),
  };
}

export type ResponseOutcome = 'committed' | 'replay' | 'http-error' | 'protocol-error';

export function responseOutcome(status: number, body: unknown): ResponseOutcome {
  if (status !== 200) return 'http-error';
  if (!body || typeof body !== 'object') return 'protocol-error';
  const result = body as Record<string, unknown>;
  const money = result.balance;
  if (result.status !== 'PROCESSED' || typeof result.transactionId !== 'string'
    || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\s\S])/i.test(result.transactionId)
    || typeof result.idempotentReplay !== 'boolean' || !money || typeof money !== 'object'
    || !('currency' in money) || money.currency !== 'BRL' || !('amount' in money)
    || typeof money.amount !== 'string' || !/^\d+\.\d{2}$(?![\s\S])/.test(money.amount)) {
    return 'protocol-error';
  }
  return result.idempotentReplay ? 'replay' : 'committed';
}
