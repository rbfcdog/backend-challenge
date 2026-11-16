import { SQL } from 'bun';
import type { Subprocess } from 'bun';
import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import { cpus, totalmem, freemem, release, platform, arch } from 'node:os';
import { lookup } from 'node:dns/promises';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { once } from 'node:events';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { CreateQueueCommand, DeleteQueueCommand, GetQueueAttributesCommand, GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';
import { databaseConfig } from '../infrastructure/database/orm.config';
import { loadOptions } from './options';
import { latencyStats, responseOutcome } from './stats';

const root = fileURLToPath(new URL('../../', import.meta.url));
const initialBalance = '1000000.00';
const amount = '0.01';
const provider = 'load-provider';
type Child = { process: Subprocess<'ignore', 'pipe', 'pipe'>; output: Promise<void>; logs: string; url: string; role: string };
type Wallet = { id: string; playerId: string; balance: { amount: string; currency: string }; version: number };
type Backlog = { observedAt: string; pending: number; oldestPendingAgeSeconds: number };
type Metrics = { lockConflicts: number; retries: number; duplicates: number; processed: number; perInstance: Record<string, number>[] };
type PhaseName = 'warmup' | 'distinct-wallet-unique' | 'hot-wallet-unique' | 'duplicate-replay';

async function deadline<T>(work: PromiseLike<T>, label: string, milliseconds: number): Promise<T> {
  const timeout = new AbortController();
  try {
    return await Promise.race([work, sleep(milliseconds, undefined, { signal: timeout.signal }).then(() => {
      throw new Error(`Timed out after ${milliseconds}ms: ${label}`);
    })]);
  } finally { timeout.abort(); }
}

async function freePort(): Promise<number> {
  const server = createServer();
  const listening = once(server, 'listening');
  server.listen(0, '127.0.0.1');
  await listening;
  const address = server.address();
  assert(address && typeof address === 'object');
  const closed = once(server, 'close');
  server.close();
  await closed;
  return address.port;
}

async function stopChild(child: Child): Promise<void> {
  if (child.process.exitCode === null && child.process.signalCode === null) {
    child.process.kill('SIGTERM');
    const force = setTimeout(() => child.process.kill('SIGKILL'), 8000);
    try { await deadline(child.process.exited, `stop ${child.role}`, 11000); }
    finally { clearTimeout(force); }
  } else await child.process.exited;
  await deadline(child.output, `logs ${child.role}`, 1000);
}

async function main(): Promise<void> {
  // Validate every control before creating any database, queue or subprocess.
  const options = loadOptions();
  if (process.argv.length > 2) throw new Error('Use LOAD_* environment controls; arbitrary target URLs/CLI arguments are not supported');
  for (const url of [options.database, options.sqs]) {
    if (url.hostname === 'localhost') {
      const addresses = await lookup('localhost', { all: true });
      assert(addresses.length && addresses.every(({ address }) => address === '::1' || address.startsWith('127.')), 'localhost must resolve only to loopback addresses');
    }
  }
  const token = crypto.randomUUID().replaceAll('-', '');
  const databaseName = `wager_load_${token}`;
  const adminUrl = new URL(options.database); adminUrl.pathname = '/postgres';
  const databaseUrl = new URL(options.database); databaseUrl.pathname = `/${databaseName}`;
  const admin = new SQL({ url: adminUrl.toString(), max: 1, connectionTimeout: 3 });
  const db = new SQL({ url: databaseUrl.toString(), max: 2, connectionTimeout: 3 });
  const sqs = new SQSClient({
    endpoint: options.sqs.toString(), region: process.env.AWS_REGION ?? 'us-east-1',
    credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
    maxAttempts: 1, requestHandler: { connectionTimeout: 2000, requestTimeout: 4000, throwOnRequestTimeout: true },
  });
  const children: Child[] = [];
  const apps: Child[] = [];
  const queues: string[] = [];
  const queueNames: string[] = [];
  const queueUrls = new Map<string, string>();
  const wallets: Wallet[] = [];
  const expectedDebits = new Map<string, bigint>();
  const abort = new AbortController();
  const errors: string[] = [];
  const cleanupErrors: string[] = [];
  let databaseCreationAttempted = false;
  let env: Record<string, string | undefined> = {};
  const started = performance.now();
  const cpuStart = process.cpuUsage();
  const report = {
    schemaVersion: 1, startedAt: new Date().toISOString(), finishedAt: null as string | null,
    success: false, errors, cleanupErrors,
    methodology: {
      model: 'fixed-count closed-loop; each worker dispatches its next operation only after consuming the previous response body',
      latency: 'performance.now dispatch through complete response body; includes HTTP, protocol and network errors; milliseconds; nearest-rank percentiles',
      limitations: ['closed-loop observed-client distributions have coordinated omission and do not establish open-loop saturation capacity',
        'clients and all app instances share this host; PostgreSQL/LocalStack may share Docker and other host workloads; not an isolated hardware experiment',
        'SQL sampler, HTTP metrics scraping and log consumption add observer overhead',
        'setup, warmup, migration, metric snapshots, outbox drain and reconciliation are excluded from measured request throughput'],
      workload: { amount, currency: 'BRL', initialBalance, ordering: 'index-based round-robin instance/wallet; deterministic operation identities within isolated database',
        phases: ['distinct-wallet-unique', 'hot-wallet-unique', 'duplicate-replay'],
        duplicate: 'all measured requests share one identical payload and idempotency key; exactly one committed debit; replays are expected successes' },
    },
    options: {
      instances: options.instances, concurrency: options.concurrency, requestsPerPhase: options.requests,
      wallets: options.wallets, warmupRequests: options.warmup, requestTimeoutMs: options.requestTimeoutMs,
      drainTimeoutMs: options.drainTimeoutMs, sampleIntervalMs: options.sampleIntervalMs,
      maxStoredBacklogSamplesPerPhase: 10000, reportPath: options.report,
      databaseHost: options.database.host, sqsEndpoint: options.sqs.origin, databaseName,
      workersEnabled: true, workerIntervalMs: 20,
    },
    environment: {} as Record<string, unknown>,
    setupDrain: null as unknown,
    warmup: null as unknown,
    phases: [] as Array<Record<string, unknown>>,
    verification: null as unknown,
    resources: { databaseName, queues, queueNames } as Record<string, unknown>,
  };
  const signalHandler = (signal: string) => { if (!abort.signal.aborted) abort.abort(new Error(`Received ${signal}`)); };
  const onInt = () => signalHandler('SIGINT');
  const onTerm = () => signalHandler('SIGTERM');
  process.on('SIGINT', onInt); process.on('SIGTERM', onTerm);

  async function query(text: string, parameters: unknown[] = [], connection = db): Promise<Record<string, unknown>[]> {
    const work = connection.unsafe(text, parameters).execute();
    const timer = setTimeout(() => work.cancel(), 5000);
    try { return await deadline(work, 'benchmark SQL', 6000); }
    finally { clearTimeout(timer); }
  }
  async function spawn(command: string[], role: string, overrides: Record<string, string> = {}, url = ''): Promise<Child> {
    abort.signal.throwIfAborted();
    const subprocess = Bun.spawn(command, { cwd: root, env: { ...env, ...overrides }, stdin: 'ignore', stdout: 'pipe', stderr: 'pipe' });
    const child: Child = { process: subprocess, output: Promise.resolve(), logs: '', url, role };
    children.push(child);
    const pump = async (stream: ReadableStream<Uint8Array>) => {
      const decoder = new TextDecoder();
      for await (const chunk of stream) child.logs = (child.logs + decoder.decode(chunk, { stream: true })).slice(-64000);
    };
    child.output = Promise.all([pump(subprocess.stdout), pump(subprocess.stderr)]).then(() => {});
    return child;
  }
  async function request(child: Child, path: string, method = 'GET', body?: unknown, key?: string) {
    const response = await fetch(`${child.url}${path}`, {
      method, redirect: 'error', signal: AbortSignal.any([abort.signal, AbortSignal.timeout(options.requestTimeoutMs)]),
      headers: { 'Content-Type': 'application/json', 'X-Correlation-Id': 'local-load', ...(key ? { 'Idempotency-Key': key } : {}) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const text = await response.text();
    return { status: response.status, text };
  }
  async function backlog(): Promise<Backlog> {
    const rows = await query(`select count(*)::text as pending,
      greatest(0,coalesce(extract(epoch from clock_timestamp()-min(occurred_at)),0))::float8 as age
      from outbox_messages where published_at is null`);
    const pending = Number(rows[0]!.pending);
    const age = Number(rows[0]!.age);
    assert(Number.isSafeInteger(pending) && pending >= 0 && Number.isFinite(age) && age >= 0, 'invalid SQL outbox observation');
    return { observedAt: new Date().toISOString(), pending, oldestPendingAgeSeconds: age };
  }
  async function drain() {
    const start = performance.now();
    let current = await backlog();
    const initial = current;
    while (current.pending > 0) {
      abort.signal.throwIfAborted();
      if (performance.now() - start > options.drainTimeoutMs) {
        return { success: false, milliseconds: performance.now() - start, initial, end: current };
      }
      await sleep(50, undefined, { signal: abort.signal });
      current = await backlog();
    }
    return { success: true, milliseconds: performance.now() - start, initial, end: current };
  }
  async function metrics(): Promise<Metrics> {
    const perInstance = await Promise.all(apps.map(async child => {
      const response = await request(child, '/metrics');
      assert.equal(response.status, 200, 'metrics HTTP status');
      const counters: Record<string, number> = {};
      for (const line of response.text.split('\n')) {
        const match = /^(wagering_[a-z_]+)(\{[^\n]*\})?\s+([0-9.eE+-]+)$/.exec(line);
        if (match) counters[match[1]!] = (counters[match[1]!] ?? 0) + Number(match[3]);
        if (match?.[1] === 'wagering_transactions_total' && match[2] === '{status="PROCESSED"}') counters.processed = Number(match[3]);
      }
      assert(Number.isFinite(counters.wagering_lock_conflicts_total), 'lock-conflict metric missing/nonfinite');
      assert(Number.isFinite(counters.processed), 'processed metric missing/nonfinite');
      assert(Number.isFinite(counters.wagering_duplicates_total), 'duplicate metric missing/nonfinite');
      assert(Number.isFinite(counters.wagering_retries_total ?? 0), 'retry metric nonfinite');
      return counters;
    }));
    return {
      lockConflicts: perInstance.reduce((sum, item) => sum + item.wagering_lock_conflicts_total!, 0),
      retries: perInstance.reduce((sum, item) => sum + (item.wagering_retries_total ?? 0), 0),
      duplicates: perInstance.reduce((sum, item) => sum + (item.wagering_duplicates_total ?? 0), 0),
      processed: perInstance.reduce((sum, item) => sum + item.processed!, 0), perInstance,
    };
  }
  async function transactionCount(name: PhaseName): Promise<number> {
    const rows = await query('select count(*)::text as count from wager_transactions where provider_id=$1 and external_transaction_id like $2 and status=$3', [provider, `${name}:%`, 'PROCESSED']);
    return Number(rows[0]!.count);
  }
  async function phase(name: PhaseName, count: number, measured: boolean) {
    const result: Record<string, unknown> = { name, requestedOperations: count, completedOperations: 0 };
    if (measured) report.phases.push(result); else report.warmup = result;
    const beforeMetrics = await metrics();
    const beforeCommits = await transactionCount(name);
    const samples: number[] = [];
    const statuses: Record<string, number> = {};
    const outcomes: Record<string, number> = {};
    const failures: Array<{ index: number; error: string }> = [];
    const observations: Backlog[] = [];
    const observationErrors: string[] = [];
    let observationErrorCount = 0;
    let observationCount = 0;
    let peakPending: number | null = null;
    let peakAge: number | null = null;
    let sampling = true;
    const samplerStop = new AbortController();
    const observe = async () => {
      try {
        const sample = await backlog();
        observationCount++;
        if (observations.length < 10000) observations.push(sample);
        peakPending = Math.max(peakPending ?? 0, sample.pending);
        peakAge = Math.max(peakAge ?? 0, sample.oldestPendingAgeSeconds);
      } catch (error) {
        observationErrorCount++;
        if (observationErrors.length < 10) observationErrors.push(String(error));
      }
    };
    await observe();
    const sampler = (async () => {
      while (sampling) {
        try { await sleep(options.sampleIntervalMs, undefined, { signal: samplerStop.signal }); }
        catch { break; }
        if (sampling) await observe();
      }
    })();
    let next = 0;
    let completed = 0;
    const started = performance.now();
    let p95: number | null = null;
    try {
      await Promise.all(Array.from({ length: Math.min(options.concurrency, count) }, async () => {
        while (!abort.signal.aborted) {
          const index = next++;
          if (index >= count) return;
          const wallet = wallets[(name === 'hot-wallet-unique' || name === 'duplicate-replay') ? 0 : index % wallets.length]!;
          const operation = `${name}:${name === 'duplicate-replay' ? 'shared' : index}`;
          if (name !== 'duplicate-replay' || index === 0) expectedDebits.set(wallet.id, expectedDebits.get(wallet.id)! + 1n);
          const input = {
            providerId: provider, externalTransactionId: operation, playerId: wallet.playerId, walletId: wallet.id,
            roundId: 'load-round', gameId: 'load-game', kind: 'BET', money: { amount, currency: 'BRL' },
          };
          const dispatch = performance.now();
          let outcome: string;
          try {
            const response = await request(apps[index % apps.length]!, '/wagering/transactions', 'POST', input, `${provider}:${operation}`);
            // Capture through body consumption, before JSON validation/aggregation.
            samples.push(performance.now() - dispatch);
            statuses[String(response.status)] = (statuses[String(response.status)] ?? 0) + 1;
            let body: unknown;
            try { body = JSON.parse(response.text); } catch { body = null; }
            outcome = responseOutcome(response.status, body);
            if (outcome.endsWith('error') && failures.length < 10) failures.push({ index, error: `HTTP ${response.status}: ${response.text.slice(0, 500)}` });
          } catch (error) {
            samples.push(performance.now() - dispatch);
            outcome = abort.signal.aborted ? 'aborted' : 'network-error';
            if (failures.length < 10) failures.push({ index, error: String(error) });
          }
          completed++;
          outcomes[outcome] = (outcomes[outcome] ?? 0) + 1;
        }
      }));
    } finally {
      const durationMs = performance.now() - started;
      sampling = false; samplerStop.abort(); await sampler;
      await observe();
      const latencies = latencyStats(samples);
      p95 = latencies.p95;
      Object.assign(result, {
        completedOperations: completed, durationMs, latencyMs: latencies, latencySamplesMs: samples,
        requestsPerSecond: durationMs > 0 ? completed * 1000 / durationMs : 0,
        statusCounts: statuses, outcomeCounts: outcomes, failures,
        errorCount: completed - (outcomes.committed ?? 0) - (outcomes.replay ?? 0),
        errorRate: completed ? (completed - (outcomes.committed ?? 0) - (outcomes.replay ?? 0)) / completed : null,
        outbox: { sampleIntervalMs: options.sampleIntervalMs, sampleCount: observationCount,
          sampleSchedule: 'nonoverlapping fixed-delay; query time adds to nominal interval; observedAt records actual spacing; first/last observations bracket requests',
          storedSampleCount: observations.length, samples: observations, peakPending, peakOldestPendingAgeSeconds: peakAge,
          observationErrorCount, observationErrors },
      });
    }
    const committed = (await transactionCount(name)) - beforeCommits;
    const drained = await drain();
    result.drain = drained;
    const afterMetrics = await metrics();
    const increments = {
      lockConflicts: afterMetrics.lockConflicts - beforeMetrics.lockConflicts,
      retries: afterMetrics.retries - beforeMetrics.retries,
      duplicates: afterMetrics.duplicates - beforeMetrics.duplicates,
      processed: afterMetrics.processed - beforeMetrics.processed,
    };
    Object.assign(result, { uniqueCommittedTransactions: committed,
      uniqueCommitsPerSecond: committed * 1000 / Number(result.durationMs),
      metrics: { window: 'request dispatch through completed outbox drain',
        before: beforeMetrics, after: afterMetrics, increments } });
    const expected = name === 'duplicate-replay' ? 1 : count;
    const expectedReplays = name === 'duplicate-replay' ? count - 1 : 0;
    assert(drained.success, `${name}: outbox did not drain`);
    assert.equal(completed, count, `${name}: interrupted before all operations completed`);
    assert.equal(result.errorCount, 0, `${name}: unexpected request/protocol errors`);
    assert.equal(observationErrorCount, 0, `${name}: SQL backlog observation failed`);
    assert.equal(committed, expected, `${name}: unexpected unique SQL commits`);
    assert.equal(outcomes.committed ?? 0, expected, `${name}: unexpected committed responses`);
    assert.equal(outcomes.replay ?? 0, expectedReplays, `${name}: unexpected replay responses`);
    assert.equal(increments.processed, expected, `${name}: processed counter mismatch`);
    assert.equal(increments.duplicates, expectedReplays, `${name}: duplicate counter mismatch`);
    assert(increments.lockConflicts >= 0 && increments.retries >= 0, `${name}: counters decreased`);
    console.log(`[load] ${name}: ${Number(result.requestsPerSecond).toFixed(1)} req/s; ${Number(result.uniqueCommitsPerSecond).toFixed(1)} commits/s; p95=${p95?.toFixed(2) ?? 'n/a'}ms; errors=${result.errorCount}; lock conflicts=${increments.lockConflicts}`);
  }

  async function dockerMetadata() {
    if (!Bun.which('docker')) return { containers: null, reason: 'docker CLI unavailable' };
    try {
      const dockerEnv = { DOCKER_HOST: 'unix:///var/run/docker.sock', DOCKER_CONTEXT: '' };
      const child = await spawn(['docker', 'ps', '--format', '{{json .}}'], 'docker-metadata', dockerEnv);
      await deadline(child.process.exited, 'docker metadata', 3000); await child.output;
      assert.equal(child.process.exitCode, 0, child.logs);
      const rows = child.logs.trim().split('\n').filter(Boolean).map(line => JSON.parse(line) as Record<string, string>);
      const ports = [options.database.port || '5432', options.sqs.port || (options.sqs.protocol === 'https:' ? '443' : '80')];
      const relevant = rows.filter(row => ports.some(port => row.Ports?.includes(`:${port}->`)));
      if (!relevant.length) return { containers: null, reason: 'no containers matching configured localhost database/SQS ports' };
      const details = [];
      for (const row of relevant) {
        const inspect = await spawn(['docker', 'inspect', '--format', '{{json .}}', row.ID!], 'docker-inspect', dockerEnv);
        await deadline(inspect.process.exited, 'docker inspect', 3000); await inspect.output;
        assert.equal(inspect.process.exitCode, 0, inspect.logs);
        const data = JSON.parse(inspect.logs) as { Id: string; Name: string; Image: string; Config: { Image: string }; State: { StartedAt: string } };
        details.push({ id: data.Id, name: data.Name, configuredImage: data.Config.Image, imageId: data.Image, startedAt: data.State.StartedAt });
      }
      return { containers: details, reason: null };
    } catch (error) { return { containers: null, reason: String(error) }; }
  }

  try {
    env = { ...process.env, DATABASE_URL: databaseUrl.toString(), SQS_ENDPOINT: options.sqs.toString(),
      AWS_REGION: process.env.AWS_REGION ?? 'us-east-1', AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      HOST: '127.0.0.1', WORKERS_ENABLED: 'true', WORKER_INTERVAL_MS: '20' };
    databaseCreationAttempted = true;
    await query(`create database "${databaseName}"`, [], admin);
    const makeQueue = async (suffix: string, attributes: Record<string, string> = {}) => {
      abort.signal.throwIfAborted();
      const name = `load-${token}-${suffix}.fifo`;
      queueNames.push(name);
      const result = await sqs.send(new CreateQueueCommand({ QueueName: name,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', ...attributes } }));
      assert(result.QueueUrl, 'CreateQueue did not return a URL');
      const url = new URL(result.QueueUrl);
      assert.equal(decodeURIComponent(url.pathname.split('/').at(-1)!), name, 'CreateQueue returned a non-owned queue');
      url.protocol = options.sqs.protocol; url.host = options.sqs.host;
      queues.push(url.toString()); queueUrls.set(name, url.toString());
      return url.toString();
    };
    const dead = await makeQueue('dlq');
    const attributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dead, AttributeNames: ['QueueArn'] }));
    assert(attributes.Attributes?.QueueArn);
    const input = await makeQueue('input', { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: attributes.Attributes.QueueArn, maxReceiveCount: '5' }) });
    const events = await makeQueue('events');
    Object.assign(env, { SQS_QUEUE_URL: input, SQS_DLQ_URL: dead, SQS_EVENTS_QUEUE_URL: events });
    const migration = await spawn([process.execPath, 'src/infrastructure/database/migrate.ts', 'up'], 'migration');
    await deadline(migration.process.exited, 'migration', 30000); await migration.output;
    assert.equal(migration.process.exitCode, 0, migration.logs);
    const require = createRequire(import.meta.url);
    const installedVersions: Record<string, unknown> = {};
    for (const name of ['@aws-sdk/client-sqs', '@mikro-orm/core', '@mikro-orm/postgresql', '@nestjs/core']) {
      try {
        const metadata = await Bun.file(require.resolve(`${name}/package.json`)).json() as { version: string };
        installedVersions[name] = metadata.version;
      }
      catch (error) { installedVersions[name] = { version: null, reason: String(error) }; }
    }
    const packageInfo = await Bun.file(`${root}/package.json`).json() as { name: string; version: string };
    report.environment = {
      observedAt: new Date().toISOString(), os: { platform: platform(), release: release(), arch: arch() },
      cpu: { logicalCount: cpus().length, models: [...new Set(cpus().map(cpu => cpu.model))], reportedMHz: cpus().map(cpu => cpu.speed) },
      memory: { totalBytes: totalmem(), freeBytesAtStart: freemem() },
      runtime: { bun: Bun.version, nodeCompatibility: process.version, versions: process.versions, executable: process.execPath },
      application: { name: packageInfo.name, version: packageInfo.version, installedVersions },
      postgres: { version: (await query('select version() as version'))[0]!.version,
        settings: await query(`select name,setting,unit,source from pg_settings where name in
          ('max_connections','shared_buffers','effective_cache_size','work_mem','max_wal_size','synchronous_commit','fsync','jit','server_version','default_transaction_isolation') order by name`) },
      databasePools: { perApplication: databaseConfig(databaseUrl.toString()).pool, samplerAndReconciliation: 2, administrative: 1 },
      docker: await dockerMetadata(),
    };
    for (let index = 0; index < options.instances; index++) {
      const port = await freePort();
      const child = await spawn([process.execPath, 'src/main.ts'], `app-${index}`, { PORT: String(port) }, `http://127.0.0.1:${port}`);
      apps.push(child);
      const end = performance.now() + 20000;
      while (true) {
        abort.signal.throwIfAborted();
        assert(child.process.exitCode === null && child.process.signalCode === null, `app exited: ${child.logs}`);
        try {
          const ready = await fetch(`${child.url}/health/live`, { redirect: 'error', signal: AbortSignal.timeout(500) });
          await ready.arrayBuffer();
          if (ready.ok) break;
        } catch { /* readiness polls only the owned child */ }
        assert(performance.now() < end, `app readiness timeout: ${child.logs}`);
        await sleep(25, undefined, { signal: abort.signal });
      }
    }
    report.resources.applications = apps.map(child => ({ pid: child.process.pid, url: child.url }));
    for (let index = 0; index < options.wallets; index++) {
      const response = await request(apps[index % apps.length]!, '/wallets', 'POST', {
        playerId: crypto.randomUUID(), initialBalance: { amount: initialBalance, currency: 'BRL' },
      });
      assert.equal(response.status, 201, response.text);
      const wallet = JSON.parse(response.text) as Wallet;
      assert.equal(wallet.balance.amount, initialBalance);
      assert.equal(wallet.balance.currency, 'BRL');
      wallets.push(wallet); expectedDebits.set(wallet.id, 0n);
    }
    const setupDrain = await drain();
    report.setupDrain = setupDrain;
    assert(setupDrain.success, 'setup outbox drain failed');
    if (options.warmup) await phase('warmup', options.warmup, false);
    for (const name of ['distinct-wallet-unique', 'hot-wallet-unique', 'duplicate-replay'] as const) await phase(name, options.requests, true);

    const verification: Record<string, unknown> = { success: false, sql: {}, wallets: [], http: [] };
    report.verification = verification;
    const rows = await query(`with ledger as (
      select wallet_id,sum(case when direction='CREDIT' then amount else -amount end) as balance,count(*) as journals
      from wallet_ledger_entries group by wallet_id
    ), liability as (
      select account_reference,sum(case when direction='CREDIT' then amount else -amount end) as balance,count(*) as postings
      from accounting_postings where account_type='WALLET_LIABILITY' group by account_reference
    ) select w.id,w.balance::text,w.version,l.balance::text as ledger_balance,a.balance::text as liability_balance,
      l.journals::text,a.postings::text from wallets w left join ledger l on l.wallet_id=w.id
      left join liability a on a.account_reference=w.id::text order by w.id`);
    verification.wallets = rows;
    assert.equal(rows.length, options.wallets);
    const http: unknown[] = [];
    verification.http = http;
    for (const row of rows) {
      const debitCount = expectedDebits.get(String(row.id))!;
      const cents = 100000000n - debitCount;
      const expected = `${cents / 100n}.${String(cents % 100n).padStart(2, '0')}`;
      assert.equal(row.balance, expected, `expected workload balance ${row.id}`);
      assert.equal(row.ledger_balance, row.balance, `ledger balance ${row.id}`);
      assert.equal(row.liability_balance, row.balance, `liability balance ${row.id}`);
      assert.equal(Number(row.journals), Number(debitCount) + 1, `journal count ${row.id}`);
      assert.equal(Number(row.postings), Number(row.journals), `liability posting count ${row.id}`);
      assert.equal(Number(row.version), Number(debitCount) + 1, `wallet version ${row.id}`);
      const response = await request(apps[http.length % apps.length]!, `/wallets/${row.id}/reconciliation`, 'POST');
      const body = JSON.parse(response.text) as { consistent: boolean; storedBalance: { amount: string }; calculatedBalance: { amount: string };
        difference: { amount: string }; accounting: { balanced: boolean; consistent: boolean; walletBalance: { amount: string }; journalCount: number; postingCount: number } };
      http.push({ walletId: row.id, status: response.status, body });
      assert.equal(response.status, 200);
      assert(body.consistent && body.accounting.balanced && body.accounting.consistent, `HTTP accounting ${row.id}`);
      assert.equal(body.storedBalance.amount, row.balance);
      assert.equal(body.calculatedBalance.amount, row.balance);
      assert.equal(body.difference.amount, '0.00');
      assert.equal(body.accounting.walletBalance.amount, row.balance);
      assert.equal(body.accounting.journalCount, Number(row.journals));
      assert.equal(body.accounting.postingCount, Number(row.journals) * 2);
    }
    const invariants = (await query(`select
      (select count(*) from wallet_ledger_entries)::text as journals,
      (select count(*) from accounting_postings)::text as postings,
      (select count(*) from (select l.id from wallet_ledger_entries l left join accounting_postings p on p.ledger_entry_id=l.id
        group by l.id having count(p.ledger_entry_id)<>2 or count(*) filter(where p.direction='DEBIT')<>1
        or count(*) filter(where p.direction='CREDIT')<>1 or coalesce(sum(p.amount) filter(where p.direction='DEBIT'),0)<>l.amount
        or coalesce(sum(p.amount) filter(where p.direction='CREDIT'),0)<>l.amount
        or bool_or(p.currency<>l.currency)) invalid)::text as invalid_journals,
      (select count(*) from accounting_postings p join wallet_ledger_entries l on l.id=p.ledger_entry_id
        join wager_transactions t on t.id=l.transaction_id where p.amount<>l.amount or p.currency<>l.currency
        or not ((p.account_type='WALLET_LIABILITY' and p.account_reference=l.wallet_id::text and p.direction=l.direction)
          or (p.account_type=case when t.kind='OPENING' then 'OPENING_CLEARING' else 'PROVIDER_CLEARING' end
            and p.account_reference=case when t.kind='OPENING' then 'opening' else t.provider_id end and p.direction<>l.direction)))::text as invalid_postings,
      (select count(*) from wallet_ledger_entries l join wager_transactions t on t.id=l.transaction_id
        where t.kind='LOSS' or t.status<>'PROCESSED')::text as forbidden_journals,
      (select count(*) from wager_transactions where status<>'PROCESSED' or kind not in ('OPENING','BET'))::text as unexpected_transactions,
      (select count(*) from wager_transactions where provider_id=$1)::text as wager_count,
      (select count(*) from wager_transactions where provider_id=$1 and external_transaction_id='duplicate-replay:shared')::text as duplicate_transactions,
      (select count(*) from outbox_messages where published_at is null)::text as pending_outbox,
      (select count(*) from outbox_messages)::text as outbox_count`, [provider]))[0]!;
    verification.sql = invariants;
    for (const field of ['invalid_journals', 'invalid_postings', 'forbidden_journals', 'unexpected_transactions', 'pending_outbox']) assert.equal(invariants[field], '0', field);
    const expectedWagers = options.warmup + options.requests * 2 + 1;
    assert.equal(Number(invariants.wager_count), expectedWagers);
    assert.equal(invariants.duplicate_transactions, '1');
    assert.equal(Number(invariants.journals), expectedWagers + options.wallets);
    assert.equal(Number(invariants.postings), (expectedWagers + options.wallets) * 2);
    assert.equal(Number(invariants.outbox_count), (expectedWagers + options.wallets) * 2);
    const finalMetrics = await metrics(); verification.metrics = finalMetrics;
    assert.equal(finalMetrics.processed, expectedWagers + options.wallets);
    assert.equal(finalMetrics.duplicates, options.requests - 1);
    verification.success = true;
    report.success = true;
  } catch (error) {
    errors.push(error instanceof Error ? error.stack ?? error.message : String(error));
    report.resources.childFailures = children.filter(child => child.role.startsWith('app')).map(child => ({ role: child.role, exitCode: child.process.exitCode, signalCode: child.process.signalCode, logs: child.logs.slice(-8000) }));
  } finally {
    const capture = async (label: string, operation: () => Promise<unknown>) => {
      try { await operation(); } catch (error) { cleanupErrors.push(`${label}: ${String(error)}`); }
    };
    await Promise.all(children.map(child => capture(`stop ${child.role}`, () => stopChild(child))));
    report.resources.processUsage = children.map(child => ({ role: child.role, pid: child.process.pid, exitCode: child.process.exitCode,
      signalCode: child.process.signalCode, usage: child.process.resourceUsage() ?? null }));
    await capture('database client close', () => deadline(db.close({ timeout: 2 }), 'close DB', 3000));
    await Promise.all(queueNames.map(name => capture(`delete queue ${name}`, async () => {
      let QueueUrl = queueUrls.get(name);
      if (!QueueUrl) {
        // A timed-out CreateQueue may still have committed; resolve only our UUID name.
        try {
          const found = await sqs.send(new GetQueueUrlCommand({ QueueName: name }));
          assert(found.QueueUrl, 'GetQueueUrl did not return a URL');
          const url = new URL(found.QueueUrl);
          assert.equal(decodeURIComponent(url.pathname.split('/').at(-1)!), name, 'GetQueueUrl returned a non-owned queue');
          url.protocol = options.sqs.protocol; url.host = options.sqs.host;
          QueueUrl = url.toString();
        } catch (error) {
          if (error instanceof Error && ['QueueDoesNotExist', 'AWS.SimpleQueueService.NonExistentQueue'].includes(error.name)) return;
          throw error;
        }
      }
      await sqs.send(new DeleteQueueCommand({ QueueUrl }));
    })));
    sqs.destroy();
    if (databaseCreationAttempted) {
      await capture(`terminate sessions ${databaseName}`, () => query('select pg_terminate_backend(pid) from pg_stat_activity where datname=$1 and pid<>pg_backend_pid()', [databaseName], admin));
      await capture(`drop ${databaseName}`, () => query(`drop database if exists "${databaseName}"`, [], admin));
    }
    await capture('admin close', () => deadline(admin.close({ timeout: 2 }), 'close admin', 3000));
    report.environment.clientUsage = { cpuMicroseconds: process.cpuUsage(cpuStart), memory: process.memoryUsage(), elapsedMs: performance.now() - started };
    report.finishedAt = new Date().toISOString();
    report.success = report.success && !cleanupErrors.length && !abort.signal.aborted;
    if (abort.signal.aborted && !errors.length) errors.push(String(abort.signal.reason));
    // Cleanup is complete. Restore normal signal termination before report I/O
    // so interruption during publication cannot be swallowed as a successful run.
    process.off('SIGINT', onInt); process.off('SIGTERM', onTerm);
    await mkdir(dirname(options.report), { recursive: true });
    await Bun.write(options.report, `${JSON.stringify(report, null, 2)}\n`);
    console.log(`[load] ${report.success ? 'PASS' : 'FAIL'}; ${report.phases.length} measured phases; report=${options.report}`);
    if (!report.success) { console.error([...errors, ...cleanupErrors].join('\n')); process.exitCode = 1; }
  }
}

if (import.meta.main) {
  try { await main(); }
  catch (error) { console.error(`[load] ${String(error)}`); process.exitCode = 1; }
}
