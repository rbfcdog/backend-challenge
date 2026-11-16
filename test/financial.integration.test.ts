import { test } from 'bun:test';
import { SQL } from 'bun';
import type { Subprocess } from 'bun';
import assert from 'node:assert/strict';
import { createServer } from 'node:net';
import { fileURLToPath } from 'node:url';
import {
  CreateQueueCommand, DeleteMessageCommand, DeleteQueueCommand,
  GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand, SQSClient,
} from '@aws-sdk/client-sqs';

const root = fileURLToPath(new URL('../', import.meta.url));
const command = 'bun --env-file=.env.example test ./test/financial.integration.test.ts --timeout 120000';
type Child = {
  process: Subprocess<'ignore', 'pipe', 'pipe'>;
  logs: string;
  output: Promise<void>;
  url: string;
};
type Wallet = { id: string; playerId: string; balance: { amount: string; currency: string }; version: number };
type Wager = {
  providerId: string; externalTransactionId: string; playerId: string; walletId: string;
  roundId: string; gameId: string; kind: string; money: { amount: string; currency: string };
  referenceExternalTransactionId?: string;
};
type Result = { transactionId: string; status: string; balance: { amount: string; currency: string }; failureCode?: string; idempotentReplay: boolean };
type Posting = {
  accountType: string; accountReference: string; direction: string;
  money: { amount: string; currency: string };
};
type Journal = {
  id: string; direction: string; money: { amount: string; currency: string }; postings: Posting[];
};
type Reconciliation = {
  consistent: boolean; difference: { amount: string }; storedBalance: { amount: string };
  calculatedBalance: { amount: string }; checkedEntries: number;
  accounting: {
    balanced: boolean; consistent: boolean; walletBalance: { amount: string };
    debits: { amount: string }; credits: { amount: string }; journalCount: number; postingCount: number;
  };
};

// Real SQS visibility, independent child processes and PostgreSQL commits run on
// platform clocks; fake timers cannot advance them. Poll only named conditions.
async function until<T>(label: string, probe: () => Promise<T | undefined | false>, timeout = 12_000): Promise<T> {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const result = await probe();
    if (result !== undefined && result !== false) return result as T;
    await Bun.sleep(20);
  }
  throw new Error(`Timed out after ${timeout}ms: ${label}`);
}

async function deadline<T>(work: Promise<T>, label: string, milliseconds: number): Promise<T> {
  // Process exits and real SQS visibility use OS clocks, not the test runner's clock.
  const timeout = Promise.withResolvers<never>();
  const timer = setTimeout(() => timeout.reject(new Error(`Timed out: ${label}`)), milliseconds);
  try {
    return await Promise.race([work, timeout.promise]);
  } finally {
    clearTimeout(timer);
  }
}

async function port(): Promise<number> {
  const server = createServer();
  const listening = Promise.withResolvers<void>();
  server.once('error', listening.reject);
  server.listen(0, '127.0.0.1', listening.resolve);
  await listening.promise;
  const address = server.address();
  assert(address && typeof address === 'object');
  const closed = Promise.withResolvers<void>();
  server.close(error => error ? closed.reject(error) : closed.resolve());
  await closed.promise;
  return address.port;
}

async function stop(child: Child): Promise<void> {
  if (child.process.exitCode !== null || child.process.signalCode !== null) {
    await child.process.exited; await child.output; return;
  }
  let forced = false;
  const force = setTimeout(() => { forced = true; child.process.kill('SIGKILL'); }, 10_000);
  try {
    child.process.kill('SIGTERM');
    await child.process.exited;
    await child.output;
    assert(!forced, `SIGTERM did not drain child ${child.process.pid}`);
  } finally {
    clearTimeout(force);
  }
}

async function request<T>(child: Child, path: string, method = 'GET', body?: unknown, key?: string): Promise<{ status: number; body: T }> {
  const response = await fetch(`${child.url}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', 'X-Correlation-Id': 'integration-suite', ...(key ? { 'Idempotency-Key': key } : {}) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(10_000),
  });
  const text = await response.text();
  let payload: T;
  try { payload = JSON.parse(text) as T; }
  catch { throw new Error(`${method} ${path}: HTTP ${response.status}, non-JSON response ${text}`); }
  return { status: response.status, body: payload };
}

function wager(wallet: Wallet, amount = '1.00', kind = 'BET', extra: Partial<Wager> = {}): Wager {
  return {
    providerId: 'integration-provider', externalTransactionId: crypto.randomUUID(),
    playerId: wallet.playerId, walletId: wallet.id, roundId: 'integration-round', gameId: 'integration-game',
    kind, money: { amount, currency: 'BRL' }, ...extra,
  };
}
const keyOf = (input: Wager) => `${input.providerId}:${input.externalTransactionId}`;
const envelope = (input: Wager, messageId = crypto.randomUUID()) => ({
  messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
  data: { ...input, idempotencyKey: keyOf(input) },
});

// One serial scenario owns every resource. It must never run against the user's
// application database or share the application's input/output queues.
test('real PostgreSQL + FIFO SQS: financial invariants, races, crash and restart recovery', async () => {
  console.log(`[integration] ${command}`);
  const token = crypto.randomUUID().replaceAll('-', '');
  const dbName = `wager_test_${token}`;
  const databaseUrl = new URL(process.env.DATABASE_URL ?? 'postgresql://wagering:wagering@localhost:5432/wagering');
  const adminUrl = new URL(databaseUrl); adminUrl.pathname = '/postgres';
  databaseUrl.pathname = `/${dbName}`;
  const admin = new SQL({ url: adminUrl.toString(), max: 1, connectionTimeout: 3 });
  const db = new SQL({ url: databaseUrl.toString(), max: 4, connectionTimeout: 3 });
  const sqs = new SQSClient({
    endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
    region: process.env.AWS_REGION ?? 'us-east-1',
    credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
    maxAttempts: 1,
    requestHandler: { connectionTimeout: 2000, requestTimeout: 4000, throwOnRequestTimeout: true },
  });
  const queues: string[] = [];
  const children: Child[] = [];
  let databaseCreated = false;
  let failed = false;
  let env: Record<string, string | undefined> = {};

  async function spawn(script: string, overrides: Record<string, string> = {}): Promise<Child> {
    const assignedPort = await port();
    const process = Bun.spawn([Bun.which('bun') ?? 'bun', script], {
      cwd: root, env: { ...env, PORT: String(assignedPort), ...overrides },
      stdout: 'pipe', stderr: 'pipe', stdin: 'ignore',
    });
    const child: Child = { process, logs: '', output: Promise.resolve(), url: `http://127.0.0.1:${assignedPort}` };
    const pump = async (stream: ReadableStream<Uint8Array>) => {
      const decoder = new TextDecoder();
      for await (const chunk of stream) child.logs = (child.logs + decoder.decode(chunk, { stream: true })).slice(-200_000);
    };
    child.output = Promise.all([pump(process.stdout as ReadableStream<Uint8Array>), pump(process.stderr as ReadableStream<Uint8Array>)]).then(() => {});
    children.push(child);
    return child;
  }
  async function app(workers = false, overrides: Record<string, string> = {}): Promise<Child> {
    const child = await spawn('src/main.ts', { WORKERS_ENABLED: String(workers), ...overrides });
    await until(`app ${child.process.pid} liveness`, async () => {
      if (child.process.exitCode !== null) throw new Error(`App exited ${child.process.exitCode}: ${child.logs}`);
      try { return (await fetch(`${child.url}/health/live`, { signal: AbortSignal.timeout(500) })).ok || undefined; }
      catch { return undefined; }
    }, 15_000);
    return child;
  }
  async function runMigration(direction: 'up' | 'down'): Promise<void> {
    const process = Bun.spawn([Bun.which('bun') ?? 'bun', 'src/infrastructure/database/migrate.ts', direction], {
      cwd: root, env, stdout: 'pipe', stderr: 'pipe', stdin: 'ignore',
    });
    const child: Child = { process, logs: '', output: Promise.resolve(), url: '' };
    child.output = Promise.all([new Response(process.stdout).text(), new Response(process.stderr).text()])
      .then(parts => { child.logs = parts.join('\n'); });
    children.push(child);
    await deadline(process.exited, `migration ${direction}`, 20_000);
    await child.output;
    assert.equal(process.exitCode, 0, `Migration ${direction}: ${child.logs}`);
  }
  async function createWallet(child: Child, amount = '100.00'): Promise<Wallet> {
    const response = await request<Wallet>(child, '/wallets', 'POST', {
      playerId: crypto.randomUUID(), initialBalance: { amount, currency: 'BRL' },
    });
    assert.equal(response.status, 201, JSON.stringify(response.body));
    assert.equal(response.body.balance.amount, amount);
    return response.body;
  }
  async function submit(child: Child, input: Wager, key = keyOf(input)) {
    return request<Result>(child, '/wagering/transactions', 'POST', input, key);
  }
  async function scalar(query: string, parameters: unknown[] = []): Promise<string> {
    const rows = await db.unsafe(query, parameters);
    return String(Object.values(rows[0])[0]);
  }
  async function send(value: unknown): Promise<void> {
    await sqs.send(new SendMessageCommand({
      QueueUrl: env.SQS_QUEUE_URL!, MessageBody: JSON.stringify(value),
      MessageGroupId: crypto.randomUUID(), MessageDeduplicationId: crypto.randomUUID(),
    }));
  }
  async function dlq(messageId: string) {
    return until(`DLQ message ${messageId}`, async () => {
      const result = await sqs.send(new ReceiveMessageCommand({
        QueueUrl: env.SQS_DLQ_URL!, MaxNumberOfMessages: 10, WaitTimeSeconds: 0,
        MessageAttributeNames: ['All'], VisibilityTimeout: 1,
      }));
      let found;
      for (const message of result.Messages ?? []) {
        const body = JSON.parse(message.Body!);
        if (body.messageId === messageId) found = message;
        await sqs.send(new DeleteMessageCommand({ QueueUrl: env.SQS_DLQ_URL!, ReceiptHandle: message.ReceiptHandle! }));
      }
      return found;
    }, 15_000);
  }
  async function faultOn(): Promise<void> {
    await db.unsafe(`
      create function integration_fail_outbox() returns trigger language plpgsql as $$
      begin raise exception 'integration transient fault' using errcode = '40001'; end $$;
      create trigger integration_fail_outbox before insert on outbox_messages
      for each row execute function integration_fail_outbox();
    `);
  }
  async function faultOff(): Promise<void> {
    await db.unsafe('drop trigger if exists integration_fail_outbox on outbox_messages; drop function if exists integration_fail_outbox()');
  }
  async function balance(wallet: Wallet, expected: string, child: Child): Promise<void> {
    const result = await request<Wallet>(child, `/wallets/${wallet.id}`);
    assert.equal(result.status, 200);
    assert.equal(result.body.balance.amount, expected);
    assert.equal(await scalar('select balance::text from wallets where id=$1', [wallet.id]), expected);
  }

  try {
    console.log('[integration] isolated database/queues; reversible migrations');
    await admin.unsafe(`create database "${dbName}"`);
    databaseCreated = true;
    const makeQueue = async (suffix: string, attributes: Record<string, string> = {}) => {
      const result = await sqs.send(new CreateQueueCommand({
        QueueName: `test-${token}-${suffix}.fifo`,
        Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false', VisibilityTimeout: '1', ...attributes },
      }));
      assert(result.QueueUrl);
      const queue = new URL(result.QueueUrl);
      const endpoint = new URL(process.env.SQS_ENDPOINT ?? 'http://localhost:4566');
      queue.protocol = endpoint.protocol; queue.host = endpoint.host;
      const accessibleUrl = queue.toString();
      queues.push(accessibleUrl); return accessibleUrl;
    };
    const dead = await makeQueue('dlq');
    const arn = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dead, AttributeNames: ['QueueArn'] }));
    assert(arn.Attributes?.QueueArn);
    const main = await makeQueue('main', {
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn.Attributes.QueueArn, maxReceiveCount: '10' }),
    });
    const events = await makeQueue('events');
    env = {
      ...process.env, DATABASE_URL: databaseUrl.toString(), SQS_ENDPOINT: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
      AWS_REGION: process.env.AWS_REGION ?? 'us-east-1', AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      SQS_QUEUE_URL: main, SQS_DLQ_URL: dead, SQS_EVENTS_QUEUE_URL: events,
      WORKERS_ENABLED: 'false', WORKER_INTERVAL_MS: '20', REFERENCE_RETRY_BASE_MS: '20',
      REFERENCE_MAX_ATTEMPTS: '3', REFERENCE_TTL_MS: '60000', SQS_VISIBILITY_TIMEOUT: '1', SQS_MAX_ATTEMPTS: '3',
    };
    await runMigration('up');
    assert.equal(await scalar("select count(*)::text from information_schema.tables where table_schema='public' and table_name in ('wallets','wager_transactions','wallet_ledger_entries','inbox_messages','outbox_messages','accounting_postings')"), '6');
    await runMigration('down');
    assert.equal(await scalar("select (to_regclass('public.accounting_postings') is null)::text"), 'true');
    assert.equal(await scalar("select (to_regclass('public.wallet_ledger_entries') is not null)::text"), 'true');
    await runMigration('down');
    assert.equal(await scalar("select (to_regclass('public.wallets') is not null)::text"), 'true');
    assert.equal(await scalar("select (to_regclass('public.wallet_ledger_entries') is null)::text"), 'true');
    await runMigration('down');
    assert.equal(await scalar("select (to_regclass('public.wallets') is null)::text"), 'true');
    await runMigration('up');

    let apps = await Promise.all([app(), app(), app()]);
    assert.equal(new Set(apps.map(child => child.process.pid)).size, 3);
    const first = apps[0]!;
    assert.equal((await fetch(`${first.url}/health/ready`)).status, 200);
    console.log('[integration] exact decimals, validation, immutable database audit');
    const exact = await createWallet(first, '9007199254740993.00');
    const cent = wager(exact, '0.01');
    const exactResult = await submit(first, cent);
    assert.equal(exactResult.status, 200);
    assert.equal(exactResult.body.balance.amount, '9007199254740992.99');
    console.log('[integration] historical double-entry backfill preserves wallet, journal and event history');
    const journalBefore = await db.unsafe('select id,wallet_id,transaction_id,direction,amount::text,currency,balance_before::text,balance_after::text,wallet_version,created_at::text from wallet_ledger_entries order by id');
    const postingsBefore = await db.unsafe('select ledger_entry_id,account_type,account_reference,direction,amount::text,currency,created_at::text from accounting_postings order by ledger_entry_id,direction');
    const walletBefore = await db.unsafe('select id,balance::text,version,updated_at::text from wallets order by id');
    const eventsBefore = await scalar('select count(*)::text from outbox_messages');
    await runMigration('down');
    assert.equal(await scalar("select (to_regclass('public.accounting_postings') is null)::text"), 'true');
    assert.deepEqual(await db.unsafe('select id,wallet_id,transaction_id,direction,amount::text,currency,balance_before::text,balance_after::text,wallet_version,created_at::text from wallet_ledger_entries order by id'), journalBefore);
    await runMigration('up');
    assert.deepEqual(await db.unsafe('select ledger_entry_id,account_type,account_reference,direction,amount::text,currency,created_at::text from accounting_postings order by ledger_entry_id,direction'), postingsBefore);
    assert.deepEqual(await db.unsafe('select id,balance::text,version,updated_at::text from wallets order by id'), walletBefore);
    assert.equal(await scalar('select count(*)::text from outbox_messages'), eventsBefore);
    const duplicateWallet = await request(first, '/wallets', 'POST', { playerId: exact.playerId, initialBalance: { amount: '0.00', currency: 'BRL' } });
    assert.equal(duplicateWallet.status, 409);
    for (const amount of [0.01, '0.001', '-1.00', '1e2', 'NaN']) {
      assert.equal((await submit(first, { ...cent, money: { amount, currency: 'BRL' } } as Wager)).status, 400);
    }
    assert.equal((await request(first, '/wagering/transactions', 'POST', cent)).status, 400);
    assert.equal((await submit(first, { ...cent, money: { amount: '0.02', currency: 'BRL' } })).status, 409);
    assert.equal((await submit(first, { ...cent, externalTransactionId: crypto.randomUUID() }, keyOf(cent))).status, 409);
    assert.equal((await submit(first, { ...cent, walletId: crypto.randomUUID() })).status, 409,
      'An established idempotency identity takes precedence over a changed missing wallet');
    assert.equal((await submit(first, { ...cent, externalTransactionId: crypto.randomUUID(), money: { amount: '1.00', currency: 'USD' } })).status, 422);
    const loss = await submit(first, wager(exact, '0.00', 'LOSS'));
    assert.equal(loss.status, 200);
    assert.equal(await scalar('select count(*)::text from wallet_ledger_entries where transaction_id=$1', [loss.body.transactionId]), '0');
    assert.equal(await scalar("select count(*)::text from outbox_messages where event_type='WalletBalanceChanged' and payload::text like $1", [`%${loss.body.transactionId}%`]), '0');
    for (const statement of [
      'update wallet_ledger_entries set amount=amount+1',
      'delete from wallet_ledger_entries', 'truncate wallet_ledger_entries',
      `update wallets set balance=-1 where id='${exact.id}'`,
      `update wallets set balance=balance+1 where id='${exact.id}'`,
      `update wager_transactions set amount=amount+1 where id='${exactResult.body.transactionId}'`,
      `delete from wager_transactions where id='${exactResult.body.transactionId}'`,
      "update outbox_messages set payload='{}'::jsonb",
      `insert into wallet_ledger_entries select gen_random_uuid(),wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after+1,wallet_version,created_at from wallet_ledger_entries limit 1`,
    ]) await assert.rejects(async () => { await db.unsafe(statement); }, `Database must reject: ${statement}`);
    await balance(exact, '9007199254740992.99', first);
    for (const statement of [
      'update accounting_postings set amount=amount+0.01',
      'delete from accounting_postings', 'truncate accounting_postings',
      "insert into accounting_postings select ledger_entry_id,account_type,account_reference,direction,amount,'USD',created_at from accounting_postings where currency='BRL' limit 1",
      "insert into accounting_postings select ledger_entry_id,account_type,'wrong-account',direction,amount,currency,created_at from accounting_postings limit 1",
    ]) await assert.rejects(async () => { await db.unsafe(statement); },
      error => typeof error === 'object' && error !== null && 'errno' in error && error.errno === '23514');

    const atomicBet = wager(exact, '1.00');
    const atomicCounts = await db.unsafe('select (select count(*) from wallet_ledger_entries)::text as journals,(select count(*) from accounting_postings)::text as postings,(select count(*) from outbox_messages)::text as events');
    await db.unsafe(`
      create function integration_skip_posting() returns trigger language plpgsql as $$
      begin if NEW.direction = 'CREDIT' then return null; end if; return NEW; end $$;
      create trigger integration_skip_posting before insert on accounting_postings
      for each row execute function integration_skip_posting();
    `);
    try {
      assert.equal((await submit(first, atomicBet)).status, 500, 'A missing accounting side must fail the deferred commit');
      assert.equal(await scalar('select count(*)::text from wager_transactions where external_transaction_id=$1', [atomicBet.externalTransactionId]), '0');
      assert.deepEqual(await db.unsafe('select (select count(*) from wallet_ledger_entries)::text as journals,(select count(*) from accounting_postings)::text as postings,(select count(*) from outbox_messages)::text as events'), atomicCounts);
      await balance(exact, '9007199254740992.99', first);
    } finally {
      await db.unsafe('drop trigger integration_skip_posting on accounting_postings; drop function integration_skip_posting()');
    }

    console.log('[integration] 50-way idempotency, 80+80 hot wallet, independent-wallet races across 3 Bun processes');
    const duplicate = await createWallet(first);
    const bet = wager(duplicate, '25.00');
    const repeated = await Promise.all(Array.from({ length: 50 }, (_, index) => submit(apps[index % 3]!, bet)));
    assert(repeated.every(result => result.status === 200), JSON.stringify(repeated));
    assert.equal(new Set(repeated.map(result => result.body.transactionId)).size, 1);
    assert.equal(repeated.filter(result => !result.body.idempotentReplay).length, 1);
    assert.equal(await scalar("select count(*)::text from wallet_ledger_entries where wallet_id=$1 and direction='DEBIT'", [duplicate.id]), '1');
    await balance(duplicate, '75.00', first);
    const hot = await createWallet(first);
    const hotBets = [wager(hot, '80.00'), wager(hot, '80.00')];
    const raced = await Promise.all(hotBets.map((input, index) => submit(apps[index]!, input)));
    assert.deepEqual(raced.map(result => result.status).sort(), [200, 422]);
    assert.equal(raced.find(result => result.status === 422)!.body.failureCode, 'INSUFFICIENT_FUNDS');
    await Promise.all(hotBets.map((input, index) => submit(apps[(index + 1) % 3]!, input)));
    await balance(hot, '20.00', first);
    assert.equal(await scalar("select count(*)::text from wallet_ledger_entries where wallet_id=$1 and direction='DEBIT'", [hot.id]), '1');
    const independent = await Promise.all(Array.from({ length: 9 }, (_, index) => createWallet(apps[index % 3]!)));
    const independentResults = await Promise.all(independent.flatMap((wallet, index) =>
      Array.from({ length: 6 }, (_, operation) => submit(apps[(index + operation) % 3]!, wager(wallet, '0.10')))));
    assert(independentResults.every(result => result.status === 200), JSON.stringify(independentResults));
    await Promise.all(independent.map(wallet => balance(wallet, '99.40', first)));
    console.log('[integration] simultaneous unique refunds and debit rollbacks consume each reference once');
    const reversalRaceWallet = await createWallet(first);
    const raceBet = wager(reversalRaceWallet, '25.00');
    const raceWin = wager(reversalRaceWallet, '25.00', 'WIN');
    assert.equal((await submit(first, raceBet)).status, 200);
    assert.equal((await submit(first, raceWin)).status, 200);
    for (const scenario of [
      { kind: 'REFUND', reference: raceBet, expectedBalance: '125.00' },
      { kind: 'ROLLBACK', reference: raceWin, expectedBalance: '100.00' },
    ]) {
      const commands = Array.from({ length: 20 }, () => wager(reversalRaceWallet, '25.00', scenario.kind,
        { referenceExternalTransactionId: scenario.reference.externalTransactionId }));
      const results = await Promise.all(commands.map((input, index) => submit(apps[index % 3]!, input)));
      assert.equal(results.filter(result => result.status === 200).length, 1);
      const rejected = results.filter(result => result.status !== 200);
      assert.equal(rejected.length, 19);
      assert(rejected.every(result => result.status === 422 && result.body.failureCode === 'ALREADY_REVERSED'));
      assert.equal(await scalar("select count(*)::text from wager_transactions where reference_external_transaction_id=$1 and kind=$2 and status='PROCESSED'",
        [scenario.reference.externalTransactionId, scenario.kind]), '1');
      await balance(reversalRaceWallet, scenario.expectedBalance, first);
    }
    assert.equal((await request<Wallet>(first, `/wallets/${reversalRaceWallet.id}`)).body.version, 5);
    assert.equal(await scalar('select count(*)::text from wallet_ledger_entries where wallet_id=$1', [reversalRaceWallet.id]), '5');
    assert.equal(await scalar('select count(*)::text from accounting_postings p join wallet_ledger_entries l on l.id=p.ledger_entry_id where l.wallet_id=$1',
      [reversalRaceWallet.id]), '10');
    assert.equal((await submit(first, wager(duplicate, '1.00', 'WIN'))).status, 200);
    await balance(duplicate, '76.00', first);
    const replay = await submit(first, bet);
    assert.equal(replay.body.balance.amount, '75.00');
    assert.equal(replay.body.idempotentReplay, true);
    assert.equal((await request(first, `/wagering/transactions/${replay.body.transactionId}`)).status, 200);
    assert.equal((await request(first, `/providers/${bet.providerId}/wagering/transactions/${bet.externalTransactionId}`)).status, 200);
    const page = await request<{ items: Journal[]; nextCursor: string | null }>(first, `/wallets/${independent[0]!.id}/ledger?limit=2`);
    assert.equal(page.status, 200);
    assert.equal(page.body.items.length, 2);
    assert(page.body.nextCursor);
    const next = await request<{ items: { id: string }[] }>(first, `/wallets/${independent[0]!.id}/ledger?limit=2&cursor=${encodeURIComponent(page.body.nextCursor)}`);
    assert.equal(next.status, 200);
    assert(!next.body.items.some(entry => page.body.items.some(prior => prior.id === entry.id)));
    for (const journal of page.body.items) {
      assert.equal(journal.postings.length, 2);
      assert.deepEqual(new Set(journal.postings.map(posting => posting.direction)), new Set(['DEBIT', 'CREDIT']));
      assert(journal.postings.every(posting => posting.money.amount === journal.money.amount && posting.money.currency === journal.money.currency));
      const liability = journal.postings.find(posting => posting.accountType === 'WALLET_LIABILITY');
      assert.equal(liability?.accountReference, independent[0]!.id);
      assert.equal(liability?.direction, journal.direction);
    }

    console.log('[integration] zero opening, funded/unfunded reversals, rejected replay and optional references');
    const zero = await createWallet(first, '0.00');
    assert.equal(zero.version, 1);
    assert.equal(await scalar('select count(*)::text from wallet_ledger_entries where wallet_id=$1', [zero.id]), '0');
    const emptyBook = await request<Reconciliation>(first, `/wallets/${zero.id}/reconciliation`, 'POST');
    assert.equal(emptyBook.body.consistent, true);
    assert.equal(emptyBook.body.accounting.postingCount, 0);
    assert.equal(emptyBook.body.accounting.debits.amount, '0.00');
    assert.equal(emptyBook.body.accounting.credits.amount, '0.00');
    const paidWin = wager(zero, '20.00', 'WIN');
    const spentBet = wager(zero, '20.00');
    assert.equal((await submit(first, paidWin)).status, 200);
    assert.equal((await submit(first, spentBet)).status, 200);
    const unfunded = wager(zero, '20.00', 'ROLLBACK', { referenceExternalTransactionId: paidWin.externalTransactionId });
    const rejectedReversal = await submit(first, unfunded);
    assert.equal(rejectedReversal.status, 422);
    assert.equal(rejectedReversal.body.failureCode, 'REVERSAL_INSUFFICIENT_FUNDS');
    assert.equal(await scalar('select count(*)::text from wallet_ledger_entries where transaction_id=$1', [rejectedReversal.body.transactionId]), '0');
    assert.equal((await submit(first, wager(zero, '5.00', 'WIN'))).status, 200);
    const rejectedReplay = await submit(first, unfunded);
    assert.equal(rejectedReplay.status, 422);
    assert.equal(rejectedReplay.body.idempotentReplay, true);
    assert.equal(rejectedReplay.body.balance.amount, '0.00');
    await balance(zero, '5.00', first);
    const partial = await submit(first, wager(zero, '19.00', 'REFUND', { referenceExternalTransactionId: spentBet.externalTransactionId }));
    assert.equal(partial.body.failureCode, 'REFERENCE_AMOUNT_MISMATCH');
    const paidRefund = wager(zero, '20.00', 'REFUND', { referenceExternalTransactionId: spentBet.externalTransactionId });
    assert.equal((await submit(first, paidRefund)).status, 200);
    assert.equal((await submit(first, wager(zero, '20.00', 'ROLLBACK', { referenceExternalTransactionId: paidRefund.externalTransactionId }))).status, 200);
    assert.equal((await submit(first, wager(zero, '20.00', 'ROLLBACK', { referenceExternalTransactionId: spentBet.externalTransactionId }))).status, 200);
    const referencedWin = wager(zero, '2.00', 'WIN', { referenceExternalTransactionId: spentBet.externalTransactionId, gameId: 'different-game' });
    assert.equal((await submit(first, referencedWin)).status, 200);
    const beforeLoss = await request<Wallet>(first, `/wallets/${zero.id}`);
    assert.equal((await submit(first, wager(zero, '0.00', 'LOSS', { referenceExternalTransactionId: spentBet.externalTransactionId }))).status, 200);
    assert.equal((await request<Wallet>(first, `/wallets/${zero.id}`)).body.version, beforeLoss.body.version);
    await balance(zero, '27.00', first);

    const maximum = await createWallet(first, '9999999999999999999999999999.99');
    const overflow = await submit(first, wager(maximum, '0.01', 'WIN'));
    assert.equal(overflow.status, 422);
    assert.equal(overflow.body.failureCode, 'BALANCE_LIMIT_EXCEEDED');
    await balance(maximum, '9999999999999999999999999999.99', first);
    assert.equal((await submit(first, wager(maximum, '9999999999999999999999999999.99'))).status, 200);
    assert.equal((await submit(first, wager(maximum, '9999999999999999999999999999.99', 'WIN'))).status, 200);
    const turnover = await request<Reconciliation>(first, `/wallets/${maximum.id}/reconciliation`, 'POST');
    assert.equal(turnover.body.consistent, true);
    assert.equal(turnover.body.accounting.debits.amount, '29999999999999999999999999999.97');
    assert.equal(turnover.body.accounting.credits.amount, turnover.body.accounting.debits.amount);
    const foreignWallet = await request<Wallet>(first, '/wallets', 'POST', {
      playerId: crypto.randomUUID(), initialBalance: { amount: '10.00', currency: 'USD' },
    });
    assert.equal(foreignWallet.status, 201);
    assert.equal((await submit(first, wager(foreignWallet.body, '1.00', 'BET', { money: { amount: '1.00', currency: 'USD' } }))).status, 200);

    console.log('[integration] out-of-order REFUND/ROLLBACK and missing reference expiration');
    const reversedWallet = await createWallet(first);
    const futureBet = wager(reversedWallet, '10.00');
    const refund = wager(reversedWallet, '10.00', 'REFUND', { referenceExternalTransactionId: futureBet.externalTransactionId });
    const pending = await submit(first, refund);
    assert.equal(pending.status, 202);
    assert.equal((await submit(first, futureBet)).status, 200);
    const futureWin = wager(reversedWallet, '5.00', 'WIN');
    const rollback = wager(reversedWallet, '5.00', 'ROLLBACK', { referenceExternalTransactionId: futureWin.externalTransactionId });
    const pendingRollback = await submit(first, rollback);
    assert.equal(pendingRollback.status, 202);
    assert.equal((await submit(first, futureWin)).status, 200);
    const missing = await submit(first, wager(reversedWallet, '1.00', 'REFUND', { referenceExternalTransactionId: 'never-exists' }));
    assert.equal(missing.status, 202);
    const unresolvedWin = wager(reversedWallet, '5.00', 'WIN', { referenceExternalTransactionId: 'not-arrived-bet' });
    assert.equal((await submit(first, unresolvedWin)).status, 202);
    for (const [input, failureCode] of [
      [wager(reversedWallet, '5.00', 'REFUND', { referenceExternalTransactionId: unresolvedWin.externalTransactionId }), 'REFERENCE_KIND_INVALID'],
      [wager(hot, '5.00', 'ROLLBACK', { referenceExternalTransactionId: unresolvedWin.externalTransactionId }), 'REFERENCE_MISMATCH'],
      [wager(reversedWallet, '6.00', 'ROLLBACK', { referenceExternalTransactionId: unresolvedWin.externalTransactionId }), 'REFERENCE_AMOUNT_MISMATCH'],
    ] as const) {
      const invalidReference = await submit(first, input);
      assert.equal(invalidReference.status, 422);
      assert.equal(invalidReference.body.failureCode, failureCode);
    }

    console.log('[integration] forced SQL failure rolls back wallet + ledger + inbox + outbox; deterministic crash before ACK');
    const crashedWallet = await createWallet(first);
    const crashInput = wager(crashedWallet, '7.00');
    const crashEnvelope = envelope(crashInput);
    const outboxBefore = await scalar('select count(*)::text from outbox_messages');
    const postingsBeforeFailure = await scalar('select count(*)::text from accounting_postings');
    await faultOn();
    try {
      assert.equal((await submit(first, crashInput)).status, 503);
      await send(crashEnvelope);
      const retryChild = await spawn('test/fixtures/consumer-child.ts');
      await deadline(retryChild.process.exited, 'one-shot consumer exits after transient failure', 25_000);
      await retryChild.output;
      assert.equal(retryChild.process.exitCode, 0, retryChild.logs);
      assert(retryChild.logs.includes('sqs_message_retry'), retryChild.logs);
      assert.equal(await scalar('select count(*)::text from wager_transactions where external_transaction_id=$1', [crashInput.externalTransactionId]), '0');
      assert.equal(await scalar('select count(*)::text from inbox_messages where message_id=$1', [crashEnvelope.messageId]), '0');
      assert.equal(await scalar('select count(*)::text from outbox_messages'), outboxBefore);
      assert.equal(await scalar('select count(*)::text from accounting_postings'), postingsBeforeFailure);
      assert.equal(await scalar("select count(*)::text from wallet_ledger_entries where wallet_id=$1 and direction='DEBIT'", [crashedWallet.id]), '0');
      await balance(crashedWallet, '100.00', first);
    } finally { await faultOff(); }
    const crashChild = await spawn('test/fixtures/consumer-child.ts', { TEST_BLOCK_ACK: 'true' });
    await until('committed consumer blocked before SQS DeleteMessage dispatch', async () => {
      if (crashChild.process.exitCode !== null) throw new Error(crashChild.logs);
      return crashChild.logs.includes('AFTER_COMMIT_BEFORE_ACK') || undefined;
    });
    assert.equal(await scalar('select count(*)::text from inbox_messages where message_id=$1 and processed_at is not null', [crashEnvelope.messageId]), '1');
    await balance(crashedWallet, '93.00', first);
    assert.equal(await scalar('select count(*)::text from outbox_messages where published_at is null'), String(Number(outboxBefore) + 2));
    crashChild.process.kill('SIGKILL'); // Intentional crash: no shutdown/visibility/ACK cleanup can run.
    await crashChild.process.exited;
    await crashChild.output;
    await Promise.all(apps.map(stop));

    console.log('[integration] restart with 3 real concurrent outbox publishers and inbox redelivery recovery');
    apps = await Promise.all([app(true), app(true), app(true)]);
    await until('crashed message redelivery acknowledged', async () => apps.some(child => child.logs.includes('sqs_message_acknowledged') && child.logs.includes(crashEnvelope.messageId)) || undefined);
    await balance(crashedWallet, '93.00', apps[0]!);
    assert.equal(await scalar("select count(*)::text from wallet_ledger_entries where wallet_id=$1 and direction='DEBIT'", [crashedWallet.id]), '1');
    await until('pending reversals and missing-reference expiry', async () => {
      const rows = await db.unsafe('select id,status,failure_code from wager_transactions where id in ($1,$2,$3)', [pending.body.transactionId, pendingRollback.body.transactionId, missing.body.transactionId]);
      return rows.length === 3 && rows.every((row: { status: string }) => ['PROCESSED', 'REJECTED'].includes(row.status)) ? rows : undefined;
    });
    assert.equal(await scalar('select status from wager_transactions where id=$1', [pending.body.transactionId]), 'PROCESSED');
    assert.equal(await scalar('select status from wager_transactions where id=$1', [pendingRollback.body.transactionId]), 'PROCESSED');
    assert.equal(await scalar('select failure_code from wager_transactions where id=$1', [missing.body.transactionId]), 'REFERENCE_NOT_FOUND');
    await balance(reversedWallet, '100.00', apps[0]!);
    assert.equal((await submit(apps[0]!, wager(reversedWallet, '10.00', 'REFUND', { referenceExternalTransactionId: futureBet.externalTransactionId }))).body.failureCode, 'ALREADY_REVERSED');
    await until('all backlog events published', async () => (await scalar('select count(*)::text from outbox_messages where published_at is null')) === '0' || undefined, 20_000);
    assert(apps.filter(child => child.logs.includes('outbox_published')).length >= 2, 'At least two distinct child processes must publish the shared backlog');

    console.log('[integration] persistent inbox duplicate/conflict, permanent DLQ and bounded transient retry/DLQ');
    const inboxWallet = await createWallet(apps[0]!);
    const message = envelope(wager(inboxWallet, '3.00'));
    await Promise.all([send(message), send(message)]);
    await until('both transport duplicates acknowledged', async () => {
      const acknowledgements = apps.reduce((total, child) => total + child.logs.split('\n').filter(line => line.includes('sqs_message_acknowledged') && line.includes(message.messageId)).length, 0);
      return acknowledgements >= 2 || undefined;
    });
    await balance(inboxWallet, '97.00', apps[0]!);
    assert.equal(await scalar('select count(*)::text from inbox_messages where message_id=$1', [message.messageId]), '1');
    await assert.rejects(async () => { await db.unsafe('update inbox_messages set payload_hash=$1 where message_id=$2', ['changed', message.messageId]); });
    await send({ ...message, data: { ...message.data, money: { amount: '4.00', currency: 'BRL' } } });
    const conflict = await dlq(message.messageId);
    assert(conflict.MessageAttributes?.failureCode?.StringValue?.includes('CONFLICT'));
    await balance(inboxWallet, '97.00', apps[0]!);
    const malformedId = crypto.randomUUID();
    await send({ messageId: malformedId, type: 'Unknown' });
    assert((await dlq(malformedId)).MessageAttributes?.failureCode?.StringValue);
    const exhausted = envelope(wager(inboxWallet, '9.00'));
    await faultOn();
    try {
      await send(exhausted);
      const deadLetter = await dlq(exhausted.messageId);
      assert.equal(deadLetter.MessageAttributes?.receiveCount?.StringValue, '3');
      assert.equal(await scalar('select count(*)::text from inbox_messages where message_id=$1', [exhausted.messageId]), '0');
      assert.equal(await scalar('select count(*)::text from wager_transactions where external_transaction_id=$1', [exhausted.data.externalTransactionId]), '0');
      await balance(inboxWallet, '97.00', apps[0]!);
      assert(apps.some(child => child.logs.includes('sqs_message_retry') && child.logs.includes(exhausted.messageId)));
    } finally { await faultOff(); }

    console.log('[integration] durable outbox retry against real missing queue; restart/reconciliation');
    await Promise.all(apps.map(stop));
    const unavailableEvents = events.replace(/[^/]+$/, `test-${token}-absent.fifo`);
    const unavailable = await app(true, { SQS_EVENTS_QUEUE_URL: unavailableEvents });
    const restartWallet = await createWallet(unavailable);
    const restartBet = wager(restartWallet, '1.00');
    assert.equal((await submit(unavailable, restartBet)).status, 200);
    await until('outbox retry attempt durably committed', async () =>
      Number(await scalar('select coalesce(max(attempts),0)::text from outbox_messages where published_at is null')) > 0 || undefined);
    await until('outbox retry structured log', async () => unavailable.logs.includes('outbox_publish_retry') || undefined);
    await stop(unavailable);
    apps = await Promise.all([app(true), app(true), app(true)]);
    await until('restart publishes all durable pending outbox events', async () => (await scalar('select count(*)::text from outbox_messages where published_at is null')) === '0' || undefined, 20_000);
    await balance(restartWallet, '99.00', apps[0]!);
    const stableReplay = await submit(apps[1]!, restartBet);
    assert.equal(stableReplay.status, 200);
    assert.equal(stableReplay.body.idempotentReplay, true);
    assert.equal(stableReplay.body.balance.amount, '99.00');

    const published = await db.unsafe('select id,payload from outbox_messages where published_at is not null');
    const expectedEvents = new Set(published.map((row: { id: string }) => row.id));
    const expectedPayloads = new Map(published.map((row: { id: string; payload: unknown }) => [row.id, row.payload]));
    const receivedEvents = new Set<string>();
    await until('every committed outbox event observed on actual events FIFO queue', async () => {
      const result = await sqs.send(new ReceiveMessageCommand({ QueueUrl: events, MaxNumberOfMessages: 10, WaitTimeSeconds: 0 }));
      for (const message of result.Messages ?? []) {
        const value = JSON.parse(message.Body!);
        assert.equal(value.version, 1); assert.equal(typeof value.eventId, 'string');
        assert.equal(typeof value.correlationId, 'string'); assert.equal(typeof value.occurredAt, 'string');
        assert(expectedEvents.has(value.eventId), `Unexpected event ${value.eventId}`);
        assert.deepEqual(value, expectedPayloads.get(value.eventId), `Persisted event payload ${value.eventId}`);
        receivedEvents.add(value.eventId);
        await sqs.send(new DeleteMessageCommand({ QueueUrl: events, ReceiptHandle: message.ReceiptHandle! }));
      }
      return receivedEvents.size === expectedEvents.size || undefined;
    }, 20_000);
    assert.deepEqual(receivedEvents, expectedEvents);
    const eventTypes = new Set(published.map((row: { payload: { eventType: string } }) => row.payload.eventType));
    for (const name of ['WagerTransactionProcessed', 'WagerTransactionRejected', 'WagerTransactionPendingReference', 'WalletBalanceChanged']) assert(eventTypes.has(name), `Missing ${name}`);
    // Deliberately corrupt only this owned DB through superuser replication mode
    // to prove reconciliation signals divergence without silently repairing it.
    await db.begin(async tx => {
      await tx.unsafe("set local session_replication_role='replica'");
      await tx.unsafe('update wallets set balance=balance+0.01 where id=$1', [restartWallet.id]);
    });
    try {
      const divergent = await request<{ consistent: boolean; difference: { amount: string } }>(apps[0]!, `/wallets/${restartWallet.id}/reconciliation`, 'POST');
      assert.equal(divergent.status, 200);
      assert.equal(divergent.body.consistent, false);
      assert.equal(divergent.body.difference.amount, '0.01');
      await balance(restartWallet, '99.01', apps[0]!);
      await until('reconciliation divergence structured log', async () => apps[0]!.logs.includes('wallet.reconciliation_mismatch') || undefined);
    } finally {
      await db.begin(async tx => {
        await tx.unsafe("set local session_replication_role='replica'");
        await tx.unsafe('update wallets set balance=$1 where id=$2', ['99.00', restartWallet.id]);
      });
    }
    console.log('[integration] accounting-only drift is reported even when wallet and original ledger agree');
    const [driftJournal] = await db.unsafe('select id from wallet_ledger_entries where wallet_id=$1 order by wallet_version desc limit 1', [restartWallet.id]);
    await db.begin(async tx => {
      await tx.unsafe("set local session_replication_role='replica'");
      await tx.unsafe("update accounting_postings set amount=amount+0.01 where ledger_entry_id=$1 and account_type='PROVIDER_CLEARING'", [driftJournal.id]);
    });
    try {
      const drifted = await request<Reconciliation>(apps[0]!, `/wallets/${restartWallet.id}/reconciliation`, 'POST');
      assert.equal(drifted.status, 200);
      assert.equal(drifted.body.difference.amount, '0.00');
      assert.equal(drifted.body.consistent, false);
      assert.equal(drifted.body.accounting.balanced, false);
      assert.equal(drifted.body.accounting.consistent, false);
    } finally {
      await db.begin(async tx => {
        await tx.unsafe("set local session_replication_role='replica'");
        await tx.unsafe("update accounting_postings p set amount=l.amount from wallet_ledger_entries l where l.id=p.ledger_entry_id and l.id=$1", [driftJournal.id]);
      });
    }
    console.log('[integration] broken balance/version history and oversized reconstruction are reported without repair');
    const [lastMovement] = await db.unsafe(
      'select id,balance_before::text,balance_after::text,wallet_version from wallet_ledger_entries where id=$1', [driftJournal.id]);
    const [openingMovement] = await db.unsafe(
      'select id,balance_before::text,balance_after::text from wallet_ledger_entries where wallet_id=$1 and wallet_version=1', [restartWallet.id]);
    const [largeDebit] = await db.unsafe(
      "select id,balance_before::text,balance_after::text from wallet_ledger_entries where wallet_id=$1 and direction='DEBIT'", [maximum.id]);
    const historyCases = [
      { wallet: restartWallet, table: 'wallet_ledger_entries', id: openingMovement.id,
        changes: 'balance_before=balance_before+0.01,balance_after=balance_after+0.01',
        restore: 'balance_before=$1,balance_after=$2', values: [openingMovement.balance_before, openingMovement.balance_after],
        calculated: '99.00', difference: '0.00' },
      { wallet: restartWallet, table: 'wallet_ledger_entries', id: lastMovement.id,
        changes: 'wallet_version=wallet_version+2',
        restore: 'wallet_version=$1', values: [lastMovement.wallet_version],
        calculated: '99.00', difference: '0.00' },
      { wallet: restartWallet, table: 'wallets', id: restartWallet.id,
        changes: 'version=version+1', restore: 'version=$1', values: [lastMovement.wallet_version],
        calculated: '99.00', difference: '0.00' },
      { wallet: maximum, table: 'wallet_ledger_entries', id: largeDebit.id,
        changes: "direction='CREDIT',balance_before=0.00,balance_after=amount",
        restore: "direction='DEBIT',balance_before=$1,balance_after=$2", values: [largeDebit.balance_before, largeDebit.balance_after],
        calculated: '29999999999999999999999999999.97', difference: '-19999999999999999999999999999.98' },
    ];
    const historyResponses: Array<{ status: number; body: Reconciliation }> = [];
    for (const scenario of historyCases) {
      await db.begin(async tx => {
        await tx.unsafe("set local session_replication_role='replica'");
        await tx.unsafe(`update ${scenario.table} set ${scenario.changes} where id=$1`, [scenario.id]);
      });
      try {
        const snapshotQuery = `select row_to_json(audit_row)::text from ${scenario.table} audit_row where id=$1`;
        const snapshot = await scalar(snapshotQuery, [scenario.id]);
        historyResponses.push(await request<Reconciliation>(apps[0]!, `/wallets/${scenario.wallet.id}/reconciliation`, 'POST'));
        assert.equal(await scalar(snapshotQuery, [scenario.id]), snapshot, 'Reconciliation must not repair financial history');
      } finally {
        await db.begin(async tx => {
          await tx.unsafe("set local session_replication_role='replica'");
          await tx.unsafe(`update ${scenario.table} set ${scenario.restore} where id=$${scenario.values.length + 1}`,
            [...scenario.values, scenario.id]);
        });
      }
    }
    assert.deepEqual(historyResponses.map(result => ({
      status: result.status, consistent: result.body.consistent,
      calculated: result.body.calculatedBalance?.amount, difference: result.body.difference?.amount,
    })), historyCases.map(scenario => ({
      status: 200, consistent: false, calculated: scenario.calculated, difference: scenario.difference,
    })));
    await balance(restartWallet, '99.00', apps[0]!);
    await balance(maximum, '9999999999999999999999999999.99', apps[0]!);
    const brokenJournals = await db.unsafe(`
      select l.id from wallet_ledger_entries l left join accounting_postings p on p.ledger_entry_id=l.id
      group by l.id having count(p.ledger_entry_id)<>2
        or sum(p.amount) filter (where p.direction='DEBIT') is distinct from l.amount
        or sum(p.amount) filter (where p.direction='CREDIT') is distinct from l.amount
        or bool_and(p.currency=l.currency and p.created_at=l.created_at) is distinct from true
    `);
    assert.equal(brokenJournals.length, 0, 'Every financial journal has one exact balanced pair in its currency');
    const trial = await db.unsafe('select currency,sum(debits)::text as debits,sum(credits)::text as credits from accounting_trial_balance group by currency');
    assert.deepEqual(new Set(trial.map(row => row.currency)), new Set(['BRL', 'USD']));
    for (const currency of trial) assert.equal(currency.debits, currency.credits);
    const invariants = await db.unsafe(`
      select w.id,w.balance::text,coalesce(sum(case when l.direction='CREDIT' then l.amount else -l.amount end),0)::numeric(30,2)::text as calculated
      from wallets w left join wallet_ledger_entries l on l.wallet_id=w.id group by w.id
    `);
    for (const row of invariants) {
      assert.equal(row.balance, row.calculated, `SQL reconstruction wallet ${row.id}`);
      const reconciliation = await request<Reconciliation>(apps[0]!, `/wallets/${row.id}/reconciliation`, 'POST');
      assert.equal(reconciliation.status, 200);
      assert.equal(reconciliation.body.consistent, true, `HTTP reconciliation wallet ${row.id}`);
      assert.equal(reconciliation.body.difference.amount, '0.00');
      assert.equal(reconciliation.body.storedBalance.amount, row.balance);
      assert.equal(reconciliation.body.calculatedBalance.amount, row.balance);
      assert.equal(reconciliation.body.accounting.balanced, true);
      assert.equal(reconciliation.body.accounting.consistent, true);
      assert.equal(reconciliation.body.accounting.walletBalance.amount, row.balance);
      assert.equal(reconciliation.body.accounting.debits.amount, reconciliation.body.accounting.credits.amount);
      assert.equal(reconciliation.body.accounting.postingCount, reconciliation.body.checkedEntries * 2);
      assert.equal(reconciliation.body.accounting.journalCount, reconciliation.body.checkedEntries);
    }
    const metrics = await fetch(`${apps[0]!.url}/metrics`);
    assert.equal(metrics.status, 200);
    assert.match(await metrics.text(), /^wagering_reconciliation_mismatches_total 6$/m);
    console.log(`[integration] PASS: ${invariants.length} wallets reconciled; ${receivedEvents.size} committed events delivered`);
  } catch (error) {
    failed = true;
    for (const child of children) console.error(`[integration child ${child.process.pid} exit=${child.process.exitCode}]\n${child.logs.slice(-8000)}`);
    throw error;
  } finally {
    const cleanupErrors: unknown[] = [];
    const capture = async (operation: () => Promise<unknown>) => { try { await operation(); } catch (error) { cleanupErrors.push(error); } };
    await Promise.all(children.map(child => capture(() => stop(child))));
    await capture(() => db.close({ timeout: 2 }));
    await Promise.all(queues.map(QueueUrl => capture(() => sqs.send(new DeleteQueueCommand({ QueueUrl })))));
    sqs.destroy();
    if (databaseCreated) await capture(async () => {
      await admin.unsafe('select pg_terminate_backend(pid) from pg_stat_activity where datname=$1 and pid<>pg_backend_pid()', [dbName]);
      await admin.unsafe(`drop database "${dbName}"`);
    });
    await capture(() => admin.close({ timeout: 2 }));
    if (cleanupErrors.length) {
      if (failed) console.error('[integration] cleanup failures', cleanupErrors);
      else throw new AggregateError(cleanupErrors, 'Integration resource cleanup failed');
    }
  }
}, 120_000);
