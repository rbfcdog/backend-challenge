import { Inject, Injectable, Logger } from '@nestjs/common';
import { IsolationLevel } from '@mikro-orm/core';
import type { EntityManager } from '@mikro-orm/postgresql';
import type { AppConfig } from '../config';
import { Money } from '../domain/money';
import { DomainError, LedgerDirection, Wallet, WalletLedgerEntry, WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../domain/wager';
import { WalletBalanceChanged, WagerTransactionPendingReference, WagerTransactionProcessed, WagerTransactionRejected } from '../domain/events';
import type { IntegrationEvent } from '../domain/events';
import { InboxMessage, OutboxMessage } from '../domain/messages';
import { APP_CONFIG, InfrastructureService } from '../infrastructure/infrastructure.service';
import { Metrics } from '../metrics';
import { AppError, canonicalHash, databaseErrorCode, isTransientInfrastructure, parseWager, text, uuid } from './contracts';
import type { ProcessingContext, ProcessResult, WagerInput } from './contracts';

interface WalletRow {
  id: string; player_id: string; currency: string; balance: string; version: number;
  created_at: string; updated_at: string;
}
interface TransactionRow {
  id: string; provider_id: string; external_transaction_id: string; idempotency_key: string;
  payload_hash: string; wallet_id: string; player_id: string; round_id: string; game_id: string;
  kind: WagerTransactionKind; amount: string; currency: string; status: WagerTransactionStatus;
  reference_external_transaction_id: string | null; reference_transaction_id: string | null;
  failure_code: string | null; balance_after: string | null; reference_attempts: number;
  next_attempt_at: string; correlation_id: string; created_at: string; processed_at: string | null;
}
interface LedgerRow {
  id: string; wallet_id: string; transaction_id: string; direction: LedgerDirection;
  amount: string; currency: string; balance_before: string; balance_after: string;
  wallet_version: number; created_at: string;
}

interface AccountingPostingRow {
  ledger_entry_id: string;
  account_type: 'WALLET_LIABILITY' | 'PROVIDER_CLEARING' | 'OPENING_CLEARING';
  account_reference: string;
  direction: LedgerDirection;
  amount: string;
  currency: string;
}

const maximum = Money.from({ amount: '9999999999999999999999999999.99', currency: 'BRL' });

async function insert(em: EntityManager, table: string, values: Record<string, unknown>, conflict = '') {
  const keys = Object.keys(values);
  return em.execute(`insert into ${table} (${keys.join(',')}) values (${keys.map(() => '?').join(',')}) ${conflict} returning *`, Object.values(values));
}

@Injectable()
export class ProcessorService {
  private readonly logger = new Logger(ProcessorService.name);

  constructor(
    @Inject(InfrastructureService) private readonly infrastructure: InfrastructureService,
    @Inject(Metrics) private readonly metrics: Metrics,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
  ) {}

  private async transaction<T>(work: (em: EntityManager) => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.infrastructure.db.em.fork().transactional(work);
      } catch (error) {
        const code = databaseErrorCode(error);
        if (['40P01', '40001', '55P03'].includes(code)) {
          this.metrics.increment('wagering_lock_conflicts_total');
          if (attempt < 2) {
            this.metrics.increment('wagering_retries_total', { worker: 'transaction' });
            await Bun.sleep(10 * 2 ** attempt);
            continue;
          }
        }
        if (error instanceof AppError || error instanceof DomainError) throw error;
        if (code === '23505') throw new AppError(409, 'CONFLICT');
        if (isTransientInfrastructure(error)) throw new AppError(503, 'INFRASTRUCTURE_UNAVAILABLE');
        throw error;
      }
    }
  }

  private async walletRow(em: EntityManager, id: string, lock = false): Promise<WalletRow> {
    const rows = await em.execute<WalletRow[]>(`select * from wallets where id = ?${lock ? ' for update' : ''}`, [id]);
    if (!rows[0]) throw new AppError(404, 'WALLET_NOT_FOUND');
    return rows[0];
  }

  private wallet(row: WalletRow): Wallet {
    return Wallet.rehydrate({
      id: row.id, playerId: row.player_id, currency: row.currency,
      balance: Money.rehydrate({ amount: row.balance, currency: row.currency }), version: row.version,
      createdAt: new Date(row.created_at), updatedAt: new Date(row.updated_at),
    });
  }

  private wager(row: TransactionRow): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: row.id, providerId: row.provider_id, externalTransactionId: row.external_transaction_id,
      idempotencyKey: row.idempotency_key, payloadHash: row.payload_hash, walletId: row.wallet_id,
      playerId: row.player_id, roundId: row.round_id, gameId: row.game_id, kind: row.kind,
      money: Money.rehydrate({ amount: row.amount, currency: row.currency }),
      createdAt: new Date(row.created_at), status: row.status,
      ...(row.reference_external_transaction_id ? { referenceExternalTransactionId: row.reference_external_transaction_id } : {}),
      ...(row.reference_transaction_id ? { referenceTransactionId: row.reference_transaction_id } : {}),
      ...(row.failure_code ? { failureCode: row.failure_code } : {}),
      ...(row.processed_at ? { processedAt: new Date(row.processed_at) } : {}),
    });
  }

  private result(row: TransactionRow, wallet: WalletRow, replay: boolean): ProcessResult {
    return {
      transactionId: row.id, status: row.status,
      balance: { amount: row.balance_after ?? wallet.balance, currency: wallet.currency },
      ...(row.failure_code ? { failureCode: row.failure_code } : {}), idempotentReplay: replay,
    };
  }

  private log(event: string, row: TransactionRow, context: ProcessingContext): void {
    this.logger.log({ event, correlationId: context.correlationId, messageId: context.messageId ?? null,
      transactionId: row.id, walletId: row.wallet_id, providerId: row.provider_id, status: row.status,
      code: row.failure_code });
  }

  private async enqueue(em: EntityManager, event: IntegrationEvent<unknown>): Promise<void> {
    const message = OutboxMessage.enqueue(event);
    await insert(em, 'outbox_messages', {
      id: message.id, aggregate_id: message.aggregateId, event_type: message.eventType,
      payload: JSON.stringify(message.payload), occurred_at: message.occurredAt,
    });
  }

  private async ledger(em: EntityManager, entry: WalletLedgerEntry): Promise<void> {
    await insert(em, 'wallet_ledger_entries', {
      id: entry.id, wallet_id: entry.walletId, transaction_id: entry.transactionId,
      direction: entry.direction, amount: entry.money.toJSON().amount, currency: entry.money.currency,
      balance_before: entry.balanceBefore.toJSON().amount, balance_after: entry.balanceAfter.toJSON().amount,
      wallet_version: entry.walletVersion, created_at: entry.createdAt,
    });
  }

  async createWallet(input: { playerId: string; initialBalance: { amount: string; currency: string } }, context: ProcessingContext) {
    const initial = Money.from(input.initialBalance);
    const now = new Date();
    const wallet = Wallet.open({ id: crypto.randomUUID(), playerId: input.playerId, initialBalance: initial, at: now });
    try {
      await this.transaction(async em => {
        await insert(em, 'wallets', { id: wallet.id, player_id: wallet.playerId, currency: wallet.currency, balance: '0.00', version: 1, created_at: now, updated_at: now });
        if (!initial.isZero()) {
          const id = crypto.randomUUID();
          const tx = WagerTransaction.create({ id, providerId: '__opening__', externalTransactionId: wallet.id,
            idempotencyKey: `opening:${wallet.id}`, payloadHash: canonicalHash(input), walletId: wallet.id,
            playerId: wallet.playerId, roundId: wallet.id, gameId: '__opening__', kind: WagerTransactionKind.Opening,
            money: initial, createdAt: now });
          tx.markProcessed(undefined, now);
          await insert(em, 'wager_transactions', {
            id, provider_id: tx.providerId, external_transaction_id: tx.externalTransactionId,
            idempotency_key: tx.idempotencyKey, payload_hash: tx.payloadHash, wallet_id: wallet.id,
            player_id: wallet.playerId, round_id: tx.roundId, game_id: tx.gameId, kind: tx.kind,
            amount: initial.toJSON().amount, currency: wallet.currency, status: tx.status,
            balance_after: initial.toJSON().amount, correlation_id: context.correlationId, created_at: now, processed_at: now,
          });
          const entry = WalletLedgerEntry.create({ id: crypto.randomUUID(), walletId: wallet.id, transactionId: id,
            direction: LedgerDirection.Credit, money: initial, balanceBefore: Money.zero(wallet.currency),
            balanceAfter: initial, createdAt: now, walletVersion: 1 });
          await this.ledger(em, entry);
          await this.enqueue(em, WagerTransactionProcessed.from(tx, initial, context));
          await this.enqueue(em, WalletBalanceChanged.from(wallet, entry, context));
        }
      });
    } catch (error) {
      if (error instanceof AppError && error.status === 409) throw new AppError(409, 'WALLET_ALREADY_EXISTS');
      throw error;
    }
    if (!initial.isZero()) this.metrics.increment('wagering_transactions_total', { status: 'PROCESSED' });
    this.logger.log({ event: 'wallet.opened', correlationId: context.correlationId, messageId: context.messageId ?? null,
      transactionId: null, walletId: wallet.id, providerId: '__opening__' });
    return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
  }

  async process(input: WagerInput, key: string, context: ProcessingContext): Promise<ProcessResult> {
    input = parseWager(input);
    key = text(key, 'Idempotency-Key');
    const hash = canonicalHash(input);
    const started = performance.now();
    try {
      const outcome = await this.transaction(async em => {
        if (context.messageId && context.consumerName) {
          const inboxHash = canonicalHash({ input, idempotencyKey: key });
          const received = InboxMessage.receive({ consumerName: context.consumerName, messageId: context.messageId, payloadHash: inboxHash });
          await insert(em, 'inbox_messages', { consumer_name: received.consumerName, message_id: received.messageId,
            payload_hash: received.payloadHash, received_at: received.receivedAt }, 'on conflict do nothing');
          const [inbox] = await em.execute<{ payload_hash: string; transaction_id: string | null; processed_at: string | null }[]>(
            'select * from inbox_messages where consumer_name = ? and message_id = ? for update', [context.consumerName, context.messageId]);
          if (inbox!.payload_hash !== inboxHash) throw new AppError(409, 'INBOX_PAYLOAD_CONFLICT');
          if (inbox!.processed_at && inbox!.transaction_id) {
            const [row] = await em.execute<TransactionRow[]>('select * from wager_transactions where id = ?', [inbox!.transaction_id]);
            if (!row) throw new AppError(503, 'INBOX_TRANSACTION_MISSING');
            const wallet = await this.walletRow(em, row.wallet_id);
            return { row, result: this.result(row, wallet, true), replay: true };
          }
        }
        // An established identity conflicts even if the changed wallet does not exist.
        // This is an unlocked precheck; INSERT uniqueness below still arbitrates races.
        const known = await em.execute<Pick<TransactionRow, 'idempotency_key' | 'payload_hash'>[]>(
          'select idempotency_key, payload_hash from wager_transactions where idempotency_key = ? or (provider_id = ? and external_transaction_id = ?)',
          [key, input.providerId, input.externalTransactionId]);
        if (known.length && (known.length !== 1 || known[0]!.idempotency_key !== key || known[0]!.payload_hash !== hash)) {
          throw new AppError(409, 'IDEMPOTENCY_CONFLICT');
        }
        // Lock before child INSERTs: foreign-key KEY SHARE -> UPDATE upgrades can deadlock.
        await this.walletRow(em, input.walletId, true);
        const now = new Date();
        const candidate = WagerTransaction.create({ ...input, id: crypto.randomUUID(), idempotencyKey: key,
          payloadHash: hash, money: Money.from(input.money), createdAt: now });
        const rows = await insert(em, 'wager_transactions', {
          id: candidate.id, provider_id: input.providerId, external_transaction_id: input.externalTransactionId,
          idempotency_key: key, payload_hash: hash, wallet_id: input.walletId, player_id: input.playerId,
          round_id: input.roundId, game_id: input.gameId, kind: input.kind, amount: input.money.amount,
          currency: input.money.currency, status: candidate.status, reference_external_transaction_id: input.referenceExternalTransactionId ?? null,
          correlation_id: context.correlationId, created_at: now,
        }, 'on conflict do nothing') as TransactionRow[];
        let row = rows[0];
        const replay = !row;
        if (!row) {
          const matches = await em.execute<TransactionRow[]>(
            'select * from wager_transactions where idempotency_key = ? or (provider_id = ? and external_transaction_id = ?) order by id for update',
            [key, input.providerId, input.externalTransactionId]);
          row = matches.find(candidate => candidate.idempotency_key === key);
          if (!row || matches.length !== 1 || row.payload_hash !== hash || row.idempotency_key !== key) {
            throw new AppError(409, 'IDEMPOTENCY_CONFLICT');
          }
        }
        const walletRow = await this.walletRow(em, row.wallet_id, true);
        if (!replay) row = await this.apply(em, row, walletRow, context, false);
        if (context.messageId && context.consumerName) {
          await em.execute('update inbox_messages set transaction_id = ?, processed_at = now() where consumer_name = ? and message_id = ?',
            [row.id, context.consumerName, context.messageId]);
        }
        return { row, result: this.result(row, walletRow, replay), replay };
      });
      if (outcome.replay) this.metrics.increment('wagering_duplicates_total');
      else this.metrics.increment('wagering_transactions_total', { status: outcome.row.status });
      this.log(outcome.replay ? 'transaction.replayed' : 'transaction.completed', outcome.row, context);
      return outcome.result;
    } finally {
      this.metrics.observe('wagering_processing_seconds', (performance.now() - started) / 1000);
    }
  }

  private async apply(em: EntityManager, row: TransactionRow, walletRow: WalletRow, context: ProcessingContext, retry: boolean): Promise<TransactionRow> {
    const tx = this.wager(row);
    const wallet = this.wallet(walletRow);
    const now = new Date();
    const events = { correlationId: context.correlationId, ...(context.messageId ? { causationId: context.messageId } : { causationId: tx.id }) };
    let reference: WagerTransaction | undefined;
    let entry: WalletLedgerEntry | undefined;
    try {
      if (tx.playerId !== wallet.playerId) throw new DomainError('WALLET_PLAYER_MISMATCH');
      if (tx.money.currency !== wallet.currency) throw new DomainError('CURRENCY_MISMATCH');
      if (tx.referenceExternalTransactionId) {
        const [ref] = await em.execute<TransactionRow[]>(
          'select * from wager_transactions where provider_id = ? and external_transaction_id = ?',
          [tx.providerId, tx.referenceExternalTransactionId]);
        if (ref) {
          reference = this.wager(ref);
          tx.validateReference(reference, false);
        }
        if (!ref || ref.status === 'PENDING' || ref.status === 'PENDING_REFERENCE') {
          const attempts = row.reference_attempts + (retry ? 1 : 0);
          if (retry && (attempts >= this.config.referenceMaxAttempts || now.getTime() - new Date(row.created_at).getTime() >= this.config.referenceTtlMs)) {
            throw new DomainError('REFERENCE_NOT_FOUND');
          }
          tx.markPendingReference();
          const next = new Date(now.getTime() + Math.min(300_000, this.config.referenceRetryBaseMs * 2 ** attempts));
          const [pending] = await em.execute<TransactionRow[]>(
            "update wager_transactions set status = 'PENDING_REFERENCE', balance_after = ?, reference_attempts = ?, next_attempt_at = ? where id = ? returning *",
            [wallet.balance.toJSON().amount, attempts, next, tx.id]);
          if (!retry) await this.enqueue(em, WagerTransactionPendingReference.from(tx, wallet.balance, events));
          return pending!;
        }
        if (ref.status !== WagerTransactionStatus.Processed) throw new DomainError('REFERENCE_NOT_PROCESSED');
        if (tx.kind === WagerTransactionKind.Refund || tx.kind === WagerTransactionKind.Rollback) {
          const reversed = await em.execute<{ id: string }[]>(
            "select id from wager_transactions where reference_transaction_id = ? and kind = ? and status = 'PROCESSED'", [ref.id, tx.kind]);
          if (reversed.length) throw new DomainError('ALREADY_REVERSED');
        }
      }
      if (tx.affectsBalance()) {
        const direction = tx.ledgerDirectionFor(reference);
        if (direction === LedgerDirection.Debit && wallet.balance.isLessThan(tx.money)) {
          throw new DomainError(tx.kind === WagerTransactionKind.Bet ? 'INSUFFICIENT_FUNDS' : 'REVERSAL_INSUFFICIENT_FUNDS');
        }
        if (direction === LedgerDirection.Credit) {
          const total = wallet.balance.add(tx.money);
          const limit = Money.from({ amount: maximum.toJSON().amount, currency: wallet.currency });
          if (limit.isLessThan(total)) throw new DomainError('BALANCE_LIMIT_EXCEEDED');
        }
        const movement = { transactionId: tx.id, money: tx.money, id: crypto.randomUUID(), at: now };
        entry = direction === LedgerDirection.Debit ? wallet.debit(movement) : wallet.credit(movement);
      }
      tx.markProcessed(reference?.id, now);
    } catch (error) {
      if (!(error instanceof DomainError)) throw error;
      tx.reject(error.code);
    }
    const [updated] = await em.execute<TransactionRow[]>(
      'update wager_transactions set status = ?, reference_transaction_id = ?, failure_code = ?, balance_after = ?, processed_at = ? where id = ? returning *',
      [tx.status, tx.referenceTransactionId ?? null, tx.failureCode ?? null, wallet.balance.toJSON().amount, now, tx.id]);
    if (tx.status === WagerTransactionStatus.Processed) {
      if (entry) await this.ledger(em, entry);
      await this.enqueue(em, WagerTransactionProcessed.from(tx, wallet.balance, events));
      if (entry) await this.enqueue(em, WalletBalanceChanged.from(wallet, entry, events));
    } else {
      await this.enqueue(em, WagerTransactionRejected.from(tx, wallet.balance, events));
    }
    return updated!;
  }

  async retryReferences(): Promise<void> {
    for (let i = 0; i < 20; i++) {
      const row = await this.transaction(async em => {
        // Match the request path's wallet -> transaction lock order.
        const [wallet] = await em.execute<WalletRow[]>(`
          select w.* from wallets w
          where exists (select 1 from wager_transactions t where t.wallet_id = w.id
            and t.status = 'PENDING_REFERENCE' and t.next_attempt_at <= now())
          order by w.id limit 1 for update of w skip locked
        `);
        if (!wallet) return undefined;
        const [pending] = await em.execute<TransactionRow[]>(
          "select * from wager_transactions where wallet_id = ? and status = 'PENDING_REFERENCE' and next_attempt_at <= now() order by next_attempt_at, id limit 1 for update skip locked",
          [wallet.id]);
        if (!pending) return undefined;
        return this.apply(em, pending, wallet, { correlationId: pending.correlation_id }, true);
      });
      if (!row) return;
      this.metrics.increment('wagering_retries_total', { worker: 'reference' });
      if (row.status !== WagerTransactionStatus.PendingReference) {
        this.metrics.increment('wagering_transactions_total', { status: row.status });
        this.log('transaction.reference_resolved', row, { correlationId: row.correlation_id });
      }
    }
  }

  async getWallet(id: string) {
    const row = await this.walletRow(this.infrastructure.db.em.fork(), uuid(id));
    return { id: row.id, playerId: row.player_id, balance: { amount: row.balance, currency: row.currency }, version: row.version };
  }

  private async transactionView(where: string, params: string[]) {
    const em = this.infrastructure.db.em.fork();
    const [row] = await em.execute<TransactionRow[]>(`select * from wager_transactions where ${where}`, params);
    if (!row) throw new AppError(404, 'TRANSACTION_NOT_FOUND');
    const wallet = await this.walletRow(em, row.wallet_id);
    return { ...this.result(row, wallet, false), providerId: row.provider_id, externalTransactionId: row.external_transaction_id,
      walletId: row.wallet_id, playerId: row.player_id, roundId: row.round_id, gameId: row.game_id, kind: row.kind,
      money: { amount: row.amount, currency: row.currency }, referenceExternalTransactionId: row.reference_external_transaction_id,
      referenceTransactionId: row.reference_transaction_id, createdAt: new Date(row.created_at),
      processedAt: row.processed_at ? new Date(row.processed_at) : null };
  }

  getTransaction(id: string) { return this.transactionView('id = ?', [uuid(id, 'transactionId')]); }
  getProviderTransaction(providerId: string, externalId: string) {
    return this.transactionView('provider_id = ? and external_transaction_id = ?', [text(providerId, 'providerId'), text(externalId, 'externalTransactionId')]);
  }

  async getLedger(id: string, cursor?: string, limit?: string) {
    id = uuid(id, 'walletId');
    const size = limit === undefined ? 50 : Number(limit);
    if ((limit !== undefined && !/^[0-9]+$(?![\s\S])/.test(limit)) || !Number.isInteger(size) || size < 1 || size > 200) throw new AppError(400, 'INVALID_LIMIT');
    let version = 0;
    if (cursor !== undefined) {
      try {
        if (cursor.length > 1024 || !/^[A-Za-z0-9_-]+$/.test(cursor)) throw new Error();
        const decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { walletId: string; version: number; v: number };
        if (decoded.v !== 1 || decoded.walletId !== id || !Number.isInteger(decoded.version) || decoded.version < 1 || decoded.version > 2147483647) throw new Error();
        version = decoded.version;
      } catch { throw new AppError(400, 'INVALID_CURSOR'); }
    }
    const em = this.infrastructure.db.em.fork();
    await this.walletRow(em, id);
    const rows = await em.execute<LedgerRow[]>(
      'select * from wallet_ledger_entries where wallet_id = ? and wallet_version > ? order by wallet_version limit ?', [id, version, size + 1]);
    const page = rows.slice(0, size);
    const postings = page.length ? await em.execute<AccountingPostingRow[]>(
      `select * from accounting_postings where ledger_entry_id in (${page.map(() => '?').join(',')}) order by direction`,
      page.map(row => row.id),
    ) : [];
    const byJournal = new Map<string, AccountingPostingRow[]>();
    for (const posting of postings) {
      const entries = byJournal.get(posting.ledger_entry_id);
      if (entries) entries.push(posting);
      else byJournal.set(posting.ledger_entry_id, [posting]);
    }
    return { items: page.map(row => ({ id: row.id, walletId: row.wallet_id, transactionId: row.transaction_id,
      direction: row.direction, money: { amount: row.amount, currency: row.currency },
      balanceBefore: { amount: row.balance_before, currency: row.currency }, balanceAfter: { amount: row.balance_after, currency: row.currency },
      walletVersion: row.wallet_version, createdAt: new Date(row.created_at),
      postings: (byJournal.get(row.id) ?? []).map(posting => ({
        accountType: posting.account_type, accountReference: posting.account_reference,
        direction: posting.direction, money: { amount: posting.amount, currency: posting.currency },
      })) })),
      nextCursor: rows.length > size ? Buffer.from(JSON.stringify({ v: 1, walletId: id, version: page.at(-1)!.wallet_version })).toString('base64url') : null };
  }

  async reconcile(id: string, context: ProcessingContext) {
    id = uuid(id, 'walletId');
    const result = await this.infrastructure.db.em.fork().transactional(async em => {
      const wallet = await this.walletRow(em, id);
      const [total] = await em.execute<{ balance: string; count: number; version: number; invalid_entries: number }[]>(`
        with history as (
          select l.direction, l.amount, l.balance_before, l.balance_after, l.wallet_version,
            lag(l.balance_after, 1, 0.00) over (order by l.wallet_version) as expected_before,
            row_number() over (order by l.wallet_version)
              + case when first_value(t.kind) over (order by l.wallet_version) = 'OPENING' then 0 else 1 end as expected_version
          from wallet_ledger_entries l join wager_transactions t on t.id = l.transaction_id
          where l.wallet_id = ?
        )
        select coalesce(sum(case when direction = 'CREDIT' then amount else -amount end), 0.00)::text as balance,
          count(*)::int as count, coalesce(max(wallet_version), 1) as version,
          count(*) filter (where balance_before <> expected_before or wallet_version <> expected_version
            or balance_after <> balance_before + case when direction = 'CREDIT' then amount else -amount end)::int as invalid_entries
        from history
      `, [id]);
      const stored = Money.rehydrate({ amount: wallet.balance, currency: wallet.currency });
      const calculated = Money.rehydrate({ amount: total!.balance, currency: wallet.currency });
      const [book] = await em.execute<{
        debits: string; credits: string; wallet_balance: string;
        journal_count: number; posting_count: number; invalid_journals: number;
      }[]>(`
        with journals as (
          select l.id, l.amount,
            count(p.ledger_entry_id) as posting_count,
            coalesce(sum(p.amount) filter (where p.direction = 'DEBIT'), 0.00) as debits,
            coalesce(sum(p.amount) filter (where p.direction = 'CREDIT'), 0.00) as credits,
            coalesce(sum(case when p.account_type = 'WALLET_LIABILITY'
              then case when p.direction = 'CREDIT' then p.amount else -p.amount end else 0.00 end), 0.00) as wallet_balance,
            coalesce(bool_and(p.currency = l.currency and p.amount = l.amount), false) as amounts_match,
            count(*) filter (where p.account_type = 'WALLET_LIABILITY'
              and p.account_reference = l.wallet_id::text and p.direction = l.direction) as wallet_postings,
            count(*) filter (where p.account_type = case when t.kind = 'OPENING' then 'OPENING_CLEARING' else 'PROVIDER_CLEARING' end
              and p.account_reference = case when t.kind = 'OPENING' then 'opening' else t.provider_id end
              and p.direction <> l.direction) as counter_postings
          from wallet_ledger_entries l
          join wager_transactions t on t.id = l.transaction_id
          left join accounting_postings p on p.ledger_entry_id = l.id
          where l.wallet_id = ?
          group by l.id, t.kind, t.provider_id
        )
        select coalesce(sum(debits), 0.00)::text as debits,
          coalesce(sum(credits), 0.00)::text as credits,
          coalesce(sum(wallet_balance), 0.00)::text as wallet_balance,
          count(*)::int as journal_count, coalesce(sum(posting_count), 0)::int as posting_count,
          count(*) filter (where posting_count <> 2 or debits <> amount or credits <> amount
            or not amounts_match or wallet_postings <> 1 or counter_postings <> 1)::int as invalid_journals
        from journals
      `, [id]);
      const debits = Money.rehydrate({ amount: book!.debits, currency: wallet.currency });
      const credits = Money.rehydrate({ amount: book!.credits, currency: wallet.currency });
      const liability = Money.rehydrate({ amount: book!.wallet_balance, currency: wallet.currency });
      const balanced = book!.invalid_journals === 0 && debits.equals(credits);
      const accountingConsistent = balanced && liability.equals(stored)
        && book!.journal_count === total!.count && book!.posting_count === total!.count * 2;
      const ledgerConsistent = total!.invalid_entries === 0 && total!.version === wallet.version && stored.equals(calculated);
      return { walletId: id, storedBalance: stored.toJSON(), calculatedBalance: calculated.toJSON(),
        difference: stored.subtract(calculated).toJSON(), consistent: ledgerConsistent && accountingConsistent,
        checkedEntries: total!.count, accounting: {
          balanced, consistent: accountingConsistent, walletBalance: liability.toJSON(),
          debits: debits.toJSON(), credits: credits.toJSON(),
          journalCount: book!.journal_count, postingCount: book!.posting_count,
        } };
    }, { isolationLevel: IsolationLevel.REPEATABLE_READ });
    if (!result.consistent) {
      this.metrics.increment('wagering_reconciliation_mismatches_total');
      this.logger.error({ event: 'wallet.reconciliation_mismatch', correlationId: context.correlationId, messageId: context.messageId ?? null,
        transactionId: null, walletId: id, providerId: null, code: 'LEDGER_DIVERGENCE' });
    }
    return result;
  }
}
