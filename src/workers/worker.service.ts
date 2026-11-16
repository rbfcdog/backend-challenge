import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import type { Message } from '@aws-sdk/client-sqs';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type { BeforeApplicationShutdown, OnApplicationBootstrap } from '@nestjs/common';
import { AppError, databaseErrorCode, isTransientInfrastructure, parseEnvelope } from '../application/contracts';
import { ProcessorService } from '../application/processor.service';
import type { AppConfig } from '../config';
import { OutboxMessage } from '../domain/messages';
import { WagerTransactionStatus } from '../domain/wager';
import { APP_CONFIG, InfrastructureService } from '../infrastructure/infrastructure.service';
import { Metrics } from '../metrics';

interface OutboxRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Readonly<Record<string, unknown>>;
  occurred_at: Date | string;
  attempts: number;
  next_attempt_at: Date | string;
}

interface MessageContext {
  correlationId: string;
  messageId?: string | null;
  sqsMessageId?: string | null;
  transactionId?: string | null;
  walletId?: string | null;
  providerId?: string | null;
}

@Injectable()
export class WorkerService implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(WorkerService.name);
  private readonly stop = new AbortController();
  private started = false;
  private stopping = false;
  private loops: Promise<void>[] = [];
  private consumerTask: Promise<void> | undefined;
  private publisherTask: Promise<boolean> | undefined;
  private referenceTask: Promise<void> | undefined;

  constructor(
    @Inject(InfrastructureService) private readonly infrastructure: InfrastructureService,
    @Inject(ProcessorService) private readonly processor: ProcessorService,
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    @Inject(Metrics) private readonly metrics: Metrics,
  ) {
    if (new Set([config.sqsQueueUrl, config.sqsDlqUrl, config.sqsEventsQueueUrl]).size !== 3) {
      throw new Error('Input, DLQ and events queue URLs must be distinct');
    }
  }

  onApplicationBootstrap(): void {
    if (this.started || this.stopping || !this.config.workersEnabled) return;
    this.started = true;
    // Do not await long polling or dependency recovery during Nest bootstrap.
    this.loops = [
      this.runLoop('consumer', () => this.consumeOnce()),
      this.runLoop('outbox', () => this.publishOutboxOnce()),
      this.runLoop('reference', () => this.retryReferencesOnce()),
    ];
  }

  async beforeApplicationShutdown(): Promise<void> {
    this.stopping = true;
    this.stop.abort();
    // The stop signal cancels receives/sleeps, never an in-flight financial commit.
    await Promise.allSettled([
      ...this.loops,
      this.consumerTask,
      this.publisherTask,
      this.referenceTask,
    ]);
    this.logger.log({ event: 'workers_drained', correlationId: null, messageId: null,
      transactionId: null, walletId: null, providerId: null });
  }

  /** One real long-poll batch; also usable with automatic workers disabled. */
  consumeOnce(): Promise<void> {
    if (this.stopping) return Promise.resolve();
    return this.consumerTask ??= this.receiveAndProcess().finally(() => {
      this.consumerTask = undefined;
    });
  }

  /** Claim/publish one due event. False means no unlocked, due event was found. */
  publishOutboxOnce(): Promise<boolean> {
    if (this.stopping) return Promise.resolve(false);
    return this.publisherTask ??= this.publishOutbox().finally(() => {
      this.publisherTask = undefined;
    });
  }

  /** The shared use case owns reference attempts, TTL and multi-instance locks. */
  retryReferencesOnce(): Promise<void> {
    if (this.stopping) return Promise.resolve();
    return this.referenceTask ??= this.processor.retryReferences().finally(() => {
      this.referenceTask = undefined;
    });
  }

  private async runLoop(worker: string, run: () => Promise<void | boolean>): Promise<void> {
    while (!this.stopping) {
      let immediate = false;
      try {
        const result = await run();
        immediate = worker === 'consumer' || result === true;
      } catch (error) {
        if (!this.stopping) this.logger.warn({ event: 'worker_iteration_failed', worker,
          code: error instanceof AppError ? error.code : isTransientInfrastructure(error) ? 'INFRASTRUCTURE_UNAVAILABLE' : 'UNEXPECTED_WORKER_ERROR',
          errorType: error instanceof Error ? error.name : 'UnknownError', databaseCode: databaseErrorCode(error) || null,
          correlationId: null, messageId: null, transactionId: null, walletId: null, providerId: null });
      }
      if (!this.stopping && !immediate) {
        try {
          await delay(this.config.workerIntervalMs, undefined, { signal: this.stop.signal });
        } catch {
          return;
        }
      }
    }
  }

  private async receiveAndProcess(): Promise<void> {
    const response = await this.infrastructure.sqs.send(new ReceiveMessageCommand({
      QueueUrl: this.config.sqsQueueUrl,
      WaitTimeSeconds: 5,
      MaxNumberOfMessages: 10,
      VisibilityTimeout: this.config.sqsVisibilityTimeout,
      MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
    }), { abortSignal: this.stop.signal });
    const messages = response.Messages ?? [];
    if (this.stopping) {
      await Promise.all(messages.map(message => this.release(message)));
      return;
    }
    await Promise.all(messages.map(message => this.processMessage(message)));
  }

  private async processMessage(message: Message): Promise<void> {
    if (!message.ReceiptHandle) {
      this.logger.warn({ event: 'sqs_receipt_missing', code: 'INVALID_SQS_RESPONSE' });
      return;
    }
    const context: MessageContext = { correlationId: randomUUID(), sqsMessageId: message.MessageId ?? null,
      messageId: null, transactionId: null, walletId: null, providerId: null };
    const heartbeatStop = new AbortController();
    const heartbeat = this.keepVisible(message.ReceiptHandle, heartbeatStop.signal, context);
    try {
      let failure: { error: unknown; permanent: boolean } | undefined;
      try {
        let value: unknown;
        try {
          value = JSON.parse(message.Body ?? '');
        } catch {
          throw new AppError(400, 'INVALID_ENVELOPE');
        }
        const envelope = parseEnvelope(value);
        const { idempotencyKey, ...input } = envelope.data;
        context.messageId = envelope.messageId;
        context.walletId = envelope.data.walletId;
        context.providerId = envelope.data.providerId;
        const result = await this.processor.process(input, idempotencyKey, {
          correlationId: context.correlationId,
          messageId: envelope.messageId,
          consumerName: 'wager-transactions',
        });
        context.transactionId = result.transactionId;
        if (result.status === WagerTransactionStatus.Failed) {
          failure = { error: new AppError(503, result.failureCode ?? 'INFRASTRUCTURE_PERMANENT'), permanent: true };
        } else {
          // process resolves only after the financial/inbox/outbox transaction commits.
          await this.infrastructure.sqs.send(new DeleteMessageCommand({
            QueueUrl: this.config.sqsQueueUrl,
            ReceiptHandle: message.ReceiptHandle,
          }));
          this.logger.log({ event: 'sqs_message_acknowledged', ...context, status: result.status });
        }
      } catch (error) {
        failure = {
          error,
          permanent: error instanceof AppError && error.status >= 400 && error.status < 500
            && error.status !== 408 && error.status !== 429,
        };
      }
      if (!failure) return;
      const code = failure.error instanceof AppError ? failure.error.code : 'INFRASTRUCTURE_UNAVAILABLE';
      const count = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
      const attempts = Number.isSafeInteger(count) && count > 0 ? count : 1;
      try {
        if (!this.stopping && (failure.permanent || attempts >= this.config.sqsMaxAttempts)) {
          // Keep extending visibility through both the DLQ send and source delete.
          await this.deadLetter(message, code, attempts, context);
        } else {
          // Drain any heartbeat request before shortening visibility, or it could
          // overwrite the retry delay / visibility-zero shutdown release.
          heartbeatStop.abort();
          await heartbeat;
          if (this.stopping) {
            await this.release(message);
          } else {
            this.metrics.increment('wagering_retries_total', { worker: 'consumer' });
            await this.changeVisibility(message.ReceiptHandle, Math.min(300, 2 ** Math.min(attempts - 1, 9)));
            this.logger.warn({ event: 'sqs_message_retry', ...context, code, attempts });
          }
        }
      } catch {
        // Never delete on a failed DLQ send or visibility update: broker redelivery wins.
        this.logger.warn({ event: 'sqs_message_settlement_failed', ...context, code: 'INFRASTRUCTURE_UNAVAILABLE' });
      }
    } finally {
      heartbeatStop.abort();
      await heartbeat;
    }
  }

  private async keepVisible(receipt: string, signal: AbortSignal, context: MessageContext): Promise<void> {
    const interval = Math.max(250, Math.min(10_000, Math.floor(this.config.sqsVisibilityTimeout * 1000 / 3)));
    while (!signal.aborted) {
      try {
        await delay(interval, undefined, { signal });
      } catch {
        return;
      }
      if (signal.aborted) return;
      try {
        await this.changeVisibility(receipt, this.config.sqsVisibilityTimeout);
      } catch {
        this.logger.warn({ event: 'sqs_visibility_heartbeat_failed', ...context, code: 'INFRASTRUCTURE_UNAVAILABLE' });
      }
    }
  }

  private changeVisibility(receipt: string, seconds: number): Promise<unknown> {
    return this.infrastructure.sqs.send(new ChangeMessageVisibilityCommand({
      QueueUrl: this.config.sqsQueueUrl,
      ReceiptHandle: receipt,
      VisibilityTimeout: seconds,
    }));
  }

  private async release(message: Message): Promise<void> {
    if (!message.ReceiptHandle) return;
    try {
      await this.changeVisibility(message.ReceiptHandle, 0);
    } catch {
      this.logger.warn({ event: 'sqs_visibility_release_failed', code: 'INFRASTRUCTURE_UNAVAILABLE' });
    }
  }

  private async deadLetter(message: Message, code: string, attempts: number, context: MessageContext): Promise<void> {
    const deduplicationId = createHash('sha256')
      .update(`${this.config.sqsQueueUrl}\n${message.MessageId ?? ''}\n${message.Body ?? ''}`).digest('hex');
    await this.infrastructure.sqs.send(new SendMessageCommand({
      QueueUrl: this.config.sqsDlqUrl,
      MessageBody: message.Body ?? JSON.stringify({ code: 'MISSING_MESSAGE_BODY' }),
      MessageAttributes: {
        failureCode: { DataType: 'String', StringValue: code },
        receiveCount: { DataType: 'Number', StringValue: String(attempts) },
        correlationId: { DataType: 'String', StringValue: context.correlationId },
      },
      ...(this.config.sqsDlqUrl.endsWith('.fifo') ? {
        MessageGroupId: message.Attributes?.MessageGroupId ?? 'wager-invalid',
        MessageDeduplicationId: deduplicationId,
      } : {}),
    }));
    this.metrics.increment('wagering_dlq_total');
    await this.infrastructure.sqs.send(new DeleteMessageCommand({
      QueueUrl: this.config.sqsQueueUrl,
      ReceiptHandle: message.ReceiptHandle!,
    }));
    this.logger.warn({ event: 'sqs_message_dead_lettered', ...context, code, attempts });
  }

  private async publishOutbox(): Promise<boolean> {
    const published = await this.infrastructure.db.em.fork().transactional(async em => {
      const rows = await em.execute<OutboxRow[]>(`
        select id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at
        from outbox_messages
        where published_at is null and next_attempt_at <= now()
        order by occurred_at, id
        limit 1 for update skip locked
      `);
      const row = rows[0];
      if (!row) return false;
      const data = row.payload.data && typeof row.payload.data === 'object'
        ? row.payload.data as Record<string, unknown> : {};
      const identifier = (value: unknown): string | null => typeof value === 'string' && value.length <= 128 ? value : null;
      const transactionId = identifier(data.transactionId);
      const causationId = identifier(row.payload.causationId);
      const context = { correlationId: identifier(row.payload.correlationId), transactionId,
        walletId: identifier(data.walletId), providerId: identifier(data.providerId),
        messageId: causationId !== transactionId ? causationId : null, causationId };
      const outbox = OutboxMessage.rehydrate({
        id: row.id,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        payload: row.payload,
        occurredAt: new Date(row.occurred_at),
        attempts: row.attempts,
        nextAttemptAt: new Date(row.next_attempt_at),
      });
      try {
        // This is a publisher-only transaction: no wallet/ledger locks or cloud I/O
        // in the financial transaction. A crash after send can repeat this eventId.
        await this.infrastructure.sqs.send(new SendMessageCommand({
          QueueUrl: this.config.sqsEventsQueueUrl,
          MessageBody: JSON.stringify(outbox.payload),
          ...(this.config.sqsEventsQueueUrl.endsWith('.fifo') ? {
            MessageGroupId: outbox.aggregateId,
            MessageDeduplicationId: outbox.id,
          } : {}),
        }));
      } catch {
        outbox.scheduleRetry(new Date());
        await em.execute(`
          update outbox_messages set attempts = ?, next_attempt_at = ?
          where id = ? and published_at is null
        `, [outbox.attempts, outbox.nextAttemptAt, outbox.id]);
        this.metrics.increment('wagering_retries_total', { worker: 'outbox' });
        this.logger.warn({ event: 'outbox_publish_retry', ...context, eventId: outbox.id, code: 'INFRASTRUCTURE_UNAVAILABLE' });
        return true;
      }
      outbox.markPublished(new Date());
      await em.execute('update outbox_messages set published_at = ? where id = ? and published_at is null',
        [outbox.publishedAt, outbox.id]);
      this.logger.log({ event: 'outbox_published', ...context, eventId: outbox.id });
      return true;
    });
    const lag = await this.infrastructure.db.em.fork().execute<{ seconds: number }[]>(`
      select greatest(0, coalesce(extract(epoch from now() - min(occurred_at)), 0))::float8 as seconds
      from outbox_messages where published_at is null
    `);
    this.metrics.set('wagering_outbox_lag_seconds', Number(lag[0]?.seconds ?? 0));
    return published;
  }
}
