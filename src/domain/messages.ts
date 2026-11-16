import { immutableJson, type IntegrationEvent } from "./events";
import { DomainError } from "./errors";

export interface ReceiveInboxProps {
  readonly messageId: string;
  readonly consumerName: string;
  readonly payloadHash: string;
  readonly receivedAt?: Date;
}

export interface InboxMessageState extends ReceiveInboxProps {
  readonly receivedAt: Date;
  readonly processedAt?: Date;
}

/** The (consumerName, messageId) persistence key defines inbox deduplication. */
export class InboxMessage {
  readonly messageId: string;
  readonly consumerName: string;
  readonly payloadHash: string;
  readonly #receivedAt: Date;
  #processedAt: Date | undefined;

  private constructor(state: InboxMessageState) {
    this.messageId = state.messageId;
    this.consumerName = state.consumerName;
    this.payloadHash = state.payloadHash;
    this.#receivedAt = new Date(state.receivedAt);
    this.#processedAt = state.processedAt === undefined ? undefined : new Date(state.processedAt);
    Object.freeze(this);
  }

  static receive(props: ReceiveInboxProps): InboxMessage {
    return new InboxMessage({ ...props, receivedAt: props.receivedAt ?? new Date() });
  }

  static rehydrate(state: InboxMessageState): InboxMessage { return new InboxMessage(state); }

  get receivedAt(): Date { return new Date(this.#receivedAt); }
  get processedAt(): Date | undefined {
    return this.#processedAt === undefined ? undefined : new Date(this.#processedAt);
  }
  isProcessed(): boolean { return this.#processedAt !== undefined; }

  /** Redelivery preserves the first completion timestamp. */
  markProcessed(at: Date): void {
    if (!this.isProcessed()) this.#processedAt = new Date(at);
  }
}

export interface OutboxMessageState {
  readonly id: string;
  readonly aggregateId: string;
  readonly eventType: string;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly occurredAt: Date;
  readonly attempts: number;
  readonly nextAttemptAt?: Date;
  readonly publishedAt?: Date;
}

/** Durable event payload with explicit retry and publication transitions. */
export class OutboxMessage {
  readonly id: string;
  readonly aggregateId: string;
  readonly eventType: string;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly #occurredAt: Date;
  #attempts: number;
  #nextAttemptAt: Date | undefined;
  #publishedAt: Date | undefined;

  private constructor(state: OutboxMessageState) {
    this.id = state.id;
    this.aggregateId = state.aggregateId;
    this.eventType = state.eventType;
    this.payload = immutableJson(state.payload);
    this.#occurredAt = new Date(state.occurredAt);
    this.#attempts = state.attempts;
    this.#nextAttemptAt = state.nextAttemptAt === undefined ? undefined : new Date(state.nextAttemptAt);
    this.#publishedAt = state.publishedAt === undefined ? undefined : new Date(state.publishedAt);
    Object.freeze(this);
  }

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage({
      id: event.eventId, aggregateId: event.aggregateId, eventType: event.eventType,
      payload: event.toJSON(), occurredAt: event.occurredAt, attempts: 0,
      nextAttemptAt: event.occurredAt,
    });
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage { return new OutboxMessage(state); }

  get occurredAt(): Date { return new Date(this.#occurredAt); }
  get attempts(): number { return this.#attempts; }
  get nextAttemptAt(): Date | undefined {
    return this.#nextAttemptAt === undefined ? undefined : new Date(this.#nextAttemptAt);
  }
  get publishedAt(): Date | undefined {
    return this.#publishedAt === undefined ? undefined : new Date(this.#publishedAt);
  }
  isPending(): boolean { return this.#publishedAt === undefined; }
  isDue(now: Date): boolean {
    return this.isPending() && (this.#nextAttemptAt === undefined || this.#nextAttemptAt.getTime() <= now.getTime());
  }

  /** Repeated acknowledgements never rewrite the original publication time. */
  markPublished(at: Date): void {
    if (this.isPending()) {
      this.#publishedAt = new Date(at);
      this.#nextAttemptAt = undefined;
    }
  }

  /** One-second exponential backoff, capped at five minutes; retries remain durable. */
  scheduleRetry(now: Date): void {
    if (!this.isPending()) throw new DomainError("OUTBOX_ALREADY_PUBLISHED");
    this.#attempts += 1;
    const delayMs = Math.min(300_000, 1_000 * 2 ** Math.min(this.#attempts - 1, 9));
    this.#nextAttemptAt = new Date(now.getTime() + delayMs);
  }
}
