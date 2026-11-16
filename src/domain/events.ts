import { Money, type MoneyProps } from "./money";
import {
  DomainError, FailureCode, LedgerDirection, Wallet, WalletLedgerEntry,
  WagerTransaction, WagerTransactionKind, WagerTransactionStatus,
} from "./wager";

export interface EventContext {
  readonly correlationId: string;
  readonly causationId?: string;
}

export interface IntegrationEventProps<T> extends EventContext {
  readonly eventId: string;
  readonly aggregateId: string;
  readonly occurredAt: Date;
  readonly data: T;
}

export type IntegrationEventEnvelope<T> = Readonly<{
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string;
  version: number;
  data: Readonly<T>;
}>;

/** Detaches JSON data from caller-owned objects and freezes every nested value. */
export function immutableJson<T>(value: T): Readonly<T> {
  const copy: T = JSON.parse(JSON.stringify(value)) as T;
  const freeze = (node: unknown): void => {
    if (node !== null && typeof node === "object") {
      for (const child of Object.values(node)) freeze(child);
      Object.freeze(node);
    }
  };
  freeze(copy);
  return copy;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly data: Readonly<T>;
  readonly #occurredAt: Date;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    if (props.causationId !== undefined) this.causationId = props.causationId;
    this.#occurredAt = new Date(props.occurredAt);
    this.data = immutableJson(props.data);
  }

  get occurredAt(): Date { return new Date(this.#occurredAt); }

  toJSON(): IntegrationEventEnvelope<T> {
    return Object.freeze({
      eventId: this.eventId, eventType: this.eventType, aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId === undefined ? {} : { causationId: this.causationId }),
      occurredAt: this.#occurredAt.toISOString(), version: this.version, data: this.data,
    });
  }
}

export interface WalletBalanceChangedData {
  readonly walletId: string;
  readonly transactionId: string;
  readonly direction: LedgerDirection;
  readonly money: MoneyProps;
  readonly balanceBefore: MoneyProps;
  readonly balanceAfter: MoneyProps;
  readonly walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    if (entry.walletId !== wallet.id || entry.walletVersion !== wallet.version
      || !entry.balanceAfter.equals(wallet.balance) || !entry.money.isPositive() || !entry.isBalanced()) {
      throw new DomainError(FailureCode.InvalidLedgerEntry);
    }
    const event = new WalletBalanceChanged({
      ...ctx, eventId: crypto.randomUUID(), aggregateId: wallet.id, occurredAt: new Date(),
      data: {
        walletId: wallet.id, transactionId: entry.transactionId, direction: entry.direction,
        money: entry.money.toJSON(), balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(), walletVersion: entry.walletVersion,
      },
    });
    Object.freeze(event);
    return event;
  }
}

export interface WagerTransactionEventData {
  readonly transactionId: string;
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: WagerTransactionKind;
  readonly status: WagerTransactionStatus;
  readonly money: MoneyProps;
  readonly balance: MoneyProps;
  readonly referenceExternalTransactionId?: string;
  readonly referenceTransactionId?: string;
  readonly failureCode?: string;
}

function transactionData(tx: WagerTransaction, balance: Money, expected: WagerTransactionStatus): WagerTransactionEventData {
  if (tx.status !== expected) {
    throw new DomainError("INVALID_EVENT_STATE");
  }
  return {
    transactionId: tx.id, providerId: tx.providerId, externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId, playerId: tx.playerId, roundId: tx.roundId, gameId: tx.gameId,
    kind: tx.kind, status: tx.status, money: tx.money.toJSON(), balance: balance.toJSON(),
    ...(tx.referenceExternalTransactionId === undefined ? {} : { referenceExternalTransactionId: tx.referenceExternalTransactionId }),
    ...(tx.referenceTransactionId === undefined ? {} : { referenceTransactionId: tx.referenceTransactionId }),
    ...(tx.failureCode === undefined ? {} : { failureCode: tx.failureCode }),
  };
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;

  static from(tx: WagerTransaction, balance: Money, ctx: EventContext): WagerTransactionProcessed {
    const event = new WagerTransactionProcessed({
      ...ctx, eventId: crypto.randomUUID(), aggregateId: tx.id, occurredAt: new Date(),
      data: transactionData(tx, balance, WagerTransactionStatus.Processed),
    });
    Object.freeze(event);
    return event;
  }
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;

  static from(tx: WagerTransaction, balance: Money, ctx: EventContext): WagerTransactionRejected {
    const event = new WagerTransactionRejected({
      ...ctx, eventId: crypto.randomUUID(), aggregateId: tx.id, occurredAt: new Date(),
      data: transactionData(tx, balance, WagerTransactionStatus.Rejected),
    });
    Object.freeze(event);
    return event;
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;

  static from(tx: WagerTransaction, balance: Money, ctx: EventContext): WagerTransactionPendingReference {
    const event = new WagerTransactionPendingReference({
      ...ctx, eventId: crypto.randomUUID(), aggregateId: tx.id, occurredAt: new Date(),
      data: transactionData(tx, balance, WagerTransactionStatus.PendingReference),
    });
    Object.freeze(event);
    return event;
  }
}
