import { expect, test } from "bun:test";
import {
  IntegrationEvent, WalletBalanceChanged, WagerTransactionPendingReference,
  WagerTransactionProcessed, WagerTransactionRejected,
} from "./events";
import { InboxMessage, OutboxMessage } from "./messages";
import { Money } from "./money";
import {
  DomainError, FailureCode, LedgerDirection, Wallet, WagerTransaction,
  WagerTransactionKind as Kind, WagerTransactionStatus as Status,
} from "./wager";

const at = new Date("2026-01-01T00:00:00.000Z");
const later = new Date("2026-01-01T00:01:00.000Z");
const money = (amount = "10.00", currency = "BRL"): Money => Money.from({ amount, currency });

function transaction(kind = Kind.Bet): WagerTransaction {
  return WagerTransaction.create({
    id: "transaction", providerId: "provider", externalTransactionId: "external", idempotencyKey: "key",
    payloadHash: "hash", walletId: "wallet", playerId: "player", roundId: "round", gameId: "game", kind,
    money: money(kind === Kind.Loss ? "0.00" : "10.00"), createdAt: at,
    ...(kind === Kind.Refund ? { referenceExternalTransactionId: "original" } : {}),
  });
}

function processedEvent(): WagerTransactionProcessed {
  const tx = transaction();
  tx.markProcessed(undefined, at);
  return WagerTransactionProcessed.from(tx, money("90.00"), { correlationId: "correlation", causationId: "message" });
}

test("inbox processing is idempotent and preserves first timestamps across rehydration", () => {
  const receivedAt = new Date(at);
  const message = InboxMessage.receive({ messageId: "message", consumerName: "consumer", payloadHash: "hash", receivedAt });
  receivedAt.setFullYear(2000);
  expect(message.receivedAt).toEqual(at);
  expect(message.isProcessed()).toBe(false);
  expect(message.processedAt).toBeUndefined();
  const processedAt = new Date(later);
  message.markProcessed(processedAt);
  processedAt.setFullYear(2000);
  message.markProcessed(new Date("2026-01-02T00:00:00.000Z"));
  expect(message.isProcessed()).toBe(true);
  expect(message.processedAt).toEqual(later);
  message.processedAt?.setFullYear(2000);
  expect(message.processedAt).toEqual(later);
  expect(Object.isFrozen(message)).toBe(true);
  expect(Reflect.set(message, "payloadHash", "changed")).toBe(false);
  const restored = InboxMessage.rehydrate({
    messageId: message.messageId, consumerName: message.consumerName, payloadHash: message.payloadHash,
    receivedAt: message.receivedAt, processedAt: later,
  });
  expect(restored.isProcessed()).toBe(true);
  expect(restored.receivedAt).toEqual(at);
  expect(restored.processedAt).toEqual(later);
  expect(InboxMessage.receive({ messageId: "auto", consumerName: "consumer", payloadHash: "hash" }).receivedAt).toBeInstanceOf(Date);
});

test("outbox enqueues the standard immutable JSON envelope and is immediately due", () => {
  const event = processedEvent();
  const message = OutboxMessage.enqueue(event);
  expect(message.id).toBe(event.eventId);
  expect(message.aggregateId).toBe(event.aggregateId);
  expect(message.eventType).toBe("WagerTransactionProcessed");
  expect(message.payload).toEqual(event.toJSON());
  expect(message.occurredAt).toEqual(event.occurredAt);
  expect(message.attempts).toBe(0);
  expect(message.isPending()).toBe(true);
  expect(message.isDue(event.occurredAt)).toBe(true);
  expect(message.isDue(new Date(event.occurredAt.getTime() - 1))).toBe(false);
  expect(Object.isFrozen(message.payload)).toBe(true);
  expect(Object.isFrozen(message.payload["data"])).toBe(true);
  expect(JSON.stringify(message.payload)).not.toContain("minorUnits");
});

test("outbox retry backoff doubles, caps at five minutes, and respects exact due boundaries", () => {
  const message = OutboxMessage.enqueue(processedEvent());
  const expectedDelays = [1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 300_000, 300_000];
  for (const [index, delay] of expectedDelays.entries()) {
    message.scheduleRetry(at);
    expect(message.attempts).toBe(index + 1);
    expect(message.nextAttemptAt).toEqual(new Date(at.getTime() + delay));
    expect(message.isDue(new Date(at.getTime() + delay - 1))).toBe(false);
    expect(message.isDue(new Date(at.getTime() + delay))).toBe(true);
  }
  message.nextAttemptAt?.setFullYear(2000);
  expect(message.nextAttemptAt).toEqual(new Date(at.getTime() + 300_000));
});

test("outbox publication is idempotent, ends eligibility, and cannot be retried", () => {
  const message = OutboxMessage.enqueue(processedEvent());
  message.scheduleRetry(at);
  const timestamp = new Date(later);
  message.markPublished(timestamp);
  timestamp.setFullYear(2000);
  message.markPublished(at);
  expect(message.publishedAt).toEqual(later);
  expect(message.isPending()).toBe(false);
  expect(message.isDue(new Date("2100-01-01T00:00:00.000Z"))).toBe(false);
  expect(message.nextAttemptAt).toBeUndefined();
  expect(message.attempts).toBe(1);
  expect(() => message.scheduleRetry(at)).toThrow(DomainError);
  expect(message.attempts).toBe(1);
  message.publishedAt?.setFullYear(2000);
  expect(message.publishedAt).toEqual(later);
});

test("outbox persisted payload is detached recursively and retry/publication state is restored", () => {
  const payload = { eventId: "event", data: { money: { amount: "1.00", currency: "BRL" } } };
  const message = OutboxMessage.rehydrate({
    id: "event", aggregateId: "wallet", eventType: "TestEvent", payload, occurredAt: at,
    attempts: 4, nextAttemptAt: later,
  });
  payload.data.money.amount = "999.00";
  expect(message.payload["data"]).toEqual({ money: { amount: "1.00", currency: "BRL" } });
  const snapshotData = message.payload["data"];
  if (snapshotData === null || typeof snapshotData !== "object" || !("money" in snapshotData)) {
    throw new Error("Expected outbox money data");
  }
  expect(Object.isFrozen(snapshotData.money)).toBe(true);
  expect(message.attempts).toBe(4);
  expect(message.isDue(at)).toBe(false);
  expect(message.isDue(later)).toBe(true);
  const published = OutboxMessage.rehydrate({
    id: "event", aggregateId: "wallet", eventType: "TestEvent", payload, occurredAt: at,
    attempts: 4, publishedAt: later,
  });
  expect(published.isPending()).toBe(false);
  expect(published.publishedAt).toEqual(later);
  const unscheduled = OutboxMessage.rehydrate({
    id: "event", aggregateId: "wallet", eventType: "TestEvent", payload, occurredAt: at, attempts: 0,
  });
  expect(unscheduled.isDue(at)).toBe(true);
});

test("wallet-balance event contains exact ledger DTOs and rejects mismatched wallet snapshots", () => {
  const wallet = Wallet.open({ id: "wallet", playerId: "player", initialBalance: money("100.00"), at });
  const entry = wallet.debit({ id: "entry", transactionId: "transaction", money: money(), at: later });
  const event = WalletBalanceChanged.from(wallet, entry, { correlationId: "correlation" });
  expect(event.eventType).toBe("WalletBalanceChanged");
  expect(event.aggregateId).toBe("wallet");
  expect(event.data).toEqual({
    walletId: "wallet", transactionId: "transaction", direction: LedgerDirection.Debit,
    money: { amount: "10.00", currency: "BRL" }, balanceBefore: { amount: "100.00", currency: "BRL" },
    balanceAfter: { amount: "90.00", currency: "BRL" }, walletVersion: 2,
  });
  expect(Object.isFrozen(event.data.money)).toBe(true);
  expect(Reflect.set(event.data.money, "amount", "999.00")).toBe(false);
  wallet.credit({ id: "second", transactionId: "second", money: money(), at: later });
  expect(() => WalletBalanceChanged.from(wallet, entry, { correlationId: "correlation" })).toThrow(DomainError);
});

test("processed events include LOSS and rejected/pending-reference events reflect real transaction state", () => {
  const loss = transaction(Kind.Loss);
  loss.markProcessed(undefined, at);
  const processed = WagerTransactionProcessed.from(loss, money("100.00"), { correlationId: "correlation" });
  expect(processed.data.kind).toBe(Kind.Loss);
  expect(processed.data.status).toBe(Status.Processed);
  expect(processed.data.money.amount).toBe("0.00");
  const rejected = transaction();
  rejected.reject(FailureCode.InsufficientFunds);
  const rejection = WagerTransactionRejected.from(rejected, Money.zero("BRL"), { correlationId: "correlation" });
  expect(rejection.data.failureCode).toBe(FailureCode.InsufficientFunds);
  expect(rejection.data.status).toBe(Status.Rejected);
  const pending = transaction(Kind.Refund);
  pending.markPendingReference();
  const pendingEvent = WagerTransactionPendingReference.from(pending, money(), { correlationId: "correlation" });
  expect(pendingEvent.data.status).toBe(Status.PendingReference);
  expect(pendingEvent.data.referenceExternalTransactionId).toBe("original");
  expect(() => WagerTransactionProcessed.from(rejected, money(), { correlationId: "correlation" })).toThrow(DomainError);
  expect(() => WagerTransactionRejected.from(loss, money(), { correlationId: "correlation" })).toThrow(DomainError);
  expect(() => WagerTransactionPendingReference.from(loss, money(), { correlationId: "correlation" })).toThrow(DomainError);
});

test("currency-mismatch rejection preserves both the rejected input currency and actual wallet currency", () => {
  const tx = transaction();
  tx.reject(FailureCode.CurrencyMismatch);
  const event = WagerTransactionRejected.from(tx, money("5.00", "USD"), { correlationId: "correlation" });
  expect(event.data.money.currency).toBe("BRL");
  expect(event.data.balance.currency).toBe("USD");
});

test("standard event envelopes have generated unique IDs, version and immutable causation metadata", () => {
  const event = processedEvent();
  const envelope = event.toJSON();
  expect(envelope.eventType).toBe("WagerTransactionProcessed");
  expect(envelope.version).toBe(1);
  expect(envelope.eventId).toMatch(/^[0-9a-f-]{36}$/);
  expect(envelope.eventId).not.toBe(processedEvent().eventId);
  expect(envelope.correlationId).toBe("correlation");
  expect(envelope.causationId).toBe("message");
  expect(envelope.occurredAt).toBe(event.occurredAt.toISOString());
  event.occurredAt.setFullYear(2000);
  expect(event.toJSON().occurredAt).toBe(envelope.occurredAt);
  expect(Object.isFrozen(event)).toBe(true);
  expect(Object.isFrozen(envelope)).toBe(true);
  expect(Object.isFrozen(envelope.data.balance)).toBe(true);
  const tx = transaction();
  tx.markProcessed(undefined, at);
  const noCausation = WagerTransactionProcessed.from(tx, money(), { correlationId: "correlation" }).toJSON();
  expect(Object.hasOwn(noCausation, "causationId")).toBe(false);
});

test("integration event base snapshots and freezes caller-owned nested JSON data", () => {
  class ExampleEvent extends IntegrationEvent<{ nested: { amount: string } }> {
    readonly eventType = "ExampleEvent";
    readonly version = 1;
    static from(data: { nested: { amount: string } }): ExampleEvent {
      return new ExampleEvent({ eventId: "event", aggregateId: "aggregate", correlationId: "correlation", occurredAt: at, data });
    }
  }
  const data = { nested: { amount: "1.00" } };
  const event = ExampleEvent.from(data);
  data.nested.amount = "999.00";
  expect(event.data.nested.amount).toBe("1.00");
  expect(Reflect.set(event.data.nested, "amount", "2.00")).toBe(false);
});
