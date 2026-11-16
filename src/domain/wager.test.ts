import { expect, test } from "bun:test";
import { Money } from "./money";
import {
  DomainError, FailureCode, InvalidTransactionStateError, LedgerDirection, Wallet, WalletLedgerEntry,
  WagerTransaction, WagerTransactionKind as Kind, WagerTransactionStatus as Status,
  type CreateWagerTransactionProps, type LedgerEntryState,
} from "./wager";

const at = new Date("2026-01-01T00:00:00.000Z");
const later = new Date("2026-01-01T00:01:00.000Z");
const money = (amount = "10.00", currency = "BRL"): Money => Money.from({ amount, currency });

function props(kind = Kind.Bet, overrides: Partial<CreateWagerTransactionProps> = {}): CreateWagerTransactionProps {
  return {
    id: "transaction", providerId: "provider", externalTransactionId: "external", idempotencyKey: "key",
    payloadHash: "hash", walletId: "wallet", playerId: "player", roundId: "round", gameId: "game", kind,
    money: money(kind === Kind.Loss ? "0.00" : "10.00"), createdAt: at,
    ...((kind === Kind.Refund || kind === Kind.Rollback) ? { referenceExternalTransactionId: "original" } : {}),
    ...overrides,
  };
}

function original(kind = Kind.Bet, overrides: Partial<CreateWagerTransactionProps> = {}): WagerTransaction {
  return WagerTransaction.rehydrate({
    ...props(kind, { id: "reference", externalTransactionId: "original", ...overrides }),
    status: Status.Processed, processedAt: at,
  });
}

function expectCode(action: () => unknown, code: string): void {
  let failure: unknown;
  try { action(); } catch (error) { failure = error; }
  expect(failure).toBeInstanceOf(DomainError);
  expect((failure as DomainError).code).toBe(code);
}

function ledger(overrides: Partial<LedgerEntryState> = {}): LedgerEntryState {
  return {
    id: "entry", walletId: "wallet", transactionId: "transaction", direction: LedgerDirection.Debit,
    money: money("10.00"), balanceBefore: money("100.00"), balanceAfter: money("90.00"),
    createdAt: at, walletVersion: 2, ...overrides,
  };
}

test("wallet debit and credit return the exact ledger movement and only then advance version", () => {
  const openedAt = new Date(at);
  const wallet = Wallet.open({ id: "wallet", playerId: "player", initialBalance: money("100.00"), at: openedAt });
  openedAt.setFullYear(2000);
  expect(wallet.version).toBe(1);
  expect(wallet.createdAt).toEqual(at);
  const debit = wallet.debit({ id: "debit", transactionId: "bet", money: money("80.00"), at: later });
  expect(debit.isBalanced()).toBe(true);
  expect(debit.direction).toBe(LedgerDirection.Debit);
  expect(debit.walletVersion).toBe(2);
  expect(debit.balanceBefore.toJSON().amount).toBe("100.00");
  expect(debit.balanceAfter).toBe(wallet.balance);
  expect(wallet.balance.toJSON().amount).toBe("20.00");
  expect(wallet.version).toBe(2);
  expect(wallet.updatedAt).toEqual(later);
  const credit = wallet.credit({ id: "credit", transactionId: "win", money: money("0.01"), at: later });
  expect(credit.direction).toBe(LedgerDirection.Credit);
  expect(credit.walletVersion).toBe(3);
  expect(wallet.balance.toJSON().amount).toBe("20.01");
  wallet.updatedAt.setFullYear(2000);
  expect(wallet.updatedAt).toEqual(later);
});

test("wallet prevents a second debit from overspending, without mutating balance/version/time", () => {
  const wallet = Wallet.open({ id: "wallet", playerId: "player", initialBalance: money("100.00"), at });
  wallet.debit({ id: "first", transactionId: "first", money: money("80.00"), at: later });
  expectCode(() => wallet.debit({ id: "second", transactionId: "second", money: money("80.00"), at }), FailureCode.InsufficientFunds);
  expect(wallet.balance.toJSON().amount).toBe("20.00");
  expect(wallet.version).toBe(2);
  expect(wallet.updatedAt).toEqual(later);
  wallet.debit({ id: "last", transactionId: "last", money: money("20.00"), at: later });
  expect(wallet.balance.isZero()).toBe(true);
  expect(wallet.version).toBe(3);
});

test("wallet rejects negative initial balances, cross-currency, negative and zero movements atomically", () => {
  expectCode(() => Wallet.open({ id: "wallet", playerId: "player", initialBalance: money().negate() }), FailureCode.InvalidAmount);
  const wallet = Wallet.open({ id: "wallet", playerId: "player", initialBalance: Money.zero("BRL"), at });
  for (const movement of ["debit", "credit"] as const) {
    for (const invalid of [Money.zero("BRL"), money().negate()]) {
      expectCode(() => wallet[movement]({ id: "entry", transactionId: "tx", money: invalid, at: later }), FailureCode.InvalidAmount);
    }
    expectCode(() => wallet[movement]({ id: "entry", transactionId: "tx", money: money("1.00", "USD"), at: later }), FailureCode.CurrencyMismatch);
  }
  expect(wallet.balance.isZero()).toBe(true);
  expect(wallet.version).toBe(1);
  expect(wallet.updatedAt).toEqual(at);
  expect(Reflect.set(wallet, "id", "changed")).toBe(false);
});

test("wallet rehydrates persisted state without treating it as a new opening", () => {
  const wallet = Wallet.rehydrate({
    id: "wallet", playerId: "player", currency: "BRL", balance: money("27.50"),
    version: 17, createdAt: at, updatedAt: later,
  });
  expect(wallet.version).toBe(17);
  expect(wallet.balance.toJSON().amount).toBe("27.50");
  expect(wallet.createdAt).toEqual(at);
  expect(wallet.updatedAt).toEqual(later);
});

test("ledger validates debit/credit arithmetic and prevents negative, zero or mismatched currency entries", () => {
  expect(WalletLedgerEntry.create(ledger()).isBalanced()).toBe(true);
  expect(WalletLedgerEntry.create(ledger({ direction: LedgerDirection.Credit, balanceAfter: money("110.00") })).isBalanced()).toBe(true);
  const invalid: Partial<LedgerEntryState>[] = [
    { balanceAfter: money("90.01") }, { money: Money.zero("BRL") }, { money: money().negate() },
    { balanceBefore: money().negate() }, { balanceAfter: money().negate() }, { walletVersion: 0 },
    { walletVersion: 1.5 }, { direction: "SIDEWAYS" as LedgerDirection },
  ];
  for (const state of invalid) {
    expectCode(() => WalletLedgerEntry.create(ledger(state)), FailureCode.InvalidLedgerEntry);
  }
  for (const state of [{ money: money("10.00", "USD") }, { balanceBefore: money("100.00", "USD") }, { balanceAfter: money("90.00", "USD") }]) {
    expectCode(() => WalletLedgerEntry.create(ledger(state)), FailureCode.CurrencyMismatch);
  }
});

test("ledger immutability includes the original props, timestamp and money; rehydration skips transition validation", () => {
  const timestamp = new Date(at);
  const state = ledger({ createdAt: timestamp });
  const entry = WalletLedgerEntry.create(state);
  timestamp.setFullYear(2000);
  entry.createdAt.setFullYear(2000);
  expect(entry.createdAt).toEqual(at);
  expect(Object.isFrozen(entry)).toBe(true);
  expect(Object.isFrozen(entry.money)).toBe(true);
  expect(Reflect.set(entry, "balanceAfter", money("999.00"))).toBe(false);
  expect(Reflect.set(entry, "direction", LedgerDirection.Credit)).toBe(false);
  expect(WalletLedgerEntry.rehydrate(ledger({ balanceAfter: money("89.00") })).isBalanced()).toBe(false);
});

test("all transaction kinds validate amount/reference presence and describe balance effects", () => {
  for (const kind of Object.values(Kind)) {
    const tx = WagerTransaction.create(props(kind));
    expect(tx.status).toBe(Status.Pending);
    expect(tx.affectsBalance()).toBe(kind !== Kind.Loss);
    expect(tx.requiresReference()).toBe(kind === Kind.Refund || kind === Kind.Rollback);
    expect(tx.matchesPayload("hash")).toBe(true);
    expect(tx.matchesPayload("other")).toBe(false);
    expectCode(() => WagerTransaction.create(props(kind, { money: money().negate() })), FailureCode.InvalidAmount);
    if (kind === Kind.Loss) {
      expectCode(() => WagerTransaction.create(props(kind, { money: money() })), FailureCode.InvalidAmount);
    } else {
      expectCode(() => WagerTransaction.create(props(kind, { money: Money.zero("BRL") })), FailureCode.InvalidAmount);
    }
  }
  for (const kind of [Kind.Refund, Kind.Rollback]) {
    const { referenceExternalTransactionId: _, ...unreferenced } = props(kind);
    expectCode(() => WagerTransaction.create(unreferenced), FailureCode.ReferenceRequired);
    expectCode(() => WagerTransaction.create(props(kind, { referenceExternalTransactionId: " " })), FailureCode.ReferenceRequired);
  }
  for (const kind of [Kind.Bet, Kind.Opening]) {
    expectCode(() => WagerTransaction.create(props(kind, { referenceExternalTransactionId: "original" })), FailureCode.ReferenceForbidden);
  }
  expectCode(() => WagerTransaction.create(props("UNKNOWN" as Kind)), FailureCode.InvalidTransactionKind);
  for (const kind of [Kind.Win, Kind.Loss]) {
    expectCode(() => WagerTransaction.create(props(kind, { referenceExternalTransactionId: "" })), FailureCode.ReferenceMismatch);
    expect(WagerTransaction.create(props(kind, { referenceExternalTransactionId: "original" })).requiresReference()).toBe(false);
  }
});

test("ledger directions cover opening, bet, win, refund and every supported rollback target", () => {
  expect(WagerTransaction.create(props(Kind.Opening)).ledgerDirectionFor()).toBe(LedgerDirection.Credit);
  expect(WagerTransaction.create(props(Kind.Bet)).ledgerDirectionFor()).toBe(LedgerDirection.Debit);
  expect(WagerTransaction.create(props(Kind.Win)).ledgerDirectionFor()).toBe(LedgerDirection.Credit);
  expect(WagerTransaction.create(props(Kind.Refund)).ledgerDirectionFor(original())).toBe(LedgerDirection.Credit);
  const rollback = WagerTransaction.create(props(Kind.Rollback));
  expect(rollback.ledgerDirectionFor(original())).toBe(LedgerDirection.Credit);
  expect(rollback.ledgerDirectionFor(original(Kind.Win))).toBe(LedgerDirection.Debit);
  expect(rollback.ledgerDirectionFor(original(Kind.Refund))).toBe(LedgerDirection.Debit);
  for (const kind of [Kind.Refund, Kind.Rollback]) {
    expectCode(() => WagerTransaction.create(props(kind)).ledgerDirectionFor(), FailureCode.ReferenceNotFound);
  }
  expectCode(() => WagerTransaction.create(props(Kind.Loss)).ledgerDirectionFor(), FailureCode.InvalidLedgerEntry);
});

test("reference checks enforce exact identity dimensions, status, type and full reversal amount", () => {
  const refund = WagerTransaction.create(props(Kind.Refund));
  refund.validateReference(original());
  // README lists the round, not the game, as a matching dimension.
  refund.validateReference(original(Kind.Bet, { gameId: "another-game" }));
  for (const mismatch of [
    { id: "transaction" }, { externalTransactionId: "other" }, { providerId: "other" },
    { playerId: "other" }, { walletId: "other" }, { roundId: "other" },
  ]) {
    expectCode(() => refund.validateReference(original(Kind.Bet, mismatch)), FailureCode.ReferenceMismatch);
  }
  expectCode(() => refund.validateReference(original(Kind.Bet, { money: money("10.00", "USD") })), FailureCode.CurrencyMismatch);
  expectCode(() => refund.validateReference(original(Kind.Bet, { money: money("9.99") })), FailureCode.ReferenceAmountMismatch);
  for (const status of [Status.Pending, Status.PendingReference, Status.Rejected, Status.Failed]) {
    const reference = WagerTransaction.rehydrate({ ...props(Kind.Bet, { id: "reference", externalTransactionId: "original" }), status });
    expectCode(() => refund.validateReference(reference), FailureCode.ReferenceNotProcessed);
  }
  for (const kind of [Kind.Opening, Kind.Win, Kind.Loss, Kind.Refund, Kind.Rollback]) {
    expectCode(() => refund.validateReference(original(kind)), FailureCode.ReferenceKindInvalid);
  }
  const rollback = WagerTransaction.create(props(Kind.Rollback));
  for (const kind of [Kind.Opening, Kind.Loss, Kind.Rollback]) {
    expectCode(() => rollback.validateReference(original(kind)), FailureCode.ReferenceKindInvalid);
  }
  expectCode(() => rollback.validateReference(original(Kind.Win, { money: money("9.00") })), FailureCode.ReferenceAmountMismatch);
});

test("unresolved references still reject immutable scope, kind and partial-reversal errors", () => {
  const pending = WagerTransaction.rehydrate({
    ...props(Kind.Win, { id: "reference", externalTransactionId: "original", referenceExternalTransactionId: "future-bet" }),
    status: Status.PendingReference,
  });
  expectCode(() => WagerTransaction.create(props(Kind.Refund)).validateReference(pending, false), FailureCode.ReferenceKindInvalid);
  expectCode(() => WagerTransaction.create(props(Kind.Rollback, { walletId: "other" })).validateReference(pending, false), FailureCode.ReferenceMismatch);
  expectCode(() => WagerTransaction.create(props(Kind.Rollback, { money: money("9.99") })).validateReference(pending, false), FailureCode.ReferenceAmountMismatch);
});


test("optional win/loss references must be processed bets, without imposing reversal amount equality", () => {
  for (const kind of [Kind.Win, Kind.Loss]) {
    const tx = WagerTransaction.create(props(kind, {
      money: money(kind === Kind.Loss ? "0.00" : "35.00"), referenceExternalTransactionId: "original",
    }));
    tx.validateReference(original());
    expectCode(() => tx.validateReference(original(Kind.Win)), FailureCode.ReferenceKindInvalid);
    expectCode(() => tx.validateReference(original(Kind.Bet, { roundId: "other" })), FailureCode.ReferenceMismatch);
  }
  expectCode(() => WagerTransaction.create(props(Kind.Win)).validateReference(original()), FailureCode.ReferenceMismatch);
});

test("pending reference can remain pending and later settle using its resolved internal reference", () => {
  const tx = WagerTransaction.create(props(Kind.Refund));
  tx.markPendingReference();
  tx.markPendingReference();
  expect(tx.status).toBe(Status.PendingReference);
  expect(tx.isTerminal()).toBe(false);
  expectCode(() => tx.markProcessed(undefined, later), FailureCode.ReferenceNotFound);
  expect(tx.status).toBe(Status.PendingReference);
  tx.validateReference(original());
  tx.markProcessed("reference", later);
  expect(tx.status).toBe(Status.Processed);
  expect(tx.referenceTransactionId).toBe("reference");
  expect(tx.processedAt).toEqual(later);
  tx.processedAt?.setFullYear(2000);
  expect(tx.processedAt).toEqual(later);
  const bet = WagerTransaction.create(props());
  expect(() => bet.markPendingReference()).toThrow(InvalidTransactionStateError);
  expectCode(() => bet.markProcessed("unexpected", later), FailureCode.ReferenceMismatch);
  expect(bet.status).toBe(Status.Pending);
});

test("every transition from processed, rejected or failed throws a programming error and preserves state", () => {
  for (const status of [Status.Processed, Status.Rejected, Status.Failed]) {
    const tx = WagerTransaction.create(props());
    if (status === Status.Processed) tx.markProcessed(undefined, at);
    else if (status === Status.Rejected) tx.reject(FailureCode.InsufficientFunds);
    else tx.fail(FailureCode.InfrastructurePermanent);
    expect(tx.isTerminal()).toBe(true);
    const code = tx.failureCode;
    for (const transition of [
      () => tx.markProcessed(undefined, later), () => tx.markPendingReference(),
      () => tx.reject("NEW_CODE"), () => tx.fail("NEW_CODE"),
    ]) {
      expect(transition).toThrow(InvalidTransactionStateError);
      expect(tx.status).toBe(status);
      expect(tx.failureCode).toBe(code);
    }
    expect(tx.affectsBalance()).toBe(true);
  }
});

test("pending-reference may reject/fail and persisted transaction rehydration never revalidates creation", () => {
  const rejected = WagerTransaction.create(props(Kind.Refund));
  rejected.markPendingReference();
  rejected.reject(FailureCode.ReferenceNotFound);
  expect(rejected.failureCode).toBe(FailureCode.ReferenceNotFound);
  expect(rejected.status).toBe(Status.Rejected);
  const failed = WagerTransaction.create(props(Kind.Refund));
  failed.markPendingReference();
  failed.fail(FailureCode.InfrastructurePermanent);
  expect(failed.failureCode).toBe(FailureCode.InfrastructurePermanent);
  const state = {
    ...props(Kind.Bet, { money: Money.zero("BRL"), referenceExternalTransactionId: "legacy" }),
    status: Status.Processed, referenceTransactionId: "resolved", processedAt: new Date(at),
  };
  const restored = WagerTransaction.rehydrate(state);
  state.processedAt.setFullYear(2000);
  expect(restored.money.isZero()).toBe(true);
  expect(restored.referenceTransactionId).toBe("resolved");
  expect(restored.processedAt).toEqual(at);
  expect(Reflect.set(restored, "kind", Kind.Win)).toBe(false);
});
