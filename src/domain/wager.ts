import { DomainError, InvalidTransactionStateError } from "./errors";
import { CurrencyMismatchError, Money } from "./money";

export { DomainError, InvalidTransactionStateError } from "./errors";

export enum WagerTransactionKind {
  Opening = "OPENING",
  Bet = "BET",
  Win = "WIN",
  Loss = "LOSS",
  Refund = "REFUND",
  Rollback = "ROLLBACK",
}

export enum WagerTransactionStatus {
  Pending = "PENDING",
  PendingReference = "PENDING_REFERENCE",
  Processed = "PROCESSED",
  Rejected = "REJECTED",
  Failed = "FAILED",
}

export enum LedgerDirection {
  Debit = "DEBIT",
  Credit = "CREDIT",
}

export enum FailureCode {
  InsufficientFunds = "INSUFFICIENT_FUNDS",
  ReversalInsufficientFunds = "REVERSAL_INSUFFICIENT_FUNDS",
  WalletPlayerMismatch = "WALLET_PLAYER_MISMATCH",
  CurrencyMismatch = "CURRENCY_MISMATCH",
  ReferenceMismatch = "REFERENCE_MISMATCH",
  ReferenceKindInvalid = "REFERENCE_KIND_INVALID",
  ReferenceNotProcessed = "REFERENCE_NOT_PROCESSED",
  ReferenceAmountMismatch = "REFERENCE_AMOUNT_MISMATCH",
  AlreadyReversed = "ALREADY_REVERSED",
  ReferenceNotFound = "REFERENCE_NOT_FOUND",
  ReferenceRequired = "REFERENCE_REQUIRED",
  ReferenceForbidden = "REFERENCE_FORBIDDEN",
  BalanceLimitExceeded = "BALANCE_LIMIT_EXCEEDED",
  InfrastructurePermanent = "INFRASTRUCTURE_PERMANENT",
  InvalidAmount = "INVALID_AMOUNT",
  InvalidTransactionKind = "INVALID_TRANSACTION_KIND",
  InvalidLedgerEntry = "INVALID_LEDGER_ENTRY",
}

export interface LedgerEntryState {
  readonly id: string;
  readonly walletId: string;
  readonly transactionId: string;
  readonly direction: LedgerDirection;
  readonly money: Money;
  readonly balanceBefore: Money;
  readonly balanceAfter: Money;
  readonly createdAt: Date;
  readonly walletVersion: number;
}

/** Append-only movement. Dates are defensive copies and Money is immutable. */
export class WalletLedgerEntry {
  readonly #state: LedgerEntryState;

  private constructor(state: LedgerEntryState) {
    this.#state = Object.freeze({ ...state, createdAt: new Date(state.createdAt) });
    Object.freeze(this);
  }

  static create(props: LedgerEntryState): WalletLedgerEntry {
    if (props.money.currency !== props.balanceBefore.currency) {
      throw new CurrencyMismatchError(props.money.currency, props.balanceBefore.currency);
    }
    if (props.money.currency !== props.balanceAfter.currency) {
      throw new CurrencyMismatchError(props.money.currency, props.balanceAfter.currency);
    }
    const entry = new WalletLedgerEntry(props);
    if (!props.money.isPositive() || props.balanceBefore.isNegative() || props.balanceAfter.isNegative()
      || !Number.isSafeInteger(props.walletVersion) || props.walletVersion < 1 || !entry.isBalanced()) {
      throw new DomainError(FailureCode.InvalidLedgerEntry);
    }
    return entry;
  }

  /** Persisted state is reconstructed without replaying creation rules. */
  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(state);
  }

  get id(): string { return this.#state.id; }
  get walletId(): string { return this.#state.walletId; }
  get transactionId(): string { return this.#state.transactionId; }
  get direction(): LedgerDirection { return this.#state.direction; }
  get money(): Money { return this.#state.money; }
  get balanceBefore(): Money { return this.#state.balanceBefore; }
  get balanceAfter(): Money { return this.#state.balanceAfter; }
  get createdAt(): Date { return new Date(this.#state.createdAt); }
  get walletVersion(): number { return this.#state.walletVersion; }

  isBalanced(): boolean {
    if (this.money.currency !== this.balanceBefore.currency || this.money.currency !== this.balanceAfter.currency) {
      return false;
    }
    if (this.direction === LedgerDirection.Debit) {
      return this.balanceBefore.subtract(this.money).equals(this.balanceAfter);
    }
    if (this.direction === LedgerDirection.Credit) {
      return this.balanceBefore.add(this.money).equals(this.balanceAfter);
    }
    return false;
  }
}

export interface WalletState {
  readonly id: string;
  readonly playerId: string;
  readonly currency: string;
  readonly balance: Money;
  readonly version: number;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

export interface WalletMovementProps {
  readonly transactionId: string;
  readonly money: Money;
  readonly id: string;
  readonly at: Date;
}

/** A wallet movement changes the balance and produces its matching ledger atomically in memory. */
export class Wallet {
  readonly id: string;
  readonly playerId: string;
  readonly currency: string;
  readonly #createdAt: Date;
  #balance: Money;
  #version: number;
  #updatedAt: Date;

  private constructor(state: WalletState) {
    this.id = state.id;
    this.playerId = state.playerId;
    this.currency = state.currency;
    this.#balance = state.balance;
    this.#version = state.version;
    this.#createdAt = new Date(state.createdAt);
    this.#updatedAt = new Date(state.updatedAt);
    Object.freeze(this);
  }

  static open(props: { id: string; playerId: string; initialBalance: Money; at?: Date }): Wallet {
    if (props.initialBalance.isNegative()) {
      throw new DomainError(FailureCode.InvalidAmount);
    }
    const at = props.at ?? new Date();
    return new Wallet({
      id: props.id, playerId: props.playerId, currency: props.initialBalance.currency,
      balance: props.initialBalance, version: 1, createdAt: at, updatedAt: at,
    });
  }

  static rehydrate(state: WalletState): Wallet { return new Wallet(state); }

  get balance(): Money { return this.#balance; }
  get version(): number { return this.#version; }
  get createdAt(): Date { return new Date(this.#createdAt); }
  get updatedAt(): Date { return new Date(this.#updatedAt); }

  debit(props: WalletMovementProps): WalletLedgerEntry {
    return this.move(props, LedgerDirection.Debit);
  }

  credit(props: WalletMovementProps): WalletLedgerEntry {
    return this.move(props, LedgerDirection.Credit);
  }

  private move(props: WalletMovementProps, direction: LedgerDirection): WalletLedgerEntry {
    if (props.money.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, props.money.currency);
    }
    // Zero movements cannot produce the strictly positive entries required by the ledger.
    if (!props.money.isPositive()) {
      throw new DomainError(FailureCode.InvalidAmount);
    }
    const after = direction === LedgerDirection.Debit
      ? this.#balance.subtract(props.money) : this.#balance.add(props.money);
    if (after.isNegative()) {
      throw new DomainError(FailureCode.InsufficientFunds);
    }
    const entry = WalletLedgerEntry.create({
      id: props.id, walletId: this.id, transactionId: props.transactionId, direction,
      money: props.money, balanceBefore: this.#balance, balanceAfter: after,
      walletVersion: this.#version + 1, createdAt: props.at,
    });
    this.#balance = after;
    this.#version = entry.walletVersion;
    this.#updatedAt = new Date(props.at);
    return entry;
  }
}

export interface CreateWagerTransactionProps {
  readonly id: string;
  readonly providerId: string;
  readonly externalTransactionId: string;
  readonly idempotencyKey: string;
  readonly payloadHash: string;
  readonly walletId: string;
  readonly playerId: string;
  readonly roundId: string;
  readonly gameId: string;
  readonly kind: WagerTransactionKind;
  readonly money: Money;
  readonly referenceExternalTransactionId?: string;
  readonly createdAt: Date;
}

export interface WagerTransactionState extends CreateWagerTransactionProps {
  readonly status: WagerTransactionStatus;
  readonly referenceTransactionId?: string;
  readonly failureCode?: string;
  readonly processedAt?: Date;
}

/** PENDING/PENDING_REFERENCE may settle once; all settled states are terminal. */
export class WagerTransaction {
  readonly #props: CreateWagerTransactionProps;
  #status: WagerTransactionStatus;
  #referenceTransactionId: string | undefined;
  #failureCode: string | undefined;
  #processedAt: Date | undefined;

  private constructor(state: WagerTransactionState) {
    this.#props = Object.freeze({ ...state, createdAt: new Date(state.createdAt) });
    this.#status = state.status;
    this.#referenceTransactionId = state.referenceTransactionId;
    this.#failureCode = state.failureCode;
    this.#processedAt = state.processedAt === undefined ? undefined : new Date(state.processedAt);
    Object.freeze(this);
  }

  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (!Object.values(WagerTransactionKind).includes(props.kind)) {
      throw new DomainError(FailureCode.InvalidTransactionKind);
    }
    if (props.kind === WagerTransactionKind.Loss ? !props.money.isZero() : !props.money.isPositive()) {
      throw new DomainError(FailureCode.InvalidAmount);
    }
    const hasReference = props.referenceExternalTransactionId !== undefined;
    if ((props.kind === WagerTransactionKind.Refund || props.kind === WagerTransactionKind.Rollback)
      && (!hasReference || props.referenceExternalTransactionId?.trim() === "")) {
      throw new DomainError(FailureCode.ReferenceRequired);
    }
    if ((props.kind === WagerTransactionKind.Bet || props.kind === WagerTransactionKind.Opening) && hasReference) {
      throw new DomainError(FailureCode.ReferenceForbidden);
    }
    if (hasReference && props.referenceExternalTransactionId?.trim() === "") {
      throw new DomainError(FailureCode.ReferenceMismatch);
    }
    return new WagerTransaction({ ...props, status: WagerTransactionStatus.Pending });
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(state);
  }

  get id(): string { return this.#props.id; }
  get providerId(): string { return this.#props.providerId; }
  get externalTransactionId(): string { return this.#props.externalTransactionId; }
  get idempotencyKey(): string { return this.#props.idempotencyKey; }
  get payloadHash(): string { return this.#props.payloadHash; }
  get walletId(): string { return this.#props.walletId; }
  get playerId(): string { return this.#props.playerId; }
  get roundId(): string { return this.#props.roundId; }
  get gameId(): string { return this.#props.gameId; }
  get kind(): WagerTransactionKind { return this.#props.kind; }
  get money(): Money { return this.#props.money; }
  get referenceExternalTransactionId(): string | undefined { return this.#props.referenceExternalTransactionId; }
  get createdAt(): Date { return new Date(this.#props.createdAt); }
  get status(): WagerTransactionStatus { return this.#status; }
  get referenceTransactionId(): string | undefined { return this.#referenceTransactionId; }
  get failureCode(): string | undefined { return this.#failureCode; }
  get processedAt(): Date | undefined {
    return this.#processedAt === undefined ? undefined : new Date(this.#processedAt);
  }

  markProcessed(referenceTransactionId: string | undefined, at: Date): void {
    this.assertNotTerminal("process");
    if (this.referenceExternalTransactionId !== undefined && referenceTransactionId === undefined) {
      throw new DomainError(FailureCode.ReferenceNotFound);
    }
    if (this.referenceExternalTransactionId === undefined && referenceTransactionId !== undefined) {
      throw new DomainError(FailureCode.ReferenceMismatch);
    }
    this.#referenceTransactionId = referenceTransactionId;
    this.#processedAt = new Date(at);
    this.#status = WagerTransactionStatus.Processed;
  }

  markPendingReference(): void {
    this.assertNotTerminal("await a reference for");
    if (this.referenceExternalTransactionId === undefined) {
      throw new InvalidTransactionStateError(this.#status, "await a reference for an unreferenced");
    }
    this.#status = WagerTransactionStatus.PendingReference;
  }

  reject(code: string): void {
    this.assertNotTerminal("reject");
    this.#failureCode = code;
    this.#status = WagerTransactionStatus.Rejected;
  }

  fail(code: string): void {
    this.assertNotTerminal("fail");
    this.#failureCode = code;
    this.#status = WagerTransactionStatus.Failed;
  }

  isTerminal(): boolean {
    return this.#status === WagerTransactionStatus.Processed
      || this.#status === WagerTransactionStatus.Rejected || this.#status === WagerTransactionStatus.Failed;
  }

  affectsBalance(): boolean { return this.kind !== WagerTransactionKind.Loss; }
  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }
  matchesPayload(hash: string): boolean { return hash === this.payloadHash; }

  /** Resolving uniqueness and detecting an already-reversed reference requires transactional persistence. */
  validateReference(reference: WagerTransaction, requireProcessed = true): void {
    if (this.referenceExternalTransactionId === undefined || reference.id === this.id
      || reference.externalTransactionId !== this.referenceExternalTransactionId
      || reference.providerId !== this.providerId || reference.playerId !== this.playerId
      || reference.walletId !== this.walletId || reference.roundId !== this.roundId) {
      throw new DomainError(FailureCode.ReferenceMismatch);
    }
    if (reference.money.currency !== this.money.currency) {
      throw new CurrencyMismatchError(this.money.currency, reference.money.currency);
    }
    const validKind = this.kind === WagerTransactionKind.Rollback
      ? reference.kind === WagerTransactionKind.Bet || reference.kind === WagerTransactionKind.Win
        || reference.kind === WagerTransactionKind.Refund
      : (this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Win
        || this.kind === WagerTransactionKind.Loss) && reference.kind === WagerTransactionKind.Bet;
    if (!validKind) {
      throw new DomainError(FailureCode.ReferenceKindInvalid);
    }
    if (this.requiresReference() && !reference.money.equals(this.money)) {
      throw new DomainError(FailureCode.ReferenceAmountMismatch);
    }
    if (requireProcessed && reference.status !== WagerTransactionStatus.Processed) {
      throw new DomainError(FailureCode.ReferenceNotProcessed);
    }
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (reference !== undefined) this.validateReference(reference);
    switch (this.kind) {
      case WagerTransactionKind.Bet: return LedgerDirection.Debit;
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win: return LedgerDirection.Credit;
      case WagerTransactionKind.Refund:
        if (reference === undefined) throw new DomainError(FailureCode.ReferenceNotFound);
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback:
        if (reference === undefined) throw new DomainError(FailureCode.ReferenceNotFound);
        return reference.kind === WagerTransactionKind.Bet ? LedgerDirection.Credit : LedgerDirection.Debit;
      case WagerTransactionKind.Loss: throw new DomainError(FailureCode.InvalidLedgerEntry);
    }
  }

  private assertNotTerminal(transition: string): void {
    if (this.isTerminal()) throw new InvalidTransactionStateError(this.#status, transition);
  }
}
