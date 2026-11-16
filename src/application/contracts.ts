import { createHash } from 'node:crypto';
import { Money } from '../domain/money';
import type { MoneyProps } from '../domain/money';
import { WagerTransactionKind, WagerTransactionStatus } from '../domain/wager';

export class AppError extends Error {
  constructor(readonly status: number, readonly code: string, message = code) {
    super(message);
    this.name = 'AppError';
  }
}

export interface WagerInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

export interface ProcessingContext {
  correlationId: string;
  messageId?: string;
  consumerName?: string;
}

export interface ProcessResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  failureCode?: string;
  idempotentReplay: boolean;
}

export function text(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length > 128 || !/^[!-~]+$(?![\s\S])/.test(value)) {
    throw new AppError(400, 'INVALID_PAYLOAD', `${name} must contain 1–128 printable ASCII characters without spaces`);
  }
  return value;
}

export function uuid(value: unknown, name = 'id'): string {
  if (typeof value !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/i.test(value)) {
    throw new AppError(400, 'INVALID_PAYLOAD', `${name} must be a UUID`);
  }
  return value.toLowerCase();
}

function object(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new AppError(400, 'INVALID_PAYLOAD', 'Expected a JSON object');
  }
  const record = value as Record<string, unknown>;
  if (Object.keys(record).some(key => !keys.includes(key))) {
    throw new AppError(400, 'INVALID_PAYLOAD', 'Unknown payload field');
  }
  return record;
}

function parseMoney(value: unknown): MoneyProps {
  const props = object(value, ['amount', 'currency']);
  if (typeof props.amount !== 'string' || !/^[0-9]+\.[0-9]{2}$(?![\s\S])/.test(props.amount)) {
    throw new AppError(400, 'INVALID_MONEY');
  }
  const integerEnd = props.amount.length - 3;
  let first = 0;
  while (first < integerEnd - 1 && props.amount.charCodeAt(first) === 48) first++;
  if (integerEnd - first > 28) throw new AppError(400, 'AMOUNT_OUT_OF_RANGE');
  try {
    return Money.from({ amount: props.amount.slice(first), currency: props.currency as string }).toJSON();
  } catch {
    throw new AppError(400, 'INVALID_MONEY', 'Money requires a nonnegative fixed-two-decimal string and an ISO currency');
  }
}

export function parseWallet(value: unknown): { playerId: string; initialBalance: MoneyProps } {
  const props = object(value, ['playerId', 'initialBalance']);
  return { playerId: uuid(props.playerId, 'playerId'), initialBalance: parseMoney(props.initialBalance) };
}

export function parseWager(value: unknown): WagerInput {
  const props = object(value, ['providerId', 'externalTransactionId', 'playerId', 'walletId', 'roundId', 'gameId', 'kind', 'money', 'referenceExternalTransactionId']);
  const kinds = [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Loss, WagerTransactionKind.Refund, WagerTransactionKind.Rollback];
  if (!kinds.includes(props.kind as WagerTransactionKind)) throw new AppError(400, 'INVALID_KIND');
  const kind = props.kind as WagerTransactionKind;
  const money = parseMoney(props.money);
  if ((kind === WagerTransactionKind.Loss) !== Money.from(money).isZero()) {
    throw new AppError(400, 'INVALID_AMOUNT', 'LOSS requires zero; financial movements require a positive amount');
  }
  const reference = props.referenceExternalTransactionId === undefined
    ? undefined : text(props.referenceExternalTransactionId, 'referenceExternalTransactionId');
  if ((kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback) && !reference) {
    throw new AppError(400, 'REFERENCE_REQUIRED');
  }
  if (kind === WagerTransactionKind.Bet && reference) throw new AppError(400, 'REFERENCE_NOT_ALLOWED');
  return {
    providerId: text(props.providerId, 'providerId'),
    externalTransactionId: text(props.externalTransactionId, 'externalTransactionId'),
    playerId: uuid(props.playerId, 'playerId'), walletId: uuid(props.walletId, 'walletId'),
    roundId: text(props.roundId, 'roundId'), gameId: text(props.gameId, 'gameId'), kind, money,
    ...(reference ? { referenceExternalTransactionId: reference } : {}),
  };
}

export function canonicalHash(value: unknown): string {
  const canonical = (item: unknown): unknown => {
    if (Array.isArray(item)) return item.map(canonical);
    if (item !== null && typeof item === 'object') {
      return Object.fromEntries(Object.entries(item).filter(([, entry]) => entry !== undefined)
        .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
        .map(([key, entry]) => [key, canonical(entry)]));
    }
    return item;
  };
  return createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
}

export function parseEnvelope(value: unknown): {
  messageId: string; type: 'WagerTransactionRequested'; occurredAt: string;
  data: WagerInput & { idempotencyKey: string };
} {
  const envelope = object(value, ['messageId', 'type', 'occurredAt', 'data']);
  if (envelope.type !== 'WagerTransactionRequested') throw new AppError(400, 'INVALID_MESSAGE_TYPE');
  if (typeof envelope.occurredAt !== 'string' || !Number.isFinite(Date.parse(envelope.occurredAt)) || new Date(envelope.occurredAt).toISOString() !== envelope.occurredAt) {
    throw new AppError(400, 'INVALID_MESSAGE_DATE');
  }
  const data = object(envelope.data, ['providerId', 'externalTransactionId', 'playerId', 'walletId', 'roundId', 'gameId', 'kind', 'money', 'referenceExternalTransactionId', 'idempotencyKey']);
  const { idempotencyKey, ...business } = data;
  return {
    messageId: text(envelope.messageId, 'messageId'), type: 'WagerTransactionRequested',
    occurredAt: envelope.occurredAt,
    data: { ...parseWager(business), idempotencyKey: text(idempotencyKey, 'idempotencyKey') },
  };
}

export function databaseErrorCode(error: unknown): string {
  let current = error;
  for (let depth = 0; depth < 5 && current && typeof current === 'object'; depth++) {
    const failure = current as { code?: string; cause?: unknown; previous?: unknown; originalError?: unknown };
    if (typeof failure.code === 'string') return failure.code;
    current = failure.cause ?? failure.previous ?? failure.originalError;
  }
  return '';
}

export function isTransientInfrastructure(error: unknown): boolean {
  const code = databaseErrorCode(error);
  if (/^08[A-Z0-9]{3}$/.test(code) || [
    '40001', '40P01', '55P03', '57014', '53300', '53400', '57P01', '57P02', '57P03', '58000', '58030',
    'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN',
  ].includes(code)) return true;
  return error instanceof Error && ['KnexTimeoutError', 'TimeoutError', 'AbortError'].includes(error.name);
}
