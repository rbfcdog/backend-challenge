import { expect, test } from 'bun:test';
import { canonicalHash, parseEnvelope, parseWager, parseWallet, text, uuid } from './contracts';

const input = {
  providerId: 'provider-a', externalTransactionId: 'bet-1',
  playerId: crypto.randomUUID(), walletId: crypto.randomUUID(), roundId: 'round-1', gameId: 'game-1',
  kind: 'BET', money: { amount: '25.00', currency: 'BRL' },
};

test('financial input rejects precision loss, unsupported kinds and inconsistent references', () => {
  for (const amount of ['1.001', '-1.00', '1e2', 'NaN', 'Infinity', '1.00\n', '10000000000000000000000000000.00', 25]) {
    expect(() => parseWager({ ...input, money: { amount, currency: 'BRL' } })).toThrow();
  }
  for (const kind of ['OPENING', 'UNKNOWN', null]) expect(() => parseWager({ ...input, kind })).toThrow();
  expect(() => parseWager({ ...input, kind: 'REFUND' })).toThrow();
  expect(() => parseWager({ ...input, referenceExternalTransactionId: 'other' })).toThrow();
  expect(() => parseWager({ ...input, kind: 'LOSS' })).toThrow();
  expect(() => parseWallet({ playerId: input.playerId, initialBalance: input.money, secret: true })).toThrow();
  expect(() => text('provider\n', 'providerId')).toThrow();
  expect(() => uuid(`${input.walletId}\n`)).toThrow();
});

test('money range validation preserves leading-zero normalization and exact numeric limits', () => {
  const normalized = parseWallet({ playerId: input.playerId,
    initialBalance: { amount: `${'0'.repeat(5000)}1.00`, currency: 'BRL' } });
  expect(normalized.initialBalance.amount).toBe('1.00');
  const maximum = '9999999999999999999999999999.99';
  expect(parseWallet({ playerId: input.playerId, initialBalance: { amount: maximum, currency: 'BRL' } }).initialBalance.amount).toBe(maximum);
});

test('canonical hashing ignores key order but distinguishes changed business payloads', () => {
  const parsed = parseWager(input);
  const reordered = { money: { currency: 'BRL', amount: '25.00' }, ...Object.fromEntries(Object.entries(input).filter(([key]) => key !== 'money').reverse()) };
  expect(canonicalHash(parseWager(reordered))).toBe(canonicalHash(parsed));
  expect(canonicalHash(parseWager({ ...input, money: { amount: '25.01', currency: 'BRL' } }))).not.toBe(canonicalHash(parsed));
  expect(canonicalHash(parseWager({ ...input, roundId: 'round-2' }))).not.toBe(canonicalHash(parsed));
});

test('queue envelope validates message identity, timestamp, key and shared financial payload', () => {
  const envelope = { messageId: 'message-1', type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(), data: { ...input, idempotencyKey: 'key-1' } };
  for (const invalid of [
    { ...envelope, type: 'OPENING' }, { ...envelope, occurredAt: 'yesterday' },
    { ...envelope, messageId: '' }, { ...envelope, data: input },
    { ...envelope, data: { ...envelope.data, money: { amount: 25, currency: 'BRL' } } },
  ]) expect(() => parseEnvelope(invalid)).toThrow();
});
