import { EntitySchema } from '@mikro-orm/core';

// Persistence shape only; financial transitions belong to the domain aggregate.
export interface WalletRecord {
  id: string;
  playerId: string;
  currency: string;
  balance: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export const walletSchema = new EntitySchema<WalletRecord>({
  name: 'WalletRecord',
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid' },
    currency: { type: 'string', length: 3 },
    balance: { type: 'decimal', precision: 30, scale: 2 },
    version: { type: 'integer', default: 1 },
    createdAt: { type: 'datetime', columnType: 'timestamptz', defaultRaw: 'now()' },
    updatedAt: { type: 'datetime', columnType: 'timestamptz', defaultRaw: 'now()' },
  },
  uniques: [{ properties: ['playerId', 'currency'] }],
});
