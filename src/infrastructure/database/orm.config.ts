import { Migrator } from '@mikro-orm/migrations';
import { defineConfig } from '@mikro-orm/postgresql';
import { Migration202611150001 } from './migrations/Migration202611150001';
import { Migration202611150002 } from './migrations/Migration202611150002';
import { Migration202611150003 } from './migrations/Migration202611150003';
import { walletSchema } from './wallet.record';

export function databaseConfig(clientUrl: string) {
  return defineConfig({
    clientUrl,
    entities: [walletSchema],
    extensions: [Migrator],
    migrations: {
      migrationsList: [Migration202611150001, Migration202611150002, Migration202611150003],
      transactional: true,
      allOrNothing: true,
      snapshot: false,
    },
    pool: { min: 0, max: 10, acquireTimeoutMillis: 3000 },
    driverOptions: { connection: { connectionTimeoutMillis: 3000, statement_timeout: 3000 } },
  });
}
