import { MikroORM } from '@mikro-orm/postgresql';
import { databaseConfig } from './orm.config';

const direction = process.argv[2];
if (direction !== 'up' && direction !== 'down') {
  throw new Error('Usage: bun src/infrastructure/database/migrate.ts <up|down>');
}
const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error('Missing environment variable: DATABASE_URL');

const orm = await MikroORM.init(databaseConfig(databaseUrl));
try {
  const migrator = orm.getMigrator();
  if (direction === 'up') await migrator.up();
  else await migrator.down();
} finally {
  await orm.close(true);
}
