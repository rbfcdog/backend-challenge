import { Migration } from '@mikro-orm/migrations';

export class Migration202611150001 extends Migration {
  override up(): void {
    this.addSql(`
      create table wallets (
        id uuid primary key,
        player_id uuid not null,
        currency varchar(3) not null check (currency ~ '^[A-Z]{3}$'),
        balance numeric(30, 2) not null check (balance >= 0),
        version integer not null default 1 check (version >= 1),
        created_at timestamptz not null default now(),
        updated_at timestamptz not null default now(),
        constraint wallets_player_id_currency_unique unique (player_id, currency)
      );
    `);
  }

  override down(): void {
    this.addSql('drop table wallets;');
  }
}
