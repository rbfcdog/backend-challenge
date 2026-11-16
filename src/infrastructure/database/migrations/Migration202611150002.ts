import { Migration } from '@mikro-orm/migrations';

/**
 * Financial writes are append-only. Ordinary DML roles must not own these tables
 * or have permission to disable/create triggers or change session_replication_role.
 * Deferred guards inspect the current rows, not an earlier queued NEW snapshot,
 * so several transactions and ledger movements can share one SQL transaction.
 */
export class Migration202611150002 extends Migration {
  override up(): void {
    this.addSql(`
      do $$
      begin
        if exists (select 1 from wallets where balance <> 0 or version <> 1) then
          raise exception 'Existing wallets require zero balance and version 1 before financial audit migration';
        end if;
      end;
      $$;

      alter table wallets add constraint wallets_balance_finite check (balance <> 'NaN'::numeric);

      create table wager_transactions (
        id uuid primary key,
        provider_id text not null check (length(btrim(provider_id)) > 0),
        external_transaction_id text not null check (length(btrim(external_transaction_id)) > 0),
        idempotency_key text not null unique check (length(btrim(idempotency_key)) > 0),
        payload_hash text not null check (length(btrim(payload_hash)) > 0),
        wallet_id uuid not null references wallets(id),
        player_id uuid not null,
        round_id text not null check (length(btrim(round_id)) > 0),
        game_id text not null check (length(btrim(game_id)) > 0),
        kind text not null check (kind in ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
        amount numeric(30, 2) not null check (amount <> 'NaN'::numeric),
        currency varchar(3) not null check (currency ~ '^[A-Z]{3}$'),
        status text not null default 'PENDING'
          check (status in ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
        reference_external_transaction_id text
          check (reference_external_transaction_id is null or length(btrim(reference_external_transaction_id)) > 0),
        reference_transaction_id uuid references wager_transactions(id),
        failure_code text check (failure_code is null or length(btrim(failure_code)) > 0),
        balance_after numeric(30, 2) check (balance_after >= 0 and balance_after <> 'NaN'::numeric),
        reference_attempts integer not null default 0 check (reference_attempts >= 0),
        next_attempt_at timestamptz not null default now(),
        correlation_id text not null check (length(btrim(correlation_id)) > 0),
        created_at timestamptz not null default now(),
        processed_at timestamptz,
        constraint wager_transactions_provider_external_unique unique (provider_id, external_transaction_id),
        constraint wager_transactions_amount_kind check (
          (kind = 'LOSS' and amount = 0) or (kind <> 'LOSS' and amount > 0)
        ),
        constraint wager_transactions_reference_shape check (
          (kind not in ('OPENING', 'BET') or reference_external_transaction_id is null)
          and (kind not in ('REFUND', 'ROLLBACK') or reference_external_transaction_id is not null)
          and (reference_transaction_id is null or reference_external_transaction_id is not null)
          and (reference_transaction_id is null or reference_transaction_id <> id)
        ),
        constraint wager_transactions_result_shape check (
          (status in ('PENDING', 'PENDING_REFERENCE') and processed_at is null and failure_code is null)
          or (status = 'PROCESSED' and processed_at is not null and failure_code is null and balance_after is not null)
          or (status in ('REJECTED', 'FAILED') and processed_at is not null and failure_code is not null and balance_after is not null)
        ),
        constraint wager_transactions_pending_reference_shape check (
          status <> 'PENDING_REFERENCE' or reference_external_transaction_id is not null
        )
      );
      create unique index wager_transactions_one_reversal
        on wager_transactions(reference_transaction_id, kind)
        where status = 'PROCESSED' and kind in ('REFUND', 'ROLLBACK');
      create index wager_transactions_reference_due
        on wager_transactions(next_attempt_at, created_at, id) where status = 'PENDING_REFERENCE';
      create index wager_transactions_wallet on wager_transactions(wallet_id, created_at, id);
      create index wager_transactions_reference on wager_transactions(reference_transaction_id)
        where reference_transaction_id is not null;

      create table wallet_ledger_entries (
        id uuid primary key,
        wallet_id uuid not null references wallets(id),
        transaction_id uuid not null references wager_transactions(id),
        direction text not null check (direction in ('CREDIT', 'DEBIT')),
        amount numeric(30, 2) not null check (amount > 0 and amount <> 'NaN'::numeric),
        currency varchar(3) not null check (currency ~ '^[A-Z]{3}$'),
        balance_before numeric(30, 2) not null check (balance_before >= 0 and balance_before <> 'NaN'::numeric),
        balance_after numeric(30, 2) not null check (balance_after >= 0 and balance_after <> 'NaN'::numeric),
        wallet_version integer not null check (wallet_version >= 1),
        created_at timestamptz not null default now(),
        constraint wallet_ledger_entries_wallet_transaction_unique unique (wallet_id, transaction_id),
        constraint wallet_ledger_entries_wallet_version_unique unique (wallet_id, wallet_version),
        constraint wallet_ledger_entries_arithmetic check (
          (direction = 'CREDIT' and balance_after = balance_before + amount)
          or (direction = 'DEBIT' and balance_after = balance_before - amount)
        )
      );
      create index wallet_ledger_entries_keyset on wallet_ledger_entries(wallet_id, created_at, id);
      create index wallet_ledger_entries_transaction on wallet_ledger_entries(transaction_id);

      create table inbox_messages (
        consumer_name text not null check (length(btrim(consumer_name)) > 0),
        message_id text not null check (length(btrim(message_id)) > 0),
        payload_hash text not null check (length(btrim(payload_hash)) > 0),
        transaction_id uuid references wager_transactions(id),
        received_at timestamptz not null default now(),
        processed_at timestamptz,
        primary key (consumer_name, message_id)
      );
      create index inbox_messages_transaction on inbox_messages(transaction_id) where transaction_id is not null;

      create table outbox_messages (
        id uuid primary key,
        aggregate_id uuid not null,
        event_type text not null check (event_type in (
          'WalletBalanceChanged', 'WagerTransactionProcessed',
          'WagerTransactionRejected', 'WagerTransactionPendingReference'
        )),
        payload jsonb not null check (jsonb_typeof(payload) = 'object'),
        occurred_at timestamptz not null,
        attempts integer not null default 0 check (attempts >= 0),
        next_attempt_at timestamptz not null default now(),
        published_at timestamptz
      );
      create index outbox_messages_due on outbox_messages(next_attempt_at, occurred_at, id)
        where published_at is null;

      create function financial_reject_audit_mutation() returns trigger language plpgsql as $$
      begin
        raise exception '% is append-only; % is forbidden', TG_TABLE_NAME, TG_OP using errcode = '23514';
      end;
      $$;

      create function financial_guard_wallet() returns trigger language plpgsql as $$
      begin
        if TG_OP = 'INSERT' then
          if NEW.balance <> 0 or NEW.version <> 1 then
            raise exception 'Wallets must start at zero balance and version 1; use OPENING ledger credit'
              using errcode = '23514';
          end if;
        elsif row(NEW.id, NEW.player_id, NEW.currency, NEW.created_at)
            is distinct from row(OLD.id, OLD.player_id, OLD.currency, OLD.created_at)
            or pg_trigger_depth() <> 2 then
          raise exception 'Wallets may only change through a ledger insertion' using errcode = '23514';
        end if;
        return NEW;
      end;
      $$;

      create function financial_guard_transaction() returns trigger language plpgsql as $$
      declare
        target_wallet wallets%rowtype;
        reference_tx wager_transactions%rowtype;
      begin
        if TG_OP = 'UPDATE' then
          if OLD.status in ('PROCESSED', 'REJECTED', 'FAILED') then
            raise exception 'Terminal transactions are immutable' using errcode = '23514';
          end if;
          if row(NEW.id, NEW.provider_id, NEW.external_transaction_id, NEW.idempotency_key,
              NEW.payload_hash, NEW.wallet_id, NEW.player_id, NEW.round_id, NEW.game_id,
              NEW.kind, NEW.amount, NEW.currency, NEW.reference_external_transaction_id,
              NEW.correlation_id, NEW.created_at)
              is distinct from row(OLD.id, OLD.provider_id, OLD.external_transaction_id, OLD.idempotency_key,
              OLD.payload_hash, OLD.wallet_id, OLD.player_id, OLD.round_id, OLD.game_id,
              OLD.kind, OLD.amount, OLD.currency, OLD.reference_external_transaction_id,
              OLD.correlation_id, OLD.created_at) then
            raise exception 'Transaction identity and payload are immutable' using errcode = '23514';
          end if;
          if OLD.reference_transaction_id is not null
              and NEW.reference_transaction_id is distinct from OLD.reference_transaction_id then
            raise exception 'Resolved transaction reference is immutable' using errcode = '23514';
          end if;
          if OLD.status = 'PENDING_REFERENCE' and NEW.status = 'PENDING' then
            raise exception 'Pending reference cannot transition back to PENDING' using errcode = '23514';
          end if;
          if NEW.reference_attempts < OLD.reference_attempts then
            raise exception 'Reference attempts cannot decrease' using errcode = '23514';
          end if;
        end if;

        -- All outcomes for one wallet serialize; independent wallets do not block.
        select * into target_wallet from wallets where id = NEW.wallet_id for update;
        if not found then
          raise exception 'Transaction wallet does not exist' using errcode = '23503';
        end if;
        if NEW.status = 'PROCESSED' then
          if NEW.player_id <> target_wallet.player_id or NEW.currency <> target_wallet.currency then
            raise exception 'Processed transaction wallet scope mismatch' using errcode = '23514';
          end if;
          if NEW.reference_external_transaction_id is not null then
            if NEW.reference_transaction_id is null then
              raise exception 'Processed transaction reference must be resolved' using errcode = '23514';
            end if;
            select * into reference_tx from wager_transactions where id = NEW.reference_transaction_id;
            if not found then
              raise exception 'Referenced transaction does not exist' using errcode = '23503';
            end if;
            if reference_tx.status <> 'PROCESSED'
                or reference_tx.provider_id <> NEW.provider_id
                or reference_tx.external_transaction_id <> NEW.reference_external_transaction_id
                or reference_tx.wallet_id <> NEW.wallet_id
                or reference_tx.player_id <> NEW.player_id
                or reference_tx.currency <> NEW.currency
                or reference_tx.round_id <> NEW.round_id then
              raise exception 'Processed reference must match provider, player, wallet, currency and round'
                using errcode = '23514';
            end if;
            if (NEW.kind in ('WIN', 'LOSS', 'REFUND') and reference_tx.kind <> 'BET')
                or (NEW.kind = 'ROLLBACK' and reference_tx.kind not in ('BET', 'WIN', 'REFUND')) then
              raise exception 'Invalid reference kind' using errcode = '23514';
            end if;
            if NEW.kind in ('REFUND', 'ROLLBACK') and NEW.amount <> reference_tx.amount then
              raise exception 'Partial reversals are forbidden' using errcode = '23514';
            end if;
          end if;
        end if;
        return NEW;
      end;
      $$;

      create function financial_apply_ledger() returns trigger language plpgsql as $$
      declare
        target_wallet wallets%rowtype;
        transaction_row wager_transactions%rowtype;
        reference_kind text;
        expected_direction text;
      begin
        select * into target_wallet from wallets where id = NEW.wallet_id for update;
        if not found then
          raise exception 'Ledger wallet does not exist' using errcode = '23503';
        end if;
        select * into transaction_row from wager_transactions where id = NEW.transaction_id;
        if not found then
          raise exception 'Ledger transaction does not exist' using errcode = '23503';
        end if;
        if transaction_row.status <> 'PROCESSED' or transaction_row.kind = 'LOSS'
            or transaction_row.wallet_id <> NEW.wallet_id
            or transaction_row.player_id <> target_wallet.player_id
            or transaction_row.currency <> NEW.currency or NEW.currency <> target_wallet.currency
            or transaction_row.amount <> NEW.amount
            or transaction_row.balance_after is distinct from NEW.balance_after then
          raise exception 'Ledger does not match a processed financial transaction' using errcode = '23514';
        end if;
        if transaction_row.kind = 'ROLLBACK' then
          select kind into reference_kind from wager_transactions where id = transaction_row.reference_transaction_id;
          expected_direction := case when reference_kind = 'BET' then 'CREDIT' else 'DEBIT' end;
        else
          expected_direction := case when transaction_row.kind = 'BET' then 'DEBIT' else 'CREDIT' end;
        end if;
        if NEW.direction <> expected_direction or NEW.balance_before <> target_wallet.balance then
          raise exception 'Ledger direction or previous balance mismatch' using errcode = '23514';
        end if;
        if transaction_row.kind = 'OPENING' then
          if NEW.wallet_version <> 1 or target_wallet.version <> 1 or target_wallet.balance <> 0
              or exists (select 1 from wallet_ledger_entries where wallet_id = NEW.wallet_id) then
            raise exception 'OPENING must be the first wallet movement at version 1' using errcode = '23514';
          end if;
        elsif NEW.wallet_version::bigint <> target_wallet.version::bigint + 1 then
          raise exception 'Ledger version must advance the wallet exactly once' using errcode = '23514';
        end if;

        -- BEFORE INSERT deliberately supports multiple movements in one INSERT.
        -- Any later check/unique failure rolls this update back with the insertion.
        update wallets set balance = NEW.balance_after, version = NEW.wallet_version,
          updated_at = NEW.created_at where id = NEW.wallet_id;
        return NEW;
      end;
      $$;

      create function financial_assert_transaction() returns trigger language plpgsql as $$
      declare
        transaction_row wager_transactions%rowtype;
        entry_row wallet_ledger_entries%rowtype;
        transaction_id_to_check uuid;
      begin
        if TG_TABLE_NAME = 'wallet_ledger_entries' then
          transaction_id_to_check := NEW.transaction_id;
        else
          transaction_id_to_check := NEW.id;
        end if;
        select * into transaction_row from wager_transactions where id = transaction_id_to_check;
        if not found then
          raise exception 'Financial transaction disappeared' using errcode = '23514';
        end if;
        if transaction_row.status = 'PENDING' or transaction_row.balance_after is null then
          raise exception 'Committed transaction must have a durable outcome and result balance' using errcode = '23514';
        end if;
        select * into entry_row from wallet_ledger_entries where transaction_id = transaction_row.id;
        if transaction_row.status = 'PROCESSED' and transaction_row.kind <> 'LOSS' then
          if not found or entry_row.wallet_id <> transaction_row.wallet_id
              or entry_row.balance_after is distinct from transaction_row.balance_after then
            raise exception 'Processed financial transaction requires exactly one matching ledger entry'
              using errcode = '23514';
          end if;
        elsif found then
          raise exception 'Nonfinancial outcome must not have a ledger entry' using errcode = '23514';
        end if;
        return null;
      end;
      $$;

      create function financial_assert_wallet() returns trigger language plpgsql as $$
      declare
        target_wallet wallets%rowtype;
        last_entry wallet_ledger_entries%rowtype;
      begin
        select * into target_wallet from wallets where id = NEW.id;
        if not found then
          raise exception 'Financial wallet disappeared' using errcode = '23514';
        end if;
        select * into last_entry from wallet_ledger_entries where wallet_id = NEW.id
          order by wallet_version desc limit 1;
        if found then
          if target_wallet.balance <> last_entry.balance_after or target_wallet.version <> last_entry.wallet_version
              or target_wallet.updated_at is distinct from last_entry.created_at then
            raise exception 'Wallet state must equal its latest ledger movement' using errcode = '23514';
          end if;
        elsif target_wallet.balance <> 0 or target_wallet.version <> 1 then
          raise exception 'Wallet without ledger must have zero balance and version 1' using errcode = '23514';
        end if;
        return null;
      end;
      $$;

      create function financial_guard_delivery_audit() returns trigger language plpgsql as $$
      begin
        if TG_TABLE_NAME = 'inbox_messages' then
          if OLD.processed_at is not null or row(NEW.consumer_name, NEW.message_id, NEW.payload_hash, NEW.received_at)
              is distinct from row(OLD.consumer_name, OLD.message_id, OLD.payload_hash, OLD.received_at)
              or (OLD.transaction_id is not null and NEW.transaction_id is distinct from OLD.transaction_id) then
            raise exception 'Inbox identity and processed content are immutable' using errcode = '23514';
          end if;
        else
          if row(NEW.id, NEW.aggregate_id, NEW.event_type, NEW.payload, NEW.occurred_at)
              is distinct from row(OLD.id, OLD.aggregate_id, OLD.event_type, OLD.payload, OLD.occurred_at)
              or OLD.published_at is not null or NEW.attempts < OLD.attempts then
            raise exception 'Outbox event and published content are immutable' using errcode = '23514';
          end if;
        end if;
        return NEW;
      end;
      $$;

      create trigger financial_wallet_guard before insert or update on wallets
        for each row execute function financial_guard_wallet();
      create trigger financial_transaction_guard before insert or update on wager_transactions
        for each row execute function financial_guard_transaction();
      create trigger financial_ledger_apply before insert on wallet_ledger_entries
        for each row execute function financial_apply_ledger();
      create trigger financial_inbox_guard before update on inbox_messages
        for each row execute function financial_guard_delivery_audit();
      create trigger financial_outbox_guard before update on outbox_messages
        for each row execute function financial_guard_delivery_audit();

      create constraint trigger financial_transaction_complete after insert or update on wager_transactions
        deferrable initially deferred for each row execute function financial_assert_transaction();
      create constraint trigger financial_ledger_complete after insert on wallet_ledger_entries
        deferrable initially deferred for each row execute function financial_assert_transaction();
      create constraint trigger financial_wallet_derived after insert or update on wallets
        deferrable initially deferred for each row execute function financial_assert_wallet();

      create trigger financial_wallet_no_delete before delete on wallets
        for each row execute function financial_reject_audit_mutation();
      create trigger financial_transaction_no_delete before delete on wager_transactions
        for each row execute function financial_reject_audit_mutation();
      create trigger financial_ledger_immutable before update or delete on wallet_ledger_entries
        for each row execute function financial_reject_audit_mutation();
      create trigger financial_inbox_no_delete before delete on inbox_messages
        for each row execute function financial_reject_audit_mutation();
      create trigger financial_outbox_no_delete before delete on outbox_messages
        for each row execute function financial_reject_audit_mutation();
      create trigger financial_wallet_no_truncate before truncate on wallets
        for each statement execute function financial_reject_audit_mutation();
      create trigger financial_transaction_no_truncate before truncate on wager_transactions
        for each statement execute function financial_reject_audit_mutation();
      create trigger financial_ledger_no_truncate before truncate on wallet_ledger_entries
        for each statement execute function financial_reject_audit_mutation();
      create trigger financial_inbox_no_truncate before truncate on inbox_messages
        for each statement execute function financial_reject_audit_mutation();
      create trigger financial_outbox_no_truncate before truncate on outbox_messages
        for each statement execute function financial_reject_audit_mutation();
    `);
  }

  override down(): void {
    this.addSql(`
      drop trigger financial_wallet_derived on wallets;
      drop trigger financial_wallet_guard on wallets;
      drop trigger financial_wallet_no_delete on wallets;
      drop trigger financial_wallet_no_truncate on wallets;
      drop table outbox_messages;
      drop table inbox_messages;
      drop table wallet_ledger_entries;
      drop table wager_transactions;
      drop function financial_guard_delivery_audit();
      drop function financial_assert_wallet();
      drop function financial_assert_transaction();
      drop function financial_apply_ledger();
      drop function financial_guard_transaction();
      drop function financial_guard_wallet();
      drop function financial_reject_audit_mutation();
      alter table wallets drop constraint wallets_balance_finite;
    `);
  }
}
