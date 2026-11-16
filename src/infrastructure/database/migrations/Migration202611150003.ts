import { Migration } from '@mikro-orm/migrations';

/**
 * The existing immutable wallet ledger is the journal header. Postings never
 * update wallets or maintain shared clearing-account balances. As in v2, write
 * roles must not own the schema or be allowed to disable/change triggers.
 */
export class Migration202611150003 extends Migration {
  override up(): void {
    this.addSql(`
      -- Held through the transactional backfill and trigger cutover: no live
      -- ledger insertion can commit without its matching accounting postings.
      lock table wallet_ledger_entries in share row exclusive mode;

      create table accounting_postings (
        ledger_entry_id uuid not null references wallet_ledger_entries(id),
        account_type text not null
          check (account_type in ('WALLET_LIABILITY', 'PROVIDER_CLEARING', 'OPENING_CLEARING')),
        account_reference text not null,
        direction text not null check (direction in ('DEBIT', 'CREDIT')),
        amount numeric(30, 2) not null
          check (amount > 0 and amount not in ('NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric)),
        currency varchar(3) not null check (currency ~ '^[A-Z]{3}$'),
        created_at timestamptz not null default now(),
        primary key (ledger_entry_id, direction)
      );
      create index accounting_postings_account
        on accounting_postings(account_type, account_reference, currency, ledger_entry_id);

      create function accounting_guard_posting() returns trigger language plpgsql as $$
      declare
        journal record;
      begin
        select ledger.wallet_id, ledger.direction, ledger.amount, ledger.currency,
          transaction_row.provider_id, transaction_row.kind,
          transaction_row.wallet_id as transaction_wallet_id,
          transaction_row.amount as transaction_amount,
          transaction_row.currency as transaction_currency, transaction_row.status
        into journal
        from wallet_ledger_entries ledger
        join wager_transactions transaction_row on transaction_row.id = ledger.transaction_id
        where ledger.id = NEW.ledger_entry_id;
        if not found then
          raise exception 'Accounting posting journal does not exist' using errcode = '23503';
        end if;
        if journal.status <> 'PROCESSED' or journal.kind = 'LOSS'
            or journal.transaction_wallet_id <> journal.wallet_id
            or journal.transaction_amount <> journal.amount
            or journal.transaction_currency <> journal.currency then
          raise exception 'Accounting posting requires a matching processed financial journal'
            using errcode = '23514';
        end if;
        if (NEW.amount = journal.amount and NEW.currency = journal.currency and (
          (NEW.account_type = 'WALLET_LIABILITY'
            and NEW.account_reference = journal.wallet_id::text
            and NEW.direction = journal.direction)
          or (NEW.account_type = case when journal.kind = 'OPENING'
                then 'OPENING_CLEARING' else 'PROVIDER_CLEARING' end
            and NEW.account_reference = case when journal.kind = 'OPENING'
                then 'opening' else journal.provider_id end
            and NEW.direction = case when journal.direction = 'DEBIT' then 'CREDIT' else 'DEBIT' end)
        )) is not true then
          raise exception 'Accounting posting account, direction, amount or currency does not match its journal'
            using errcode = '23514';
        end if;
        return NEW;
      end;
      $$;

      create function accounting_create_postings() returns trigger language plpgsql as $$
      begin
        -- One statement writes the pair; any posting failure also rolls back
        -- the journal, the v2 wallet update and its enclosing transaction.
        insert into accounting_postings
          (ledger_entry_id, account_type, account_reference, direction, amount, currency, created_at)
        select NEW.id, posting.account_type, posting.account_reference, posting.direction,
          NEW.amount, NEW.currency, NEW.created_at
        from wager_transactions transaction_row
        cross join lateral (values
          ('WALLET_LIABILITY', NEW.wallet_id::text, NEW.direction),
          (case when transaction_row.kind = 'OPENING' then 'OPENING_CLEARING' else 'PROVIDER_CLEARING' end,
           case when transaction_row.kind = 'OPENING' then 'opening' else transaction_row.provider_id end,
           case when NEW.direction = 'DEBIT' then 'CREDIT' else 'DEBIT' end)
        ) as posting(account_type, account_reference, direction)
        where transaction_row.id = NEW.transaction_id;
        return null;
      end;
      $$;

      create function accounting_assert_pair() returns trigger language plpgsql as $$
      declare
        journal_id uuid;
        journal wallet_ledger_entries%rowtype;
        posting_count bigint;
        debit_total numeric;
        credit_total numeric;
        currency_matches boolean;
      begin
        if TG_TABLE_NAME = 'wallet_ledger_entries' then
          journal_id := NEW.id;
        else
          journal_id := NEW.ledger_entry_id;
        end if;
        -- Inspect current rows rather than a deferred event's NEW snapshot.
        select * into journal from wallet_ledger_entries where id = journal_id;
        if not found then
          raise exception 'Accounting journal disappeared' using errcode = '23514';
        end if;
        select count(*), sum(amount) filter (where direction = 'DEBIT'),
          sum(amount) filter (where direction = 'CREDIT'), bool_and(currency = journal.currency)
        into posting_count, debit_total, credit_total, currency_matches
        from accounting_postings where ledger_entry_id = journal_id;
        if posting_count <> 2 or debit_total is distinct from journal.amount
            or credit_total is distinct from journal.amount or currency_matches is distinct from true then
          raise exception 'Accounting journal requires exactly one balanced debit and credit in its currency'
            using errcode = '23514';
        end if;
        return null;
      end;
      $$;

      create trigger accounting_posting_guard before insert on accounting_postings
        for each row execute function accounting_guard_posting();
      -- Alphabetically before the ledger pair constraint, including when callers
      -- explicitly use SET CONSTRAINTS ALL IMMEDIATE.
      create trigger accounting_ledger_create_postings after insert on wallet_ledger_entries
        for each row execute function accounting_create_postings();
      create constraint trigger accounting_ledger_pair_complete after insert on wallet_ledger_entries
        deferrable initially deferred for each row execute function accounting_assert_pair();
      create constraint trigger accounting_posting_pair_complete after insert on accounting_postings
        deferrable initially deferred for each row execute function accounting_assert_pair();
      create trigger accounting_posting_immutable before update or delete on accounting_postings
        for each row execute function financial_reject_audit_mutation();
      create trigger accounting_posting_no_truncate before truncate on accounting_postings
        for each statement execute function financial_reject_audit_mutation();

      -- Set-wise historical backfill preserves the original journal timestamp;
      -- it creates no wallet movement, transaction, inbox or outbox event.
      insert into accounting_postings
        (ledger_entry_id, account_type, account_reference, direction, amount, currency, created_at)
      select ledger.id, posting.account_type, posting.account_reference, posting.direction,
        ledger.amount, ledger.currency, ledger.created_at
      from wallet_ledger_entries ledger
      join wager_transactions transaction_row on transaction_row.id = ledger.transaction_id
      cross join lateral (values
        ('WALLET_LIABILITY', ledger.wallet_id::text, ledger.direction),
        (case when transaction_row.kind = 'OPENING' then 'OPENING_CLEARING' else 'PROVIDER_CLEARING' end,
         case when transaction_row.kind = 'OPENING' then 'opening' else transaction_row.provider_id end,
         case when ledger.direction = 'DEBIT' then 'CREDIT' else 'DEBIT' end)
      ) as posting(account_type, account_reference, direction);

      -- Unbounded numeric SUMs avoid overflowing numeric(30, 2) turnover totals.
      -- Balance is signed credit net (credits - debits), not a cash balance;
      -- clearing accounts can be positive or negative independently by currency.
      create view accounting_trial_balance as
      select account_type, account_reference, currency,
        coalesce(sum(amount) filter (where direction = 'DEBIT'), 0.00) as debits,
        coalesce(sum(amount) filter (where direction = 'CREDIT'), 0.00) as credits,
        coalesce(sum(amount) filter (where direction = 'CREDIT'), 0.00)
          - coalesce(sum(amount) filter (where direction = 'DEBIT'), 0.00) as balance
      from accounting_postings
      group by account_type, account_reference, currency;
      comment on view accounting_trial_balance is
        'Per-currency accounting balances; balance is credits minus debits, not cash or settlement.';
    `);
  }

  override down(): void {
    this.addSql(`
      lock table wallet_ledger_entries in share row exclusive mode;
      drop view accounting_trial_balance;
      drop trigger accounting_ledger_create_postings on wallet_ledger_entries;
      drop trigger accounting_ledger_pair_complete on wallet_ledger_entries;
      drop table accounting_postings;
      drop function accounting_assert_pair();
      drop function accounting_create_postings();
      drop function accounting_guard_posting();
    `);
  }
}
