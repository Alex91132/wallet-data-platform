-- OLTP-схема кошелька. Инварианты денег держит сама БД, а не код приложения:
-- двойная запись (сумма проводок операции = 0), неотрицательный баланс, допустимые переходы статусов,
-- одна заявка KYC «на рассмотрении», один кошелёк на пользователя, идемпотентность запросов.

create extension if not exists pgcrypto;
create schema if not exists wallet;
set search_path = wallet;

create type kyc_level as enum ('none', 'basic', 'full');
create type op_kind   as enum ('topup', 'p2p', 'purchase', 'refund', 'cashback', 'withdrawal');
create type op_status as enum ('created', 'succeeded', 'failed', 'reversed');

-- ------------------------------------------------------------------ справочники правил
create table kyc_limits (
    level             kyc_level primary key,
    max_balance       numeric(14, 2) not null,
    daily_outflow     numeric(14, 2) not null,
    p2p_allowed       boolean not null
);
comment on table kyc_limits is 'Лимиты по уровню идентификации (как у электронных кошельков по 115-ФЗ, упрощённо)';
insert into kyc_limits values
    ('none',   15000,  15000, false),
    ('basic',  60000,  60000, true),
    ('full',  600000, 600000, true);

-- ------------------------------------------------------------------ пользователи и KYC
create table users (
    user_id       bigint generated always as identity primary key,
    created_at    timestamptz not null default now(),
    phone         text not null unique check (phone ~ '^\+7\d{10}$'),
    email         text unique,
    full_name     text not null,
    birth_date    date,
    birth_year    smallint,            -- для аналитики: дата рождения в хранилище не уходит
    city          text,
    channel       text not null check (channel in ('organic', 'referral', 'ads_social', 'ads_search', 'partner')),
    kyc_level     kyc_level not null default 'none',
    status        text not null default 'active' check (status in ('active', 'blocked')),
    updated_at    timestamptz not null default now()
);
comment on table users is 'Клиенты кошелька. ПДн (телефон, e-mail, ФИО, дата рождения) в хранилище не реплицируются в открытом виде';
comment on column users.channel is 'Канал привлечения';
comment on column users.kyc_level is 'Текущий уровень идентификации — определяет лимиты';

create table kyc_checks (
    kyc_id         bigint generated always as identity primary key,
    user_id        bigint not null references users,
    level          kyc_level not null check (level <> 'none'),
    status         text not null default 'pending' check (status in ('pending', 'approved', 'rejected')),
    submitted_at   timestamptz not null,
    decided_at     timestamptz,
    reject_reason  text,
    check ((status = 'pending') = (decided_at is null)),
    check (decided_at >= submitted_at),
    check ((status = 'rejected') = (reject_reason is not null))
);
comment on table kyc_checks is 'Заявки на идентификацию; у пользователя не больше одной заявки на рассмотрении';
create unique index kyc_one_pending on kyc_checks (user_id) where status = 'pending';
create index kyc_user on kyc_checks (user_id);

-- ------------------------------------------------------------------ счета
create table accounts (
    account_id      bigint generated always as identity primary key,
    owner_type      text not null check (owner_type in ('user', 'merchant', 'system')),
    user_id         bigint references users,
    code            text unique,                 -- системные счета: cash_in, cash_out, fees, cashback
    currency        char(3) not null default 'RUB',
    balance         numeric(14, 2) not null default 0,
    allow_negative  boolean not null default false,
    created_at      timestamptz not null default now(),
    check (balance >= 0 or allow_negative),
    check ((owner_type = 'user') = (user_id is not null)),
    check ((owner_type = 'system') = (code is not null))
);
comment on table accounts is 'Счета: кошелёк пользователя, счёт мерчанта, системные (ввод/вывод денег, комиссии, кешбэк)';
comment on column accounts.balance is 'Денормализованный остаток; источник истины — ledger_entries, сверка в tests/ и dbt';
create unique index accounts_one_wallet on accounts (user_id) where owner_type = 'user';

insert into accounts (owner_type, code, allow_negative) values
    ('system', 'cash_in',  true),     -- деньги, пришедшие извне (банк-эквайер): уходит в минус на сумму пополнений
    ('system', 'cash_out', false),    -- выведенные деньги
    ('system', 'fees',     false),    -- комиссионный доход
    ('system', 'cashback', true);     -- расход на кешбэк

create table merchants (
    merchant_id  bigint generated always as identity primary key,
    name         text not null,
    category     text not null,
    mcc          char(4) not null,
    city         text,
    fee_rate     numeric(5, 4) not null check (fee_rate between 0 and 0.05),
    account_id   bigint not null unique references accounts,
    created_at   timestamptz not null default now()
);
comment on column merchants.fee_rate is 'Комиссия с оплаты, удерживается из суммы мерчанту';

-- ------------------------------------------------------------------ операции и проводки
create table operations (
    op_id              uuid primary key default gen_random_uuid(),
    kind               op_kind not null,
    status             op_status not null default 'created',
    initiator_user_id  bigint references users,
    idempotency_key    text,
    amount             numeric(14, 2) not null check (amount > 0),
    fee                numeric(14, 2) not null default 0 check (fee >= 0 and fee < amount),
    from_account       bigint not null references accounts,
    to_account         bigint not null references accounts,
    merchant_id        bigint references merchants,
    parent_op_id       uuid references operations,
    failure_reason     text,
    created_at         timestamptz not null default now(),
    updated_at         timestamptz not null default now(),
    unique (initiator_user_id, idempotency_key),
    check (from_account <> to_account),
    check ((kind in ('refund', 'cashback')) = (parent_op_id is not null)),
    check ((kind = 'purchase') = (merchant_id is not null) or kind in ('refund', 'cashback')),
    check ((status = 'failed') = (failure_reason is not null))
);
comment on table operations is 'Денежные операции. Повтор запроса с тем же Idempotency-Key возвращает ту же операцию';
create index operations_initiator on operations (initiator_user_id, created_at);
create index operations_parent on operations (parent_op_id) where parent_op_id is not null;
create index operations_merchant on operations (merchant_id, created_at) where merchant_id is not null;

create table ledger_entries (
    entry_id    bigint generated always as identity primary key,
    op_id       uuid not null references operations,
    account_id  bigint not null references accounts,
    amount      numeric(14, 2) not null check (amount <> 0),
    created_at  timestamptz not null
);
comment on table ledger_entries is 'Двойная запись: у каждой операции сумма проводок = 0 (проверяется при COMMIT)';
create index ledger_account_time on ledger_entries (account_id, created_at);
create index ledger_op on ledger_entries (op_id);

create table op_status_history (
    op_id        uuid not null references operations,
    from_status  op_status,
    to_status    op_status not null,
    changed_at   timestamptz not null,
    primary key (op_id, changed_at, to_status)
);

-- ------------------------------------------------------------------ outbox и эксперименты
create table outbox (
    event_id        uuid primary key default gen_random_uuid(),
    aggregate_type  text not null,
    aggregate_id    text not null,
    event_type      text not null,
    payload         jsonb not null,
    created_at      timestamptz not null default now()
);
comment on table outbox is 'Transactional outbox: доменные события пишутся в той же транзакции, что и изменения; Debezium публикует их в Kafka';

create table experiments (
    experiment_key  text primary key,
    description     text not null,
    variants        text[] not null,
    started_at      timestamptz not null,
    ended_at        timestamptz
);
create table experiment_assignments (
    experiment_key  text not null references experiments,
    user_id         bigint not null references users,
    variant         text not null,
    assigned_at     timestamptz not null,
    primary key (experiment_key, user_id)
);
