-- Роли и доступ (пароли — только для локального стенда, задаются в docker-compose).
--   wallet_app      — сервис: только вызывает функции API (SECURITY DEFINER), прямых прав на таблицы нет
--   analyst         — чтение без ПДн: таблица users закрыта, вместо неё users_masked
--   merchant_portal — кабинет мерчанта: видит только свои операции (row-level security)
--   debezium        — CDC: репликация по публикации, ПДн в публикацию не входят
set search_path = wallet;

-- роли общие для кластера: скрипт применяется и к тестовой базе
do $$
begin
    if not exists (select from pg_roles where rolname = 'wallet_app') then create role wallet_app login password 'app_local'; end if;
    if not exists (select from pg_roles where rolname = 'analyst') then create role analyst login password 'analyst_local'; end if;
    if not exists (select from pg_roles where rolname = 'merchant_portal') then create role merchant_portal login password 'merchant_local'; end if;
    if not exists (select from pg_roles where rolname = 'debezium') then create role debezium login replication password 'dbz_local'; end if;
end $$;

grant usage on schema wallet to wallet_app, analyst, merchant_portal, debezium;

-- функции API выполняются с правами владельца: сервису не нужен прямой доступ к деньгам
do $$
declare f text;
begin
    for f in select p.oid::regprocedure::text from pg_proc p join pg_namespace n on n.oid = p.pronamespace
             where n.nspname = 'wallet' and p.proname in ('create_user', 'submit_kyc', 'decide_kyc', 'topup', 'transfer',
                                                         'purchase', 'refund', 'withdraw')
    loop
        execute format('alter function %s security definer set search_path = wallet, pg_temp', f);
        execute format('grant execute on function %s to wallet_app', f);
    end loop;
end $$;
revoke execute on all functions in schema wallet from public;
grant select on operations, kyc_limits to wallet_app;     -- статус операции по id для GET /operations/{id}

-- аналитик: всё, кроме открытых ПДн
grant select on all tables in schema wallet to analyst;
revoke select on users from analyst;
create view users_masked with (security_barrier) as
select user_id, created_at, updated_at,
       left(phone, 2) || '*******' || right(phone, 2)                       as phone_masked,
       case when email is null then null else '***@' || split_part(email, '@', 2) end as email_domain,
       birth_year, city, channel, kyc_level, status
from users;
comment on view users_masked is 'Пользователи без открытых ПДн — для аналитиков';
grant select on users_masked to analyst;

-- кабинет мерчанта: строки операций фильтруются по app.merchant_id, выставленному в сессии
alter table operations enable row level security;
create policy merchant_own_ops on operations for select to merchant_portal
    using (merchant_id = nullif(current_setting('app.merchant_id', true), '')::bigint);
create policy service_all on operations for all to wallet_app, analyst, debezium using (true) with check (true);
grant select on operations to merchant_portal;
grant select on merchants to merchant_portal;

-- CDC: только нужные хранилищу таблицы; у users — список столбцов без ПДн (PostgreSQL 15+)
grant select on all tables in schema wallet to debezium;
create publication dwh_cdc for table
    users (user_id, created_at, updated_at, birth_year, city, channel, kyc_level, status),
    kyc_checks, accounts, merchants, operations, ledger_entries, experiments, experiment_assignments, outbox;
