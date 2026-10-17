-- Бизнес-логика денег — в БД: одна транзакция на операцию, блокировки счетов в фиксированном порядке
-- (нет дедлоков), идемпотентность по (инициатор, ключ), отказы записываются как операции со статусом failed.
-- Параметр p_at — «время запроса»: сервис передаёт now(), симулятор — модельное время.
set search_path = wallet;

-- ------------------------------------------------------------------ инварианты
create function trg_ledger_balanced() returns trigger language plpgsql as $$
declare s numeric;
begin
    select coalesce(sum(amount), 0) into s from wallet.ledger_entries where op_id = new.op_id;
    if s <> 0 then
        raise exception 'ledger: операция % не сбалансирована (сумма проводок %)', new.op_id, s using errcode = '23514';
    end if;
    return null;
end $$;
-- проверка в момент COMMIT: внутри транзакции проводки добавляются по одной
create constraint trigger ledger_balanced after insert on ledger_entries
    deferrable initially deferred for each row execute function trg_ledger_balanced();

create function trg_op_status() returns trigger language plpgsql as $$
begin
    if tg_op = 'UPDATE' and new.status is distinct from old.status then
        if not ((old.status = 'created' and new.status in ('succeeded', 'failed'))
             or (old.status = 'succeeded' and new.status = 'reversed')) then
            raise exception 'operations: переход % → % запрещён', old.status, new.status using errcode = '23514';
        end if;
    end if;
    return new;
end $$;
create trigger op_status_guard before update of status on operations for each row execute function trg_op_status();

create function trg_op_history() returns trigger language plpgsql as $$
begin
    if tg_op = 'INSERT' or new.status is distinct from old.status then
        insert into wallet.op_status_history values (new.op_id, case when tg_op = 'UPDATE' then old.status end, new.status, new.updated_at);
    end if;
    return null;
end $$;
create trigger op_history after insert or update of status on operations for each row execute function trg_op_history();

-- ------------------------------------------------------------------ служебное
create function _event(p_type text, p_agg text, p_id text, p_payload jsonb, p_at timestamptz) returns void
language sql as $$
    insert into wallet.outbox (aggregate_type, aggregate_id, event_type, payload, created_at)
    values (p_agg, p_id, p_type, p_payload, p_at)
$$;

create function _wallet_of(p_user bigint) returns bigint language sql stable as $$
    select account_id from wallet.accounts where user_id = p_user and owner_type = 'user'
$$;
create function _system(p_code text) returns bigint language sql stable as $$
    select account_id from wallet.accounts where code = p_code
$$;

-- проводка: деньги уходят со счёта p_from на p_to; баланс — денормализация, CHECK (balance >= 0) страхует
create function _move(p_op uuid, p_from bigint, p_to bigint, p_amount numeric, p_at timestamptz) returns void
language plpgsql as $$
begin
    if p_amount = 0 then return; end if;
    insert into wallet.ledger_entries (op_id, account_id, amount, created_at)
    values (p_op, p_from, -p_amount, p_at), (p_op, p_to, p_amount, p_at);
    update wallet.accounts set balance = balance - p_amount where account_id = p_from;
    update wallet.accounts set balance = balance + p_amount where account_id = p_to;
end $$;

-- исходящий поток счёта за календарный день по Москве
create function _outflow_today(p_account bigint, p_at timestamptz) returns numeric language sql stable as $$
    select coalesce(-sum(amount), 0) from wallet.ledger_entries
    where account_id = p_account and amount < 0
      and created_at >= date_trunc('day', p_at at time zone 'Europe/Moscow') at time zone 'Europe/Moscow'
      and created_at <  (date_trunc('day', p_at at time zone 'Europe/Moscow') + interval '1 day') at time zone 'Europe/Moscow'
$$;

-- блокировка счетов в порядке id: два встречных перевода не могут взаимно заблокироваться.
-- Именно FOR NO KEY UPDATE, а не FOR UPDATE: вставка операции и проводок уже взяла на счета FOR KEY SHARE
-- (проверка внешних ключей), а FOR UPDATE с ней конфликтует — две параллельные транзакции ждали бы друг друга.
-- Тест test_no_double_spend ловил этот дедлок; баланс — не ключевой столбец, NO KEY UPDATE достаточно.
create function _lock(variadic p_accounts bigint[]) returns void language sql as $$
    select null from (select 1 from wallet.accounts where account_id = any(p_accounts) order by account_id
                      for no key update) t
$$;

-- ответ API
create type op_result as (op_id uuid, status op_status, failure_reason text, replayed boolean);

-- идемпотентность: та же пара (инициатор, ключ) → та же операция
create function _replay(p_user bigint, p_key text) returns op_result language sql stable as $$
    select op_id, status, failure_reason, true from wallet.operations
    where initiator_user_id = p_user and idempotency_key = p_key
$$;

-- создать операцию; если ключ уже занят параллельным запросом — вернуть null (вызов вернёт повтор)
create function _open(p_kind op_kind, p_user bigint, p_key text, p_amount numeric, p_from bigint, p_to bigint,
                      p_at timestamptz, p_merchant bigint default null, p_parent uuid default null) returns uuid
language plpgsql as $$
declare v uuid;
begin
    insert into wallet.operations (kind, initiator_user_id, idempotency_key, amount, from_account, to_account,
                                   merchant_id, parent_op_id, created_at, updated_at)
    values (p_kind, p_user, p_key, p_amount, p_from, p_to, p_merchant, p_parent, p_at, p_at)
    returning op_id into v;
    return v;
exception when unique_violation then
    return null;
end $$;

create function _finish(p_op uuid, p_status op_status, p_reason text, p_at timestamptz, p_fee numeric default 0)
returns op_result language plpgsql as $$
declare o wallet.operations;
begin
    update wallet.operations set status = p_status, failure_reason = p_reason, fee = p_fee, updated_at = p_at
    where op_id = p_op returning * into o;
    perform wallet._event('operation.' || p_status, 'operation', p_op::text,
        jsonb_build_object('op_id', o.op_id, 'kind', o.kind, 'user_id', o.initiator_user_id, 'amount', o.amount,
                           'fee', o.fee, 'merchant_id', o.merchant_id, 'failure_reason', p_reason), p_at);
    return (p_op, p_status, p_reason, false)::wallet.op_result;
end $$;

-- ------------------------------------------------------------------ пользователи
create function create_user(p_phone text, p_email text, p_full_name text, p_birth_date date, p_city text,
                            p_channel text, p_at timestamptz default now()) returns bigint
language plpgsql as $$
declare v bigint;
begin
    insert into wallet.users (created_at, updated_at, phone, email, full_name, birth_date, birth_year, city, channel)
    values (p_at, p_at, p_phone, p_email, p_full_name, p_birth_date, extract(year from p_birth_date), p_city, p_channel)
    returning user_id into v;
    insert into wallet.accounts (owner_type, user_id, created_at) values ('user', v, p_at);
    perform wallet._event('user.registered', 'user', v::text,
        jsonb_build_object('user_id', v, 'channel', p_channel, 'city', p_city), p_at);
    return v;
end $$;

create function create_merchant(p_name text, p_category text, p_mcc text, p_city text, p_fee_rate numeric,
                                p_at timestamptz default now()) returns bigint
language plpgsql as $$
declare a bigint; v bigint;
begin
    insert into wallet.accounts (owner_type, created_at) values ('merchant', p_at) returning account_id into a;
    insert into wallet.merchants (name, category, mcc, city, fee_rate, account_id, created_at)
    values (p_name, p_category, p_mcc, p_city, p_fee_rate, a, p_at) returning merchant_id into v;
    return v;
end $$;

create function submit_kyc(p_user bigint, p_level kyc_level, p_at timestamptz default now()) returns bigint
language plpgsql as $$
declare v bigint;
begin
    insert into wallet.kyc_checks (user_id, level, submitted_at) values (p_user, p_level, p_at) returning kyc_id into v;
    perform wallet._event('kyc.submitted', 'user', p_user::text, jsonb_build_object('kyc_id', v, 'level', p_level), p_at);
    return v;
end $$;

create function decide_kyc(p_kyc bigint, p_approved boolean, p_reason text default null,
                           p_at timestamptz default now()) returns void
language plpgsql as $$
declare k wallet.kyc_checks;
begin
    update wallet.kyc_checks
       set status = case when p_approved then 'approved' else 'rejected' end,
           decided_at = p_at, reject_reason = case when p_approved then null else coalesce(p_reason, 'other') end
     where kyc_id = p_kyc and status = 'pending'
    returning * into k;
    if not found then raise exception 'kyc %: нет заявки на рассмотрении', p_kyc; end if;
    if p_approved then
        update wallet.users set kyc_level = greatest(kyc_level, k.level), updated_at = p_at where user_id = k.user_id;
    end if;
    perform wallet._event(case when p_approved then 'kyc.approved' else 'kyc.rejected' end, 'user', k.user_id::text,
        jsonb_build_object('kyc_id', k.kyc_id, 'level', k.level, 'reason', k.reject_reason), p_at);
end $$;

-- ------------------------------------------------------------------ деньги
-- p_fail — отказ внешнего провайдера (эквайер/банк): позволяет воспроизводить инциденты
create function topup(p_user bigint, p_amount numeric, p_key text, p_at timestamptz default now(),
                      p_fail text default null) returns op_result
language plpgsql as $$
declare r wallet.op_result; v_op uuid; w bigint := wallet._wallet_of(p_user); lim wallet.kyc_limits; bal numeric;
begin
    r := wallet._replay(p_user, p_key); if r.op_id is not null then return r; end if;
    v_op := wallet._open('topup', p_user, p_key, p_amount, wallet._system('cash_in'), w, p_at);
    if v_op is null then return wallet._replay(p_user, p_key); end if;
    perform wallet._lock(w, wallet._system('cash_in'));
    select l.* into lim from wallet.kyc_limits l join wallet.users u on u.kyc_level = l.level where u.user_id = p_user;
    select balance into bal from wallet.accounts where account_id = w;
    if p_fail is not null then return wallet._finish(v_op, 'failed', p_fail, p_at); end if;
    if bal + p_amount > lim.max_balance then return wallet._finish(v_op, 'failed', 'balance_limit', p_at); end if;
    perform wallet._move(v_op, wallet._system('cash_in'), w, p_amount, p_at);
    return wallet._finish(v_op, 'succeeded', null, p_at);
end $$;

create function transfer(p_from_user bigint, p_to_user bigint, p_amount numeric, p_key text,
                         p_at timestamptz default now()) returns op_result
language plpgsql as $$
declare r wallet.op_result; v_op uuid; a bigint := wallet._wallet_of(p_from_user); b bigint := wallet._wallet_of(p_to_user);
        ls wallet.kyc_limits; lr wallet.kyc_limits; sender wallet.users; bal_a numeric; bal_b numeric;
begin
    r := wallet._replay(p_from_user, p_key); if r.op_id is not null then return r; end if;
    if b is null then raise exception 'получатель % не найден', p_to_user using errcode = 'P0002'; end if;
    v_op := wallet._open('p2p', p_from_user, p_key, p_amount, a, b, p_at);
    if v_op is null then return wallet._replay(p_from_user, p_key); end if;
    perform wallet._lock(a, b);
    select * into sender from wallet.users where user_id = p_from_user;
    select l.* into ls from wallet.kyc_limits l where l.level = sender.kyc_level;
    select l.* into lr from wallet.kyc_limits l join wallet.users u on u.kyc_level = l.level where u.user_id = p_to_user;
    select balance into bal_a from wallet.accounts where account_id = a;
    select balance into bal_b from wallet.accounts where account_id = b;
    if sender.status <> 'active' then return wallet._finish(v_op, 'failed', 'blocked', p_at); end if;
    if not ls.p2p_allowed then return wallet._finish(v_op, 'failed', 'kyc_required', p_at); end if;
    if bal_a < p_amount then return wallet._finish(v_op, 'failed', 'insufficient_funds', p_at); end if;
    if wallet._outflow_today(a, p_at) + p_amount > ls.daily_outflow then
        return wallet._finish(v_op, 'failed', 'daily_limit', p_at);
    end if;
    if bal_b + p_amount > lr.max_balance then return wallet._finish(v_op, 'failed', 'recipient_limit', p_at); end if;
    perform wallet._move(v_op, a, b, p_amount, p_at);
    return wallet._finish(v_op, 'succeeded', null, p_at);
end $$;

-- покупка у мерчанта: комиссия мерчанта — доход сервиса; кешбэк — отдельная операция в той же транзакции.
-- Эксперимент cashback_boost: в группе test кешбэк 3 % первые 30 дней после регистрации, иначе 1 %.
create function purchase(p_user bigint, p_merchant bigint, p_amount numeric, p_key text,
                         p_at timestamptz default now(), p_fail text default null) returns op_result
language plpgsql as $$
declare r wallet.op_result; v_op uuid; v_cb uuid; w bigint := wallet._wallet_of(p_user); m wallet.merchants;
        u wallet.users; lim wallet.kyc_limits; bal numeric; v_fee numeric; v_rate numeric := 0.01; v_cash numeric;
begin
    r := wallet._replay(p_user, p_key); if r.op_id is not null then return r; end if;
    select * into m from wallet.merchants where merchant_id = p_merchant;
    v_op := wallet._open('purchase', p_user, p_key, p_amount, w, m.account_id, p_at, p_merchant);
    if v_op is null then return wallet._replay(p_user, p_key); end if;
    perform wallet._lock(w, m.account_id, wallet._system('fees'), wallet._system('cashback'));
    select * into u from wallet.users where user_id = p_user;
    select * into lim from wallet.kyc_limits where level = u.kyc_level;
    select balance into bal from wallet.accounts where account_id = w;
    if p_fail is not null then return wallet._finish(v_op, 'failed', p_fail, p_at); end if;
    if u.status <> 'active' then return wallet._finish(v_op, 'failed', 'blocked', p_at); end if;
    if bal < p_amount then return wallet._finish(v_op, 'failed', 'insufficient_funds', p_at); end if;
    if wallet._outflow_today(w, p_at) + p_amount > lim.daily_outflow then
        return wallet._finish(v_op, 'failed', 'daily_limit', p_at);
    end if;
    v_fee := round(p_amount * m.fee_rate, 2);
    perform wallet._move(v_op, w, m.account_id, p_amount, p_at);
    perform wallet._move(v_op, m.account_id, wallet._system('fees'), v_fee, p_at);
    r := wallet._finish(v_op, 'succeeded', null, p_at, v_fee);

    select case when a.variant = 'test' then 0.03 else 0.01 end into v_rate
      from wallet.experiment_assignments a join wallet.experiments e using (experiment_key)
     where a.user_id = p_user and a.experiment_key = 'cashback_boost' and p_at < u.created_at + interval '30 days';
    v_cash := round(p_amount * coalesce(v_rate, 0.01), 2);
    if v_cash >= 0.01 then
        v_cb := wallet._open('cashback', null, null, v_cash, wallet._system('cashback'), w, p_at, p_merchant, v_op);
        perform wallet._move(v_cb, wallet._system('cashback'), w, v_cash, p_at);
        perform wallet._finish(v_cb, 'succeeded', null, p_at);
    end if;
    return r;
end $$;

-- возврат покупки (инициирует мерчант): доля комиссии возвращается пропорционально, сумма возвратов ≤ покупки
create function refund(p_purchase uuid, p_amount numeric, p_key text, p_at timestamptz default now()) returns op_result
language plpgsql as $$
declare r wallet.op_result; v_op uuid; p wallet.operations; m wallet.merchants; done numeric; v_fee numeric; bal numeric;
begin
    select * into p from wallet.operations where op_id = p_purchase and kind = 'purchase' and status = 'succeeded';
    if not found then raise exception 'покупка % не найдена или не проведена', p_purchase using errcode = 'P0002'; end if;
    r := wallet._replay(p.initiator_user_id, p_key); if r.op_id is not null then return r; end if;
    select * into m from wallet.merchants where merchant_id = p.merchant_id;
    v_op := wallet._open('refund', p.initiator_user_id, p_key, p_amount, m.account_id, p.from_account, p_at, p.merchant_id, p.op_id);
    if v_op is null then return wallet._replay(p.initiator_user_id, p_key); end if;
    perform wallet._lock(m.account_id, p.from_account, wallet._system('fees'));
    select coalesce(sum(amount), 0) into done from wallet.operations
     where parent_op_id = p.op_id and kind = 'refund' and status = 'succeeded';
    if done + p_amount > p.amount then return wallet._finish(v_op, 'failed', 'refund_exceeds_purchase', p_at); end if;
    v_fee := round(p.fee * p_amount / p.amount, 2);
    select balance into bal from wallet.accounts where account_id = m.account_id;
    if bal < p_amount - v_fee then return wallet._finish(v_op, 'failed', 'merchant_insufficient_funds', p_at); end if;
    perform wallet._move(v_op, m.account_id, p.from_account, p_amount - v_fee, p_at);
    perform wallet._move(v_op, wallet._system('fees'), p.from_account, v_fee, p_at);
    return wallet._finish(v_op, 'succeeded', null, p_at);
end $$;

-- вывод на карту: комиссия 1 %, не меньше 50 ₽; только для идентифицированных
create function withdraw(p_user bigint, p_amount numeric, p_key text, p_at timestamptz default now()) returns op_result
language plpgsql as $$
declare r wallet.op_result; v_op uuid; w bigint := wallet._wallet_of(p_user); u wallet.users; lim wallet.kyc_limits;
        bal numeric; v_fee numeric := greatest(50, round(p_amount * 0.01, 2));
begin
    r := wallet._replay(p_user, p_key); if r.op_id is not null then return r; end if;
    if p_amount <= v_fee then raise exception 'сумма вывода % не больше комиссии %', p_amount, v_fee using errcode = '22023'; end if;
    v_op := wallet._open('withdrawal', p_user, p_key, p_amount, w, wallet._system('cash_out'), p_at);
    if v_op is null then return wallet._replay(p_user, p_key); end if;
    perform wallet._lock(w, wallet._system('cash_out'), wallet._system('fees'));
    select * into u from wallet.users where user_id = p_user;
    select * into lim from wallet.kyc_limits where level = u.kyc_level;
    select balance into bal from wallet.accounts where account_id = w;
    if u.kyc_level = 'none' then return wallet._finish(v_op, 'failed', 'kyc_required', p_at); end if;
    if bal < p_amount then return wallet._finish(v_op, 'failed', 'insufficient_funds', p_at); end if;
    if wallet._outflow_today(w, p_at) + p_amount > lim.daily_outflow then
        return wallet._finish(v_op, 'failed', 'daily_limit', p_at);
    end if;
    perform wallet._move(v_op, w, wallet._system('cash_out'), p_amount - v_fee, p_at);
    perform wallet._move(v_op, w, wallet._system('fees'), v_fee, p_at);
    return wallet._finish(v_op, 'succeeded', null, p_at, v_fee);
end $$;

-- сверка: денормализованные остатки против суммы проводок
create view balance_mismatches as
select a.account_id, a.balance, coalesce(l.s, 0) as ledger_sum
from accounts a left join (select account_id, sum(amount) s from ledger_entries group by account_id) l using (account_id)
where a.balance <> coalesce(l.s, 0);
