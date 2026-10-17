{{ config(order_by='user_id') }}
-- Правило «дропа»: после пополнения за 2 часа ≥ 5 переводов разным получателям, почти все — круглыми суммами,
-- получатели — свежие кошельки (зарегистрированы не раньше 14 дней назад). Признаки сохраняются для разбора аналитиком.
with topups as (
    select user_id, created_at as topup_at, amount as topup_amount
    from {{ ref('fct_operations') }} where kind = 'topup' and is_success
),
p2p as (
    select o.user_id, o.counterparty_user_id, o.amount, o.created_at, u.signup_at as recipient_signup
    from {{ ref('fct_operations') }} o
    join {{ ref('dim_users') }} u on u.user_id = o.counterparty_user_id
    where o.kind = 'p2p' and o.is_success
),
bursts as (
    select t.user_id, t.topup_at, any(t.topup_amount) as topup_amount,
           count()                                                         as transfers_2h,
           uniqExact(p.counterparty_user_id)                               as recipients_2h,
           round(countIf(p.amount % 1000 = 0) / count(), 3)                as round_share,
           round(countIf(p.recipient_signup > p.created_at - interval 14 day) / count(), 3) as fresh_recipient_share,
           sum(p.amount)                                                   as outflow_2h
    from topups t
    join p2p p on p.user_id = t.user_id
    where p.created_at >= t.topup_at and p.created_at < t.topup_at + interval 2 hour
    group by t.user_id, t.topup_at
)
select
    assumeNotNull(user_id) as user_id,
    max(transfers_2h) as max_transfers_2h, max(recipients_2h) as max_recipients_2h,
    max(round_share) as max_round_share, max(fresh_recipient_share) as max_fresh_share,
    max(outflow_2h) as max_outflow_2h, min(topup_at) as first_burst_at,
    max_transfers_2h >= 5 and max_recipients_2h >= 5 and max_round_share >= 0.8 and max_fresh_share >= 0.5 as flagged
from bursts
group by user_id
having max(transfers_2h) >= 3
