{{ config(order_by='user_id') }}
-- A/B cashback_boost: метрики каждого участника за первые 30 дней (пока действует кешбэк 3 %)
-- и активность на днях 31–60 — остаётся ли эффект, когда повышенный кешбэк закончился.
with p as (
    select a.user_id as user_id, a.variant as variant, u.signup_at as signup_at, u.channel as channel
    from {{ ref('stg_experiment_assignments') }} a
    join {{ ref('dim_users') }} u on u.user_id = a.user_id
    where a.experiment_key = 'cashback_boost'
),
ops as (
    select o.user_id, o.kind, o.amount, o.fee, o.cashback_amount, o.created_at, o.created_date, p.signup_at
    from {{ ref('fct_operations') }} o join p on p.user_id = o.user_id
    where o.is_success and o.kind in ('purchase', 'refund')
),
exposed as (
    select distinct user_id from {{ ref('stg_app_events') }}
    where event_name = 'experiment_exposure' and experiment_key = 'cashback_boost'
)
select
    assumeNotNull(p.user_id)                                                                          as user_id,
    p.variant, p.channel, p.signup_at,
    p.user_id in (select user_id from exposed)                                                        as exposed,
    countIf(o.kind = 'purchase' and o.created_at < p.signup_at + interval 30 day)                     as purchases_30d,
    coalesce(sumIf(o.amount, o.kind = 'purchase' and o.created_at < p.signup_at + interval 30 day), 0)          as gmv_30d,
    coalesce(sumIf(o.fee, o.kind = 'purchase' and o.created_at < p.signup_at + interval 30 day), 0)             as fee_30d,
    coalesce(sumIf(o.cashback_amount, o.kind = 'purchase' and o.created_at < p.signup_at + interval 30 day), 0) as cashback_30d,
    fee_30d - cashback_30d                                                                            as net_30d,
    uniqExactIf(o.created_date, o.kind = 'purchase' and o.created_at >= p.signup_at + interval 30 day
                               and o.created_at < p.signup_at + interval 60 day)                     as active_days_31_60
from p left join ops o on o.user_id = p.user_id
group by p.user_id, p.variant, p.channel, p.signup_at
