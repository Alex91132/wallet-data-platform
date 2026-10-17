-- Ключевые моменты пути пользователя: KYC, первое пополнение, первая покупка, первый перевод
with ops as (
    select initiator_user_id as user_id, kind, amount, created_at
    from {{ ref('stg_operations') }} where status = 'succeeded' and initiator_user_id is not null
),
kyc as (
    select user_id, min(submitted_at) as kyc_submitted_at, minIf(decided_at, status = 'approved') as kyc_approved_at,
           countIf(status = 'rejected') as kyc_rejections
    from {{ ref('stg_kyc_checks') }} group by user_id
),
ev as (
    select user_id, minIf(client_ts, event_name = 'kyc_started') as kyc_started_at
    from {{ ref('stg_app_events') }} where user_id is not null group by user_id
),
money as (
    select user_id,
           minIf(created_at, kind = 'topup')    as first_topup_at,
           minIf(created_at, kind = 'purchase') as first_purchase_at,
           minIf(created_at, kind = 'p2p')      as first_p2p_at,
           countIf(kind = 'purchase')           as purchases,
           sumIf(amount, kind = 'purchase')     as purchases_gmv
    from ops where kind in ('topup', 'purchase', 'p2p') group by user_id
)
select u.user_id as user_id, ev.kyc_started_at, kyc.kyc_submitted_at, kyc.kyc_approved_at, kyc.kyc_rejections,
       money.first_topup_at, money.first_purchase_at, money.first_p2p_at, money.purchases, money.purchases_gmv
from {{ ref('stg_users') }} u
left join kyc on kyc.user_id = u.user_id
left join ev on ev.user_id = u.user_id
left join money on money.user_id = u.user_id
