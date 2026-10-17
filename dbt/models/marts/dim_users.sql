{{ config(order_by='user_id') }}
-- Пользователь: атрибуты, когорта, группа эксперимента и вехи активации
select
    assumeNotNull(u.user_id)                                              as user_id,
    u.created_at                                                          as signup_at,
    assumeNotNull(toDate(u.created_at, '{{ var("tz") }}'))                as signup_date,
    assumeNotNull(toMonday(toDate(u.created_at, '{{ var("tz") }}')))      as signup_week,
    assumeNotNull(toStartOfMonth(toDate(u.created_at, '{{ var("tz") }}'))) as signup_month,
    assumeNotNull(u.channel) as channel, u.city, u.birth_year,
    multiIf(u.birth_year is null, 'unknown', 2026 - u.birth_year < 25, '18–24', 2026 - u.birth_year < 35, '25–34',
            2026 - u.birth_year < 45, '35–44', 2026 - u.birth_year < 55, '45–54', '55+') as age_group,
    u.kyc_level, u.status,
    ea.variant                                                            as cashback_variant,
    m.kyc_started_at, m.kyc_submitted_at, m.kyc_approved_at, m.kyc_rejections,
    m.first_topup_at, m.first_purchase_at, m.first_p2p_at,
    coalesce(m.purchases, 0)                                              as purchases,
    coalesce(m.purchases_gmv, toDecimal64(0, 2))                          as purchases_gmv,
    m.first_purchase_at is not null
        and m.first_purchase_at < u.created_at + interval 7 day           as activated_7d
from {{ ref('stg_users') }} u
left join {{ ref('int_user_milestones') }} m on m.user_id = u.user_id
left join (select user_id, variant from {{ ref('stg_experiment_assignments') }} where experiment_key = 'cashback_boost') ea
       on ea.user_id = u.user_id
