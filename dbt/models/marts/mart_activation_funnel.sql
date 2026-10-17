{{ config(order_by='(signup_week, channel)') }}
-- Воронка онбординга по неделе регистрации и каналу: шаг засчитывается, если случился в первые 30 дней
select
    signup_week,
    channel,
    count()                                                                       as signed_up,
    countIf(kyc_started_at < signup_at + interval 30 day)                         as kyc_started,
    countIf(kyc_submitted_at < signup_at + interval 30 day)                       as kyc_submitted,
    countIf(kyc_approved_at < signup_at + interval 30 day)                        as kyc_approved,
    countIf(first_topup_at < signup_at + interval 30 day)                         as topped_up,
    countIf(first_purchase_at < signup_at + interval 30 day)                      as purchased,
    countIf(first_p2p_at < signup_at + interval 30 day)                           as sent_p2p,
    round(purchased / signed_up, 4)                                               as activation_30d
from {{ ref('dim_users') }}
group by signup_week, channel
