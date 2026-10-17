{{ config(order_by='(form_version, week)') }}
-- Пошаговая воронка формы KYC по версиям формы (v2 вышла в 3.2.0) и неделям.
-- Попытка = сессия с kyc_started; версия формы — из шагов, а если до шагов не дошли — по версии приложения.
with attempts as (
    select session_id,
           any(user_id)                                                    as user_id,
           min(client_ts)                                                  as started_at,
           anyIf(form_version, form_version is not null)                   as fv,
           any(app_version)                                                as app_version,
           maxIf(1, event_name = 'kyc_step_completed' and kyc_step = 'passport') as passport,
           maxIf(1, event_name = 'kyc_step_completed' and kyc_step = 'selfie')   as selfie,
           maxIf(1, event_name = 'kyc_step_completed' and kyc_step = 'address')  as address,
           maxIf(1, event_name = 'kyc_submitted')                         as submitted
    from {{ ref('stg_app_events') }}
    where session_id in (select session_id from {{ ref('stg_app_events') }} where event_name = 'kyc_started')
    group by session_id
)
select
    assumeNotNull(coalesce(fv, if(arrayMap(x -> toUInt32OrZero(x), splitByChar('.', app_version)) >= [3, 2, 0], 'v2', 'v1')))  as form_version,
    assumeNotNull(toMonday(toDate(started_at, '{{ var("tz") }}')))           as week,
    count()                                    as started,
    sum(passport)                              as passport_done,
    sum(selfie)                                as selfie_done,
    sum(address)                               as address_done,
    sum(submitted)                             as submitted_done,
    round(submitted_done / started, 4)         as completion_rate
from attempts
group by form_version, week
