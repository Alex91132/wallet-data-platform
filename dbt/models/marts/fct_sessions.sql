{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='session_id',
    order_by='(session_date, session_id)',
    partition_by='toYYYYMM(session_date)'
) }}
-- Сессии приложения, инкрементально. Ключевой момент — поздние события: телефон из офлайна присылает пачку
-- через часы и дни. Поэтому водяной знак — время ПРИЁМА (received_at), а не время события, и пересчитываются
-- целиком все сессии, которых коснулись события, пришедшие после прошлого запуска (с запасом late_lookback_hours).
with touched as (
    select distinct session_id from {{ ref('stg_app_events') }}
    {% if is_incremental() %}
    where received_at > (select max(last_received_at) - interval {{ var('late_lookback_hours') }} hour from {{ this }})
    {% endif %}
)
select
    session_id,
    anyIf(user_id, user_id is not null)                         as user_id,
    any(anonymous_id)                                           as anonymous_id,
    any(platform)                                               as platform,
    max(app_version)                                            as app_version,
    min(client_ts)                                              as started_at,
    max(client_ts)                                              as ended_at,
    assumeNotNull(toDate(min(client_ts), '{{ var("tz") }}'))    as session_date,
    dateDiff('second', min(client_ts), max(client_ts))          as duration_s,
    count()                                                     as events,
    countIf(event_name = 'screen_view')                         as screens,
    maxIf(1, event_name in ('purchase_started', 'transfer_started', 'topup_started')) as had_money_intent,
    max(received_at)                                            as last_received_at,
    max(lateness_s) > 3600                                      as arrived_late
from {{ ref('stg_app_events') }}
where session_id in (select session_id from touched)
group by session_id
