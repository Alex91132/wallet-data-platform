-- События приложения: повторные доставки одного event_id убраны (первая по времени приёма), свойства разобраны.
-- lateness_s — на сколько событие опоздало: офлайн-устройства присылают пачки через часы и дни.
select
    event_id,
    event_name,
    user_id,
    anonymous_id,
    session_id,
    client_ts,
    received_at,
    toDate(client_ts, '{{ var("tz") }}')                       as event_date,
    dateDiff('second', client_ts, received_at)                 as lateness_s,
    platform,
    app_version,
    JSONExtract(properties, 'screen', 'Nullable(String)')          as screen,
    JSONExtract(properties, 'amount', 'Nullable(Float64)')         as amount,
    JSONExtract(properties, 'level', 'Nullable(String)')           as kyc_level,
    JSONExtract(properties, 'step', 'Nullable(String)')            as kyc_step,
    JSONExtract(properties, 'form_version', 'Nullable(String)')    as form_version,
    JSONExtract(properties, 'channel', 'Nullable(String)')         as channel,
    JSONExtract(properties, 'experiment_key', 'Nullable(String)')  as experiment_key,
    JSONExtract(properties, 'variant', 'Nullable(String)')         as variant
from {{ source('raw', 'app_events') }}
order by event_id, ingested_at
limit 1 by event_id
