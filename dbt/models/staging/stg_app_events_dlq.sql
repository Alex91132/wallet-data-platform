-- Нарушения контракта событий: тип ошибки — поле, на котором споткнулась проверка
select
    received_at,
    toDate(received_at, '{{ var("tz") }}') as received_date,
    multiIf(error like '%session_id%', 'missing_session_id',
            error like '%app_version%', 'bad_app_version',
            error like '%amount%', 'invalid_amount',
            error like '%Additional properties%', 'unknown_field',
            error like '%user_id%', 'missing_user_id',
            'other') as error_type,
    error,
    raw
from {{ source('raw', 'app_events_dlq') }}
