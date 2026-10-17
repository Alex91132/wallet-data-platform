-- Доменные события (outbox): user.registered, kyc.*, operation.*
select event_id, event_type, aggregate, aggregate_id, created_at, payload
from {{ source('raw', 'domain_events') }} final
