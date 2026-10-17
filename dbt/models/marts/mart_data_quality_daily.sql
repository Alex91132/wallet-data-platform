{{ config(order_by='day') }}
-- Качество трекинга по дням приёма: дубли повторной отправки, опоздавшие события, нарушения контракта.
with raw_counts as (
    select toDate(received_at, '{{ var("tz") }}') as day, count() as raw_events, uniqExact(event_id) as unique_events
    from {{ source('raw', 'app_events') }} group by day
),
late as (
    select toDate(received_at, '{{ var("tz") }}') as day,
           countIf(lateness_s > 3600)                          as late_events,
           quantile(0.99)(lateness_s)                          as lateness_p99_s
    from {{ ref('stg_app_events') }} group by day
),
dlq as (
    select received_date as day, count() as dlq_events, topK(1)(error_type)[1] as top_error
    from {{ ref('stg_app_events_dlq') }} group by day
)
select
    assumeNotNull(r.day)                                               as day,
    r.raw_events, r.unique_events,
    r.raw_events - r.unique_events                                     as duplicates,
    round((r.raw_events - r.unique_events) / r.raw_events, 4)          as duplicate_rate,
    l.late_events, round(l.late_events / r.unique_events, 4)           as late_rate, l.lateness_p99_s,
    coalesce(d.dlq_events, 0)                                          as dlq_events,
    round(coalesce(d.dlq_events, 0) / (r.raw_events + coalesce(d.dlq_events, 0)), 4) as contract_violation_rate,
    d.top_error
from raw_counts r
left join late l on l.day = r.day
left join dlq d on d.day = r.day
