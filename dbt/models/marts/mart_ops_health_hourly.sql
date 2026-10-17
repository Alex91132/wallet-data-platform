{{ config(order_by='(hour, kind)') }}
-- Здоровье платежей по часам: доля отказов и главная причина — так виден сбой провайдера.
-- Бизнес-отказы (нет денег, лимит) и технические (provider_timeout) разделены: тревога — только по техническим.
select
    assumeNotNull(toStartOfHour(created_at, '{{ var("tz") }}'))                     as hour,
    kind,
    count()                                                                         as attempts,
    countIf(not is_success)                                                         as failed,
    countIf(failure_reason = 'provider_timeout')                                    as technical_failed,
    round(technical_failed / attempts, 4)                                           as technical_failure_rate,
    topKIf(1)(failure_reason, not is_success)[1]                                    as top_reason
from {{ ref('fct_operations') }}
where kind in ('topup', 'purchase', 'p2p', 'withdrawal')
group by hour, kind
