{{ config(order_by='(user_id, valid_from)') }}
-- История пользователя (SCD2) из журнала CDC: интервалы, когда действовали уровень KYC и статус.
-- Снапшоты dbt не нужны: журнал изменений уже содержит каждую версию строки.
with v as (
    -- одна версия на позицию WAL (повторная доставка из Kafka не плодит версии)
    select user_id, _lsn, any(kyc_level) as kyc_level, any(status) as status, any(updated_at) as updated_at
    from {{ ref('stg_users_history') }}
    group by user_id, _lsn
),
marked as (
    select *,
           lagInFrame(kyc_level || '|' || status, 1, '') over w as prev_state
    from v
    window w as (partition by user_id order by _lsn rows between unbounded preceding and unbounded following)
),
changes as (
    select * from marked where kyc_level || '|' || status != prev_state   -- только изменения отслеживаемых полей
)
select
    assumeNotNull(user_id)                                    as user_id,
    kyc_level,
    status,
    assumeNotNull(updated_at)                                 as valid_from,
    nullIf(leadInFrame(updated_at, 1) over w, toDateTime64(0, 6, 'UTC')) as valid_to,
    row_number() over w = count() over (partition by user_id) as is_current
from changes
window w as (partition by user_id order by _lsn rows between unbounded preceding and unbounded following)
