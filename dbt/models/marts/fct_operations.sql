{{ config(order_by='(created_date, op_id)', partition_by='toYYYYMM(created_date)') }}
-- Операции в финальном статусе. user_id — клиент, чья это операция: инициатор, а для кешбэка — получатель.
-- cashback_amount — кешбэк, начисленный за покупку (дочерняя операция).
with o as (select * from {{ ref('stg_operations') }}),
cb as (
    select parent_op_id, sum(amount) as cashback_amount
    from o where kind = 'cashback' and status = 'succeeded' group by parent_op_id
)
select
    assumeNotNull(o.op_id)                                           as op_id,
    assumeNotNull(o.kind)                                            as kind,
    assumeNotNull(o.status)                                          as status,
    o.failure_reason                                                 as failure_reason,
    coalesce(o.initiator_user_id, dst.user_id)                       as user_id,
    if(o.kind = 'p2p', dst.user_id, null)                            as counterparty_user_id,
    o.merchant_id                                                    as merchant_id,
    m.category                                                       as merchant_category,
    o.parent_op_id                                                   as parent_op_id,
    o.amount                                                         as amount,
    o.fee                                                            as fee,
    coalesce(cb.cashback_amount, toDecimal64(0, 2))                  as cashback_amount,
    o.created_at                                                     as created_at,
    assumeNotNull(toDate(o.created_at, '{{ var("tz") }}'))           as created_date,
    toHour(o.created_at, '{{ var("tz") }}')                          as created_hour,
    o.status = 'succeeded'                                           as is_success
from o
left join {{ ref('int_account_owners') }} dst on dst.account_id = o.to_account
left join {{ ref('stg_merchants') }} m on m.merchant_id = o.merchant_id
left join cb on cb.parent_op_id = o.op_id
