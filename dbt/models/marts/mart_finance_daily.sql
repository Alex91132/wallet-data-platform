{{ config(order_by='day') }}
-- Деньги по дням — из проводок (источник истины), а не из операций:
-- выручка = чистое движение по счёту комиссий, расход на кешбэк = списания со счёта кешбэка.
with le as (
    select l.*, o.code, toDate(l.created_at, '{{ var("tz") }}') as day
    from {{ ref('stg_ledger_entries') }} l
    join {{ ref('int_account_owners') }} o on o.account_id = l.account_id
),
ledger as (
    select day,
           sumIf(amount, code = 'fees')              as fee_revenue,
           -sumIf(amount, code = 'cashback')         as cashback_cost,
           -sumIf(amount, code = 'cash_in')          as topup_volume,
           sumIf(amount, code = 'cash_out')          as withdrawal_volume
    from le group by day
),
ops as (
    select created_date as day,
           sumIf(amount, kind = 'purchase' and is_success)  as purchase_gmv,
           countIf(kind = 'purchase' and is_success)        as purchases,
           sumIf(amount, kind = 'p2p' and is_success)       as p2p_volume,
           sumIf(amount, kind = 'refund' and is_success)    as refund_volume,
           uniqExactIf(user_id, kind = 'purchase' and is_success) as paying_users,
           countIf(not is_success)                          as failed_ops,
           count()                                          as all_ops
    from {{ ref('fct_operations') }} where kind != 'cashback' group by day
)
select
    assumeNotNull(ops.day)                                  as day,
    ops.purchase_gmv, ops.purchases, ops.paying_users, ops.p2p_volume, ops.refund_volume,
    ledger.topup_volume, ledger.withdrawal_volume,
    ledger.fee_revenue, ledger.cashback_cost,
    ledger.fee_revenue - ledger.cashback_cost               as net_revenue,
    round(ledger.fee_revenue / nullIf(ops.purchase_gmv, 0), 5) as take_rate,
    round(ops.failed_ops / ops.all_ops, 4)                  as failure_rate
from ops left join ledger on ledger.day = ops.day
