-- Сверка: остатки счетов, приехавшие по CDC, равны сумме проводок по счёту
select a.account_id, a.balance, l.ledger_sum
from {{ ref('stg_accounts') }} a
left join (select account_id, sum(amount) as ledger_sum from {{ ref('stg_ledger_entries') }} group by account_id) l
       on l.account_id = a.account_id
where a.balance != coalesce(l.ledger_sum, toDecimal64(0, 2))
