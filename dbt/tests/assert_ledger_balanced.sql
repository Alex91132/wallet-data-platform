-- Двойная запись в хранилище: у каждой операции сумма проводок = 0
select op_id, sum(amount) as total
from {{ ref('stg_ledger_entries') }}
group by op_id
having total != 0
