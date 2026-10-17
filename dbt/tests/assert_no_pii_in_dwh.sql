-- В хранилище не попадают открытые персональные данные: ни столбцов, ни значений в журнале CDC
select database, table, name
from system.columns
where database in ('raw', 'analytics') and match(lower(name), '^(phone|email|full_name|birth_date|passport)$')
union all
select 'raw', 'cdc_log', 'payload'
from {{ source('raw', 'cdc_log') }}
where source_table = 'users' and match(payload, '"(phone|email|full_name|birth_date)"')
limit 10
