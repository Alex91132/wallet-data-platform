-- Sample ratio mismatch: группы A/B 50/50 — хи-квадрат с 1 степенью свободы не больше 10,83 (p > 0,001)
with c as (select countIf(variant = 'control') as a, countIf(variant = 'test') as b from {{ ref('mart_experiment_cashback') }})
select a, b, pow(a - (a + b) / 2, 2) / ((a + b) / 2) + pow(b - (a + b) / 2, 2) / ((a + b) / 2) as chi2
from c
where chi2 > 10.83
