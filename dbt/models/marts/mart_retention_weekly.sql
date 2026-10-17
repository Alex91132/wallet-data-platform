{{ config(order_by='(signup_week, week_n)') }}
-- Транзакционное удержание: доля когорты (неделя регистрации), сделавшая хотя бы одну свою операцию
-- (пополнение, покупка, перевод, вывод) на неделе N после регистрации. Неполные недели в конце периода отброшены.
with activity as (
    select distinct user_id, toMonday(created_date) as week
    from {{ ref('fct_operations') }}
    where is_success and kind in ('topup', 'purchase', 'p2p', 'withdrawal')
),
cohorts as (select user_id, signup_week, channel from {{ ref('dim_users') }}),
sizes as (select signup_week, count() as cohort_size from cohorts group by signup_week),
last_week as (select toMonday(max(created_date)) - 7 as w from {{ ref('fct_operations') }})
select
    c.signup_week                                       as signup_week,
    toUInt16(dateDiff('week', c.signup_week, a.week))   as week_n,
    any(s.cohort_size)                                  as cohort_size,
    uniqExact(a.user_id)                                as active_users,
    round(active_users / cohort_size, 4)                as retention
from cohorts c
join activity a on a.user_id = c.user_id
join sizes s on s.signup_week = c.signup_week
where a.week >= c.signup_week and a.week <= (select w from last_week)
group by signup_week, week_n
