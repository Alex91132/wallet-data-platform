{{ config(order_by='day') }}
-- DAU / WAU / MAU по открытиям приложения (зарегистрированные пользователи), «липкость» DAU/MAU.
-- Скользящие окна: каждый активный день пользователя «размножается» на 28 дней вперёд.
with active as (
    select distinct user_id, event_date as day
    from {{ ref('stg_app_events') }} where user_id is not null and event_name = 'app_open'
),
spread as (
    select user_id, day + n as d, n from active array join range(28) as n
),
bounds as (select min(day) as lo, max(day) as hi from active)
select
    assumeNotNull(d)                       as day,
    uniqExactIf(user_id, n = 0)            as dau,
    uniqExactIf(user_id, n < 7)            as wau,
    uniqExact(user_id)                     as mau,
    round(dau / nullIf(mau, 0), 4)         as stickiness
from spread
where d between (select lo + 27 from bounds) and (select hi from bounds)
group by d
