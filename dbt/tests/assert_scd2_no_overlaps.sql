-- SCD2: интервалы версий одного пользователя не пересекаются, ровно одна текущая версия
select user_id
from {{ ref('dim_users_scd2') }}
group by user_id
having countIf(is_current) != 1
    or countIf(valid_to is null) != 1
    or arrayExists((a, b) -> a > b, arraySort(groupArray(valid_from)), arrayPushBack(arraySlice(arraySort(groupArray(valid_from)), 2), toDateTime64('2100-01-01', 6, 'UTC')))
