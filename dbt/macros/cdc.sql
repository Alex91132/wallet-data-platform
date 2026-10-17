{#
  Журнал CDC (raw.cdc_log) хранит каждую версию строки OLTP как JSON.
  cdc_current — последняя версия каждой строки (порядок — позиция в WAL), удалённые отброшены.
  cdc_versions — все версии: из них строится история (SCD2).
  fields — словарь «столбец: тип ClickHouse»; разбор JSON — здесь, а не при приёме (ELT).
#}

{% macro json_field(col, ch_type) -%}
    {%- if ch_type.startswith('Decimal') -%}
        toDecimal64OrNull(JSONExtractString(payload, '{{ col }}'), {{ ch_type[8:-1] or 2 }})
    {%- elif ch_type.startswith('DateTime') -%}
        parseDateTime64BestEffortOrNull(JSONExtractString(payload, '{{ col }}'), 6, 'UTC')
    {%- elif ch_type == 'UUID' -%}
        toUUIDOrNull(JSONExtractString(payload, '{{ col }}'))
    {%- elif ch_type == 'String' -%}
        JSONExtract(payload, '{{ col }}', 'Nullable(String)')
    {%- else -%}
        JSONExtract(payload, '{{ col }}', 'Nullable({{ ch_type }})')
    {%- endif -%}
{%- endmacro %}

{% macro cdc_current(table, fields) %}
select
    {%- for col, ch_type in fields.items() %}
    {{ json_field(col, ch_type) }} as {{ col }},
    {%- endfor %}
    last_lsn as _lsn,
    last_source_ts as _source_ts
from (
    -- псевдонимы не совпадают с именами столбцов: в ClickHouse псевдоним виден во всём SELECT
    -- и затенил бы столбец внутри соседних агрегатов
    select key,
           argMax(cdc.payload, (cdc.lsn, cdc.kafka_offset))   as payload,
           max(cdc.lsn)                                       as last_lsn,
           argMax(cdc.source_ts, (cdc.lsn, cdc.kafka_offset)) as last_source_ts
    from {{ source('raw', 'cdc_log') }} as cdc
    where cdc.source_table = '{{ table }}'
    group by key
)
where JSONExtractString(payload, '__deleted') != 'true'
{% endmacro %}

{% macro cdc_versions(table, fields) %}
select
    {%- for col, ch_type in fields.items() %}
    {{ json_field(col, ch_type) }} as {{ col }},
    {%- endfor %}
    op as _op,
    lsn as _lsn,
    kafka_offset as _offset
from {{ source('raw', 'cdc_log') }}
where source_table = '{{ table }}'
{% endmacro %}
