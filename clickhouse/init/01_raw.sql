-- Приём из Kafka (Redpanda) в ClickHouse: «бронзовый» слой без бизнес-логики.
--   raw.cdc_log        — журнал изменений всех таблиц OLTP как есть (JSON); разбор и «последняя версия» — в dbt.
--                        Журнал хранит всю историю изменений → из него строится SCD2 без снапшотов.
--   raw.app_events     — события приложения, прошедшие контракт, — каждая доставка, включая повторы SDK.
--                        Дубли убирает staging (dbt): если схлопнуть их здесь, долю дублей уже не измерить.
--   raw.app_events_dlq — нарушившие контракт: сколько и почему (мониторинг качества трекинга).
--   raw.domain_events  — доменные события из outbox.
create database if not exists raw;

-- ------------------------------------------------------------------ CDC
create table raw.kafka_cdc (payload String) engine = Kafka settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'pg.wallet.users,pg.wallet.kyc_checks,pg.wallet.accounts,pg.wallet.merchants,pg.wallet.operations,pg.wallet.ledger_entries,pg.wallet.experiments,pg.wallet.experiment_assignments',
    kafka_group_name = 'ch_cdc', kafka_format = 'JSONAsString', kafka_num_consumers = 1, kafka_max_block_size = 65536;

create table raw.cdc_log (
    source_table    LowCardinality(String),
    key             String,
    payload         String,
    op              LowCardinality(String) comment 'r — снапшот, c — вставка, u — изменение, d — удаление',
    lsn             UInt64 comment 'Позиция в WAL: порядок изменений одной строки',
    source_ts       DateTime64(3, 'UTC') comment 'Когда изменение прочитал Debezium',
    kafka_partition UInt16,
    kafka_offset    UInt64,
    ingested_at     DateTime64(3, 'UTC') default now64(3)
) engine = MergeTree partition by source_table order by (source_table, key, lsn, kafka_offset)
comment 'Журнал изменений OLTP (Debezium, ПДн исключены на источнике)';

create materialized view raw.mv_cdc to raw.cdc_log as
select replaceOne(_topic, 'pg.wallet.', '')                     as source_table,
       _key                                                      as key,
       payload,
       JSONExtractString(payload, '__op')                        as op,
       JSONExtractUInt(payload, '__source_lsn')                  as lsn,
       fromUnixTimestamp64Milli(JSONExtractInt(payload, '__source_ts_ms')) as source_ts,
       _partition                                                as kafka_partition,
       _offset                                                   as kafka_offset
from raw.kafka_cdc;

-- ------------------------------------------------------------------ события приложения
create table raw.kafka_app_events (payload String) engine = Kafka settings
    kafka_broker_list = 'redpanda:9092', kafka_topic_list = 'app_events',
    kafka_group_name = 'ch_app_events', kafka_format = 'JSONAsString', kafka_max_block_size = 65536;

create table raw.app_events (
    event_id        UUID,
    event_name      LowCardinality(String),
    user_id         Nullable(UInt64),
    anonymous_id    String,
    session_id      String,
    client_ts       DateTime64(3, 'UTC'),
    received_at     DateTime64(3, 'UTC'),
    platform        LowCardinality(String),
    app_version     LowCardinality(String),
    schema_version  LowCardinality(String),
    properties      String,
    ingested_at     DateTime64(3, 'UTC') default now64(3)
) engine = MergeTree
partition by toYYYYMM(client_ts)
order by (event_name, toDate(client_ts), event_id)
comment 'События приложения после проверки контракта; все доставки, повторы SDK (тот же event_id) не схлопнуты';

create materialized view raw.mv_app_events to raw.app_events as
select toUUID(JSONExtractString(payload, 'event_id'))                                  as event_id,
       JSONExtractString(payload, 'event_name')                                        as event_name,
       if(JSONExtractUInt(payload, 'user_id') = 0, null, JSONExtractUInt(payload, 'user_id')) as user_id,
       JSONExtractString(payload, 'anonymous_id')                                      as anonymous_id,
       JSONExtractString(payload, 'session_id')                                        as session_id,
       parseDateTime64BestEffort(JSONExtractString(payload, 'client_ts'), 3, 'UTC')    as client_ts,
       parseDateTime64BestEffort(JSONExtractString(payload, 'received_at'), 3, 'UTC')  as received_at,
       JSONExtractString(payload, 'platform')                                          as platform,
       JSONExtractString(payload, 'app_version')                                       as app_version,
       JSONExtractString(payload, 'schema_version')                                    as schema_version,
       JSONExtractRaw(payload, 'properties')                                           as properties
from raw.kafka_app_events;

create table raw.kafka_app_events_dlq (payload String) engine = Kafka settings
    kafka_broker_list = 'redpanda:9092', kafka_topic_list = 'app_events_dlq',
    kafka_group_name = 'ch_app_events_dlq', kafka_format = 'JSONAsString';

create table raw.app_events_dlq (
    received_at  DateTime64(3, 'UTC'),
    error        String,
    raw          String,
    ingested_at  DateTime64(3, 'UTC') default now64(3)
) engine = MergeTree order by received_at comment 'События, нарушившие контракт';

create materialized view raw.mv_app_events_dlq to raw.app_events_dlq as
select parseDateTime64BestEffort(JSONExtractString(payload, 'received_at'), 3, 'UTC') as received_at,
       JSONExtractString(payload, 'error') as error,
       JSONExtractString(payload, 'raw')   as raw
from raw.kafka_app_events_dlq;

-- ------------------------------------------------------------------ доменные события (outbox)
-- исходный столбец не называем payload: псевдоним payload ниже затенил бы его во всех выражениях
create table raw.kafka_domain (msg String) engine = Kafka settings
    kafka_broker_list = 'redpanda:9092', kafka_topic_list = 'domain.user,domain.operation',
    kafka_group_name = 'ch_domain', kafka_format = 'JSONAsString';

create table raw.domain_events (
    event_id     UUID,
    event_type   LowCardinality(String),
    aggregate    LowCardinality(String),
    aggregate_id String,
    created_at   DateTime64(6, 'UTC'),
    payload      String,
    ingested_at  DateTime64(3, 'UTC') default now64(3)
) engine = ReplacingMergeTree(ingested_at) partition by toYYYYMM(created_at) order by (event_type, event_id)
comment 'Доменные события сервиса (transactional outbox)';

create materialized view raw.mv_domain to raw.domain_events as
select toUUID(JSONExtractString(msg, 'event_id'))                                 as event_id,
       JSONExtractString(msg, 'event_type')                                       as event_type,
       replaceOne(_topic, 'domain.', '')                                              as aggregate,
       replaceAll(_key, '"', '')                                                      as aggregate_id,
       parseDateTime64BestEffort(JSONExtractString(msg, 'created_at'), 6, 'UTC')  as created_at,
       JSONExtractRaw(msg, 'payload')                                             as payload
from raw.kafka_domain;
