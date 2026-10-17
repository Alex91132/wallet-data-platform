# Модель данных и поведение системы

## ER-модель (OLTP, PostgreSQL)

```mermaid
erDiagram
    USERS ||--o{ KYC_CHECKS : "подаёт"
    USERS ||--|| ACCOUNTS : "кошелёк"
    MERCHANTS ||--|| ACCOUNTS : "счёт мерчанта"
    KYC_LIMITS ||--o{ USERS : "уровень"
    USERS ||--o{ OPERATIONS : "инициирует"
    ACCOUNTS ||--o{ OPERATIONS : "откуда / куда"
    MERCHANTS ||--o{ OPERATIONS : "покупки, возвраты"
    OPERATIONS ||--o{ OPERATIONS : "кешбэк и возвраты → покупка"
    OPERATIONS ||--|{ LEDGER_ENTRIES : "проводки, сумма = 0"
    ACCOUNTS ||--o{ LEDGER_ENTRIES : ""
    OPERATIONS ||--o{ OP_STATUS_HISTORY : "журнал статусов"
    EXPERIMENTS ||--o{ EXPERIMENT_ASSIGNMENTS : ""
    USERS ||--o{ EXPERIMENT_ASSIGNMENTS : "группа"

    USERS {
        bigint user_id PK
        text phone UK "ПДн"
        text email UK "ПДн"
        text full_name "ПДн"
        smallint birth_year
        text channel
        kyc_level kyc_level
        text status
    }
    ACCOUNTS {
        bigint account_id PK
        text owner_type "user | merchant | system"
        bigint user_id FK "один кошелёк на пользователя"
        text code UK "cash_in, cash_out, fees, cashback"
        numeric balance "≥ 0, кроме системных"
    }
    OPERATIONS {
        uuid op_id PK
        op_kind kind
        op_status status
        bigint initiator_user_id FK
        text idempotency_key "UK вместе с инициатором"
        numeric amount "> 0"
        numeric fee
        bigint from_account FK
        bigint to_account FK
        uuid parent_op_id FK
        text failure_reason "только у failed"
    }
    LEDGER_ENTRIES {
        bigint entry_id PK
        uuid op_id FK
        bigint account_id FK
        numeric amount "≠ 0, знак = направление"
    }
    KYC_CHECKS {
        bigint kyc_id PK
        bigint user_id FK
        kyc_level level
        text status "pending | approved | rejected"
    }
```

Почему так:
- **Двойная запись.** Деньги не «появляются» в поле `balance`: каждое движение — пара проводок с суммой ноль.
  Пополнение уводит в минус системный счёт `cash_in`, поэтому сумма всех остатков в системе всегда 0 —
  это проверяет сверка. `balance` — денормализация для быстрых проверок лимитов; источник истины — проводки.
- **Отказ — тоже операция.** Аналитике нужны попытки, а не только успехи (доля отказов, причины, сбои провайдера).
- **Идемпотентность — ограничение БД**, а не проверка в коде: `unique (initiator_user_id, idempotency_key)`
  защищает и от одновременных повторов.

## Состояния операции

```mermaid
stateDiagram-v2
    [*] --> created: запрос принят, ключ уникален
    created --> succeeded: проверки пройдены, проводки записаны
    created --> failed: бизнес-правило или отказ провайдера
    succeeded --> reversed: сторно (ручное решение)
    failed --> [*]
    succeeded --> [*]
```

Переходы проверяет триггер `op_status_guard`; любой другой переход — ошибка. Журнал — `op_status_history`.

## Состояния заявки KYC

```mermaid
stateDiagram-v2
    [*] --> pending: submit_kyc (одна на пользователя)
    pending --> approved: decide_kyc(true) — уровень клиента повышается
    pending --> rejected: decide_kyc(false, причина)
    rejected --> [*]: клиент может подать новую заявку
    approved --> [*]
```

## Перевод: последовательность и надёжная доставка данных

```mermaid
sequenceDiagram
    autonumber
    participant App as Приложение
    participant API as Wallet API
    participant PG as PostgreSQL
    participant DBZ as Debezium
    participant K as Redpanda (Kafka)
    participant CH as ClickHouse

    App->>API: POST /transfers (Idempotency-Key)
    API->>PG: select * from transfer(from, to, amount, key)
    activate PG
    Note over PG: одна транзакция
    PG->>PG: повтор ключа? → вернуть прежнюю операцию
    PG->>PG: insert operations (created)
    PG->>PG: блокировка счетов по возрастанию id (FOR NO KEY UPDATE)
    PG->>PG: проверки: статус, KYC, остаток, дневной лимит
    PG->>PG: проводки (−/+), остатки, status = succeeded
    PG->>PG: insert outbox (operation.succeeded)
    PG-->>API: op_id, status
    deactivate PG
    API-->>App: 201 Created (200 + Idempotent-Replayed при повторе)
    PG--)DBZ: WAL (логическая репликация, публикация без ПДн)
    DBZ--)K: pg.wallet.operations, pg.wallet.ledger_entries, domain.operation
    K--)CH: Kafka engine → raw.cdc_log, raw.domain_events
    Note over CH: dbt: staging → marts, тесты, сверка с источником
```

Почему outbox, а не «записать в БД и отправить в Kafka из кода»: при падении сервиса между этими шагами событие
теряется или уходит без изменения данных. Outbox пишется в той же транзакции, а публикует его Debezium из WAL —
событие есть ровно тогда, когда есть изменение.

## Хранилище (ClickHouse)

```mermaid
flowchart LR
    subgraph raw [raw — приём из Kafka]
        cdc[cdc_log<br/>все версии строк OLTP]
        ev[app_events<br/>ReplacingMergeTree по event_id]
        dlq[app_events_dlq]
        dom[domain_events]
    end
    subgraph stg [staging — dbt, представления]
        s1[stg_users / stg_operations / stg_ledger_entries …<br/>последняя версия строки]
        s2[stg_users_history<br/>все версии]
        s3[stg_app_events]
    end
    subgraph marts [marts — dbt, таблицы]
        f1[fct_operations]
        f2[fct_sessions — инкрементально]
        d1[dim_users]
        d2[dim_users_scd2]
        m[mart_* — финансы, воронки, удержание,<br/>эксперимент, здоровье, антифрод, качество]
    end
    cdc --> s1 & s2
    ev --> s3
    dlq --> m
    s1 --> f1 & d1
    s2 --> d2
    s3 --> f2 & m
    f1 & d1 --> m
```
