"""Сверка источника и хранилища: после CDC число строк и суммы денег в ClickHouse должны совпасть с PostgreSQL.

    python scripts/reconcile.py            # код возврата 1, если есть расхождения

Сравнивается последняя версия каждой строки (staging-модели dbt) с таблицами OLTP. Перед сверкой скрипт ждёт,
пока ClickHouse догонит источник (CDC асинхронен), — до --wait секунд.
"""
from __future__ import annotations

import argparse
import sys
import time
from decimal import Decimal

import clickhouse_connect
import psycopg

PG = "postgresql://analyst:analyst_local@localhost:5433/wallet"   # сверка — с правами аналитика, без доступа к ПДн

CHECKS = [
    # (что сравниваем, SQL в PostgreSQL, SQL в ClickHouse)
    ("пользователи", "select count(*) from wallet.users_masked", "select count() from analytics.stg_users"),
    ("заявки KYC", "select count(*), count(*) filter (where status = 'approved') from wallet.kyc_checks",
     "select count(), countIf(status = 'approved') from analytics.stg_kyc_checks"),
    ("счета и сумма остатков", "select count(*), sum(balance) from wallet.accounts",
     "select count(), sum(balance) from analytics.stg_accounts"),
    ("операции по статусам",
     "select count(*), count(*) filter (where status = 'succeeded'), sum(amount) filter (where status = 'succeeded') from wallet.operations",
     "select count(), countIf(status = 'succeeded'), sumIf(amount, status = 'succeeded') from analytics.stg_operations"),
    ("проводки", "select count(*), sum(abs(amount)) from wallet.ledger_entries",
     "select count(), sum(abs(amount)) from analytics.stg_ledger_entries"),
    ("комиссионный доход", "select balance from wallet.accounts where code = 'fees'",
     "select sum(amount) from analytics.stg_ledger_entries where account_id = (select account_id from analytics.stg_accounts where code = 'fees')"),
    ("назначения в эксперимент", "select count(*), count(*) filter (where variant = 'test') from wallet.experiment_assignments",
     "select count(), countIf(variant = 'test') from analytics.stg_experiment_assignments"),
]


def norm(row) -> tuple:
    return tuple(Decimal(str(v)).quantize(Decimal("0.01")) if v is not None else None for v in row)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", type=int, default=180)
    a = ap.parse_args()
    ch = clickhouse_connect.get_client(host="localhost", port=8123, username="dwh", password="dwh_local")
    with psycopg.connect(PG) as pg:
        t0 = time.time()
        while True:
            results = [(name, norm(pg.execute(p).fetchone()), norm(ch.query(c).result_rows[0])) for name, p, c in CHECKS]
            bad = [r for r in results if r[1] != r[2]]
            if not bad or time.time() - t0 > a.wait:
                break
            time.sleep(10)                      # CDC догоняет: ждём и сверяем снова
    width = max(len(n) for n, *_ in results)
    for name, src, dwh in results:
        print(f"{'OK ' if src == dwh else 'РАЗН'} {name:<{width}}  PostgreSQL {src}  ClickHouse {dwh}")
    print("расхождений нет" if not bad else f"расхождений: {len(bad)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
