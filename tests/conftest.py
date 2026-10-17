"""Тесты гоняются в отдельной базе wallet_test, собранной из тех же скриптов db/init, что и рабочая."""
from __future__ import annotations

import os
import threading
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[1]
ADMIN_DSN = os.getenv("PG_ADMIN_DSN", "postgresql://postgres:postgres_local@localhost:5433/postgres")
TEST_DSN = ADMIN_DSN.rsplit("/", 1)[0] + "/wallet_test"


@pytest.fixture(scope="session")
def dsn() -> str:
    with psycopg.connect(ADMIN_DSN, autocommit=True) as c:
        c.execute("drop database if exists wallet_test with (force)")
        c.execute("create database wallet_test")
    with psycopg.connect(TEST_DSN, autocommit=True) as c:
        for f in sorted((ROOT / "db" / "init").glob("*.sql")):
            c.execute(f.read_text(encoding="utf-8"))
        c.execute("alter database wallet_test set search_path = wallet, public")
    return TEST_DSN


@pytest.fixture
def db(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        yield c


_seq = iter(range(10**6))


def new_user(c, level: str = "basic", balance: float = 0) -> int:
    """Пользователь с кошельком, нужным уровнем KYC и стартовым остатком."""
    n = next(_seq)
    uid = c.execute("select create_user(%s, null, 'Тест', '1990-01-01', 'Москва', 'organic')",
                    (f"+7900{n:07d}",)).fetchone()[0]
    if level != "none":
        k = c.execute("select submit_kyc(%s, %s)", (uid, level)).fetchone()[0]
        c.execute("select decide_kyc(%s, true)", (k,))
    if balance:
        r = c.execute("select status, failure_reason from topup(%s, %s, %s)", (uid, balance, f"seed-{n}")).fetchone()
        assert r[0] == "succeeded", r
    return uid


def balance(c, uid: int) -> float:
    return float(c.execute("select balance from accounts where user_id = %s", (uid,)).fetchone()[0])


def run_parallel(dsn: str, n: int, fn, isolation: str | None = None) -> list:
    """n потоков, у каждого своё соединение; стартуют одновременно (барьер). Возвращает результат или исключение."""
    barrier = threading.Barrier(n)
    out: list = [None] * n

    def worker(i):
        with psycopg.connect(dsn, autocommit=isolation is None) as c:
            if isolation:
                c.isolation_level = getattr(psycopg.IsolationLevel, isolation)
            barrier.wait()
            try:
                out[i] = fn(c, i)
                if not c.autocommit:
                    c.commit()
            except Exception as e:  # noqa: BLE001 — исход каждого потока проверяет тест
                if not c.autocommit:
                    c.rollback()
                out[i] = e

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return out
