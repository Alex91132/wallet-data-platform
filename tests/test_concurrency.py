"""Конкурентность: аномалии воспроизводятся на учебных таблицах, а функции кошелька от них защищены.

Каждая «демонстрация» сначала показывает, что аномалия реальна (иначе защита ничего не доказывает),
затем — как её устраняет блокировка или уровень изоляции.
"""
from __future__ import annotations

import threading

import psycopg
import pytest
from psycopg import errors

from conftest import balance, new_user, run_parallel


# ---------------------------------------------------------------- потерянное обновление
@pytest.fixture
def demo_acc(db):
    db.execute("drop table if exists demo_acc; create table demo_acc (id int primary key, balance numeric not null)")
    db.execute("insert into demo_acc values (1, 1000)")
    return db


def _read_then_write(dsn, lock: str, isolation: str | None):
    """Два списания по 300 по схеме «прочитал остаток в приложении → записал новое значение».
    Оба потока читают до того, как кто-то запишет (второй барьер)."""
    gate = threading.Barrier(2)

    def debit(c, i):
        cur = c.execute(f"select balance from demo_acc where id = 1 {lock}").fetchone()[0]
        try:
            gate.wait(timeout=1)          # с FOR UPDATE второй поток ждёт блокировку и сюда не доходит вовремя
        except threading.BrokenBarrierError:
            pass
        c.execute("update demo_acc set balance = %s where id = 1", (cur - 300,))
        return "ok"

    return run_parallel(dsn, 2, debit, isolation=isolation)


def test_lost_update_reproduced_at_read_committed(dsn, demo_acc):
    res = _read_then_write(dsn, "", "READ_COMMITTED")
    assert res == ["ok", "ok"]
    # списали дважды по 300, а остаток уменьшился один раз: второе обновление затёрло первое
    assert demo_acc.execute("select balance from demo_acc").fetchone()[0] == 700


def test_lost_update_prevented_by_select_for_update(dsn, demo_acc):
    res = _read_then_write(dsn, "for update", "READ_COMMITTED")
    assert res == ["ok", "ok"]
    assert demo_acc.execute("select balance from demo_acc").fetchone()[0] == 400


def test_lost_update_detected_at_repeatable_read(dsn, demo_acc):
    res = _read_then_write(dsn, "", "REPEATABLE_READ")
    # второй писатель получает serialization_failure (40001) — приложение обязано повторить транзакцию
    assert sum(isinstance(r, errors.SerializationFailure) for r in res) == 1
    assert demo_acc.execute("select balance from demo_acc").fetchone()[0] == 700


# ---------------------------------------------------------------- write skew
@pytest.fixture
def demo_spend(db):
    db.execute("drop table if exists demo_spend; create table demo_spend (account int, amount numeric)")
    return db


def _spend_with_limit(dsn, isolation):
    """Правило «за день не больше 1000»: каждый проверяет сумму и добавляет 600. Строки разные — конфликта записи нет."""
    gate = threading.Barrier(2)

    def spend(c, i):
        spent = c.execute("select coalesce(sum(amount), 0) from demo_spend where account = 1").fetchone()[0]
        gate.wait(timeout=5)
        if spent + 600 > 1000:
            return "rejected"
        c.execute("insert into demo_spend values (1, 600)")
        return "ok"

    return run_parallel(dsn, 2, spend, isolation=isolation)


def test_write_skew_passes_repeatable_read(dsn, demo_spend):
    res = _spend_with_limit(dsn, "REPEATABLE_READ")
    assert res == ["ok", "ok"]                         # обе транзакции зафиксированы
    assert demo_spend.execute("select sum(amount) from demo_spend").fetchone()[0] == 1200   # лимит нарушен


def test_write_skew_caught_by_serializable(dsn, demo_spend):
    res = _spend_with_limit(dsn, "SERIALIZABLE")
    assert sum(isinstance(r, errors.SerializationFailure) for r in res) == 1
    assert demo_spend.execute("select sum(amount) from demo_spend").fetchone()[0] == 600


# ---------------------------------------------------------------- дедлок
def test_deadlock_on_opposite_lock_order(dsn, db):
    db.execute("drop table if exists demo_pair; create table demo_pair (id int primary key, v int)")
    db.execute("insert into demo_pair values (1, 0), (2, 0)")
    gate = threading.Barrier(2)

    def cross(c, i):
        first, second = (1, 2) if i == 0 else (2, 1)
        c.execute("update demo_pair set v = v + 1 where id = %s", (first,))
        gate.wait(timeout=5)
        c.execute("update demo_pair set v = v + 1 where id = %s", (second,))
        return "ok"

    res = run_parallel(dsn, 2, cross, isolation="READ_COMMITTED")
    assert sum(isinstance(r, errors.DeadlockDetected) for r in res) == 1


# ---------------------------------------------------------------- функции кошелька
def test_no_double_spend(dsn, db):
    """10 одновременных переводов по 300 с остатка 1000: проходят ровно три."""
    a, b = new_user(db, balance=1000), new_user(db)
    res = run_parallel(dsn, 10, lambda c, i: c.execute(
        "select status, failure_reason from transfer(%s, %s, 300, %s)", (a, b, f"ds-{a}-{i}")).fetchone())
    assert sorted(r[0] for r in res).count("succeeded") == 3
    assert {r[1] for r in res if r[0] == "failed"} == {"insufficient_funds"}
    assert balance(db, a) == 100 and balance(db, b) == 900


def test_daily_limit_not_bypassed_concurrently(dsn, db):
    """Лимит basic — 60 000 в день. Два параллельных перевода по 40 000: write skew невозможен —
    функция держит блокировку счёта отправителя, пока считает дневной оборот."""
    a, b, c2 = new_user(db, "full", 0), new_user(db, "full"), new_user(db, "full")
    db.execute("update users set kyc_level = 'full' where user_id = %s", (a,))
    db.execute("select topup(%s, 100000, %s)", (a, f"lim-{a}"))
    db.execute("update users set kyc_level = 'basic' where user_id = %s", (a,))
    res = run_parallel(dsn, 2, lambda c, i: c.execute(
        "select status, failure_reason from transfer(%s, %s, 40000, %s)", (a, [b, c2][i], f"lim-{a}-{i}")).fetchone())
    assert sorted(r[1] or "" for r in res) == ["", "daily_limit"]


def test_opposite_transfers_do_not_deadlock(dsn, db):
    """Встречные переводы A→B и B→A: блокировки берутся по возрастанию id счёта, дедлока нет."""
    a, b = new_user(db, balance=10000), new_user(db, balance=10000)
    res = run_parallel(dsn, 20, lambda c, i: c.execute(
        "select status from transfer(%s, %s, 10, %s)", ((a, b) if i % 2 else (b, a)) + (f"x-{a}-{i}",)).fetchone()[0])
    assert res == ["succeeded"] * 20
    assert balance(db, a) + balance(db, b) == 20000


def test_idempotency_under_concurrency(dsn, db):
    """Клиент повторил запрос 8 раз параллельно (таймаут сети): одна операция, деньги зачислены один раз."""
    a = new_user(db)
    res = run_parallel(dsn, 8, lambda c, i: c.execute(
        "select op_id, replayed from topup(%s, 500, 'same-key')", (a,)).fetchone())
    assert len({r[0] for r in res}) == 1
    assert sum(not r[1] for r in res) == 1
    assert balance(db, a) == 500


# ---------------------------------------------------------------- инварианты схемы
def test_unbalanced_ledger_rejected_at_commit(dsn, db):
    a = new_user(db, balance=100)
    op = db.execute("select op_id from operations where initiator_user_id = %s", (a,)).fetchone()[0]
    acc = db.execute("select account_id from accounts where user_id = %s", (a,)).fetchone()[0]
    with psycopg.connect(dsn) as c:
        c.execute("insert into ledger_entries (op_id, account_id, amount, created_at) values (%s, %s, 5, now())", (op, acc))
        with pytest.raises(errors.CheckViolation, match="не сбалансирована"):
            c.commit()


def test_forbidden_status_transition(db):
    a, b = new_user(db, level="none"), new_user(db)
    op = db.execute("select op_id from transfer(%s, %s, 10, 'st')", (a, b)).fetchone()   # p2p без KYC → failed
    with pytest.raises(errors.CheckViolation, match="запрещён"):
        db.execute("update operations set status = 'succeeded', failure_reason = null where op_id = %s", (op[0],))


def test_balance_cannot_go_negative(db):
    a = new_user(db, balance=10)
    with pytest.raises(errors.CheckViolation):
        db.execute("update accounts set balance = -1 where user_id = %s", (a,))


def test_ledger_matches_balances(db):
    assert db.execute("select count(*) from balance_mismatches").fetchone()[0] == 0
    assert db.execute("select count(*) from (select op_id from ledger_entries group by op_id having sum(amount) <> 0) t"
                      ).fetchone()[0] == 0


# ---------------------------------------------------------------- доступ
def test_rls_merchant_sees_only_own_operations(dsn, db):
    m1 = db.execute("select create_merchant('A', 'cafe', '5814', 'Москва', 0.01)").fetchone()[0]
    m2 = db.execute("select create_merchant('B', 'cafe', '5814', 'Москва', 0.01)").fetchone()[0]
    u = new_user(db, balance=1000)
    db.execute("select purchase(%s, %s, 100, 'rls1')", (u, m1))
    db.execute("select purchase(%s, %s, 100, 'rls2')", (u, m2))
    with psycopg.connect(dsn.replace("postgres:postgres_local", "merchant_portal:merchant_local"), autocommit=True) as c:
        c.execute("select set_config('app.merchant_id', %s, false)", (str(m1),))
        rows = c.execute("select distinct merchant_id from wallet.operations").fetchall()
    assert rows == [(m1,)]


def test_analyst_cannot_read_pii(dsn):
    with psycopg.connect(dsn.replace("postgres:postgres_local", "analyst:analyst_local"), autocommit=True) as c:
        with pytest.raises(errors.InsufficientPrivilege):
            c.execute("select phone from wallet.users limit 1")
        row = c.execute("select phone_masked from wallet.users_masked limit 1").fetchone()
    assert row is None or "*" in row[0]
