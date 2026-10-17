"""HTTP API кошелька и сборщик клиентских событий.

Деньги — тонкая обёртка над функциями БД (вся логика и инварианты там): сервис ходит под ролью wallet_app,
у которой нет прав на таблицы — только EXECUTE на функции. Денежные запросы требуют заголовок Idempotency-Key:
повтор с тем же ключом возвращает ту же операцию (200 и Idempotent-Replayed: true), новая операция — 201.

/track и /track/batch — сборщик событий приложения: проверка по контракту (contracts/app_event.schema.json),
валидные — в Kafka-топик app_events, нарушившие контракт — в app_events_dlq с причиной.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import threading
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Literal

import psycopg
from fastapi import FastAPI, Header, HTTPException, Response
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel, Field

PG_DSN = os.getenv("PG_DSN", "postgresql://wallet_app:app_local@localhost:5433/wallet")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
SCHEMA = json.loads((Path(__file__).parent / "contracts" / "app_event.schema.json").read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())

app = FastAPI(title="Wallet API", version="1.0.0",
              description="Кошелёк: регистрация, KYC, пополнение, переводы, оплата, возвраты, вывод; сбор событий приложения.")

_local = threading.local()


def db() -> psycopg.Connection:
    """Одно соединение на поток воркера; автокоммит — каждая функция БД сама является транзакцией."""
    c = getattr(_local, "conn", None)
    if c is None or c.closed:
        c = _local.conn = psycopg.connect(PG_DSN, autocommit=True)
    return c


# ---------------------------------------------------------------- модели
Money = Annotated[Decimal, Field(gt=0, max_digits=14, decimal_places=2, examples=["1500.00"])]
IdemKey = Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=128,
                                description="Уникален для каждого намерения клиента; повтор запроса — с тем же ключом")]


class OperationResult(BaseModel):
    op_id: uuid.UUID
    status: Literal["created", "succeeded", "failed", "reversed"]
    failure_reason: str | None = Field(None, examples=["insufficient_funds", "daily_limit", "kyc_required"])


class UserIn(BaseModel):
    phone: str = Field(pattern=r"^\+7\d{10}$", examples=["+79991234567"])
    email: str | None = None
    full_name: str
    birth_date: dt.date
    city: str | None = None
    channel: Literal["organic", "referral", "ads_social", "ads_search", "partner"]


class KycIn(BaseModel):
    level: Literal["basic", "full"]


class KycDecision(BaseModel):
    approved: bool
    reason: str | None = Field(None, examples=["document_unreadable"])


class TopupIn(BaseModel):
    user_id: int
    amount: Money


class TransferIn(BaseModel):
    from_user_id: int
    to_user_id: int
    amount: Money


class PurchaseIn(BaseModel):
    user_id: int
    merchant_id: int
    amount: Money


class RefundIn(BaseModel):
    purchase_op_id: uuid.UUID
    amount: Money


class WithdrawIn(BaseModel):
    user_id: int
    amount: Money


def _money(sql: str, args: tuple, response: Response) -> OperationResult:
    try:
        op_id, status, reason, replayed = db().execute(sql, args).fetchone()
    except psycopg.errors.NoDataFound as e:          # P0002: получатель / покупка не найдены
        raise HTTPException(404, str(e).split("\n")[0]) from e
    except psycopg.errors.InvalidParameterValue as e:
        raise HTTPException(422, str(e).split("\n")[0]) from e
    response.status_code = 200 if replayed else 201
    if replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return OperationResult(op_id=op_id, status=status, failure_reason=reason)


# ---------------------------------------------------------------- пользователи и KYC
@app.post("/users", status_code=201, tags=["users"], summary="Регистрация: пользователь и кошелёк")
def create_user(body: UserIn) -> dict:
    try:
        uid = db().execute("select wallet.create_user(%s, %s, %s, %s, %s, %s)",
                           (body.phone, body.email, body.full_name, body.birth_date, body.city, body.channel)).fetchone()[0]
    except psycopg.errors.UniqueViolation as e:
        raise HTTPException(409, "пользователь с таким телефоном или e-mail уже есть") from e
    return {"user_id": uid}


@app.post("/users/{user_id}/kyc", status_code=201, tags=["kyc"], summary="Подать заявку на идентификацию")
def submit_kyc(user_id: int, body: KycIn) -> dict:
    try:
        kid = db().execute("select wallet.submit_kyc(%s, %s)", (user_id, body.level)).fetchone()[0]
    except psycopg.errors.UniqueViolation as e:
        raise HTTPException(409, "заявка уже на рассмотрении") from e
    return {"kyc_id": kid, "status": "pending"}


@app.post("/kyc/{kyc_id}/decision", tags=["kyc"], summary="Решение по заявке (для сотрудника комплаенса)")
def decide_kyc(kyc_id: int, body: KycDecision) -> dict:
    db().execute("select wallet.decide_kyc(%s, %s, %s)", (kyc_id, body.approved, body.reason))
    return {"kyc_id": kyc_id, "status": "approved" if body.approved else "rejected"}


# ---------------------------------------------------------------- деньги
@app.post("/topups", status_code=201, tags=["money"], response_model=OperationResult, summary="Пополнение кошелька")
def topup(body: TopupIn, response: Response, key: IdemKey) -> OperationResult:
    return _money("select * from wallet.topup(%s, %s, %s)", (body.user_id, body.amount, key), response)


@app.post("/transfers", status_code=201, tags=["money"], response_model=OperationResult, summary="Перевод другому пользователю")
def transfer(body: TransferIn, response: Response, key: IdemKey) -> OperationResult:
    if body.from_user_id == body.to_user_id:
        raise HTTPException(422, "перевод самому себе")
    return _money("select * from wallet.transfer(%s, %s, %s, %s)", (body.from_user_id, body.to_user_id, body.amount, key), response)


@app.post("/purchases", status_code=201, tags=["money"], response_model=OperationResult, summary="Оплата у мерчанта (с кешбэком)")
def purchase(body: PurchaseIn, response: Response, key: IdemKey) -> OperationResult:
    return _money("select * from wallet.purchase(%s, %s, %s, %s)", (body.user_id, body.merchant_id, body.amount, key), response)


@app.post("/refunds", status_code=201, tags=["money"], response_model=OperationResult, summary="Возврат по покупке")
def refund(body: RefundIn, response: Response, key: IdemKey) -> OperationResult:
    return _money("select * from wallet.refund(%s, %s, %s)", (body.purchase_op_id, body.amount, key), response)


@app.post("/withdrawals", status_code=201, tags=["money"], response_model=OperationResult, summary="Вывод на карту")
def withdraw(body: WithdrawIn, response: Response, key: IdemKey) -> OperationResult:
    return _money("select * from wallet.withdraw(%s, %s, %s)", (body.user_id, body.amount, key), response)


@app.get("/operations/{op_id}", tags=["money"], response_model=OperationResult, summary="Статус операции")
def get_operation(op_id: uuid.UUID) -> OperationResult:
    row = db().execute("select op_id, status, failure_reason from wallet.operations where op_id = %s", (op_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "операция не найдена")
    return OperationResult(op_id=row[0], status=row[1], failure_reason=row[2])


# ---------------------------------------------------------------- сбор событий
_producer = None


def producer():
    global _producer
    if _producer is None:
        from confluent_kafka import Producer
        _producer = Producer({"bootstrap.servers": KAFKA, "linger.ms": 50, "compression.type": "zstd",
                              "enable.idempotence": True})
    return _producer


def _route(ev: dict, received: str) -> bool:
    errors = sorted(VALIDATOR.iter_errors(ev), key=lambda e: list(e.path))
    key = str(ev.get("user_id") or ev.get("anonymous_id") or "")
    if errors:
        reason = "; ".join(f"{'/'.join(map(str, e.path)) or '(root)'}: {e.message}" for e in errors[:3])
        producer().produce("app_events_dlq", key=key, value=json.dumps(
            {"received_at": received, "error": reason, "raw": json.dumps(ev, ensure_ascii=False)}, ensure_ascii=False))
        return False
    producer().produce("app_events", key=key, value=json.dumps({**ev, "received_at": received}, ensure_ascii=False))
    return True


@app.post("/track", status_code=202, tags=["events"], summary="Одно событие приложения")
def track(event: dict) -> dict:
    ok = _route(event, dt.datetime.now(dt.UTC).isoformat())
    producer().poll(0)
    return {"accepted": int(ok), "rejected": int(not ok)}


@app.post("/track/batch", status_code=202, tags=["events"], summary="Пачка событий (до 1000) — как шлёт мобильный SDK")
def track_batch(events: list[dict], x_received_at: Annotated[str | None, Header()] = None) -> dict:
    if len(events) > 1000:
        raise HTTPException(413, "не больше 1000 событий в пачке")
    received = x_received_at or dt.datetime.now(dt.UTC).isoformat()   # симулятор передаёт модельное время приёма
    ok = sum(_route(e, received) for e in events)
    producer().poll(0)
    return {"accepted": ok, "rejected": len(events) - ok}


@app.post("/track/flush", include_in_schema=False)
def flush() -> dict:
    return {"pending": producer().flush(30)}


@app.get("/health", tags=["service"])
def health() -> dict:
    db().execute("select 1")
    return {"status": "ok"}
