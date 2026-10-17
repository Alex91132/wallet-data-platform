"""Симулятор полугода жизни кошелька: деньги — через функции БД (те же, что у API), события — через сборщик /track.

    python -m wallet.sim --days 182 --start 2026-03-02

В историю заложены сюжеты, которые аналитика должна найти сама:
  * день 40–54  — рекламная кампания в соцсетях: много регистраций, слабая активация и удержание;
  * день 50–110 — A/B-тест cashback_boost: 3 % кешбэка вместо 1 % первые 30 дней (половине новичков);
  * день 100    — релиз 3.2.0 с новой формой KYC (v2): шаг «селфи» проще, доходимость выше;
  * день 140    — сбой эквайера с 10:00 до 16:00: 35 % пополнений и оплат падают с provider_timeout;
  * день 120–130 — группа мошенников: пополнение и сразу переводы круглыми суммами на свежие кошельки;
  * клиентские события: 3 % сессий приходят с опозданием до 3 суток, 1 % дублей, 0,5 % нарушают контракт.
"""
from __future__ import annotations

import argparse
import datetime as dt
import heapq
import math
import os
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field

import psycopg
import requests

MSK = dt.timezone(dt.timedelta(hours=3))
CHANNELS = {  # доля, вовлечённость, срок «жизни» (дни), вероятность начать KYC, одобрения KYC, доля «спящих»
    "organic":    (0.35, 1.00, 70, 0.70, 0.90, 0.35),
    "referral":   (0.15, 1.25, 90, 0.82, 0.92, 0.25),
    "ads_social": (0.25, 0.55, 25, 0.50, 0.74, 0.62),
    "ads_search": (0.15, 0.90, 55, 0.66, 0.88, 0.42),
    "partner":    (0.10, 0.80, 45, 0.60, 0.86, 0.48),
}
CITIES = [("Москва", 0.34), ("Санкт-Петербург", 0.14), ("Новосибирск", 0.06), ("Екатеринбург", 0.06), ("Казань", 0.05),
          ("Нижний Новгород", 0.04), ("Красноярск", 0.04), ("Самара", 0.04), ("Краснодар", 0.05), ("Ростов-на-Дону", 0.04),
          ("Тюмень", 0.03), ("Уфа", 0.04), ("Пермь", 0.03), ("Воронеж", 0.04)]
MERCHANT_TYPES = [  # категория, MCC, комиссия, медианный чек, вес
    ("supermarket", "5411", 0.010, 1400, 0.30), ("cafe", "5814", 0.015, 450, 0.22), ("taxi", "4121", 0.020, 520, 0.12),
    ("pharmacy", "5912", 0.012, 900, 0.08), ("electronics", "5732", 0.018, 6500, 0.04), ("clothing", "5651", 0.020, 3200, 0.06),
    ("games", "5816", 0.025, 700, 0.08), ("telecom", "4814", 0.008, 600, 0.10)]
NAMES = ["Алексей", "Мария", "Иван", "Анна", "Дмитрий", "Елена", "Сергей", "Ольга", "Андрей", "Наталья", "Павел", "Юлия"]
SURNAMES = ["Иванов", "Смирнов", "Кузнецов", "Попов", "Васильев", "Петров", "Соколов", "Михайлов", "Новиков", "Фёдоров"]


@dataclass(order=True)
class Task:
    at: dt.datetime
    seq: int
    kind: str = field(compare=False)
    payload: dict = field(compare=False, default_factory=dict)


@dataclass
class User:
    uid: int
    anon: str
    platform: str
    channel: str
    signup: dt.datetime
    engagement: float
    life: float
    city: str
    kyc: str = "none"
    kyc_pending: bool = False
    balance: float = 0.0
    variant: str | None = None
    fraud: bool = False
    dormant: bool = False          # зарегистрировался и не вернулся
    version: str = "3.0.0"
    cashback_until: dt.datetime | None = None


class Sim:
    def __init__(self, dsn: str, collector: str, start: dt.date, days: int, base: float, seed: int):
        self.r = random.Random(seed)
        self.db = psycopg.connect(dsn, autocommit=True)
        self.db.execute("set search_path = wallet")
        self.collector = collector.rstrip("/")
        self.http = requests.Session()
        self.start = dt.datetime.combine(start, dt.time(), MSK)
        self.days, self.base = days, base
        self.users: list[User] = []
        self.by_id: dict[int, User] = {}
        self.queue: list[Task] = []
        self._seq = 0
        self.events: dict[str, list[dict]] = defaultdict(list)   # время приёма (час) → события
        self.merchants: list[tuple[int, str, int, float]] = []
        self.purchases: list[tuple[str, float, int]] = []
        self.stats = defaultdict(int)
        self.phone_n = self.r.randrange(10**8, 9 * 10**8)

    # ---------------------------------------------------------------- утилиты
    def day(self, d: int) -> dt.datetime:
        return self.start + dt.timedelta(days=d)

    def schedule(self, at: dt.datetime, kind: str, **payload):
        self._seq += 1
        heapq.heappush(self.queue, Task(at, self._seq, kind, payload))

    def moment(self, day_start: dt.datetime) -> dt.datetime:
        """Время внутри дня по суточному профилю: пики утром и вечером."""
        h = self.r.choices(range(24), weights=[1, .5, .3, .3, .3, .6, 1.5, 3, 4, 4, 4, 4.5, 5, 4.5, 4.5, 4.5, 5, 5.5, 6.5, 7,
                                              6.5, 5, 3.5, 2])[0]
        return day_start + dt.timedelta(hours=h, minutes=self.r.random() * 60)

    def version_at(self, t: dt.datetime) -> str:
        d = (t - self.start).days
        return "3.0.0" if d < 60 else "3.1.0" if d < 100 else "3.2.0" if d < 150 else "3.2.1"

    def money(self, mean: float, sigma: float = 0.8, lo: float = 10, hi: float = 300000) -> float:
        return round(min(hi, max(lo, self.r.lognormvariate(math.log(mean), sigma))), 0)

    def call(self, sql: str, args: tuple):
        return self.db.execute(sql, args).fetchone()

    # ---------------------------------------------------------------- события приложения
    def ev(self, u: User | None, name: str, t: dt.datetime, session: str, anon: str, platform: str, **props) -> dict:
        e = {"schema_version": "1.0", "event_id": str(uuid.uuid4()), "event_name": name,
             "user_id": u.uid if u else None, "anonymous_id": anon, "session_id": session,
             "client_ts": t.astimezone(dt.UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
             "platform": platform, "app_version": self.version_at(t), "properties": props}
        return e

    def emit(self, events: list[dict], t: dt.datetime):
        """Сессия уходит пачкой; иногда — из офлайна с опозданием, иногда SDK повторяет отправку, изредка ломает формат."""
        late = self.r.random() < 0.03
        recv = t + (dt.timedelta(hours=self.r.uniform(1, 72)) if late else dt.timedelta(seconds=self.r.uniform(1, 30)))
        out = []
        for e in events:
            if self.r.random() < 0.005:
                e = self.corrupt(dict(e))
            out.append(e)
            if self.r.random() < 0.01:
                out.append(e)                                  # дубль: тот же event_id
        self.stats["events_late"] += len(out) if late else 0
        self.events[recv.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:00:00+00:00")].extend(out)

    def corrupt(self, e: dict) -> dict:
        kind = self.r.choice(["no_session", "bad_version", "neg_amount", "extra_field"])
        if kind == "no_session":
            e.pop("session_id", None)
        elif kind == "bad_version":
            e["app_version"] = "3.2"
        elif kind == "neg_amount":
            e["event_name"], e["properties"] = "topup_started", {"amount": -100}
        else:
            e["debug"] = True
        self.stats["events_corrupted"] += 1
        return e

    def flush_events(self, until: dt.datetime | None = None):
        keys = sorted(k for k in self.events if until is None or k < until.astimezone(dt.UTC).isoformat())
        for k in keys:
            evs = self.events.pop(k)
            for i in range(0, len(evs), 1000):
                for attempt in range(5):
                    try:
                        r = self.http.post(f"{self.collector}/track/batch", json=evs[i:i + 1000],
                                           headers={"X-Received-At": k}, timeout=60)
                        r.raise_for_status()
                        break
                    except requests.RequestException:
                        time.sleep(2 * (attempt + 1))
                else:
                    raise RuntimeError("сборщик событий недоступен")
                res = r.json()
                self.stats["events_sent"] += len(evs[i:i + 1000])
                self.stats["events_rejected"] += res["rejected"]

    # ---------------------------------------------------------------- мир
    def setup(self):
        self.db.execute("insert into experiments values ('cashback_boost', 'Кешбэк 3 %% вместо 1 %% первые 30 дней после регистрации', "
                        "'{control,test}', %s, %s) on conflict do nothing", (self.day(50), self.day(110)))
        n = 0
        for cat, mcc, fee, _, w in MERCHANT_TYPES:
            for _ in range(max(3, int(140 * w))):
                n += 1
                city = self.r.choices([c for c, _ in CITIES], [p for _, p in CITIES])[0]
                mid = self.call("select create_merchant(%s, %s, %s, %s, %s::numeric, %s)",
                                (f"{cat.capitalize()} №{n}", cat, mcc, city, fee, self.start - dt.timedelta(days=30)))[0]
                self.merchants.append((mid, cat, int(mcc), fee))

    def signups(self, d: int, day_start: dt.datetime):
        weekday = [1.1, 1.05, 1.0, 1.0, 1.05, 0.85, 0.8][day_start.weekday()]
        n = self.r.gammavariate(8, self.base * weekday * (1 + 0.003 * d) / 8)
        mix = {c: p for c, (p, *_) in CHANNELS.items()}
        if 40 <= d <= 54:
            mix["ads_social"] *= 3.2                         # кампания в соцсетях
        for _ in range(int(n)):
            ch = self.r.choices(list(mix), list(mix.values()))[0]
            self.new_user(ch, self.moment(day_start), d)
        for _ in range(int(n * 0.45)):                       # установили приложение и не зарегистрировались
            t, anon = self.moment(day_start), uuid.uuid4().hex[:16]
            s, p = uuid.uuid4().hex[:12], self.r.choice(["ios", "android", "android"])
            self.emit([self.ev(None, "app_open", t, s, anon, p),
                       self.ev(None, "screen_view", t + dt.timedelta(seconds=5), s, anon, p, screen="onboarding")], t)
        if 120 <= d <= 130 and self.r.random() < 0.6:
            for _ in range(self.r.randint(1, 3)):
                self.new_user("ads_social", self.moment(day_start), d, fraud=True)

    def new_user(self, ch: str, t: dt.datetime, d: int, fraud: bool = False):
        _, eng, life, *_ = CHANNELS[ch]
        self.phone_n += self.r.randint(1, 50)
        name = f"{self.r.choice(NAMES)} {self.r.choice(SURNAMES)}"
        city = self.r.choices([c for c, _ in CITIES], [p for _, p in CITIES])[0]
        birth = dt.date(self.r.randint(1960, 2006), self.r.randint(1, 12), self.r.randint(1, 28))
        email = f"user{self.phone_n}@{self.r.choice(['mail.ru', 'yandex.ru', 'gmail.com'])}" if self.r.random() < 0.7 else None
        uid = self.call("select create_user(%s, %s, %s, %s, %s, %s, %s)", (f"+79{self.phone_n:09d}", email, name, birth, city, ch, t))[0]
        u = User(uid, uuid.uuid4().hex[:16], self.r.choice(["ios", "android", "android", "web"]), ch, t,
                 self.r.lognormvariate(math.log(eng), 0.6), self.r.expovariate(1 / life) + 3, city, fraud=fraud)
        u.dormant = not fraud and self.r.random() < CHANNELS[ch][5]
        self.users.append(u)
        self.by_id[uid] = u
        s = uuid.uuid4().hex[:12]
        evs = [self.ev(None, "app_open", t - dt.timedelta(minutes=3), s, u.anon, u.platform),
               self.ev(None, "screen_view", t - dt.timedelta(minutes=2), s, u.anon, u.platform, screen="onboarding"),
               self.ev(u, "signup_completed", t, s, u.anon, u.platform, channel=ch)]
        if 50 <= d < 110 and not fraud:
            u.variant = self.r.choice(["control", "test"])
            self.db.execute("insert into experiment_assignments values ('cashback_boost', %s, %s, %s)", (uid, u.variant, t))
            u.cashback_until = t + dt.timedelta(days=30)
            evs.append(self.ev(u, "screen_view", t + dt.timedelta(seconds=20), s, u.anon, u.platform, screen="cashback_banner"))
            evs.append(self.ev(u, "experiment_exposure", t + dt.timedelta(seconds=21), s, u.anon, u.platform,
                               experiment_key="cashback_boost", variant=u.variant))
        self.emit(evs, t)
        self.stats["users"] += 1
        p_kyc = 0.97 if fraud else CHANNELS[ch][3] * (0.25 if u.dormant else 1)
        if self.r.random() < p_kyc:
            self.schedule(t + dt.timedelta(hours=self.r.expovariate(1 / (2 if fraud else 30))), "kyc_start", uid=uid)
        if fraud:
            self.schedule(t + dt.timedelta(hours=self.r.uniform(30, 60)), "fraud_burst", uid=uid)

    def kyc_start(self, u: User, t: dt.datetime):
        if u.kyc != "none" or u.kyc_pending:
            return
        form = "v2" if (t - self.start).days >= 100 else "v1"
        s = uuid.uuid4().hex[:12]
        evs = [self.ev(u, "app_open", t, s, u.anon, u.platform),
               self.ev(u, "screen_view", t + dt.timedelta(seconds=4), s, u.anon, u.platform, screen="kyc_form"),
               self.ev(u, "kyc_started", t + dt.timedelta(seconds=6), s, u.anon, u.platform, level="basic")]
        tt = t + dt.timedelta(seconds=10)
        # доходимость по шагам: в форме v2 упростили селфи
        p_step = {"passport": 0.86, "selfie": 0.70 if form == "v1" else 0.88, "address": 0.93}
        if u.fraud:
            p_step = {k: 1.0 for k in p_step}
        for step, p in p_step.items():
            if self.r.random() > p:
                self.emit(evs, t)
                return
            tt += dt.timedelta(seconds=self.r.uniform(20, 180))
            evs.append(self.ev(u, "kyc_step_completed", tt, s, u.anon, u.platform, step=step, form_version=form))
        evs.append(self.ev(u, "kyc_submitted", tt + dt.timedelta(seconds=3), s, u.anon, u.platform, level="basic"))
        self.emit(evs, t)
        kid = self.call("select submit_kyc(%s, 'basic', %s)", (u.uid, tt + dt.timedelta(seconds=3)))[0]
        u.kyc_pending = True
        approve_p = 0.99 if u.fraud else CHANNELS[u.channel][4]
        self.schedule(tt + dt.timedelta(hours=self.r.lognormvariate(math.log(10), 0.9)), "kyc_decide", uid=u.uid, kid=kid,
                      ok=self.r.random() < approve_p)

    def kyc_decide(self, u: User, t: dt.datetime, kid: int, ok: bool):
        reason = None if ok else self.r.choice(["document_unreadable", "selfie_mismatch", "data_mismatch", "sanctions_check"])
        self.call("select decide_kyc(%s, %s, %s, %s), 1", (kid, ok, reason, t))
        u.kyc_pending = False
        if ok:
            u.kyc = "basic"
        elif self.r.random() < 0.5:                         # половина отклонённых пробует ещё раз
            self.schedule(t + dt.timedelta(days=self.r.uniform(0.5, 6)), "kyc_start", uid=u.uid)

    # ---------------------------------------------------------------- активность
    def p_active(self, u: User, t: dt.datetime) -> float:
        age = (t - u.signup).total_seconds() / 86400
        if age < 0:
            return 0
        if u.dormant:                                       # пара заходов в первые дни — и всё
            return 0.25 if age < 3 else 0.0
        # быстрое угасание интереса + устойчивое «ядро» пользователей
        p = 0.24 * u.engagement * (0.35 * math.exp(-age / u.life) + 0.65 * math.exp(-age / (u.life * 6)))
        if u.variant == "test" and u.cashback_until and t < u.cashback_until:
            p *= 1.12                                       # кешбэк 3 % немного подстёгивает активность новичков
        return min(0.9, p)

    def session(self, u: User, t: dt.datetime, incident: bool):
        s = uuid.uuid4().hex[:12]
        evs = [self.ev(u, "app_open", t, s, u.anon, u.platform), self.ev(u, "screen_view", t + dt.timedelta(seconds=2), s,
                                                                          u.anon, u.platform, screen="home")]
        tt = t + dt.timedelta(seconds=5)
        if u.dormant:                                       # посмотрел и ушёл: без денег
            evs.append(self.ev(u, "screen_view", tt, s, u.anon, u.platform, screen=self.r.choice(["topup", "pay", "profile"])))
            self.emit(evs, t)
            return
        lim_bal = {"none": 15000, "basic": 60000}.get(u.kyc, 600000)
        # пополнение, если денег мало
        if u.balance < 1500 and self.r.random() < 0.75:
            amount = min(self.money(3500, 0.7, 300, 50000), max(0, lim_bal - u.balance))
            if amount >= 100:
                evs += [self.ev(u, "screen_view", tt, s, u.anon, u.platform, screen="topup"),
                        self.ev(u, "topup_started", tt + dt.timedelta(seconds=3), s, u.anon, u.platform, amount=amount)]
                tt += dt.timedelta(seconds=20)
                if self.r.random() < 0.88:                  # 12 % бросают на странице банка
                    fail = "provider_timeout" if incident and self.r.random() < 0.35 else None
                    st = self.call("select status from topup(%s, %s::numeric, %s, %s, %s)", (u.uid, amount, uuid.uuid4().hex, tt, fail))[0]
                    if st == "succeeded":
                        u.balance += amount
                    self.stats["ops"] += 1
        # покупки
        mult = 1.12 if (u.variant == "test" and u.cashback_until and t < u.cashback_until) else 1.0
        for _ in range(self.r.choices([0, 1, 2, 3], [0.25, 0.45 / mult, 0.2 * mult, 0.1 * mult])[0]):
            mid, cat, _, _ = self.r.choice(self.merchants)
            median = next(m for c, _, _, m, _ in MERCHANT_TYPES if c == cat)
            amount = self.money(median, 0.7)
            if amount > u.balance and self.r.random() < 0.85:   # видит остаток и не пытается платить
                continue
            evs += [self.ev(u, "screen_view", tt, s, u.anon, u.platform, screen="pay"),
                    self.ev(u, "purchase_started", tt + dt.timedelta(seconds=2), s, u.anon, u.platform, amount=amount)]
            tt += dt.timedelta(seconds=self.r.uniform(8, 40))
            fail = "provider_timeout" if incident and self.r.random() < 0.35 else None
            op, st = self.call("select op_id, status from purchase(%s, %s, %s::numeric, %s, %s, %s)",
                               (u.uid, mid, amount, uuid.uuid4().hex, tt, fail))
            self.stats["ops"] += 1
            if st == "succeeded":
                rate = 0.03 if mult > 1 else 0.01
                u.balance += -amount + round(amount * rate, 2)
                if self.r.random() < 0.02:                  # мерчант вернёт деньги через 1–10 дней
                    self.schedule(tt + dt.timedelta(days=self.r.uniform(1, 10)), "refund", uid=u.uid, op=str(op),
                                  amount=amount if self.r.random() < 0.7 else round(amount * self.r.uniform(0.2, 0.8), 0))
        # перевод
        if u.kyc != "none" and self.r.random() < 0.18 and u.balance > 300 and len(self.users) > 10:
            to = self.r.choice(self.users)
            if to.uid != u.uid:
                amount = min(self.money(1500, 0.8, 100, 20000), u.balance)
                evs += [self.ev(u, "screen_view", tt, s, u.anon, u.platform, screen="transfer"),
                        self.ev(u, "transfer_started", tt + dt.timedelta(seconds=3), s, u.anon, u.platform, amount=amount)]
                tt += dt.timedelta(seconds=25)
                st = self.call("select status from transfer(%s, %s, %s::numeric, %s, %s)", (u.uid, to.uid, amount, uuid.uuid4().hex, tt))[0]
                self.stats["ops"] += 1
                if st == "succeeded":
                    u.balance -= amount
                    to.balance += amount
        # вывод
        if u.kyc != "none" and u.balance > 3000 and self.r.random() < 0.03:
            amount = round(u.balance * self.r.uniform(0.3, 0.9), 0)
            st = self.call("select status from withdraw(%s, %s::numeric, %s, %s)", (u.uid, amount, uuid.uuid4().hex, tt))[0]
            self.stats["ops"] += 1
            if st == "succeeded":
                u.balance -= amount
        if self.r.random() < 0.5:
            evs.append(self.ev(u, "screen_view", tt + dt.timedelta(seconds=5), s, u.anon, u.platform,
                               screen=self.r.choice(["history", "profile", "home"])))
        self.emit(evs, t)

    def fraud_burst(self, u: User, t: dt.datetime):
        """Пополнение до лимита и за час — переводы круглыми суммами на свежие кошельки."""
        if u.kyc == "none":
            return
        self.call("select status from topup(%s, 50000, %s, %s)", (u.uid, uuid.uuid4().hex, t))
        fresh = [x for x in self.users[-300:] if x.uid != u.uid] or self.users
        tt = t
        for _ in range(self.r.randint(6, 12)):
            tt += dt.timedelta(minutes=self.r.uniform(2, 8))
            self.call("select status from transfer(%s, %s, 5000, %s, %s)",
                      (u.uid, self.r.choice(fresh).uid, uuid.uuid4().hex, tt))
            self.stats["ops"] += 1

    # ---------------------------------------------------------------- цикл
    def run(self):
        self.setup()
        t0 = time.time()
        for d in range(self.days):
            day_start = self.day(d)
            self.signups(d, day_start)
            incident_from, incident_to = self.day(140) + dt.timedelta(hours=10), self.day(140) + dt.timedelta(hours=16)
            for u in self.users:
                if u.fraud:
                    continue
                p = self.p_active(u, day_start)
                for _ in range(2 if self.r.random() < p * 0.3 else 1):
                    if self.r.random() < p:
                        self.schedule(self.moment(day_start), "session", uid=u.uid)
            day_end = day_start + dt.timedelta(days=1)
            while self.queue and self.queue[0].at < day_end:
                task = heapq.heappop(self.queue)
                u = self.by_id[task.payload["uid"]]
                if task.kind == "session":
                    self.session(u, task.at, incident_from <= task.at < incident_to)
                elif task.kind == "kyc_start":
                    self.kyc_start(u, task.at)
                elif task.kind == "kyc_decide":
                    self.kyc_decide(u, task.at, task.payload["kid"], task.payload["ok"])
                elif task.kind == "refund":
                    st = self.call("select status from refund(%s::uuid, %s::numeric, %s, %s)",
                                   (task.payload["op"], task.payload["amount"], uuid.uuid4().hex, task.at))[0]
                    if st == "succeeded":
                        u.balance += task.payload["amount"]
                    self.stats["ops"] += 1
                elif task.kind == "fraud_burst":
                    self.fraud_burst(u, task.at)
            self.flush_events(until=day_end)
            if d % 10 == 0 or d == self.days - 1:
                print(f"день {d:3d} {day_start:%Y-%m-%d}: пользователей {self.stats['users']}, операций {self.stats['ops']}, "
                      f"событий {self.stats['events_sent']} ({time.time() - t0:.0f} с)", flush=True)
        self.flush_events()
        self.http.post(f"{self.collector}/track/flush", timeout=60)
        print(dict(self.stats))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=182)
    ap.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2026, 3, 2))
    ap.add_argument("--signups", type=float, default=45, help="регистраций в день в начале периода")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    Sim(os.getenv("PG_DSN", "postgresql://postgres:postgres_local@localhost:5433/wallet"),
        os.getenv("COLLECTOR_URL", "http://localhost:8000"), a.start, a.days, a.signups, a.seed).run()


if __name__ == "__main__":
    main()
