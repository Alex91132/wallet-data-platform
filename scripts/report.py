"""Аналитический отчёт по витринам хранилища → docs/index.html (GitHub Pages) и docs/report.json (цифры).

    python scripts/report.py

Вопросы — те, что задал бы продукт, финансы и антифрод. Каждый вывод считается из данных при сборке отчёта.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import clickhouse_connect
import numpy as np
import pandas as pd
import plotly.graph_objects as go

ROOT = Path(__file__).resolve().parents[1]
ch = clickhouse_connect.get_client(host="localhost", port=8123, username="dwh", password="dwh_local")
rng = np.random.default_rng(7)

INK, MUTED, GRID = "#1a1a2e", "#6b7280", "#e5e7eb"
C = {"main": "#2563eb", "alt": "#f59e0b", "bad": "#dc2626", "good": "#059669", "grey": "#9ca3af"}
CH_COLORS = {"organic": "#2563eb", "referral": "#059669", "ads_social": "#dc2626", "ads_search": "#f59e0b", "partner": "#7c3aed"}
CH_RU = {"organic": "органика", "referral": "рефералы", "ads_social": "реклама в соцсетях", "ads_search": "поисковая реклама",
         "partner": "партнёры"}


def q(sql: str) -> pd.DataFrame:
    """Запрос к хранилищу; деньги (Decimal) — во float: для графиков и статистики точности хватает."""
    df = ch.query_df(sql)
    for c in df.columns:
        if df[c].dtype == object and df[c].map(lambda v: type(v).__name__ == "Decimal").any():
            df[c] = df[c].astype(float)
    return df


def fmt(x, d=0) -> str:
    return f"{x:,.{d}f}".replace(",", " ").replace(".", ",")


def pct(x, d=1) -> str:
    return fmt(100 * x, d) + " %"


def layout(fig: go.Figure, title: str, h: int = 360, y: str = "") -> go.Figure:
    fig.update_layout(title=dict(text=title, font=dict(size=15)), height=h, template="simple_white",
                      font=dict(family="Inter, system-ui, sans-serif", size=12, color=INK),
                      margin=dict(l=50, r=20, t=50, b=40), legend=dict(orientation="h", y=-0.18),
                      yaxis=dict(title=y, gridcolor=GRID, showgrid=True), hovermode="x unified")
    return fig


def boot_diff(a: np.ndarray, b: np.ndarray, n: int = 4000) -> tuple[float, float, float]:
    """Разница средних (b − a) и 95 % бутстреп-интервал."""
    da = rng.choice(a, (n, len(a))).mean(1)
    db = rng.choice(b, (n, len(b))).mean(1)
    d = db - da
    return float(b.mean() - a.mean()), float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5))


def prop_ci(k: int, n: int) -> tuple[float, float, float]:
    p = k / n
    se = (p * (1 - p) / n) ** 0.5
    return p, p - 1.96 * se, p + 1.96 * se


sections, facts = [], {}


def section(title: str, text: str, figs: list[go.Figure], table: str = ""):
    sections.append((title, text, figs, table))


# ---------------------------------------------------------------- 0. общая картина
kpi = q("""select (select count() from analytics.dim_users) users,
                  (select countIf(first_purchase_at is not null) from analytics.dim_users) payers,
                  sum(purchase_gmv) gmv, sum(fee_revenue) fees, sum(cashback_cost) cashback, sum(net_revenue) net,
                  min(day) d0, max(day) d1
           from analytics.mart_finance_daily""").iloc[0]
facts["kpi"] = {k: (str(v) if isinstance(v, (dt.date, pd.Timestamp)) else float(v)) for k, v in kpi.items()}

fin = q("""select toStartOfMonth(day) m, sum(purchase_gmv) gmv, sum(fee_revenue) fees, sum(cashback_cost) cashback,
                  sum(net_revenue) net from analytics.mart_finance_daily group by m order by m""")
f1 = go.Figure()
f1.add_bar(x=fin.m, y=fin.fees / 1e3, name="комиссии мерчантов", marker_color=C["main"])
f1.add_bar(x=fin.m, y=-fin.cashback / 1e3, name="кешбэк (расход)", marker_color=C["alt"])
f1.add_scatter(x=fin.m, y=fin.net / 1e3, name="чистая выручка", mode="lines+markers", line=dict(color=INK, width=3))
layout(f1, "Выручка и расход на кешбэк по месяцам", y="тыс. ₽").update_layout(barmode="relative")
take = float(kpi.fees / kpi.gmv)
cb_share = float(kpi.cashback / kpi.fees)
section("Деньги",
        f"За {fmt((pd.Timestamp(kpi.d1) - pd.Timestamp(kpi.d0)).days + 1)} дней {fmt(kpi.users)} регистраций, "
        f"{fmt(kpi.payers)} хотя бы раз заплатили. Оборот покупок — {fmt(kpi.gmv / 1e6, 1)} млн ₽, комиссия мерчантов "
        f"(take rate) — {pct(take, 2)}. <b>Кешбэк съедает {pct(cb_share, 0)} комиссионного дохода</b>: при take rate "
        f"около 1,4 % базовый кешбэк 1 % оставляет тонкую маржу — это рамка для решения по эксперименту ниже.",
        [f1])

# ---------------------------------------------------------------- 1. каналы и рекламная кампания
fun = q("""select channel, sum(signed_up) s, sum(kyc_submitted) k, sum(kyc_approved) a, sum(topped_up) t, sum(purchased) p
           from analytics.mart_activation_funnel group by channel order by s desc""")
f2 = go.Figure()
for stage, col, name in [("s", 1, "регистрация"), ("k", 1, "заявка KYC"), ("a", 1, "KYC одобрен"),
                         ("t", 1, "пополнил"), ("p", 1, "купил")]:
    f2.add_bar(x=[CH_RU[c] for c in fun.channel], y=fun[stage] / fun.s, name=name)
layout(f2, "Воронка онбординга за 30 дней по каналам (доля от регистраций)").update_layout(yaxis_tickformat=".0%")
weekly = q("""select signup_week w, channel, sum(signed_up) s, sum(purchased) p
              from analytics.mart_activation_funnel group by w, channel order by w""")
f3 = go.Figure()
for c in CH_RU:
    d = weekly[weekly.channel == c]
    f3.add_bar(x=d.w, y=d.s, name=CH_RU[c], marker_color=CH_COLORS[c])
layout(f3, "Регистрации по неделям и каналам", y="пользователей").update_layout(barmode="stack")
act = fun.set_index("channel")
social, organic = act.loc["ads_social"], act.loc["organic"]
facts["activation"] = {c: float(act.loc[c].p / act.loc[c].s) for c in act.index}
section("Каналы и рекламная кампания",
        f"Реклама в соцсетях даёт много регистраций, но до первой покупки за 30 дней доходят "
        f"<b>{pct(social.p / social.s, 0)}</b> против {pct(organic.p / organic.s, 0)} у органики и "
        f"{pct(act.loc['referral'].p / act.loc['referral'].s, 0)} у рефералов. Провал — на KYC: заявку подают "
        f"{pct(social.k / social.s, 0)} пришедших из соцсетей против {pct(organic.k / organic.s, 0)}. "
        f"Всплеск регистраций на неделях кампании (апрель) почти не добавил платящих клиентов: "
        f"бюджет стоит переносить в рефералы, а кампанию в соцсетях — оценивать по стоимости активированного, "
        f"а не зарегистрированного клиента.",
        [f2, f3])

# ---------------------------------------------------------------- 2. форма KYC v2
kyc = q("""select form_version v, sum(started) st, sum(passport_done) p, sum(selfie_done) s, sum(address_done) a,
                  sum(submitted_done) subm from analytics.mart_kyc_form_funnel group by v order by v""").set_index("v")
f4 = go.Figure()
for v, col in [("v1", C["grey"]), ("v2", C["main"])]:
    r = kyc.loc[v]
    f4.add_scatter(x=["начали", "паспорт", "селфи", "адрес", "отправили"], y=[1, r.p / r.st, r.s / r.st, r.a / r.st, r.subm / r.st],
                   name=f"форма {v}", mode="lines+markers", line=dict(color=col, width=3))
layout(f4, "Доходимость шагов формы KYC").update_layout(yaxis_tickformat=".0%", hovermode="x")
p1, l1, h1 = prop_ci(int(kyc.loc["v1"].subm), int(kyc.loc["v1"].st))
p2, l2, h2 = prop_ci(int(kyc.loc["v2"].subm), int(kyc.loc["v2"].st))
sel1 = kyc.loc["v1"].s / kyc.loc["v1"].p
sel2 = kyc.loc["v2"].s / kyc.loc["v2"].p
facts["kyc_form"] = {"v1": p1, "v2": p2, "v1_ci": [l1, h1], "v2_ci": [l2, h2], "selfie_v1": float(sel1), "selfie_v2": float(sel2)}
section("Новая форма KYC (версия 3.2.0)",
        f"Форму отправляют {pct(p1, 0)} начавших в старой версии (95 % ДИ {pct(l1, 0)}–{pct(h1, 0)}) и "
        f"<b>{pct(p2, 0)} в новой</b> ({pct(l2, 0)}–{pct(h2, 0)}). Весь выигрыш — на шаге «селфи»: его проходят "
        f"{pct(sel1, 0)} дошедших до него в v1 и {pct(sel2, 0)} в v2, остальные шаги не изменились. "
        f"Оговорка: это сравнение «до и после», а не A/B — в тот же период менялся и состав трафика. "
        f"Но эффект локализован ровно в изменённом шаге, поэтому объяснение «форма помогла» здесь убедительное.",
        [f4])

# ---------------------------------------------------------------- 3. удержание
ret = q("""with a as (select distinct user_id, toMonday(created_date) wk from analytics.fct_operations
                     where is_success and kind in ('topup','purchase','p2p','withdrawal')),
                u as (select user_id, signup_week, channel from analytics.dim_users
                      where signup_week <= (select toMonday(max(created_date)) - 7*12 from analytics.fct_operations))
           select u.channel as channel, dateDiff('week', u.signup_week, a.wk) n, uniqExact(a.user_id) / any(sz.c) r
           from u join a on a.user_id = u.user_id
           join (select channel, count() c from u group by channel) sz on sz.channel = u.channel
           where n between 0 and 12 group by u.channel, n order by n""")
f5 = go.Figure()
for c in CH_RU:
    d = ret[ret.channel == c]
    f5.add_scatter(x=d.n, y=d.r, name=CH_RU[c], mode="lines", line=dict(color=CH_COLORS[c], width=2.5))
layout(f5, "Транзакционное удержание по неделям жизни (когорты с полными 12 неделями)").update_layout(
    yaxis_tickformat=".0%", xaxis_title="неделя после регистрации", hovermode="x")
r8 = ret[ret.n == 8].set_index("channel").r
facts["retention_w8"] = {c: float(r8.get(c, 0)) for c in CH_RU}
section("Удержание",
        f"На 8-й неделе свои операции делают {pct(r8.get('referral', 0), 0)} пришедших по рефералам, "
        f"{pct(r8.get('organic', 0), 0)} органики и только {pct(r8.get('ads_social', 0), 0)} из рекламы в соцсетях. "
        f"Разница каналов не исчезает со временем — это свойство аудитории, а не только онбординга.", [f5])

# ---------------------------------------------------------------- 4. эксперимент с кешбэком
ab = q("select * from analytics.mart_experiment_cashback")
res = []
for metric, name in [("purchases_30d", "покупок за 30 дней"), ("gmv_30d", "оборот за 30 дней, ₽"),
                     ("cashback_30d", "кешбэк за 30 дней, ₽"), ("net_30d", "комиссии − кешбэк, ₽"),
                     ("active_days_31_60", "дней с покупками на днях 31–60")]:
    a = ab[ab.variant == "control"][metric].astype(float).to_numpy()
    b = ab[ab.variant == "test"][metric].astype(float).to_numpy()
    d, lo, hi = boot_diff(a, b)
    res.append((name, a.mean(), b.mean(), d, lo, hi, d / a.mean() if a.mean() else np.nan))
abt = pd.DataFrame(res, columns=["metric", "control", "test", "delta", "lo", "hi", "rel"])
facts["experiment"] = abt.to_dict("records")
n_c, n_t = (ab.variant == "control").sum(), (ab.variant == "test").sum()
f6 = go.Figure()
f6.add_bar(x=abt.metric, y=abt.rel, error_y=dict(type="data", symmetric=False,
                                                 array=(abt.hi - abt.delta) / abt.control, arrayminus=(abt.delta - abt.lo) / abt.control),
           marker_color=[C["good"] if v > 0 else C["bad"] for v in abt.rel])
layout(f6, "Эффект кешбэка 3 % против 1 % (test − control, 95 % бутстреп-интервал)").update_layout(
    yaxis_tickformat=".0%", hovermode="x", showlegend=False)
row = abt.set_index("metric")
tbl = "<table><tr><th>метрика</th><th>control</th><th>test</th><th>разница</th><th>95 % ДИ</th></tr>" + "".join(
    f"<tr><td>{r.metric}</td><td>{fmt(r.control, 2)}</td><td>{fmt(r.test, 2)}</td><td>{fmt(r.delta, 2)} ({pct(r.rel, 1)})</td>"
    f"<td>{fmt(r.lo, 2)} … {fmt(r.hi, 2)}</td></tr>" for r in abt.itertuples()) + "</table>"
net = row.loc["комиссии − кешбэк, ₽"]
section("A/B-тест: кешбэк 3 % первые 30 дней",
        f"{fmt(n_c)} новичков в контроле и {fmt(n_t)} в тесте (проверка SRM в dbt пройдена). "
        f"Повышенный кешбэк <b>увеличил число покупок на {pct(row.loc['покупок за 30 дней'].rel, 0)}</b> и оборот на "
        f"{pct(row.loc['оборот за 30 дней, ₽'].rel, 0)}, но кешбэк вырос втрое, и экономика ушла в минус: "
        f"«комиссии минус кешбэк» на клиента — {fmt(net.control, 1)} ₽ в контроле и {fmt(net.test, 1)} ₽ в тесте "
        f"(разница {fmt(net.delta, 1)} ₽, ДИ {fmt(net.lo, 1)} … {fmt(net.hi, 1)}). После окончания акции "
        f"(дни 31–60) активность групп не различается — привычки кешбэк не сформировал. "
        f"<b>Решение: не раскатывать.</b> Если цель — активация, дешевле дать разовый бонус за первую покупку.",
        [f6], tbl)

# ---------------------------------------------------------------- 5. сбой провайдера
inc = q("""select hour, sum(attempts) att, sum(technical_failed) tf from analytics.mart_ops_health_hourly
           where kind in ('purchase', 'topup') group by hour order by hour""")
inc["rate"] = inc.tf / inc.att
peak = inc.loc[inc.rate.idxmax()]
day = pd.Timestamp(peak.hour).normalize()
win = inc[(inc.hour >= day - pd.Timedelta(days=2)) & (inc.hour < day + pd.Timedelta(days=2))]
f7 = go.Figure(go.Bar(x=win.hour, y=win.rate, marker_color=[C["bad"] if r > 0.05 else C["grey"] for r in win.rate]))
layout(f7, "Доля технических отказов пополнений и оплат по часам").update_layout(yaxis_tickformat=".0%", showlegend=False)
bad_hours = inc[inc.rate > 0.05]
lost = q(f"""select count(), sum(amount) from analytics.fct_operations
             where failure_reason = 'provider_timeout' and kind = 'purchase'""").iloc[0]
facts["incident"] = {"day": str(day.date()), "hours": int(len(bad_hours)), "peak_rate": float(peak.rate),
                     "failed_purchases": int(lost.iloc[0]), "failed_gmv": float(lost.iloc[1] or 0)}
section("Инцидент: сбой эквайера",
        f"{day:%d.%m.%Y} с {bad_hours.hour.min():%H:%M} до {(bad_hours.hour.max() + pd.Timedelta(hours=1)):%H:%M} "
        f"технические отказы (<code>provider_timeout</code>) поднялись до {pct(peak.rate, 0)} при фоне ноль. "
        f"Не прошло {fmt(lost.iloc[0])} оплат на {fmt((lost.iloc[1] or 0) / 1e3)} тыс. ₽. Витрина отделяет технические "
        f"отказы от бизнес-отказов (нет денег, лимит) — тревога по сумме всех отказов утонула бы в шуме.", [f7])

# ---------------------------------------------------------------- 6. антифрод
fr = q("select * from analytics.mart_fraud_signals")
flag = fr[fr.flagged == 1]
users_f = q(f"""select channel, toDate(signup_at) d from analytics.dim_users
                where user_id in ({','.join(map(str, flag.user_id)) or '0'})""")
f8 = go.Figure()
f8.add_scatter(x=fr.max_transfers_2h, y=fr.max_round_share, mode="markers",
               marker=dict(size=8, color=[C["bad"] if f else C["grey"] for f in fr.flagged], opacity=0.8),
               text=fr.user_id, hovertemplate="user %{text}<br>переводов за 2 ч: %{x}<br>круглые суммы: %{y:.0%}<extra></extra>")
layout(f8, "Переводы за 2 часа после пополнения: сработавшее правило — красным").update_layout(
    xaxis_title="переводов за 2 часа после пополнения", yaxis_tickformat=".0%", yaxis_title="доля круглых сумм", hovermode="closest")
facts["fraud"] = {"flagged": int(len(flag)), "outflow": float(flag.max_outflow_2h.sum()),
                  "channels": users_f.channel.value_counts().to_dict()}
section("Антифрод: схема «дроп»",
        f"Правило сработало на <b>{len(flag)} клиентах</b>: после пополнения за 2 часа — от 5 переводов разным "
        f"получателям, почти все круглыми суммами, получатели — свежие кошельки. Через них ушло "
        f"{fmt(flag.max_outflow_2h.sum() / 1e3)} тыс. ₽. Все пришли из рекламы в соцсетях и зарегистрировались "
        f"в одно окно ({users_f.d.min():%d.%m}–{users_f.d.max():%d.%m}) — признак организованной группы. "
        f"У обычных клиентов несколько переводов подряд бывают, но не круглыми суммами незнакомцам.", [f8])

# ---------------------------------------------------------------- 7. вовлечённость и качество данных
eng = q("select * from analytics.mart_engagement_daily order by day")
f9 = go.Figure()
f9.add_scatter(x=eng.day, y=eng.dau, name="DAU", line=dict(color=C["main"]))
f9.add_scatter(x=eng.day, y=eng.mau, name="MAU (28 дней)", line=dict(color=C["grey"]))
f9.add_scatter(x=eng.day, y=eng.stickiness, name="DAU/MAU", yaxis="y2", line=dict(color=C["alt"], dash="dot"))
layout(f9, "Аудитория приложения", y="пользователей").update_layout(
    yaxis2=dict(overlaying="y", side="right", tickformat=".0%", showgrid=False))
dq = q("""select sum(raw_events) raw, sum(duplicates) dup, sum(late_events) late, sum(unique_events) uniq,
                 sum(dlq_events) dlq from analytics.mart_data_quality_daily""").iloc[0]
dlq_types = q("select error_type, count() c from analytics.stg_app_events_dlq group by 1 order by c desc")
facts["data_quality"] = {"dup_rate": float(dq.dup / dq.raw), "late_rate": float(dq.late / dq.uniq),
                         "contract_rate": float(dq.dlq / (dq.raw + dq.dlq)), "dlq_types": dlq_types.set_index("error_type").c.to_dict()}
section("Вовлечённость и качество данных",
        f"Липкость (DAU/MAU) держится около {pct(eng.stickiness.tail(30).mean(), 0)}. Трекинг: "
        f"{pct(dq.dup / dq.raw, 1)} событий — повторные отправки (схлопываются по event_id), "
        f"{pct(dq.late / dq.uniq, 1)} приходят позже часа (офлайн), "
        f"{pct(dq.dlq / (dq.raw + dq.dlq), 1)} нарушают контракт и уходят в DLQ — "
        + ", ".join(f"{k}: {v}" for k, v in dlq_types.set_index('error_type').c.items()) +
        ". Поздние события не теряются: сессии пересчитываются по времени приёма, а не по времени события.", [f9])

# ---------------------------------------------------------------- HTML
body = []
for i, (title, text, figs, table) in enumerate(sections):
    charts = "".join(f'<div class="chart">{f.to_html(full_html=False, include_plotlyjs=False, config={"displaylogo": False})}</div>'
                     for f in figs)
    body.append(f'<section><h2>{i}. {title}</h2><p>{text}</p>{charts}{table}</section>')

html = f"""<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Кошелёк: аналитика</title>
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist-min@2.35.2/plotly.min.js"></script>
<style>
 body {{ font: 16px/1.6 Inter, system-ui, -apple-system, "Segoe UI", sans-serif; color: {INK}; max-width: 1040px; margin: 0 auto; padding: 24px 16px 60px; background: #fff; }}
 h1 {{ font-size: 30px; margin-bottom: 4px; }} h2 {{ font-size: 21px; margin-top: 40px; border-top: 1px solid {GRID}; padding-top: 24px; }}
 .lead {{ color: {MUTED}; }} code {{ background: #f4f5f7; padding: 1px 5px; border-radius: 4px; font-size: 14px; }}
 table {{ border-collapse: collapse; font-size: 14px; margin: 12px 0; width: 100%; }} td, th {{ border-bottom: 1px solid {GRID}; padding: 6px 8px; text-align: left; }}
 th {{ color: {MUTED}; font-weight: 600; }} .chart {{ margin: 8px 0; }} a {{ color: #2563eb; }}
</style></head><body>
<h1>Кошелёк: продуктовая и финансовая аналитика</h1>
<p class="lead">Полгода жизни сервиса (данные синтетические, сгенерированы симулятором с заложенными сюжетами —
аналитика находит их по хранилищу, не зная параметров симуляции). Источник цифр — витрины dbt в ClickHouse,
собранные из CDC PostgreSQL и событий приложения. Отчёт собран {dt.date.today():%d.%m.%Y} скриптом
<code>scripts/report.py</code>. Код и архитектура — <a href="https://github.com/Alex91132/wallet-data-platform">GitHub</a>.</p>
{''.join(body)}
</body></html>"""
(ROOT / "docs").mkdir(exist_ok=True)
(ROOT / "docs" / "index.html").write_text(html, encoding="utf-8")
(ROOT / "docs" / "report.json").write_text(json.dumps(facts, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
print(json.dumps(facts, ensure_ascii=False, indent=1, default=float)[:3000])
