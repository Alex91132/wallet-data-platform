"""Один вход в проект: стек → симуляция → dbt → сверка → отчёт → тесты.

    python scripts/run_all.py all            # полный прогон, ~45 минут (40 из них — симуляция)
    python scripts/run_all.py up             # только стек и регистрация CDC-коннекторов
    python scripts/run_all.py seed --days 30
    python scripts/run_all.py test

У каждой команды проверяется предусловие (Docker, dbt) и печатается оценка времени —
чтобы «command not found» не всплывал на середине прогона.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"
DBT = ROOT / "dbt"
PY = sys.executable

HINTS = {
    "docker": "Поставьте Docker Desktop и запустите его — без него не поднять стек.",
    "dbt": "pip install dbt-clickhouse (или активируйте venv, где он стоит).",
}


def need(binary: str) -> None:
    if shutil.which(binary) is None:
        sys.exit(f"нет «{binary}» в PATH. {HINTS[binary]}")


def sh(args: list[str], cwd: Path = ROOT) -> None:
    print("→", " ".join(args), flush=True)
    try:
        subprocess.run(args, cwd=cwd, check=True)
    except subprocess.CalledProcessError as e:
        sys.exit(f"остановился: «{' '.join(args)}» вернула код {e.returncode}")


# ------------------------------------------------------------------ шаги
def up(_: argparse.Namespace) -> None:
    need("docker")
    sh(["docker", "compose", "up", "-d", "--build"])
    sh([PY, "scripts/bootstrap.py"])          # ждёт Connect и API, регистрирует коннекторы


def seed(a: argparse.Namespace) -> None:
    sh([PY, "-m", "wallet.sim", "--days", str(a.days)], cwd=APP)


def staging(_: argparse.Namespace) -> None:
    need("dbt")
    sh(["dbt", "run", "--profiles-dir", ".", "-s", "staging"], cwd=DBT)


def reconcile(a: argparse.Namespace) -> None:
    sh([PY, "scripts/reconcile.py", "--wait", str(a.wait)])


def build(_: argparse.Namespace) -> None:
    need("dbt")
    sh(["dbt", "build", "--profiles-dir", "."], cwd=DBT)


def report(_: argparse.Namespace) -> None:
    sh([PY, "scripts/report.py"])


def test(_: argparse.Namespace) -> None:
    sh([PY, "-m", "pytest", "tests", "-q"])


def down(_: argparse.Namespace) -> None:
    need("docker")
    sh(["docker", "compose", "down"])


STEPS = {
    "up": (up, "поднять стек и зарегистрировать коннекторы", "~3 мин"),
    "seed": (seed, "полгода жизни сервиса через API", "~40 мин"),
    "staging": (staging, "представления для сверки", "~20 с"),
    "reconcile": (reconcile, "сверить источник и хранилище", "~1–10 мин"),
    "build": (build, "все модели и тесты dbt", "~1 мин"),
    "report": (report, "docs/index.html и docs/report.json", "~30 с"),
    "test": (test, "16 тестов конкурентности (нужен живой PostgreSQL)", "~10 с"),
    "down": (down, "остановить стек", "—"),
}


def full(a: argparse.Namespace) -> None:
    """Полный прогон: то, что нужно для отчёта, в правильном порядке."""
    for name in ("up", "seed", "staging", "reconcile", "build", "report"):
        print(f"\n=== {name}: {STEPS[name][1]} ({STEPS[name][2]}) ===", flush=True)
        STEPS[name][0](a)


def main() -> None:
    ap = argparse.ArgumentParser(description="Один вход в проект: шаги по порядку и с проверками")
    sub = ap.add_subparsers(dest="cmd", required=True)

    for name, (_, what, dur) in STEPS.items():
        p = sub.add_parser(name, help=f"{what} ({dur})")
        p.set_defaults(run=STEPS[name][0])
        if name == "seed":
            p.add_argument("--days", type=int, default=182, help="длина симуляции, дней")
        if name == "reconcile":
            p.add_argument("--wait", type=int, default=600, help="сколько ждать догоняющего CDC, секунд")

    p_all = sub.add_parser("all", help="полный прогон: up → seed → staging → reconcile → build → report")
    p_all.add_argument("--days", type=int, default=182)
    p_all.add_argument("--wait", type=int, default=600)
    p_all.set_defaults(run=full)

    a = ap.parse_args()
    a.run(a)


if __name__ == "__main__":
    main()
