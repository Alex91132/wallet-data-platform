"""Регистрация CDC-коннекторов после `docker compose up -d` (повторный запуск безопасен: PUT конфигурации).

    python scripts/bootstrap.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests

CONNECT = "http://localhost:8083"
ROOT = Path(__file__).resolve().parents[1]


def wait(url: str, what: str, timeout: int = 240) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if requests.get(url, timeout=5).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    sys.exit(f"{what} не поднялся за {timeout} с")


def main() -> None:
    wait(f"{CONNECT}/connectors", "Kafka Connect")
    wait("http://localhost:8000/health", "API")
    for f in sorted((ROOT / "connect").glob("*.json")):
        spec = json.loads(f.read_text(encoding="utf-8"))
        r = requests.put(f"{CONNECT}/connectors/{spec['name']}/config", json=spec["config"], timeout=30)
        r.raise_for_status()
        print(f"{spec['name']}: {'создан' if r.status_code == 201 else 'обновлён'}")
    time.sleep(10)
    for name, st in requests.get(f"{CONNECT}/connectors?expand=status", timeout=30).json().items():
        tasks = [t["state"] for t in st["status"]["tasks"]]
        print(f"{name}: {st['status']['connector']['state']}, задачи: {tasks}")


if __name__ == "__main__":
    main()
