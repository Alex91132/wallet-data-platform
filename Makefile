# Один вход в проект. Логика — в scripts/run_all.py, здесь только короткие имена целей.
#   make all     полный прогон (~45 минут: сборка стека, симуляция, dbt, сверка, отчёт)
#   make test    16 тестов конкурентности
PY ?= python

.PHONY: all up seed staging reconcile build report test down

all:       ; $(PY) scripts/run_all.py all
up:        ; $(PY) scripts/run_all.py up
seed:      ; $(PY) scripts/run_all.py seed
staging:   ; $(PY) scripts/run_all.py staging
reconcile: ; $(PY) scripts/run_all.py reconcile
build:     ; $(PY) scripts/run_all.py build
report:    ; $(PY) scripts/run_all.py report
test:      ; $(PY) scripts/run_all.py test
down:      ; $(PY) scripts/run_all.py down
