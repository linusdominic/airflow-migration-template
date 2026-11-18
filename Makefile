.PHONY: install selftest seed demo list test test-postgres airflow-install test-airflow airflow-run lint

AIRFLOW_VERSION ?= 3.4.0
PYTHON_VERSION  ?= 3.12
DAY             ?= 2026-11-10

install:
	uv sync

selftest: ## can this environment run pipelines? Local by default; set PLATFORM_ENV for another
	uv run dp selftest

seed: ## sample data for the example pipelines, into .local/
	uv run dp seed

demo: seed ## five days of all three example pipelines on DuckDB
	uv run dp backfill customers_history --from 2026-11-10 --to 2026-11-14
	uv run dp backfill loans_daily --from 2026-11-10 --to 2026-11-14
	uv run dp backfill payments_incremental --from 2026-11-10 --to 2026-11-14

list:
	uv run dp list

test: ## DuckDB only: no server, no network
	uv run pytest

test-postgres: ## the same pipelines on PostgreSQL. Set TEST_POSTGRES_URL
	uv run pytest -m postgres

airflow-install: ## Airflow in its own environment, pinned with the official constraints
	uv venv .venv-airflow --python $(PYTHON_VERSION)
	uv pip install --python .venv-airflow/bin/python "apache-airflow==$(AIRFLOW_VERSION)" \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt"
	uv pip install --python .venv-airflow/bin/python -e . pytest "psycopg[binary]" fsspec

test-airflow: ## the DAGs, checked with Airflow itself
	.venv-airflow/bin/python -m pytest -m airflow

airflow-run: ## run one DAG inside Airflow for one date, against the local DuckDB warehouse
	AIRFLOW_HOME=$(CURDIR)/airflow_home AIRFLOW__CORE__DAGS_FOLDER=$(CURDIR)/dags AIRFLOW__CORE__LOAD_EXAMPLES=False \
		.venv-airflow/bin/airflow db migrate
	.venv-airflow/bin/dp seed
	AIRFLOW_HOME=$(CURDIR)/airflow_home AIRFLOW__CORE__DAGS_FOLDER=$(CURDIR)/dags AIRFLOW__CORE__LOAD_EXAMPLES=False \
		.venv-airflow/bin/airflow dags test loans_daily $(DAY)

lint:
	uv run ruff check .
	uv run ruff format --check .
