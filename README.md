# airflow-migration-template

[![CI](https://github.com/linusdominic/airflow-migration-template/actions/workflows/ci.yml/badge.svg)](https://github.com/linusdominic/airflow-migration-template/actions/workflows/ci.yml)

A data platform to migrate legacy ETL jobs into. It gives you the structure most teams
end up building halfway through a migration: pipelines that do not know which scheduler
runs them, warehouse adapters that make the same pipeline run on DuckDB, PostgreSQL or
Redshift, tests that prove a pipeline can be re-run and backfilled, and a self-test that
tells you whether an environment is ready before anything is scheduled.

```
$ dp selftest
PASS  configuration          environment local, 3 connections
PASS  pipeline definitions   3 pipelines, 18 tasks, no cycles or missing dependencies
PASS  connection: warehouse  connected (duckdb), SELECT 1 returned 1
PASS  connection: source     connected (duckdb), SELECT 1 returned 1
PASS  store: landing         wrote, listed, read and deleted a file
PASS  warehouse: write path  create, load, upsert, history, checks and rollback on duckdb
PASS  platform state         run log and watermark tables exist, watermark round trip
PASS  sql templates          18 SQL statements render for duckdb with no undefined variable

8 of 8 checks passed in the local environment.
```

It is a template and a portfolio project. It is not a converter: it does not read Talend
job files or generate code from them. Every result below comes from a run on my machine on
18 November 2026, and the command that reproduces it is next to it.

## The problem

At a company I worked at, we moved our ETL from Talend jobs to Airflow. The jobs
themselves were not the hard part. The hard part was everything a Talend job had been
quietly getting from the tool, which we now had to supply ourselves:

- **Where do connections live?** Talend had context groups for Dev and Prod. The first
  Airflow DAGs had connection strings and schema names scattered through them.
- **Can it be run twice?** A Talend job that failed halfway was fixed by hand. In Airflow,
  retries and backfills happen on their own, so a task that is not safe to repeat will
  eventually corrupt something.
- **How do you test it?** A DAG that only runs inside Airflow, against the production
  warehouse, cannot be tested on a laptop. So it is not tested.
- **How do you know the new pipeline matches the old job?** Without a way to run both and
  compare, the switch-over is an act of faith.

This repository is the starting point I would want for that work now.

## What you get

| Concern | What is here | Where |
|---|---|---|
| Pipelines independent of the scheduler | A pipeline is a plain description: tasks and what each runs after. Airflow is one way to run it; the command line and the tests are others | `pipeline.py`, `runner.py` |
| Airflow | One adapter turns a pipeline into a DAG. Each DAG file is three lines. It is the only module that imports Airflow, and a test enforces that | `airflow_adapter.py`, `dags/` |
| Any warehouse | Tasks ask for a `Warehouse` by name. DuckDB, PostgreSQL and Redshift adapters satisfy the same interface, and the SQL sticks to what all three accept | `ports.py`, `warehouses/` |
| Any cloud | Files go through an `ObjectStore`: a folder locally, or `s3://`, `gs://`, `abfs://` through fsspec | `stores.py` |
| Environments | `config/local.yml`, `dev.yml`, `prod.yml`, chosen by `PLATFORM_ENV`. Production takes every value from environment variables | `config.py`, `config/` |
| Transformations | SQL files with a handful of variables: the schema names, the logical date, the dialect | `sql.py` |
| Repeatable loads | `Upsert` (delete then insert in one transaction), a mart that is replaced one date at a time, and a history task that refuses to run out of order | `tasks/`, `pipelines/` |
| Incremental loads | A watermark that moves only after the load has been reconciled against its source | `tasks/load.py`, `state.py` |
| Data quality | Checks that run together and fail the pipeline once, listing every problem | `tasks/quality.py` |
| An audit trail | Every task, in every run, is written to `platform.run_log` in the warehouse | `state.py` |
| A self-test | `dp selftest` exercises connections, the write path, the object store and every SQL template in whatever environment is configured | `selftest.py` |
| A skeleton | `dp new-pipeline NAME` creates a pipeline, its SQL, its DAG and a test that passes | `scaffold.py` |
| A migration guide | Where each Talend concept goes, the order to work in, and a checklist per job | [docs/migration-guide.md](docs/migration-guide.md) |

## What was verified, and how

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/results-dark.svg">
  <img alt="What was verified on 18 November 2026: 33 tests pass on DuckDB, including five days of all three pipelines with re-runs and out-of-order runs; the self-test passes 8 of 8 checks on PostgreSQL 16 and the five-day run passes there with no pipeline changed; 6 DAG tests pass in Airflow 3.4.0 and a full loans_daily run loaded 60 loans. Not run: Redshift, S3, GCS and Azure storage, the container files, and Airflow 2." src="docs/img/results-light.svg">
</picture>

| | Result | How |
|---|---|---|
| DuckDB | 33 tests pass. Five days of all three pipelines load, reconcile, and survive re-runs and out-of-order runs | `make test` |
| PostgreSQL 16 | The same five-day run passes as one more test, with no change to any pipeline, and the self-test passes 8 of 8 | `make test-postgres`, and `dp selftest` with `PLATFORM_ENV=dev` |
| Airflow 3.4.0 | 6 DAG tests pass, and a full run of `loans_daily` inside Airflow finished with every task successful (1.8 seconds on the last run) and loaded 60 loans | `make test-airflow`, `make airflow-run` |
| Redshift | **Not run.** I have no cluster. The adapter and the SQL it generates are covered by unit tests only | |
| S3, GCS, Azure storage | **Not run.** The fsspec store is tested against fsspec's in-memory file system only | |
| The container files | **Not built or run.** I ran Airflow from a virtual environment | |
| Airflow 2 | **Not run.** The adapter has fallback imports for it that have never executed | |

CI repeats the DuckDB, PostgreSQL and Airflow rows on every push.

## Quickstart

Nothing to install but Python, and nothing to start:

```bash
uv sync
uv run dp selftest            # is this environment able to run pipelines? Works from any folder
make demo                     # sample data, then five days of all three pipelines
make test
```

`make demo` writes a DuckDB warehouse to `.local/`. Look inside it:

```bash
uv run python -c "import duckdb; print(duckdb.connect('.local/warehouse.duckdb').sql('select * from marts.loan_portfolio_daily order by 1, 2, 3'))"
```

Against PostgreSQL:

```bash
export PLATFORM_ENV=dev
export WAREHOUSE_URL=postgresql://user:password@localhost:5432/warehouse
export SOURCE_URL=$WAREHOUSE_URL
uv run dp selftest
```

Inside Airflow:

```bash
make airflow-install          # Airflow 3.4.0 in .venv-airflow, with the official constraints
make test-airflow
make airflow-run              # one full run of loans_daily, for 2026-11-10
```

## How it is put together

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/architecture-dark.svg">
  <img alt="Architecture: Airflow DAGs (through airflow_adapter.py, the only Airflow import), the dp command and pytest all call runner.run_task, which writes every task of every run to platform.run_log and runs the pipeline&#x27;s tasks (LoadCsv, CopyQuery, RunSql, Upsert, Scd2, QualityChecks). Tasks reach data only through the ports.py interfaces: Warehouse (DuckDB, PostgreSQL or Redshift adapter) and ObjectStore (a local folder, or S3, GCS and Azure through fsspec)." src="docs/img/architecture-light.svg">
</picture>

<details>
<summary>The same diagram as text, with more detail</summary>

```mermaid
flowchart TB
    subgraph run["Ways to run a pipeline"]
        A["Airflow DAG<br/>dags/*.py"]
        C["dp run / dp backfill"]
        T["pytest"]
        S["dp selftest"]
    end
    A --> AD["airflow_adapter.py<br/>the only Airflow import"]
    AD --> R
    C --> R
    T --> R
    S --> R
    R["runner.run_task<br/>one place a task is executed"] --> P["Pipeline<br/>tasks and dependencies"]
    P --> K["Tasks<br/>LoadCsv, CopyQuery, RunSql,<br/>Upsert, Scd2, QualityChecks"]
    K --> W{{"Warehouse<br/>interface"}}
    K --> O{{"ObjectStore<br/>interface"}}
    W --> D["DuckDB"]
    W --> PG["PostgreSQL"]
    W --> RS["Redshift"]
    O --> L["Local folder"]
    O --> F["S3, GCS, Azure<br/>through fsspec"]
```

</details>

Three rules hold the design together, and each has a test.

**A task depends on interfaces, never on a driver.** `ports.py` defines `Warehouse` and
`ObjectStore`. A task calls `ctx.warehouse()` and gets whatever the environment is
configured with. `test_only_the_adapters_import_a_database_driver_or_a_cloud_library`
fails if any other module imports `duckdb`, `psycopg`, `boto3` or `fsspec`.

**Only one module knows about Airflow.** `test_only_the_adapter_and_the_dag_files_import_airflow`
fails if anything else does. That is what lets the pipelines be tested without Airflow
installed, and it means changing scheduler would be a change to one file.

**There is one place a task is executed.** The command line, the tests, the self-test and
the Airflow worker all go through `runner.run_task`. What you test on a laptop is the code
that runs in production.

### A pipeline

```python
PIPELINE = Pipeline(
    name="payments_incremental",
    description="Copies new and changed payments from the source database and reconciles the totals.",
    schedule="30 6 * * *",
    sql_dir=Path(__file__).parent / "sql",
    tasks=(
        RunSql(name="create_tables", sql="create_tables.sql"),
        CopyQuery(
            name="extract_changed",
            after=("create_tables",),
            sql=...,
            table="{{ staging }}.payments_changed",
            columns=COLUMNS,
        ),
        QualityChecks(name="check_staged", after=("extract_changed",), checks=(NotNull(...), Unique(...))),
        Upsert(
            name="upsert_core",
            after=("check_staged",),
            target="{{ core }}.payments",
            source="{{ staging }}.payments_changed",
            keys=("payment_id",),
            columns=...,
        ),
        QualityChecks(name="reconcile", after=("upsert_core",), checks=(Reconcile(...), Reconcile(...))),
        SetWatermark(
            name="advance_watermark",
            after=("reconcile",),
            watermark="payments",
            sql="SELECT MAX(updated_at) FROM {{ staging }}.payments_changed",
        ),
    ),
)
```

and its DAG file, whole:

```python
from dataplatform.airflow_adapter import build_dag
from pipelines.payments_incremental import PIPELINE

dag = build_dag(PIPELINE)
```

### The three example pipelines

Each is one of the three shapes almost every legacy job has.

| Pipeline | Shape | What it shows |
|---|---|---|
| `loans_daily` | A file in, tables out | Landing raw text, typing and cleaning in SQL, checks before anything is published, a mart rebuilt per date |
| `payments_incremental` | Database to database, only what changed | A watermark, an upsert, and reconciliation of counts and totals against the source before the watermark moves |
| `customers_history` | A dimension that keeps its history | Type 2 history from a daily snapshot, in two SQL statements that are the same on every warehouse |

The sample data is a few days of a small lender, generated from a seed. The files are a
little dirty on purpose: stray spaces, mixed case, and the same loan twice in one file.

### Running twice, and backfilling

Airflow retries tasks and backfills dates without asking. So every example pipeline is
tested for three things (`tests/test_pipelines.py`):

- Running the same date twice leaves every table identical.
- Running an older date after newer ones leaves every table identical.
- A failure stops the pipeline before anything is published, and an incremental load's
  watermark does not move.

Getting the second one right changed the design. My first `loans_daily` updated a
"current loans" table in place, which is what Talend's insert-or-update does. Re-running
an old date then put old values back over new ones. Now every version of a loan is kept,
keyed by when it changed, and the current table and the daily mart are derived from the
versions. The history pipeline cannot be made order-independent that way, so it refuses:

```
ScdOrderError: core.dim_customer already has history up to 2026-11-12. A snapshot for
2026-11-10 cannot be applied after it. To backfill, rebuild the dimension from its first date.
```

### The self-test

`dp selftest` is for the moment after a deployment and before the first scheduled run. In
whatever environment `PLATFORM_ENV` selects, it connects to every warehouse, round-trips a
file through every object store, and on the warehouse runs a create, load, upsert, history
update, quality check and a deliberately failed transaction in a scratch schema that it
then drops. It also renders every SQL template for that warehouse's dialect. It reports
every failure, not just the first, and exits non-zero if there is one.

The same command on PostgreSQL:

```
PASS  connection: warehouse  connected (postgres), SELECT 1 returned 1
PASS  warehouse: write path  create, load, upsert, history, checks and rollback on postgres
PASS  sql templates          18 SQL statements render for postgres with no undefined variable

8 of 8 checks passed in the dev environment.
```

## Adding to it

**A pipeline.** `dp new-pipeline vendor_invoices` creates the definition, two SQL files,
the DAG file and a test that passes. Edit from there.

**A warehouse.** Write a class with `execute`, `query`, `insert_rows`, `transaction` and
`close`, give it a dialect, and add its URI scheme to `WAREHOUSES` in `connections.py`.
Snowflake or BigQuery would each be one file. Then run the five-day test against it.

**A kind of task.** Any object with a `name`, an `after` and a `run(ctx)` method is a
task. `tasks/scd.py` is 65 lines and a good one to copy.

**Connections from Airflow or a secrets manager.** `Connections` takes resolvers:
functions from a name to a URI. Set `PLATFORM_AIRFLOW_CONNECTIONS=1` and the adapter looks
each name up in Airflow first.

## Design decisions

- **Portable SQL over clever SQL.** Upserts are delete-then-insert in a transaction, not
  `MERGE` or `ON CONFLICT`, because that is what all three warehouses accept. The one
  warehouse-specific line in the examples is Redshift's distribution and sort keys, and it
  goes through the dialect.
- **Staging is text.** Files land as `VARCHAR` and are typed in SQL, where a bad value
  can be seen and handled, and where the rule is in a file that gets reviewed.
- **State lives in the warehouse.** The run log and the watermarks are tables, so they are
  backed up with the data and can be queried like anything else.
- **Checks report everything.** A run that fails on the first of five problems gets fixed
  five times.
- **Small, flat classes.** A task is a frozen dataclass with one method. There is no base
  class to inherit from and no plugin system.

## Limitations

- It does not convert Talend jobs. The guide tells a person where things go.
- Redshift, cloud object stores, the container files and Airflow 2 were not run. See the
  table above.
- `LoadCsv` and `CopyQuery` move rows through Python. That suits the sizes most jobs
  handle and is the wrong tool for very large loads, which should be bulk loaded.
- The SQL statement splitter is simple: a statement ends at a semicolon at the end of a
  line. It does not understand procedural blocks.
- No alerting is configured. Airflow's notifications are the place for it.
- The sample data is synthetic and small. Nothing here measures performance.
- Nothing in this repository is the original company's code, jobs or data.

## Project layout

```
config/                  one file per environment: local, dev, prod
src/dataplatform/
  ports.py               the Warehouse and ObjectStore interfaces
  warehouses/            DuckDB, PostgreSQL and Redshift adapters
  stores.py              local folder, and any cloud bucket through fsspec
  config.py              settings, with ${VARIABLES} from the environment
  connections.py         a connection's name to an adapter
  pipeline.py            Pipeline, Task and RunContext: no scheduler in sight
  sql.py                 SQL files with variables
  tasks/                 LoadCsv, CopyQuery, RunSql, Upsert, SetWatermark, Scd2, quality checks
  state.py               the run log and the watermarks
  runner.py              runs a task or a pipeline in this process
  airflow_adapter.py     a pipeline to an Airflow DAG
  selftest.py            dp selftest
  scaffold.py            dp new-pipeline
  sample.py              the sample data
  cli.py                 the dp command
pipelines/               one folder per pipeline: definition and SQL
dags/                    three lines per pipeline
tests/                   40 tests: 33 on DuckDB, 1 on PostgreSQL, 6 inside Airflow
docs/migration-guide.md  Talend to this template
```

## License

MIT
