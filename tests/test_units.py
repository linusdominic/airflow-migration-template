"""The pieces, one at a time: configuration, connections, SQL, the pipeline graph, stores."""

import ast
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

from dataplatform import selftest, sql
from dataplatform.config import ROOT, ConfigError, expand, load_settings
from dataplatform.connections import Connections
from dataplatform.pipeline import Pipeline, PipelineError, as_literal
from dataplatform.ports import ObjectStore, Warehouse
from dataplatform.registry import load_pipelines
from dataplatform.runner import context
from dataplatform.scaffold import new_pipeline
from dataplatform.stores import FsspecStore, LocalStore
from dataplatform.tasks import RunSql, Scd2, Upsert
from dataplatform.warehouses.duckdb import DuckDBWarehouse
from dataplatform.warehouses.postgres import PostgresDialect, RedshiftDialect, RedshiftWarehouse
from pipelines.loans_daily import PIPELINE as LOANS

# ── configuration ──────────────────────────────────────────────────────────────


def test_variables_are_expanded_with_defaults_and_missing_ones_are_errors():
    assert expand("duckdb://${HOME_DIR:-.local}/w.duckdb", {}) == "duckdb://.local/w.duckdb"
    assert expand("duckdb://${HOME_DIR:-.local}/w.duckdb", {"HOME_DIR": "/data"}) == "duckdb:///data/w.duckdb"
    with pytest.raises(ConfigError, match="WAREHOUSE_URL is not set"):
        expand("${WAREHOUSE_URL}", {})


def test_each_environment_loads_and_production_takes_everything_from_the_environment():
    local = load_settings("local", environ={})
    assert local.env == "local" and local.connections["warehouse"].startswith("duckdb://")
    with pytest.raises(ConfigError, match="connection 'warehouse' in config/prod.yml"):
        load_settings("prod", environ={})
    env = {
        "WAREHOUSE_URL": "redshift://u:p@host:5439/db",
        "SOURCE_URL": "postgresql://u:p@h/db",
        "LANDING_URL": "s3://b/p",
    }
    prod = load_settings("prod", environ=env)
    assert prod.connections == {"warehouse": env["WAREHOUSE_URL"], "source": env["SOURCE_URL"], "landing": "s3://b/p"}
    assert "redshift" not in (ROOT / "config" / "prod.yml").read_text().split("connections:")[1]  # no URI in the file


def test_a_deployment_can_override_one_connection_and_an_unknown_environment_says_what_exists():
    settings = load_settings("local", environ={"PLATFORM_CONN_WAREHOUSE": "postgresql://x/y", "PLATFORM_ENV": "local"})
    assert settings.connections["warehouse"] == "postgresql://x/y"
    with pytest.raises(ConfigError, match="Environments here: dev, local, prod"):
        load_settings("staging", environ={})


# ── connections ────────────────────────────────────────────────────────────────


def test_the_scheme_picks_the_adapter_and_a_connection_is_opened_once(platform, tmp_path):
    warehouse = platform.connections.warehouse()
    assert isinstance(warehouse, DuckDBWarehouse) and isinstance(warehouse, Warehouse)
    assert platform.connections.warehouse() is warehouse
    assert isinstance(platform.connections.store("landing"), LocalStore)
    with pytest.raises(ConfigError, match="no connection named 'nowhere'"):
        platform.connections.warehouse("nowhere")
    with pytest.raises(ConfigError, match="is not a warehouse"):
        platform.connections.warehouse("landing")


def test_another_system_can_supply_a_connection(platform):
    asked = []
    connections = Connections(platform.settings, resolvers=[lambda name: asked.append(name) or None])
    assert connections.uri("warehouse") == platform.settings.connections["warehouse"]  # falls back to the file
    assert asked == ["warehouse"]
    secret = Connections(platform.settings, resolvers=[lambda name: "duckdb://:memory:" if name == "vault" else None])
    assert secret.warehouse("vault").query("SELECT 1 AS ok") == [{"ok": 1}]


# ── stores ─────────────────────────────────────────────────────────────────────


@pytest.fixture(params=["local", "fsspec"])
def store(request, tmp_path) -> ObjectStore:
    return LocalStore(tmp_path) if request.param == "local" else FsspecStore(f"memory://store-{id(request)}")


def test_stores_behave_the_same(store):
    assert isinstance(store, ObjectStore)
    assert not store.exists("loans/a.csv") and store.list() == []
    store.write_text("loans/a.csv", "x,y\n1,2\n")
    store.write_text("loans/b.csv", "x\n")
    store.write_text("other/c.csv", "x\n")
    assert store.read_text("loans/a.csv") == "x,y\n1,2\n"
    assert store.list("loans/") == ["loans/a.csv", "loans/b.csv"]
    store.delete("loans/a.csv")
    store.delete("loans/a.csv")  # deleting what is not there is not an error
    assert store.list("loans/") == ["loans/b.csv"]


def test_a_local_store_cannot_be_escaped(tmp_path):
    with pytest.raises(ValueError, match="outside the store"):
        LocalStore(tmp_path / "landing").read_text("../../etc/passwd")


# ── SQL ────────────────────────────────────────────────────────────────────────


def test_values_become_safe_literals():
    assert as_literal(None) == "NULL" and as_literal(True) == "TRUE" and as_literal(42) == "42"
    assert as_literal(date(2026, 11, 17)) == "DATE '2026-11-17'"
    assert as_literal(datetime(2026, 11, 17, 6, 30)) == "TIMESTAMP '2026-11-17 06:30:00'"
    assert as_literal("O'Brien; DROP TABLE x") == "'O''Brien; DROP TABLE x'"


def test_templates_know_the_schemas_the_date_and_the_dialect(platform):
    ctx = context(LOANS, date(2026, 11, 17), platform.settings, platform.connections)
    text = "SELECT * FROM {{ core }}.loans WHERE d = {{ logical_date | lit }} AND t < {{ interval_end }} -- {{ ds }} {{ dialect.name }}"
    assert sql.render(text, ctx) == (
        "SELECT * FROM core.loans WHERE d = DATE '2026-11-17' AND t < TIMESTAMP '2026-11-18 00:00:00' -- 2026-11-17 duckdb"
    )
    with pytest.raises(Exception, match="'cor' is undefined"):
        sql.render("SELECT * FROM {{ cor }}.loans", ctx)  # a typo is an error, not an empty string


def test_statements_are_split_on_line_ending_semicolons_only():
    text = "-- setup\nCREATE TABLE a (x VARCHAR(10));\n\nINSERT INTO a VALUES ('one; two');\n-- done\n"
    assert sql.statements(text) == ["-- setup\nCREATE TABLE a (x VARCHAR(10))", "INSERT INTO a VALUES ('one; two')"]


def test_redshift_gets_distribution_and_sort_keys_and_the_others_get_nothing():
    assert (
        RedshiftDialect().table_options(dist_key="loan_id", sort_key="paid_at") == "DISTKEY (loan_id) SORTKEY (paid_at)"
    )
    assert PostgresDialect().table_options(dist_key="loan_id", sort_key="paid_at") == ""
    assert RedshiftWarehouse._native("redshift://u:p@host:5439/db") == "postgresql://u:p@host:5439/db"
    assert RedshiftWarehouse._options() == {"prepare_threshold": None}


def test_the_sql_each_task_generates_is_portable(platform):
    """No ON CONFLICT, no MERGE: only statements DuckDB, PostgreSQL and Redshift all accept."""
    ctx = context(LOANS, date(2026, 11, 17), platform.settings, platform.connections)
    upsert = Upsert("u", "{{ core }}.t", "{{ staging }}.s", ("id", "day"), ("id", "day", "v"))
    assert upsert.statements(ctx) == [
        "DELETE FROM core.t USING staging.s AS s WHERE core.t.id = s.id AND core.t.day = s.day",
        "INSERT INTO core.t (id, day, v) SELECT id, day, v FROM staging.s AS s",
    ]
    close, open_new = Scd2("h", "{{ core }}.d", "{{ staging }}.s", "id", ("a", "b")).statements(ctx)
    assert close == (
        "UPDATE core.d SET valid_to = DATE '2026-11-17', is_current = FALSE FROM staging.s AS s "
        "WHERE core.d.id = s.id AND core.d.is_current AND (core.d.a IS DISTINCT FROM s.a OR core.d.b IS DISTINCT FROM s.b)"
    )
    assert "LEFT JOIN core.d AS t ON t.id = s.id AND t.is_current WHERE t.id IS NULL" in open_new


# ── the pipeline graph ─────────────────────────────────────────────────────────


def test_tasks_run_in_dependency_order_whatever_order_they_are_written_in():
    tasks = (RunSql("c", "SELECT 1", after=("a", "b")), RunSql("b", "SELECT 1", after=("a",)), RunSql("a", "SELECT 1"))
    assert [t.name for t in Pipeline("p", "d", tasks).ordered()] == ["a", "b", "c"]


@pytest.mark.parametrize(
    ("tasks", "message"),
    [
        ((RunSql("a", "SELECT 1"), RunSql("a", "SELECT 2")), "duplicate task names: a"),
        ((RunSql("a", "SELECT 1", after=("ghost",)),), "runs after 'ghost', which does not exist"),
        ((RunSql("a", "SELECT 1", after=("b",)), RunSql("b", "SELECT 1", after=("a",))), "in a cycle: a, b"),
    ],
)
def test_a_badly_described_pipeline_is_rejected_when_it_is_loaded(tasks, message):
    with pytest.raises(PipelineError, match=message):
        Pipeline("p", "d", tasks).ordered()


def test_every_pipeline_in_the_repository_loads_and_has_a_dag_file():
    pipelines = load_pipelines()
    assert set(pipelines) == {"customers_history", "loans_daily", "payments_incremental"}
    for name in pipelines:
        dag = (ROOT / "dags" / f"{name}.py").read_text()
        assert f"from pipelines.{name} import PIPELINE" in dag and "build_dag(PIPELINE)" in dag


# ── the rules of the architecture ──────────────────────────────────────────────


def imports_of(path: Path) -> set[str]:
    found = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            found |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module.split(".")[0])
    return found


def test_only_the_adapter_and_the_dag_files_import_airflow():
    offenders = [
        str(path.relative_to(ROOT))
        for folder in ("src", "pipelines")
        for path in (ROOT / folder).rglob("*.py")
        if "airflow" in imports_of(path) and path.name != "airflow_adapter.py"
    ]
    assert offenders == []


def test_only_the_adapters_import_a_database_driver_or_a_cloud_library():
    allowed = {"duckdb.py", "postgres.py", "stores.py"}
    drivers = {"duckdb", "psycopg", "psycopg2", "redshift_connector", "boto3", "fsspec", "s3fs", "gcsfs"}
    offenders = [
        f"{path.relative_to(ROOT)}: {sorted(imports_of(path) & drivers)}"
        for folder in ("src", "pipelines")
        for path in (ROOT / folder).rglob("*.py")
        if imports_of(path) & drivers and path.name not in allowed
    ]
    assert offenders == []


# ── the self-test and the scaffold ─────────────────────────────────────────────


def test_selftest_passes_on_a_working_environment_and_cleans_up(platform):
    outcomes = selftest.run(platform.settings, platform.connections)
    assert [o.name for o in outcomes if not o.passed] == []
    assert {o.name for o in outcomes} >= {
        "connection: warehouse",
        "store: landing",
        "warehouse: write path",
        "sql templates",
    }
    schemas = platform.rows("SELECT schema_name FROM information_schema.schemata")
    assert not any(row["schema_name"].startswith("selftest_") for row in schemas)
    assert platform.rows("SELECT * FROM platform.watermarks") == []


def test_selftest_reports_every_failure_and_carries_on(tmp_path, platform):
    from dataplatform.config import Settings

    broken = Settings("test", {**platform.settings.connections, "source": "postgresql://nobody@127.0.0.1:1/none"}, {})
    outcomes = {o.name: o for o in selftest.run(broken, Connections(broken))}
    assert outcomes["connection: source"].passed is False
    assert outcomes["connection: warehouse"].passed and outcomes["warehouse: write path"].passed


def test_a_new_pipeline_is_created_whole_and_refuses_bad_names(tmp_path):
    created = new_pipeline("vendor_invoices", tmp_path)
    assert sorted(str(p.relative_to(tmp_path)) for p in created) == [
        "dags/vendor_invoices.py",
        "pipelines/vendor_invoices/__init__.py",
        "pipelines/vendor_invoices/sql/create_tables.sql",
        "pipelines/vendor_invoices/sql/transform.sql",
        "tests/test_vendor_invoices.py",
    ]
    definition = (tmp_path / "pipelines/vendor_invoices/__init__.py").read_text()
    assert (
        'name="vendor_invoices"' in definition and 'path="vendor_invoices/vendor_invoices_{{ ds }}.csv"' in definition
    )
    compile(definition, "pipeline", "exec")
    compile((tmp_path / "tests/test_vendor_invoices.py").read_text(), "test", "exec")
    with pytest.raises(FileExistsError):
        new_pipeline("vendor_invoices", tmp_path)
    with pytest.raises(ValueError):
        new_pipeline("Vendor-Invoices", tmp_path)


def test_the_command_finds_the_pipelines_from_any_folder(tmp_path):
    # No PYTHONPATH and a different working directory: what a cron job or a container gives you.
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    done = subprocess.run(
        [sys.executable, "-m", "dataplatform.cli", "list"], cwd=tmp_path, env=env, capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    assert "loans_daily" in done.stdout


def test_a_relative_local_path_is_inside_the_project_not_the_working_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = load_settings("local", environ={})
    store = Connections(settings).store("landing")
    assert store.root == ROOT / ".local" / "landing"
