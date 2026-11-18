"""Check that this environment can actually run pipelines, before one is scheduled.

The same checks run anywhere: on a laptop against DuckDB and a folder, or on a server
against the configured warehouse and bucket. They use a scratch schema and a scratch
folder, and remove both afterwards. Nothing in the real schemas is touched except the
platform's own log and watermark tables, which are created if missing.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from dataplatform import sql as sqlfile
from dataplatform.config import Settings
from dataplatform.connections import WAREHOUSES, Connections
from dataplatform.pipeline import Pipeline, RunContext
from dataplatform.registry import load_pipelines
from dataplatform.state import ensure_tables, get_watermark, set_watermark
from dataplatform.tasks import NotNull, QualityChecks, RunSql, Scd2, Unique, Upsert


@dataclass
class Outcome:
    name: str
    passed: bool
    detail: str


def _scheme(uri: str) -> str:
    return uri.split("://", 1)[0]


def _attempt(name: str, check: Callable[[], str]) -> Outcome:
    try:
        return Outcome(name, True, check())
    except Exception as error:  # a self-test reports every failure, it does not stop at one
        return Outcome(name, False, f"{type(error).__name__}: {error}".splitlines()[0][:200])


def _warehouse_round_trip(ctx: RunContext, scratch: str) -> str:
    """Create, load, upsert, track history, check, roll back: what every pipeline relies on."""
    warehouse = ctx.warehouse()
    tasks = (
        RunSql(
            "setup",
            f"CREATE SCHEMA {scratch};\n"
            f"CREATE TABLE {scratch}.src (id VARCHAR(10), v VARCHAR(10));\n"
            f"CREATE TABLE {scratch}.tgt (id VARCHAR(10), v VARCHAR(10));\n"
            f"CREATE TABLE {scratch}.dim (id VARCHAR(10), v VARCHAR(10), valid_from DATE, "
            "valid_to DATE, is_current BOOLEAN);\n"
            f"INSERT INTO {scratch}.src VALUES ('a', '1');\n"
            f"INSERT INTO {scratch}.src VALUES ('b', '2');\n"
            f"INSERT INTO {scratch}.tgt VALUES ('a', 'old');\n",
        ),
        Upsert("upsert", f"{scratch}.tgt", f"{scratch}.src", ("id",), ("id", "v"), after=("setup",)),
        Scd2("history", f"{scratch}.dim", f"{scratch}.src", "id", ("v",), after=("setup",)),
        QualityChecks(
            "checks",
            (NotNull(f"{scratch}.tgt", "v"), Unique(f"{scratch}.tgt", ("id",))),
            after=("upsert", "history"),
        ),
    )
    probe = Pipeline("selftest", "scratch", tasks, schedule=None)
    probe_ctx = RunContext(probe, ctx.logical_date, ctx.settings, ctx.connections)
    try:
        for task in probe.ordered():
            task.run(probe_ctx)
        rows = warehouse.query(f"SELECT id, v FROM {scratch}.tgt ORDER BY id")
        if rows != [{"id": "a", "v": "1"}, {"id": "b", "v": "2"}]:
            raise AssertionError(f"upsert left {rows}")
        try:
            with warehouse.transaction():
                warehouse.execute(f"DELETE FROM {scratch}.tgt")
                raise RuntimeError("roll this back")
        except RuntimeError:
            pass
        if warehouse.query(f"SELECT COUNT(*) AS n FROM {scratch}.tgt")[0]["n"] != 2:
            raise AssertionError("a failed transaction was not rolled back")
    finally:
        warehouse.execute(f"DROP SCHEMA IF EXISTS {scratch} CASCADE")
    return f"create, load, upsert, history, checks and rollback on {warehouse.dialect.name}"


def run(settings: Settings, connections: Connections | None = None) -> list[Outcome]:
    connections = connections or Connections(settings)
    scratch = f"selftest_{uuid.uuid4().hex[:8]}"
    pipelines: dict[str, Pipeline] = {}
    outcomes = [Outcome("configuration", True, f"environment {settings.env}, {len(settings.connections)} connections")]

    def load() -> str:
        pipelines.update(load_pipelines())
        tasks = sum(len(p.tasks) for p in pipelines.values())
        return f"{len(pipelines)} pipelines, {tasks} tasks, no cycles or missing dependencies"

    outcomes.append(_attempt("pipeline definitions", load))

    for name, uri in settings.connections.items():
        if _scheme(uri) in WAREHOUSES:

            def reach(name: str = name) -> str:
                rows = connections.warehouse(name).query("SELECT 1 AS ok")
                return f"connected ({connections.warehouse(name).dialect.name}), SELECT 1 returned {rows[0]['ok']}"

            outcomes.append(_attempt(f"connection: {name}", reach))
        else:

            def files(name: str = name) -> str:
                store, path = connections.store(name), f"_selftest/{scratch}.txt"
                store.write_text(path, "ok")
                try:
                    if store.read_text(path) != "ok" or path not in store.list("_selftest/"):
                        raise AssertionError("a file written could not be read back or listed")
                finally:
                    store.delete(path)
                return "wrote, listed, read and deleted a file"

            outcomes.append(_attempt(f"store: {name}", files))

    if "warehouse" not in settings.connections:
        return [*outcomes, Outcome("warehouse", False, "no connection named 'warehouse'")]
    anchor = next(iter(pipelines.values()), Pipeline("selftest", "scratch", (), schedule=None))
    ctx = RunContext(anchor, date(2026, 1, 1), settings, connections)

    outcomes.append(_attempt("warehouse: write path", lambda: _warehouse_round_trip(ctx, scratch)))

    def state() -> str:
        ensure_tables(ctx)
        probe = RunContext(
            Pipeline(f"selftest_{scratch}", "scratch", (), schedule=None),
            ctx.logical_date,
            settings,
            connections,
        )
        set_watermark(probe, "probe", "2026-01-01 00:00:00")
        try:
            if get_watermark(probe, "probe") != "2026-01-01 00:00:00":
                raise AssertionError("a watermark written could not be read back")
        finally:
            table = f"{settings.schema('platform')}.watermarks"
            ctx.warehouse().execute(f"DELETE FROM {table} WHERE pipeline = 'selftest_{scratch}'")
        return "run log and watermark tables exist, watermark round trip"

    outcomes.append(_attempt("platform state", state))

    def templates() -> str:
        count = 0
        for pipeline in pipelines.values():
            for task in pipeline.tasks:
                if isinstance(task, RunSql):
                    task_ctx = RunContext(pipeline, ctx.logical_date, settings, connections)
                    count += len(
                        sqlfile.statements(
                            sqlfile.render(sqlfile.load(task_ctx, task.sql), task_ctx, {"params": task.params})
                        )
                    )
        return f"{count} SQL statements render for {ctx.warehouse().dialect.name} with no undefined variable"

    outcomes.append(_attempt("sql templates", templates))
    return outcomes
