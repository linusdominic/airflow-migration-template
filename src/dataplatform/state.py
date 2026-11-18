"""What the platform remembers between runs: a log of every task, and watermarks.

Both live in the warehouse, in the ``platform`` schema, so they move with the data and can
be queried like anything else.
"""

from datetime import datetime
from typing import Any

from dataplatform.pipeline import RunContext, as_literal

TABLES = """
CREATE SCHEMA IF NOT EXISTS {platform};
CREATE TABLE IF NOT EXISTS {platform}.run_log (
    run_id VARCHAR(32), pipeline VARCHAR(200), task VARCHAR(200), logical_date DATE,
    status VARCHAR(20), row_count BIGINT, detail VARCHAR(2000),
    started_at TIMESTAMP, finished_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS {platform}.watermarks (
    pipeline VARCHAR(200), name VARCHAR(200), value VARCHAR(100), updated_at TIMESTAMP
);
"""


def ensure_tables(ctx: RunContext) -> None:
    warehouse = ctx.warehouse()
    for statement in TABLES.format(platform=ctx.settings.schema("platform")).split(";"):
        if statement.strip():
            warehouse.execute(statement)


def log_task(ctx: RunContext, task: str, status: str, rows: int, detail: str, started: datetime) -> None:
    ctx.warehouse().insert_rows(
        f"{ctx.settings.schema('platform')}.run_log",
        [
            "run_id",
            "pipeline",
            "task",
            "logical_date",
            "status",
            "row_count",
            "detail",
            "started_at",
            "finished_at",
        ],
        [
            (
                ctx.run_id,
                ctx.pipeline.name,
                task,
                ctx.logical_date,
                status,
                rows,
                detail[:2000],
                started,
                datetime.now(),
            )
        ],
    )


def get_watermark(ctx: RunContext, name: str, default: Any = None) -> Any:
    table = f"{ctx.settings.schema('platform')}.watermarks"
    rows = ctx.warehouse().query(
        f"SELECT value FROM {table} WHERE pipeline = {as_literal(ctx.pipeline.name)} AND name = {as_literal(name)}"
    )
    return rows[0]["value"] if rows else default


def set_watermark(ctx: RunContext, name: str, value: Any) -> None:
    table = f"{ctx.settings.schema('platform')}.watermarks"
    where = f"pipeline = {as_literal(ctx.pipeline.name)} AND name = {as_literal(name)}"
    warehouse = ctx.warehouse()
    with warehouse.transaction():
        warehouse.execute(f"DELETE FROM {table} WHERE {where}")
        warehouse.insert_rows(
            table,
            ["pipeline", "name", "value", "updated_at"],
            [(ctx.pipeline.name, name, str(value), datetime.now())],
        )
