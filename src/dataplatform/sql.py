"""SQL files with a few variables in them.

A transformation is a ``.sql`` file. It may use:

    {{ staging }} {{ core }} {{ marts }} {{ platform }}   the schema playing each role here
    {{ ds }}                                              the logical date, 2026-11-17
    {{ interval_start }} {{ interval_end }}               the day's bounds, as timestamps
    {{ value | lit }}                                     a Python value as a SQL literal
    {{ dialect.name }} {{ dialect.table_options(...) }}   for the rare warehouse-specific line
    {{ watermark("name") | lit }}                         where an incremental load got to

A file may hold several statements, each ending with a semicolon at the end of a line.
An undefined variable is an error, not an empty string.
"""

from pathlib import Path
from typing import Any

from jinja2 import Environment, StrictUndefined

from dataplatform.pipeline import RunContext, as_literal

_env = Environment(undefined=StrictUndefined, keep_trailing_newline=True, autoescape=False)
_env.filters["lit"] = as_literal


def variables(ctx: RunContext, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    from dataplatform.state import get_watermark

    schema = ctx.settings.schema
    return {
        "staging": schema("staging"),
        "core": schema("core"),
        "marts": schema("marts"),
        "platform": schema("platform"),
        "ds": ctx.logical_date.isoformat(),
        "ds_nodash": ctx.logical_date.strftime("%Y%m%d"),
        "logical_date": ctx.logical_date,
        "interval_start": as_literal(ctx.interval_start),
        "interval_end": as_literal(ctx.interval_end),
        "dialect": ctx.warehouse().dialect,
        "watermark": lambda name, default=None: get_watermark(ctx, name, default),
        **(extra or {}),
    }


def render(text: str, ctx: RunContext, extra: dict[str, Any] | None = None) -> str:
    return _env.from_string(text).render(**variables(ctx, extra))


def load(ctx: RunContext, sql: str) -> str:
    """``sql`` is either a file name in the pipeline's sql folder or the SQL itself."""
    if sql.endswith(".sql") and "\n" not in sql:
        folder = ctx.pipeline.sql_dir
        if folder is None:
            raise FileNotFoundError(f"{ctx.pipeline.name} has no sql_dir, so {sql} cannot be found")
        return (Path(folder) / sql).read_text()
    return sql


def statements(text: str) -> list[str]:
    """Split on semicolons that end a line. Comment-only pieces are dropped."""
    found, current = [], []
    for line in text.splitlines():
        current.append(line)
        if line.rstrip().endswith(";"):
            found.append("\n".join(current).rstrip().rstrip(";"))
            current = []
    found.append("\n".join(current))
    cleaned = []
    for statement in found:
        code = [ln for ln in statement.splitlines() if ln.strip() and not ln.strip().startswith("--")]
        if code:
            cleaned.append(statement.strip())
    return cleaned
