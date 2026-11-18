"""Get data in: from a file in the landing zone, or from another database."""

import csv
import io
from dataclasses import dataclass

from dataplatform import sql as sqlfile
from dataplatform.pipeline import RunContext, TaskResult


def _replace_table(ctx: RunContext, connection: str, table: str, columns: dict[str, str]) -> None:
    """Staging tables are scratch: each run starts from an empty one."""
    warehouse = ctx.warehouse(connection)
    schema = table.split(".")[0]
    warehouse.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    definition = ", ".join(f"{name} {kind}" for name, kind in columns.items())
    warehouse.execute(f"CREATE TABLE IF NOT EXISTS {table} ({definition})")
    warehouse.execute(f"DELETE FROM {table}")


@dataclass(frozen=True)
class LoadCsv:
    """Load one CSV file from an object store into a staging table, as text.

    Everything lands as VARCHAR and is typed later, in SQL, where a bad value can be seen
    and handled. An empty field becomes NULL. Running it again replaces what it loaded.
    """

    name: str
    path: str  # may use {{ ds }}, for example loans/loans_{{ ds }}.csv
    table: str  # may use {{ staging }}
    columns: tuple[str, ...]
    after: tuple[str, ...] = ()
    store: str = "landing"
    connection: str = "warehouse"
    required: bool = True  # a missing file is an error unless this is False

    def run(self, ctx: RunContext) -> TaskResult:
        path = sqlfile.render(self.path, ctx)
        table = sqlfile.render(self.table, ctx)
        store = ctx.store(self.store)
        _replace_table(ctx, self.connection, table, dict.fromkeys(self.columns, "VARCHAR(4000)"))
        if not store.exists(path):
            if self.required:
                raise FileNotFoundError(f"{path} is not in the {self.store} store")
            return TaskResult(rows=0, detail=f"{path} not found, nothing loaded")
        reader = csv.DictReader(io.StringIO(store.read_text(path)))
        missing = [column for column in self.columns if column not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{path} has no column named {missing[0]!r}")
        rows = [[record[column] or None for column in self.columns] for record in reader]
        written = ctx.warehouse(self.connection).insert_rows(table, self.columns, rows)
        return TaskResult(rows=written, detail=path)


@dataclass(frozen=True)
class CopyQuery:
    """Run a query on one database and load its result into a staging table on another.

    The query may use ``{{ watermark("name") | lit }}`` and ``{{ interval_end }}`` to take
    only what changed since the last successful run.
    """

    name: str
    sql: str  # a file name or inline SQL, run on ``source``
    table: str
    columns: dict[str, str]  # column -> type, in the order the query returns them
    after: tuple[str, ...] = ()
    source: str = "source"
    connection: str = "warehouse"

    def run(self, ctx: RunContext) -> TaskResult:
        query = sqlfile.render(sqlfile.load(ctx, self.sql), ctx)
        table = sqlfile.render(self.table, ctx)
        records = ctx.warehouse(self.source).query(query)
        _replace_table(ctx, self.connection, table, self.columns)
        names = list(self.columns)
        rows = [[record[name] for name in names] for record in records]
        written = ctx.warehouse(self.connection).insert_rows(table, names, rows)
        return TaskResult(rows=written, detail=f"from {self.source}")
