"""Put data where it belongs, in a way that can be run twice."""

from dataclasses import dataclass

from dataplatform import sql as sqlfile
from dataplatform.pipeline import RunContext, TaskResult
from dataplatform.state import set_watermark


@dataclass(frozen=True)
class Upsert:
    """Insert new rows and replace changed ones, matched on ``keys``.

    Done as delete-then-insert inside one transaction, which every warehouse here supports
    and which gives the same result however many times it runs. ``source`` is a table or a
    SELECT in brackets. If the source holds several rows for one key, the insert keeps them
    all, so deduplicate in the source.
    """

    name: str
    target: str
    source: str
    keys: tuple[str, ...]
    columns: tuple[str, ...]
    after: tuple[str, ...] = ()
    connection: str = "warehouse"

    def statements(self, ctx: RunContext) -> list[str]:
        target = sqlfile.render(self.target, ctx)
        source = sqlfile.render(self.source, ctx)
        match = " AND ".join(f"{target}.{key} = s.{key}" for key in self.keys)
        names = ", ".join(self.columns)
        return [
            f"DELETE FROM {target} USING {source} AS s WHERE {match}",
            f"INSERT INTO {target} ({names}) SELECT {names} FROM {source} AS s",
        ]

    def run(self, ctx: RunContext) -> TaskResult:
        warehouse = ctx.warehouse(self.connection)
        delete, insert = self.statements(ctx)
        with warehouse.transaction():
            replaced = max(warehouse.execute(delete), 0)
            written = max(warehouse.execute(insert), 0)
        return TaskResult(rows=written, detail=f"{replaced} replaced, {written - replaced} new")


@dataclass(frozen=True)
class SetWatermark:
    """Record how far an incremental load got. Put it last, so it only moves on success."""

    name: str
    watermark: str
    sql: str  # a query returning one value; NULL leaves the watermark where it was
    after: tuple[str, ...] = ()

    def run(self, ctx: RunContext) -> TaskResult:
        rows = ctx.warehouse().query(sqlfile.render(sqlfile.load(ctx, self.sql), ctx))
        value = next(iter(rows[0].values())) if rows else None
        if value is None:
            return TaskResult(detail="nothing new, watermark unchanged")
        set_watermark(ctx, self.watermark, value)
        return TaskResult(detail=f"{self.watermark} = {value}")
