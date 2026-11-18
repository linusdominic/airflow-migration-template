"""Keep the history of a dimension (slowly changing dimension, type 2)."""

from dataclasses import dataclass

from dataplatform import sql as sqlfile
from dataplatform.pipeline import RunContext, TaskResult, as_literal


class ScdOrderError(Exception):
    """An older snapshot was applied to a dimension that already has newer history."""


@dataclass(frozen=True)
class Scd2:
    """Compare today's snapshot with the current rows of a dimension and keep the history.

    A row whose tracked columns changed is closed (``valid_to`` set to the logical date,
    ``is_current`` false) and a new current row is opened. A key that is new gets its first
    row. Running the same date again changes nothing, because the snapshot then matches the
    current rows. Running an earlier date after a later one is refused.

    The target needs: the key, the tracked columns, ``valid_from``, ``valid_to`` and
    ``is_current``.
    """

    name: str
    target: str
    source: str  # today's full snapshot: a table or a SELECT in brackets
    key: str
    tracked: tuple[str, ...]
    after: tuple[str, ...] = ()
    connection: str = "warehouse"

    def statements(self, ctx: RunContext) -> list[str]:
        target = sqlfile.render(self.target, ctx)
        source = sqlfile.render(self.source, ctx)
        day = as_literal(ctx.logical_date)
        # IS DISTINCT FROM treats NULL as a value, so a change to or from NULL is seen.
        changed = " OR ".join(f"{target}.{c} IS DISTINCT FROM s.{c}" for c in self.tracked)
        columns = ", ".join([self.key, *self.tracked])
        selected = ", ".join(f"s.{c}" for c in [self.key, *self.tracked])
        return [
            f"UPDATE {target} SET valid_to = {day}, is_current = FALSE FROM {source} AS s "
            f"WHERE {target}.{self.key} = s.{self.key} AND {target}.is_current AND ({changed})",
            f"INSERT INTO {target} ({columns}, valid_from, valid_to, is_current) "
            f"SELECT {selected}, {day}, NULL, TRUE FROM {source} AS s "
            f"LEFT JOIN {target} AS t ON t.{self.key} = s.{self.key} AND t.is_current "
            f"WHERE t.{self.key} IS NULL",
        ]

    def run(self, ctx: RunContext) -> TaskResult:
        warehouse = ctx.warehouse(self.connection)
        target = sqlfile.render(self.target, ctx)
        latest = warehouse.query(f"SELECT MAX(valid_from) AS latest FROM {target}")[0]["latest"]
        if latest is not None and latest > ctx.logical_date:
            # History is built forwards. Applying an older snapshot on top would rewrite it.
            raise ScdOrderError(
                f"{target} already has history up to {latest}. A snapshot for {ctx.logical_date} "
                "cannot be applied after it. To backfill, rebuild the dimension from its first date."
            )
        close, open_new = self.statements(ctx)
        with warehouse.transaction():
            closed = max(warehouse.execute(close), 0)
            opened = max(warehouse.execute(open_new), 0)
        return TaskResult(rows=opened, detail=f"{closed} versions closed, {opened} opened")
