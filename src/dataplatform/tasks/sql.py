"""Run a transformation written in SQL."""

from dataclasses import dataclass, field
from typing import Any

from dataplatform import sql as sqlfile
from dataplatform.pipeline import RunContext, TaskResult


@dataclass(frozen=True)
class RunSql:
    """Render a ``.sql`` file (or inline SQL) and run its statements in one transaction."""

    name: str
    sql: str
    after: tuple[str, ...] = ()
    connection: str = "warehouse"
    params: dict[str, Any] = field(default_factory=dict)

    def rendered(self, ctx: RunContext) -> list[str]:
        text = sqlfile.render(sqlfile.load(ctx, self.sql), ctx, {"params": self.params})
        return sqlfile.statements(text)

    def run(self, ctx: RunContext) -> TaskResult:
        warehouse = ctx.warehouse(self.connection)
        affected = 0
        todo = self.rendered(ctx)
        with warehouse.transaction():
            for statement in todo:
                affected += max(warehouse.execute(statement), 0)
        return TaskResult(rows=affected, detail=f"{len(todo)} statements")
