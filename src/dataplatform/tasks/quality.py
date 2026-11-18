"""Checks that stop a pipeline before bad data is published."""

from dataclasses import dataclass
from typing import Any, Protocol

from dataplatform import sql as sqlfile
from dataplatform.pipeline import RunContext, TaskResult, as_literal


class QualityError(Exception):
    """One or more checks failed. The message lists every one of them."""


class Check(Protocol):
    def problem(self, ctx: RunContext) -> str | None:
        """What is wrong, or None if nothing is."""


def _count(ctx: RunContext, connection: str, sql: str) -> Any:
    rows = ctx.warehouse(connection).query(sqlfile.render(sql, ctx))
    return next(iter(rows[0].values()))


@dataclass(frozen=True)
class NotNull:
    table: str
    column: str
    connection: str = "warehouse"

    def problem(self, ctx: RunContext) -> str | None:
        bad = _count(ctx, self.connection, f"SELECT COUNT(*) FROM {self.table} WHERE {self.column} IS NULL")
        return f"{self.table}.{self.column} is NULL in {bad} rows" if bad else None


@dataclass(frozen=True)
class Unique:
    table: str
    columns: tuple[str, ...]
    connection: str = "warehouse"

    def problem(self, ctx: RunContext) -> str | None:
        keys = ", ".join(self.columns)
        bad = _count(
            ctx,
            self.connection,
            f"SELECT COUNT(*) FROM (SELECT {keys} FROM {self.table} GROUP BY {keys} HAVING COUNT(*) > 1) d",
        )
        return f"{self.table} has {bad} duplicated values of ({keys})" if bad else None


@dataclass(frozen=True)
class AcceptedValues:
    table: str
    column: str
    values: tuple[str, ...]
    connection: str = "warehouse"

    def problem(self, ctx: RunContext) -> str | None:
        allowed = ", ".join(as_literal(value) for value in self.values)
        bad = _count(
            ctx,
            self.connection,
            f"SELECT COUNT(*) FROM {self.table} WHERE {self.column} NOT IN ({allowed})",
        )
        return f"{self.table}.{self.column} has {bad} rows outside {list(self.values)}" if bad else None


@dataclass(frozen=True)
class RowCount:
    table: str
    at_least: int = 1
    where: str = "1 = 1"
    connection: str = "warehouse"

    def problem(self, ctx: RunContext) -> str | None:
        found = _count(ctx, self.connection, f"SELECT COUNT(*) FROM {self.table} WHERE {self.where}")
        if found >= self.at_least:
            return None
        return f"{self.table} has {found} rows where {self.where}, expected at least {self.at_least}"


@dataclass(frozen=True)
class Reconcile:
    """Two queries, usually one on the source and one on the warehouse, must agree."""

    label: str
    left_sql: str
    right_sql: str
    left: str = "source"
    right: str = "warehouse"
    tolerance: float = 0.0

    def problem(self, ctx: RunContext) -> str | None:
        left = _count(ctx, self.left, self.left_sql) or 0
        right = _count(ctx, self.right, self.right_sql) or 0
        if abs(float(left) - float(right)) <= self.tolerance:
            return None
        return f"{self.label}: {self.left} has {left}, {self.right} has {right}"


@dataclass(frozen=True)
class QualityChecks:
    """Run every check, then fail once with all the problems, not just the first."""

    name: str
    checks: tuple[Check, ...]
    after: tuple[str, ...] = ()

    def run(self, ctx: RunContext) -> TaskResult:
        problems = [found for check in self.checks if (found := check.problem(ctx))]
        if problems:
            raise QualityError("; ".join(problems))
        return TaskResult(detail=f"{len(self.checks)} checks passed")
