"""What the SQL warehouses share: running statements over a DB-API connection."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from dataplatform.ports import Row, chunks


class GenericDialect:
    """Portable SQL. The pipelines stick to the subset all three warehouses accept."""

    name = "generic"

    def placeholder(self) -> str:
        return "?"

    def table_options(self, *, dist_key: str | None = None, sort_key: str | None = None) -> str:
        return ""


class SqlWarehouse:
    """A warehouse reached through a DB-API 2.0 connection."""

    dialect: Any = GenericDialect()
    batch_size = 1000

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._depth = 0  # how many transaction() blocks we are inside

    def execute(self, sql: str) -> int:
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
            affected = cursor.rowcount if cursor.rowcount is not None else -1
        finally:
            cursor.close()
        self._commit()
        return affected

    def query(self, sql: str) -> list[Row]:
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
            names = [column[0] for column in cursor.description]
            rows = [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]
        finally:
            cursor.close()
        self._commit()
        return rows

    def insert_rows(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> int:
        if not rows:
            return 0
        marks = ", ".join([self.dialect.placeholder()] * len(columns))
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks})"
        cursor = self._connection.cursor()
        try:
            for batch in chunks(rows, self.batch_size):
                cursor.executemany(sql, [tuple(row) for row in batch])
        finally:
            cursor.close()
        self._commit()
        return len(rows)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._depth += 1
        try:
            yield
        except BaseException:
            self._depth -= 1
            if self._depth == 0:
                self._connection.rollback()
            raise
        self._depth -= 1
        self._commit()

    def _commit(self) -> None:
        if self._depth == 0:
            self._connection.commit()

    def close(self) -> None:
        self._connection.close()
