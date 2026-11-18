"""DuckDB: the warehouse on a laptop and in the tests. One file, no server."""

from pathlib import Path

import duckdb

from dataplatform.warehouses.base import GenericDialect, SqlWarehouse

_WRITES = {"INSERT", "UPDATE", "DELETE", "MERGE"}


class DuckDBDialect(GenericDialect):
    name = "duckdb"


class DuckDBWarehouse(SqlWarehouse):
    dialect = DuckDBDialect()

    def __init__(self, path: str = ":memory:") -> None:
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(path)
        # DuckDB commits each statement unless a transaction is open, so open one and let
        # the base class commit it, the way the other drivers behave.
        connection.begin()
        super().__init__(_Explicit(connection))


class _Explicit:
    """Makes DuckDB's connection behave like a DB-API one that is always in a transaction."""

    def __init__(self, connection: "duckdb.DuckDBPyConnection") -> None:
        self._connection = connection

    def cursor(self) -> "_Cursor":
        return _Cursor(self._connection)

    def commit(self) -> None:
        self._connection.commit()
        self._connection.begin()

    def rollback(self) -> None:
        self._connection.rollback()
        self._connection.begin()

    def close(self) -> None:
        self._connection.close()


class _Cursor:
    """Statements run on the connection itself, so they share its open transaction."""

    def __init__(self, connection: "duckdb.DuckDBPyConnection") -> None:
        self._connection = connection
        self._result = None
        self.rowcount = -1

    def execute(self, sql: str) -> None:
        self._result = self._connection.execute(sql)
        self.rowcount = -1
        words = sql.split(None, 1)
        if words and words[0].upper() in _WRITES and self._result.description:
            counted = self._result.fetchall()  # a write reports its row count as one row
            self.rowcount = counted[0][0] if counted else -1
            self._result = None

    def executemany(self, sql: str, rows) -> None:
        self._connection.executemany(sql, rows)

    @property
    def description(self):
        return self._result.description

    def fetchall(self):
        return self._result.fetchall()

    def close(self) -> None:
        self._result = None
