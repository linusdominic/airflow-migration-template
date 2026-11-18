"""The two things a pipeline depends on, as interfaces.

A task never imports a database driver or a cloud SDK. It asks for a ``Warehouse`` or an
``ObjectStore`` by name and gets whatever the environment is configured with: DuckDB and a
folder on a laptop, Redshift and S3 in production. Adding a warehouse means writing one
class that satisfies ``Warehouse``. Nothing else changes.
"""

from collections.abc import Iterator, Sequence
from contextlib import AbstractContextManager
from typing import Any, Protocol, runtime_checkable

Row = dict[str, Any]


@runtime_checkable
class Warehouse(Protocol):
    """A SQL database that pipelines read from and write to."""

    dialect: "Dialect"

    def execute(self, sql: str) -> int:
        """Run one statement. Returns the number of rows it affected, or -1 if unknown."""

    def query(self, sql: str) -> list[Row]:
        """Run a SELECT and return its rows."""

    def insert_rows(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> int:
        """Append rows to a table. Returns how many were written."""

    def transaction(self) -> AbstractContextManager[None]:
        """Everything inside either all happens or none of it does."""

    def close(self) -> None: ...


@runtime_checkable
class Dialect(Protocol):
    """Where the SQL of one warehouse differs from the others."""

    name: str

    def placeholder(self) -> str:
        """The parameter marker of the driver: ``?`` or ``%s``."""

    def table_options(self, *, dist_key: str | None = None, sort_key: str | None = None) -> str:
        """Extra clauses for CREATE TABLE. Empty everywhere except column stores."""


@runtime_checkable
class ObjectStore(Protocol):
    """Files: a folder locally, a bucket in the cloud."""

    def read_text(self, path: str) -> str: ...

    def write_text(self, path: str, text: str) -> None: ...

    def exists(self, path: str) -> bool: ...

    def list(self, prefix: str = "") -> list[str]: ...

    def delete(self, path: str) -> None: ...


def chunks(rows: Sequence[Sequence[Any]], size: int) -> Iterator[Sequence[Sequence[Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]
