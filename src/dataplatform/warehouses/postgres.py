"""PostgreSQL, and Redshift, which speaks the same protocol with a narrower SQL."""

from collections.abc import Sequence
from typing import Any

from dataplatform.warehouses.base import GenericDialect, SqlWarehouse


class PostgresDialect(GenericDialect):
    name = "postgres"

    def placeholder(self) -> str:
        return "%s"


class RedshiftDialect(PostgresDialect):
    name = "redshift"

    def table_options(self, *, dist_key: str | None = None, sort_key: str | None = None) -> str:
        parts = []
        if dist_key:
            parts.append(f"DISTKEY ({dist_key})")
        if sort_key:
            parts.append(f"SORTKEY ({sort_key})")
        return " ".join(parts)


class PostgresWarehouse(SqlWarehouse):
    dialect = PostgresDialect()

    def __init__(self, uri: str) -> None:
        try:
            import psycopg
        except ImportError:
            raise ImportError("PostgreSQL and Redshift need the driver: install dataplatform[postgres]") from None
        super().__init__(psycopg.connect(self._native(uri), **self._options()))

    @staticmethod
    def _native(uri: str) -> str:
        return uri

    @staticmethod
    def _options() -> dict[str, Any]:
        return {}


class RedshiftWarehouse(PostgresWarehouse):
    """Redshift through the PostgreSQL driver.

    Not run against a live cluster. What differs from PostgreSQL is here and in the dialect:
    the URI scheme, no server-side prepared statements, and bulk loads, which on Redshift
    should go through COPY from the object store, never row by row.
    """

    dialect = RedshiftDialect()
    batch_size = 500

    @staticmethod
    def _native(uri: str) -> str:
        return "postgresql://" + uri.split("://", 1)[1]

    @staticmethod
    def _options() -> dict[str, Any]:
        return {"prepare_threshold": None}

    def copy_from_store(
        self,
        table: str,
        columns: Sequence[str],
        url: str,
        iam_role: str,
        options: str = "CSV IGNOREHEADER 1",
    ) -> int:
        """Bulk load a file that is already in S3. The way to load anything large."""
        return self.execute(f"COPY {table} ({', '.join(columns)}) FROM '{url}' IAM_ROLE '{iam_role}' {options}")
