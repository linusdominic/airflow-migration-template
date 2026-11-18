"""Turn a connection's name into something a task can use.

A URI's scheme decides which adapter is built. Tasks never see the URI.

    duckdb:///path/to/file.duckdb   duckdb://:memory:
    postgresql://user:pass@host/db
    redshift://user:pass@host:5439/db
    file:///some/folder             s3://bucket/prefix   gs://...   abfs://...
"""

from collections.abc import Callable
from pathlib import Path

from dataplatform.config import ROOT, ConfigError, Settings
from dataplatform.ports import ObjectStore, Warehouse
from dataplatform.stores import FsspecStore, LocalStore

# Something outside this module (Airflow, a secrets manager) may know a connection's URI.
Resolver = Callable[[str], str | None]


def _local(path: str) -> Path:
    """A relative path means inside the project, wherever the command was run from."""
    return Path(path) if Path(path).is_absolute() else ROOT / path


def _duckdb(uri: str) -> Warehouse:
    from dataplatform.warehouses.duckdb import DuckDBWarehouse

    path = uri.split("://", 1)[1]
    return DuckDBWarehouse(":memory:" if path in ("", ":memory:") else str(_local(path)))


def _postgres(uri: str) -> Warehouse:
    from dataplatform.warehouses.postgres import PostgresWarehouse

    return PostgresWarehouse(uri)


def _redshift(uri: str) -> Warehouse:
    from dataplatform.warehouses.postgres import RedshiftWarehouse

    return RedshiftWarehouse(uri)


WAREHOUSES: dict[str, Callable[[str], Warehouse]] = {
    "duckdb": _duckdb,
    "postgresql": _postgres,
    "postgres": _postgres,
    "redshift": _redshift,
}


class Connections:
    """Opens each named connection once and hands the same one out after that."""

    def __init__(self, settings: Settings, resolvers: list[Resolver] | None = None) -> None:
        self.settings = settings
        self._resolvers = resolvers or []
        self._open: dict[str, Warehouse] = {}

    def uri(self, name: str) -> str:
        for resolve in self._resolvers:
            found = resolve(name)
            if found:
                return found
        try:
            return self.settings.connections[name]
        except KeyError:
            known = ", ".join(sorted(self.settings.connections)) or "none"
            env = self.settings.env
            raise ConfigError(f"no connection named {name!r} in the {env} environment. Known: {known}") from None

    def warehouse(self, name: str = "warehouse") -> Warehouse:
        if name not in self._open:
            uri = self.uri(name)
            scheme = uri.split("://", 1)[0]
            if scheme not in WAREHOUSES:
                raise ConfigError(f"connection {name!r} is not a warehouse: scheme {scheme!r}")
            self._open[name] = WAREHOUSES[scheme](uri)
        return self._open[name]

    def store(self, name: str = "landing") -> ObjectStore:
        uri = self.uri(name)
        if uri.startswith("file://"):
            return LocalStore(_local(uri.removeprefix("file://")))
        return FsspecStore(uri)

    def close(self) -> None:
        for warehouse in self._open.values():
            warehouse.close()
        self._open.clear()
