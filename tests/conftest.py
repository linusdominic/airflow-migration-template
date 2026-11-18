"""A whole platform per test: a DuckDB warehouse and source in memory, files in a temp folder.

Set TEST_POSTGRES_URL to run the tests marked ``postgres`` against a real PostgreSQL.
"""

import os
import uuid
from dataclasses import dataclass
from datetime import date

import pytest

from dataplatform import sample
from dataplatform.config import Settings
from dataplatform.connections import Connections
from dataplatform.pipeline import Pipeline, TaskResult
from dataplatform.runner import context, run_pipeline
from dataplatform.stores import LocalStore

SCHEMAS = {"staging": "staging", "core": "core", "marts": "marts", "platform": "platform"}


@dataclass
class Platform:
    settings: Settings
    connections: Connections
    landing: LocalStore

    def run(self, pipeline: Pipeline, day: date) -> dict[str, TaskResult]:
        return run_pipeline(context(pipeline, day, self.settings, self.connections))

    def rows(self, sql: str, connection: str = "warehouse") -> list[dict]:
        return self.connections.warehouse(connection).query(sql)

    def value(self, sql: str, connection: str = "warehouse"):
        return next(iter(self.rows(sql, connection)[0].values()))

    def seed(self, days: int = 5) -> list[date]:
        return sample.write(self.connections, days)["days"]


def _platform(tmp_path, warehouse_uri: str, schemas: dict[str, str]) -> Platform:
    settings = Settings(
        env="test",
        connections={
            "warehouse": warehouse_uri,
            "source": f"duckdb://{tmp_path}/source.duckdb",
            "landing": f"file://{tmp_path}/landing",
        },
        schemas=schemas,
    )
    return Platform(settings, Connections(settings), LocalStore(tmp_path / "landing"))


@pytest.fixture
def platform(tmp_path):
    made = _platform(tmp_path, "duckdb://:memory:", SCHEMAS)
    yield made
    made.connections.close()


@pytest.fixture
def postgres_platform(tmp_path):
    """The same platform on PostgreSQL, in schemas of its own that are dropped afterwards."""
    uri = os.environ.get("TEST_POSTGRES_URL")
    if not uri:
        pytest.skip("TEST_POSTGRES_URL is not set")
    tag = uuid.uuid4().hex[:8]
    schemas = {role: f"t{tag}_{role}" for role in SCHEMAS}
    made = _platform(tmp_path, uri, schemas)
    yield made
    for schema in schemas.values():
        made.connections.warehouse().execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    made.connections.close()
