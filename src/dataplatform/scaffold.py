"""Start a new pipeline from a skeleton: the definition, its SQL, its DAG and its test."""

import re
from pathlib import Path

PIPELINE = '''"""{title}.

Migrated from: <the legacy job this replaces>
Owner: <team>
"""

from pathlib import Path

from dataplatform.pipeline import Pipeline
from dataplatform.tasks import LoadCsv, NotNull, QualityChecks, RowCount, RunSql, Unique, Upsert

COLUMNS = ("id", "name", "updated_at")

PIPELINE = Pipeline(
    name="{name}",
    description="<one sentence: what this produces and for whom>",
    schedule="@daily",
    tags=("migrated",),
    sql_dir=Path(__file__).parent / "sql",
    tasks=(
        RunSql(name="create_tables", sql="create_tables.sql"),
        LoadCsv(
            name="load_file",
            path="{name}/{name}_{{{{ ds }}}}.csv",
            table="{{{{ staging }}}}.{name}_raw",
            columns=COLUMNS,
        ),
        RunSql(name="transform", sql="transform.sql", after=("create_tables", "load_file")),
        QualityChecks(
            name="check",
            after=("transform",),
            checks=(
                RowCount("{{{{ staging }}}}.{name}_clean"),
                NotNull("{{{{ staging }}}}.{name}_clean", "id"),
                Unique("{{{{ staging }}}}.{name}_clean", ("id",)),
            ),
        ),
        Upsert(
            name="upsert",
            after=("check",),
            target="{{{{ core }}}}.{name}",
            source="{{{{ staging }}}}.{name}_clean",
            keys=("id",),
            columns=COLUMNS,
        ),
    ),
)
'''

CREATE = """CREATE SCHEMA IF NOT EXISTS {{{{ staging }}}};
CREATE SCHEMA IF NOT EXISTS {{{{ core }}}};

CREATE TABLE IF NOT EXISTS {{{{ core }}}}.{name} (
    id          VARCHAR(40),
    name        VARCHAR(200),
    updated_at  TIMESTAMP
) {{{{ dialect.table_options(dist_key="id", sort_key="updated_at") }}}};
"""

TRANSFORM = """-- What the legacy job did between reading and writing goes here, as SQL.
DROP TABLE IF EXISTS {{{{ staging }}}}.{name}_clean;

CREATE TABLE {{{{ staging }}}}.{name}_clean AS
SELECT
    TRIM(id)                       AS id,
    TRIM(name)                     AS name,
    CAST(updated_at AS TIMESTAMP)  AS updated_at
FROM {{{{ staging }}}}.{name}_raw;
"""

DAG = '''"""Airflow DAG for the {name} pipeline. The pipeline itself is in pipelines/{name}."""

from dataplatform.airflow_adapter import build_dag
from pipelines.{name} import PIPELINE

dag = build_dag(PIPELINE)
'''

TEST = """from datetime import date

from pipelines.{name} import PIPELINE


def test_{name}_loads_and_can_be_run_twice(platform):
    day = date(2026, 1, 5)
    platform.landing.write_text(
        f"{name}/{name}_{{day}}.csv",
        "id,name,updated_at\\n1,first,2026-01-05 08:00:00\\n2,second,2026-01-05 09:00:00\\n",
    )
    platform.run(PIPELINE, day)
    platform.run(PIPELINE, day)  # a second run must change nothing
    assert platform.rows("SELECT id, name FROM core.{name} ORDER BY id") == [
        {{"id": "1", "name": "first"}},
        {{"id": "2", "name": "second"}},
    ]
"""


def new_pipeline(name: str, root: Path) -> list[Path]:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
        raise ValueError("a pipeline name is lower case letters, digits and underscores, starting with a letter")
    folder = root / "pipelines" / name
    if folder.exists():
        raise FileExistsError(f"pipelines/{name} already exists")
    files = {
        folder / "__init__.py": PIPELINE.format(name=name, title=name.replace("_", " ").capitalize()),
        folder / "sql" / "create_tables.sql": CREATE.format(name=name),
        folder / "sql" / "transform.sql": TRANSFORM.format(name=name),
        root / "dags" / f"{name}.py": DAG.format(name=name),
        root / "tests" / f"test_{name}.py": TEST.format(name=name),
    }
    for path, text in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return list(files)
