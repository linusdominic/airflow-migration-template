"""The DAGs, checked with Airflow itself. Skipped where Airflow is not installed.

uv pip install -e ".[airflow]" pytest && pytest -m airflow
"""

from datetime import date

import pytest

pytest.importorskip("airflow")
pytestmark = pytest.mark.airflow

from dataplatform import airflow_adapter  # noqa: E402
from dataplatform.config import ROOT  # noqa: E402
from dataplatform.pipeline import Pipeline  # noqa: E402
from dataplatform.registry import load_pipelines  # noqa: E402
from dataplatform.tasks import RunSql  # noqa: E402


@pytest.fixture(scope="module")
def dags():
    return {name: airflow_adapter.build_dag(pipeline) for name, pipeline in load_pipelines().items()}


def test_every_dag_file_imports_without_error():
    for path in sorted((ROOT / "dags").glob("*.py")):
        namespace: dict = {}
        exec(compile(path.read_text(), str(path), "exec"), namespace)
        assert namespace["dag"].dag_id == path.stem


def test_each_dag_mirrors_its_pipeline(dags):
    for name, pipeline in load_pipelines().items():
        dag = dags[name]
        assert sorted(dag.task_ids) == sorted([*(task.name for task in pipeline.tasks), airflow_adapter.DONE])
        for task in pipeline.tasks:
            upstream = {t.task_id for t in dag.get_task(task.name).upstream_list}
            assert upstream == set(task.after), f"{name}.{task.name}"


def test_dags_carry_production_defaults(dags):
    for dag in dags.values():
        assert dag.catchup is False and dag.max_active_runs == 1
        assert dag.default_args["retries"] == 2 and dag.default_args["owner"] == "data-platform"
        assert dag.tags and dag.description


def test_one_closing_task_announces_the_pipelines_output_after_every_final_task(dags):
    dag = dags["loans_daily"]
    assert [t.task_id for t in dag.tasks if t.outlets] == [airflow_adapter.DONE]
    done = dag.get_task(airflow_adapter.DONE)
    assert sorted(t.task_id for t in done.upstream_list) == ["build_mart", "refresh_current"]
    assert [str(o.uri).rstrip("/") for o in done.outlets] == ["pipeline://loans_daily"]


def test_a_pipeline_that_reads_another_is_scheduled_on_its_output():
    downstream = Pipeline(
        "summary", "reads loans", (RunSql("a", "SELECT 1"),), schedule="@daily", upstream=("loans_daily",)
    )
    dag = airflow_adapter.build_dag(downstream)
    assert "pipeline://loans_daily" in str(
        dag.timetable.asset_condition if hasattr(dag.timetable, "asset_condition") else dag.schedule
    )


def test_an_airflow_task_runs_the_same_code_as_the_command_line(platform, monkeypatch):
    """Call what an Airflow worker calls, with the context Airflow passes, for one whole pipeline."""
    from datetime import datetime

    from pipelines.customers_history import PIPELINE

    day = date(2026, 3, 2)
    platform.landing.write_text(
        f"customers/customers_{day}.csv", "customer_id,segment,state,credit_band\nC1,homeowner,TX,good\n"
    )
    monkeypatch.setattr(airflow_adapter, "load_settings", lambda: platform.settings)
    monkeypatch.setattr(
        airflow_adapter, "Connections", lambda settings, resolvers=None: _KeepOpen(platform.connections)
    )
    for task in PIPELINE.ordered():
        summary = airflow_adapter.execute(
            "customers_history", task.name, logical_date=datetime(2026, 3, 2), run_id="manual__2026-03-02"
        )
        assert "rows" in summary
    assert platform.rows("SELECT customer_id, valid_from, is_current FROM core.dim_customer") == [
        {"customer_id": "C1", "valid_from": day, "is_current": True}
    ]
    logged = platform.rows("SELECT DISTINCT run_id FROM platform.run_log")
    assert logged == [{"run_id": "manual__2026-03-02"}]


class _KeepOpen:
    """The test owns the connections, so the adapter's close() must not close them."""

    def __init__(self, connections):
        self._connections = connections

    def __getattr__(self, name):
        return getattr(self._connections, name)

    def close(self) -> None:
        pass
