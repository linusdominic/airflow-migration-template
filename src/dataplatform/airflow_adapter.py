"""Turn a pipeline into an Airflow DAG. The only module that imports Airflow.

Each task of the pipeline becomes one Airflow task that calls ``runner.run_task``, the
same function the command line and the tests call. Airflow contributes what it is good
at: the schedule, retries, the logical date, the UI and alerting. It does not contain any
of the pipeline's logic, so a pipeline can be tested without Airflow installed, and moving
to another scheduler would mean rewriting this one file.

Written for Airflow 3. It falls back to the Airflow 2 import paths if those are what is
installed.
"""

import os
from datetime import datetime, timedelta
from typing import Any

try:  # Airflow 3
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG, Asset
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.datasets import Dataset as Asset
    from airflow.operators.python import PythonOperator

from dataplatform.config import load_settings, project_on_path
from dataplatform.connections import Connections
from dataplatform.pipeline import Pipeline

DONE = "pipeline_done"

project_on_path()  # a DAG file imports its pipeline right after importing this module


def _done() -> None:
    """Nothing to do: reaching this task is the signal."""


def asset_for(pipeline_name: str) -> "Asset":
    """What a pipeline produces, as Airflow sees it. Downstream pipelines wait on this."""
    return Asset(f"pipeline://{pipeline_name}")


def _airflow_connection(name: str) -> str | None:
    """Look a connection up in Airflow, if this deployment keeps them there."""
    if os.environ.get("PLATFORM_AIRFLOW_CONNECTIONS") != "1":
        return None
    try:
        try:
            from airflow.sdk import Connection
        except ImportError:
            from airflow.models import Connection
        return Connection.get(name).get_uri() if hasattr(Connection, "get") else None
    except Exception:
        return None  # not defined in Airflow: fall back to the config file


def execute(pipeline_name: str, task_name: str, **context: Any) -> str:
    """What every Airflow task runs. Returns a one-line summary, which Airflow keeps as XCom."""
    from dataplatform.registry import get_pipeline
    from dataplatform.runner import context as run_context
    from dataplatform.runner import run_task

    moment = context.get("logical_date") or context.get("data_interval_start") or datetime.now()
    settings = load_settings()
    connections = Connections(settings, resolvers=[_airflow_connection])
    ctx = run_context(
        get_pipeline(pipeline_name),
        moment.date(),
        settings,
        connections,
        run_id=str(context.get("run_id", ""))[-32:] or None,
    )
    try:
        result = run_task(ctx, task_name)
    finally:
        connections.close()
    return f"{result.rows} rows. {result.detail}"


def build_dag(pipeline: Pipeline) -> "DAG":
    ordered = pipeline.ordered()  # validates the graph when the DAG file is parsed
    upstream = [asset_for(name) for name in pipeline.upstream]
    dag = DAG(
        dag_id=pipeline.name,
        description=pipeline.description,
        # A pipeline that reads another's output runs when that output is updated.
        schedule=upstream or pipeline.schedule,
        start_date=datetime.combine(pipeline.start_date, datetime.min.time()),
        catchup=False,
        max_active_runs=1,  # runs of one pipeline never overlap, so loads never interleave
        tags=list(pipeline.tags),
        default_args={
            "owner": pipeline.owner,
            "retries": pipeline.retries,
            "retry_delay": timedelta(minutes=5),
        },
    )
    operators = {}
    for task in ordered:
        operators[task.name] = PythonOperator(
            task_id=task.name,
            python_callable=execute,
            op_kwargs={"pipeline_name": pipeline.name, "task_name": task.name},
            dag=dag,
        )
    for task in ordered:
        for name in task.after:
            operators[name] >> operators[task.name]
    # One closing task announces the pipeline's output, once, after everything has finished.
    # A pipeline with several final tasks would otherwise announce it several times.
    depended_on = {name for task in ordered for name in task.after}
    done = PythonOperator(task_id=DONE, python_callable=_done, outlets=[asset_for(pipeline.name)], dag=dag)
    for task in ordered:
        if task.name not in depended_on:
            operators[task.name] >> done
    return dag
