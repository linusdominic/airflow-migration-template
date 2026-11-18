"""Run a pipeline, or one task of it, in this process.

``run_task`` is the single place a task is executed, whoever asks: the command line, the
tests, the self-test, or an Airflow worker. So what is tested locally is what runs in
production.
"""

from datetime import date, datetime

from dataplatform.config import Settings, load_settings
from dataplatform.connections import Connections
from dataplatform.pipeline import Pipeline, RunContext, TaskResult
from dataplatform.state import ensure_tables, log_task


def context(
    pipeline: Pipeline,
    logical_date: date,
    settings: Settings | None = None,
    connections: Connections | None = None,
    run_id: str | None = None,
) -> RunContext:
    settings = settings or load_settings()
    ctx = RunContext(pipeline, logical_date, settings, connections or Connections(settings))
    if run_id:
        ctx.run_id = run_id
    return ctx


def run_task(ctx: RunContext, name: str) -> TaskResult:
    """Run one task and write what happened to the run log, success or failure."""
    task = ctx.pipeline.task(name)
    ensure_tables(ctx)
    started = datetime.now()
    try:
        result = task.run(ctx)
    except Exception as error:
        log_task(ctx, name, "failed", 0, f"{type(error).__name__}: {error}", started)
        raise
    log_task(ctx, name, "success", result.rows, result.detail, started)
    return result


def run_pipeline(ctx: RunContext) -> dict[str, TaskResult]:
    """Run every task in dependency order. Stops at the first failure."""
    return {task.name: run_task(ctx, task.name) for task in ctx.pipeline.ordered()}
