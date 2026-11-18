"""A pipeline, described without reference to any scheduler.

A pipeline is a name, a schedule and a set of tasks with dependencies between them. The
same description is run in-process by ``dataplatform.runner`` (on a laptop, in tests, in
the self-test) and turned into an Airflow DAG by ``dataplatform.airflow_adapter``. Tasks
never import Airflow, so they can be tested without it.
"""

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from dataplatform.config import Settings
from dataplatform.connections import Connections
from dataplatform.ports import ObjectStore, Warehouse


class PipelineError(Exception):
    """The pipeline is described wrongly: a duplicate name, a missing dependency, a cycle."""


@dataclass
class TaskResult:
    rows: int = 0
    detail: str = ""


@dataclass
class RunContext:
    """Everything a task is given. It asks this for what it needs and imports nothing else."""

    pipeline: "Pipeline"
    logical_date: date  # the day of data this run is for
    settings: Settings
    connections: Connections
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    def warehouse(self, name: str = "warehouse") -> Warehouse:
        return self.connections.warehouse(name)

    def store(self, name: str = "landing") -> ObjectStore:
        return self.connections.store(name)

    @property
    def interval_start(self) -> datetime:
        return datetime.combine(self.logical_date, time.min)

    @property
    def interval_end(self) -> datetime:
        """The first moment after the logical date. Data up to here belongs to this run."""
        return self.interval_start + timedelta(days=1)


@runtime_checkable
class Task(Protocol):
    name: str
    after: tuple[str, ...]

    def run(self, ctx: RunContext) -> TaskResult: ...


@dataclass(frozen=True)
class Pipeline:
    name: str
    description: str
    tasks: tuple[Task, ...]
    schedule: str | None = "@daily"
    start_date: date = date(2026, 1, 1)
    owner: str = "data-platform"
    tags: tuple[str, ...] = ()
    upstream: tuple[str, ...] = ()  # pipelines whose data this one reads
    sql_dir: Path | None = None  # where this pipeline's .sql files live
    retries: int = 2

    def task(self, name: str) -> Task:
        for task in self.tasks:
            if task.name == name:
                return task
        raise PipelineError(f"{self.name} has no task named {name!r}")

    def ordered(self) -> list[Task]:
        """The tasks in an order that respects their dependencies. Validates the graph."""
        names = [task.name for task in self.tasks]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise PipelineError(f"{self.name}: duplicate task names: {', '.join(duplicates)}")
        for task in self.tasks:
            missing = [name for name in task.after if name not in names]
            if missing:
                raise PipelineError(f"{self.name}: task {task.name!r} runs after {missing[0]!r}, which does not exist")
        done: list[Task] = []
        waiting = list(self.tasks)
        while waiting:
            ready = [t for t in waiting if all(dep in {d.name for d in done} for dep in t.after)]
            if not ready:
                stuck = ", ".join(task.name for task in waiting)
                raise PipelineError(f"{self.name}: these tasks depend on each other in a cycle: {stuck}")
            done += ready
            waiting = [task for task in waiting if task not in ready]
        return done


def as_literal(value: Any) -> str:
    """A Python value written as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"
