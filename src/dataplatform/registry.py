"""Find the pipelines: every package under ``pipelines/`` that defines ``PIPELINE``."""

import importlib
import pkgutil

from dataplatform.config import project_on_path
from dataplatform.pipeline import Pipeline, PipelineError


def load_pipelines(package: str = "pipelines") -> dict[str, Pipeline]:
    project_on_path()
    root = importlib.import_module(package)
    found: dict[str, Pipeline] = {}
    for module in pkgutil.iter_modules(root.__path__):
        pipeline = getattr(importlib.import_module(f"{package}.{module.name}"), "PIPELINE", None)
        if isinstance(pipeline, Pipeline):
            if pipeline.name in found:
                raise PipelineError(f"two pipelines are named {pipeline.name!r}")
            pipeline.ordered()  # a broken graph fails here, at load, not at 3 a.m.
            found[pipeline.name] = pipeline
    for pipeline in found.values():
        for name in pipeline.upstream:
            if name not in found:
                raise PipelineError(f"{pipeline.name} reads from {name!r}, which is not a pipeline")
    return dict(sorted(found.items()))


def get_pipeline(name: str) -> Pipeline:
    pipelines = load_pipelines()
    if name not in pipelines:
        raise PipelineError(f"no pipeline named {name!r}. There are: {', '.join(pipelines)}")
    return pipelines[name]
