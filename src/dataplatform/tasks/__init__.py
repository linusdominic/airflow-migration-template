"""The building blocks pipelines are made of. Each is one small class with one job."""

from dataplatform.tasks.extract import CopyQuery, LoadCsv
from dataplatform.tasks.load import SetWatermark, Upsert
from dataplatform.tasks.quality import (
    AcceptedValues,
    NotNull,
    QualityChecks,
    QualityError,
    Reconcile,
    RowCount,
    Unique,
)
from dataplatform.tasks.scd import Scd2, ScdOrderError
from dataplatform.tasks.sql import RunSql

__all__ = [
    "AcceptedValues", "CopyQuery", "LoadCsv", "NotNull", "QualityChecks", "QualityError",
    "Reconcile", "RowCount", "RunSql", "Scd2", "ScdOrderError", "SetWatermark", "Unique", "Upsert",
]  # fmt: skip
