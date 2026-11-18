"""Airflow DAG for the payments_incremental pipeline. The pipeline itself is in pipelines/payments_incremental."""

from dataplatform.airflow_adapter import build_dag
from pipelines.payments_incremental import PIPELINE

dag = build_dag(PIPELINE)
