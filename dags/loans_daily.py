"""Airflow DAG for the loans_daily pipeline. The pipeline itself is in pipelines/loans_daily."""

from dataplatform.airflow_adapter import build_dag
from pipelines.loans_daily import PIPELINE

dag = build_dag(PIPELINE)
