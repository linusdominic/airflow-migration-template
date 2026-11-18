"""Airflow DAG for the customers_history pipeline. The pipeline itself is in pipelines/customers_history."""

from dataplatform.airflow_adapter import build_dag
from pipelines.customers_history import PIPELINE

dag = build_dag(PIPELINE)
