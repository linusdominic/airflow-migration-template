"""Payments from the operational database into the warehouse, only what changed.

In Talend terms: tDBInput with a context variable holding the last run's timestamp ->
tDBOutput (insert or update). Here the timestamp is a watermark that moves only after the
load has been reconciled against the source, so a failed run is simply run again.
"""

from pathlib import Path

from dataplatform.pipeline import Pipeline
from dataplatform.tasks import (
    CopyQuery,
    NotNull,
    QualityChecks,
    Reconcile,
    RunSql,
    SetWatermark,
    Unique,
    Upsert,
)

COLUMNS = {
    "payment_id": "VARCHAR(40)",
    "loan_id": "VARCHAR(40)",
    "amount": "NUMERIC(18, 2)",
    "method": "VARCHAR(20)",
    "paid_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
}
# The window this run is responsible for: after the watermark, up to the end of the day.
WINDOW = "updated_at > CAST({{ watermark('payments', '1970-01-01 00:00:00') | lit }} AS TIMESTAMP) AND updated_at < {{ interval_end }}"

PIPELINE = Pipeline(
    name="payments_incremental",
    description="Copies new and changed payments from the source database and reconciles the totals.",
    schedule="30 6 * * *",
    tags=("lending", "incremental"),
    sql_dir=Path(__file__).parent / "sql",
    tasks=(
        RunSql(name="create_tables", sql="create_tables.sql"),
        CopyQuery(
            name="extract_changed",
            after=("create_tables",),
            sql=f"SELECT payment_id, loan_id, amount, method, paid_at, updated_at FROM payments WHERE {WINDOW}",
            table="{{ staging }}.payments_changed",
            columns=COLUMNS,
        ),
        QualityChecks(
            name="check_staged",
            after=("extract_changed",),
            checks=(
                NotNull("{{ staging }}.payments_changed", "payment_id"),
                Unique("{{ staging }}.payments_changed", ("payment_id",)),
            ),
        ),
        Upsert(
            name="upsert_core",
            after=("check_staged",),
            target="{{ core }}.payments",
            source="{{ staging }}.payments_changed",
            keys=("payment_id",),
            columns=tuple(COLUMNS),
        ),
        QualityChecks(
            name="reconcile",
            after=("upsert_core",),
            checks=(
                Reconcile(
                    "payments up to the end of the day",
                    left_sql="SELECT COUNT(*) FROM payments WHERE updated_at < {{ interval_end }}",
                    right_sql="SELECT COUNT(*) FROM {{ core }}.payments WHERE updated_at < {{ interval_end }}",
                ),
                Reconcile(
                    "amount paid up to the end of the day",
                    left_sql="SELECT COALESCE(SUM(amount), 0) FROM payments WHERE updated_at < {{ interval_end }}",
                    right_sql="SELECT COALESCE(SUM(amount), 0) FROM {{ core }}.payments WHERE updated_at < {{ interval_end }}",
                    tolerance=0.005,
                ),
            ),
        ),
        SetWatermark(
            name="advance_watermark",
            after=("reconcile",),
            watermark="payments",
            sql="SELECT MAX(updated_at) FROM {{ staging }}.payments_changed",
        ),
    ),
)
