"""A customer dimension that keeps its history, built from a daily snapshot file.

In Talend terms: tFileInputDelimited -> tDBSCD (type 2). Here the comparison is two SQL
statements in ``Scd2``, the same on every warehouse.
"""

from pathlib import Path

from dataplatform.pipeline import Pipeline
from dataplatform.tasks import LoadCsv, NotNull, QualityChecks, RowCount, RunSql, Scd2, Unique

TRACKED = ("segment", "state", "credit_band")

PIPELINE = Pipeline(
    name="customers_history",
    description="Tracks changes to each customer's segment, state and credit band over time.",
    schedule="0 5 * * *",
    tags=("lending", "dimension"),
    sql_dir=Path(__file__).parent / "sql",
    tasks=(
        RunSql(name="create_tables", sql="create_tables.sql"),
        LoadCsv(
            name="load_snapshot",
            path="customers/customers_{{ ds }}.csv",
            table="{{ staging }}.customers_raw",
            columns=("customer_id", *TRACKED),
        ),
        QualityChecks(
            name="check_snapshot",
            after=("load_snapshot",),
            checks=(
                RowCount("{{ staging }}.customers_raw"),
                NotNull("{{ staging }}.customers_raw", "customer_id"),
                Unique("{{ staging }}.customers_raw", ("customer_id",)),
            ),
        ),
        Scd2(
            name="track_changes",
            after=("create_tables", "check_snapshot"),
            target="{{ core }}.dim_customer",
            source="{{ staging }}.customers_raw",
            key="customer_id",
            tracked=TRACKED,
        ),
        QualityChecks(
            name="check_dimension",
            after=("track_changes",),
            checks=(
                # Exactly one current row per customer, whatever happened before.
                Unique("(SELECT customer_id FROM {{ core }}.dim_customer WHERE is_current) c", ("customer_id",)),
            ),
        ),
    ),
)
