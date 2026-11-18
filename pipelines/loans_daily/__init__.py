"""Daily loan file from the landing zone into the core table, then a portfolio mart.

In Talend terms: tFileInputDelimited -> tMap -> tDBOutput (insert or update), followed by
a second subjob that aggregates. Here each of those is one task, and the tMap is a SQL file.

One thing is done differently from the usual insert-or-update, on purpose. Updating a
"current" table in place is only right if files always arrive in order. So every version
of a loan is kept, and the current table and the daily mart are derived from the versions.
Any date can then be re-run or backfilled, in any order, and give the same result.
"""

from pathlib import Path

from dataplatform.pipeline import Pipeline
from dataplatform.tasks import (
    AcceptedValues,
    LoadCsv,
    NotNull,
    QualityChecks,
    RowCount,
    RunSql,
    Unique,
    Upsert,
)

COLUMNS = (
    "loan_id",
    "customer_id",
    "product",
    "principal",
    "rate",
    "status",
    "state",
    "originated_at",
    "updated_at",
)

PIPELINE = Pipeline(
    name="loans_daily",
    description="Loads the day's loan file, keeps every version of each loan, and rebuilds the day's portfolio mart.",
    schedule="0 6 * * *",
    tags=("lending", "file"),
    sql_dir=Path(__file__).parent / "sql",
    tasks=(
        RunSql(name="create_tables", sql="create_tables.sql"),
        LoadCsv(
            name="load_file",
            path="loans/loans_{{ ds }}.csv",
            table="{{ staging }}.loans_raw",
            columns=COLUMNS,
        ),
        RunSql(name="clean", sql="clean.sql", after=("create_tables", "load_file")),
        QualityChecks(
            name="check_staged",
            after=("clean",),
            checks=(
                RowCount("{{ staging }}.loans_clean"),
                NotNull("{{ staging }}.loans_clean", "loan_id"),
                NotNull("{{ staging }}.loans_clean", "principal"),
                Unique("{{ staging }}.loans_clean", ("loan_id",)),
                AcceptedValues(
                    "{{ staging }}.loans_clean", "status", ("active", "paid_off", "delinquent", "charged_off")
                ),
            ),
        ),
        # Every version of every loan is kept, keyed by when it changed. Loading the same
        # file twice, or an old file after a new one, cannot damage anything.
        Upsert(
            name="upsert_versions",
            after=("check_staged",),
            target="{{ core }}.loan_versions",
            source="{{ staging }}.loans_clean",
            keys=("loan_id", "updated_at"),
            columns=COLUMNS,
        ),
        RunSql(name="refresh_current", sql="refresh_current.sql", after=("upsert_versions",)),
        RunSql(name="build_mart", sql="portfolio_daily.sql", after=("upsert_versions",)),
    ),
)
