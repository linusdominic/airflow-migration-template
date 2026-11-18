"""The example pipelines, end to end, on DuckDB and (when a database is given) PostgreSQL.

These are the properties a migrated pipeline has to keep: it loads what the source has,
it can be run twice, it can be backfilled in any order, and it stops when the data is bad.
"""

from datetime import date, datetime

import pytest

from dataplatform.tasks import QualityError, ScdOrderError
from pipelines.customers_history import PIPELINE as CUSTOMERS
from pipelines.loans_daily import PIPELINE as LOANS
from pipelines.payments_incremental import PIPELINE as PAYMENTS

ALL = (LOANS, PAYMENTS, CUSTOMERS)
TABLES = ("core.loan_versions", "core.loans", "core.payments", "core.dim_customer", "marts.loan_portfolio_daily")


def snapshot(p) -> dict[str, list[tuple]]:
    """Every row of every table the pipelines own, in a stable order."""
    s = p.settings.schema
    names = {t: f"{s(t.split('.')[0])}.{t.split('.')[1]}" for t in TABLES}
    return {
        t: sorted(tuple(str(v) for v in r.values()) for r in p.rows(f"SELECT * FROM {n}")) for t, n in names.items()
    }


def run_all(p, days) -> None:
    for day in days:
        for pipeline in ALL:
            p.run(pipeline, day)


def check_five_days(p) -> None:
    days = p.seed()
    run_all(p, days)
    s = p.settings.schema
    source_count = p.value("SELECT COUNT(*) FROM payments", "source")
    source_total = p.value("SELECT SUM(amount) FROM payments", "source")
    assert p.value(f"SELECT COUNT(*) FROM {s('core')}.loans") == 92
    assert p.value(f"SELECT COUNT(*) FROM {s('core')}.loan_versions") == 116  # 92 loans and 24 later changes
    assert p.value(f"SELECT COUNT(*) FROM {s('core')}.payments") == source_count == 250
    assert p.value(f"SELECT SUM(amount) FROM {s('core')}.payments") == source_total
    assert p.value(f"SELECT COUNT(*) FROM {s('core')}.dim_customer WHERE is_current") == 40
    assert p.value(f"SELECT COUNT(*) FROM {s('core')}.dim_customer") == 52  # 40 customers and 12 changes
    per_day = p.rows(
        f"SELECT as_of_date, SUM(loans) AS loans FROM {s('marts')}.loan_portfolio_daily GROUP BY 1 ORDER BY 1"
    )
    assert [int(r["loans"]) for r in per_day] == [60, 68, 76, 84, 92]
    assert p.value(f"SELECT COUNT(*) FROM {s('platform')}.run_log WHERE status <> 'success'") == 0

    before = snapshot(p)
    run_all(p, [days[-1]])  # the same day again
    assert snapshot(p) == before
    p.run(LOANS, days[1])  # an old day after newer ones
    p.run(PAYMENTS, days[1])
    assert snapshot(p) == before


def test_five_days_on_duckdb(platform):
    check_five_days(platform)


@pytest.mark.postgres
def test_five_days_on_postgres(postgres_platform):
    check_five_days(postgres_platform)


def test_loans_are_cleaned_and_the_latest_row_in_a_file_wins(platform):
    day = date(2026, 3, 2)
    platform.landing.write_text(
        f"loans/loans_{day}.csv",
        "loan_id,customer_id,product,principal,rate,status,state,originated_at,updated_at\n"
        " L1 ,C1,  PURCHASE ,250000.00,0.0650,Active,tx,2026-03-02,2026-03-02 08:00:00\n"
        "L1,C1,purchase,250000.00,0.0650,DELINQUENT,TX,2026-03-02,2026-03-02 17:00:00\n"
        "L2,C2,refinance,180000,0.0575,active,oh,2026-03-02,2026-03-02 09:00:00\n",
    )
    platform.run(LOANS, day)
    assert platform.rows("SELECT loan_id, product, status, state, principal FROM core.loans ORDER BY loan_id") == [
        {"loan_id": "L1", "product": "purchase", "status": "delinquent", "state": "TX", "principal": 250000},
        {"loan_id": "L2", "product": "refinance", "status": "active", "state": "OH", "principal": 180000},
    ]


def test_bad_data_stops_the_pipeline_before_anything_is_published(platform):
    day = date(2026, 3, 2)
    header = "loan_id,customer_id,product,principal,rate,status,state,originated_at,updated_at\n"
    platform.landing.write_text(
        f"loans/loans_{day}.csv",
        header + "L1,C1,purchase,250000,0.065,active,TX,2026-03-02,2026-03-02 08:00:00\n"
        ",C2,refinance,,0.0575,on_fire,OH,2026-03-02,2026-03-02 09:00:00\n",
    )
    with pytest.raises(QualityError) as failure:
        platform.run(LOANS, day)
    message = str(failure.value)
    assert "loan_id is NULL in 1 rows" in message and "principal is NULL" in message and "on_fire" not in message
    assert "outside" in message  # every failed check is reported, not just the first
    assert platform.value("SELECT COUNT(*) FROM core.loans") == 0
    log = platform.rows("SELECT task, status FROM platform.run_log WHERE status = 'failed'")
    assert log == [{"task": "check_staged", "status": "failed"}]


def test_a_missing_file_is_an_error(platform):
    with pytest.raises(FileNotFoundError, match="loans/loans_2026-03-02.csv"):
        platform.run(LOANS, date(2026, 3, 2))


def test_incremental_load_picks_up_corrections_and_new_rows(platform):
    days = platform.seed(2)
    platform.run(PAYMENTS, days[0])
    loaded = platform.value("SELECT COUNT(*) FROM core.payments")
    source = platform.connections.warehouse("source")
    known = platform.rows("SELECT payment_id, amount FROM core.payments ORDER BY payment_id LIMIT 1")[0]
    later = datetime(2026, 11, 11, 9, 0)
    source.execute(
        f"UPDATE payments SET amount = amount + 100, updated_at = TIMESTAMP '{later}' "
        f"WHERE payment_id = '{known['payment_id']}'"
    )
    results = platform.run(PAYMENTS, days[1])
    assert "1 replaced" in results["upsert_core"].detail
    corrected = platform.value(f"SELECT amount FROM core.payments WHERE payment_id = '{known['payment_id']}'")
    assert corrected == known["amount"] + 100
    assert platform.value("SELECT COUNT(*) FROM core.payments") > loaded
    assert platform.value("SELECT COUNT(*) FROM core.payments") == platform.value(
        "SELECT COUNT(*) FROM payments", "source"
    )


def test_watermark_does_not_move_when_reconciliation_fails(platform):
    days = platform.seed(2)
    platform.run(PAYMENTS, days[0])
    before = platform.rows("SELECT value FROM platform.watermarks")
    # Something else deletes a row from the warehouse: the totals no longer agree.
    platform.connections.warehouse().execute(
        "DELETE FROM core.payments WHERE payment_id = (SELECT MIN(payment_id) FROM core.payments)"
    )
    with pytest.raises(QualityError, match="payments up to the end of the day: source has"):
        platform.run(PAYMENTS, days[1])
    assert platform.rows("SELECT value FROM platform.watermarks") == before


def test_customer_history_records_what_changed_and_when(platform):
    first, second = date(2026, 3, 2), date(2026, 3, 3)
    header = "customer_id,segment,state,credit_band\n"
    platform.landing.write_text(
        f"customers/customers_{first}.csv", header + "C1,homeowner,TX,good\nC2,investor,OH,fair\n"
    )
    platform.landing.write_text(
        f"customers/customers_{second}.csv",
        header + "C1,homeowner,TX,excellent\nC2,investor,OH,fair\nC3,homeowner,AZ,good\n",
    )
    platform.run(CUSTOMERS, first)
    platform.run(CUSTOMERS, second)
    platform.run(CUSTOMERS, second)  # again: nothing changes
    history = platform.rows(
        "SELECT customer_id, credit_band, valid_from, valid_to, is_current FROM core.dim_customer ORDER BY customer_id, valid_from"
    )
    assert history == [
        {"customer_id": "C1", "credit_band": "good", "valid_from": first, "valid_to": second, "is_current": False},
        {"customer_id": "C1", "credit_band": "excellent", "valid_from": second, "valid_to": None, "is_current": True},
        {"customer_id": "C2", "credit_band": "fair", "valid_from": first, "valid_to": None, "is_current": True},
        {"customer_id": "C3", "credit_band": "good", "valid_from": second, "valid_to": None, "is_current": True},
    ]


def test_an_old_snapshot_cannot_rewrite_newer_history(platform):
    days = platform.seed(3)
    for day in days:
        platform.run(CUSTOMERS, day)
    with pytest.raises(ScdOrderError, match="already has history up to 2026-11-12"):
        platform.run(CUSTOMERS, days[0])
