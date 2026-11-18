"""Sample data for the example pipelines: a few days of a small lender.

Generated from a seed, so every run and every test sees the same thing. It writes daily
files to the landing store (loans that are new or changed that day, and a full customer
snapshot) and fills a ``payments`` table in the source database.

The files are deliberately a little dirty, the way real ones are: stray spaces, mixed
case, and now and then the same loan twice in one file.
"""

import csv
import io
import random
from datetime import date, datetime, timedelta

from dataplatform.connections import Connections

START = date(2026, 11, 10)
PRODUCTS = ["purchase", "refinance", "cash_out"]
STATUSES = ["active", "active", "active", "delinquent", "paid_off", "charged_off"]
STATES = ["TX", "OH", "AZ", "GA", "CO", "FL"]
SEGMENTS = ["first_time_buyer", "homeowner", "investor"]
BANDS = ["excellent", "good", "fair", "poor"]
LOAN_COLUMNS = [
    "loan_id",
    "customer_id",
    "product",
    "principal",
    "rate",
    "status",
    "state",
    "originated_at",
    "updated_at",
]
PAYMENT_COLUMNS = ["payment_id", "loan_id", "amount", "method", "paid_at", "updated_at"]


def days(count: int, start: date = START) -> list[date]:
    return [start + timedelta(days=offset) for offset in range(count)]


def _csv(columns: list[str], rows: list[dict]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def generate(count: int = 5, seed: int = 7) -> dict:
    """Everything for ``count`` days: files by path, and the final state of ``payments``."""
    rng = random.Random(seed)
    customers = {
        f"C{n:03d}": {
            "segment": rng.choice(SEGMENTS),
            "state": rng.choice(STATES),
            "credit_band": rng.choice(BANDS),
        }
        for n in range(1, 41)
    }
    loans: dict[str, dict] = {}
    payments: dict[str, dict] = {}
    files: dict[str, str] = {}
    for index, day in enumerate(days(count)):
        stamp = datetime.combine(day, datetime.min.time())
        changed: list[dict] = []
        # New loans, then status changes on some older ones.
        for _ in range(60 if index == 0 else 8):
            loan_id = f"L{len(loans) + 1:05d}"
            loans[loan_id] = {
                "loan_id": loan_id,
                "customer_id": rng.choice(list(customers)),
                "product": rng.choice(PRODUCTS),
                "principal": f"{rng.randrange(80_000, 620_000, 500)}.00",
                "rate": f"{rng.uniform(0.045, 0.0825):.4f}",
                "status": "active",
                "state": rng.choice(STATES),
                "originated_at": day.isoformat(),
                "updated_at": (stamp + timedelta(hours=rng.randint(8, 17))).isoformat(sep=" "),
            }
            changed.append(dict(loans[loan_id]))
        if index:
            for loan_id in rng.sample(sorted(loans)[:60], 6):
                loans[loan_id]["status"] = rng.choice(STATUSES)
                loans[loan_id]["updated_at"] = (stamp + timedelta(hours=18, minutes=rng.randint(0, 59))).isoformat(
                    sep=" "
                )
                changed.append(dict(loans[loan_id]))
        # Dirt: stray spaces and case, and one loan listed twice with an older version first.
        changed[0] = {
            **changed[0],
            "product": f"  {changed[0]['product'].upper()} ",
            "state": changed[0]["state"].lower(),
        }
        older = {**changed[1], "status": "active", "updated_at": stamp.isoformat(sep=" ")}
        files[f"loans/loans_{day}.csv"] = _csv(LOAN_COLUMNS, [older, *changed])

        # A full customer snapshot. After the first day, three customers change.
        if index:
            for customer_id in rng.sample(sorted(customers), 3):
                field = rng.choice(["segment", "state", "credit_band"])
                options = {"segment": SEGMENTS, "state": STATES, "credit_band": BANDS}[field]
                customers[customer_id][field] = rng.choice([o for o in options if o != customers[customer_id][field]])
        files[f"customers/customers_{day}.csv"] = _csv(
            ["customer_id", "segment", "state", "credit_band"],
            [{"customer_id": cid, **attrs} for cid, attrs in sorted(customers.items())],
        )

        # Payments made today, and corrections today to three earlier ones.
        for _ in range(50):
            payment_id = f"P{len(payments) + 1:06d}"
            paid = stamp + timedelta(hours=rng.randint(7, 20), minutes=rng.randint(0, 59))
            payments[payment_id] = {
                "payment_id": payment_id,
                "loan_id": rng.choice(sorted(loans)),
                "amount": round(rng.uniform(400, 4200), 2),
                "method": rng.choice(["ach", "card", "check"]),
                "paid_at": paid,
                "updated_at": paid,
            }
        if index:
            for payment_id in rng.sample(sorted(payments)[: 50 * index], 3):
                payments[payment_id]["amount"] = round(payments[payment_id]["amount"] + rng.choice([-25, 10, 50]), 2)
                payments[payment_id]["updated_at"] = stamp + timedelta(hours=21, minutes=rng.randint(0, 59))
    return {"files": files, "payments": list(payments.values()), "days": days(count)}


def write(connections: Connections, count: int = 5, seed: int = 7) -> dict:
    """Write the sample files to ``landing`` and the payments to ``source``."""
    data = generate(count, seed)
    store = connections.store("landing")
    for path, text in data["files"].items():
        store.write_text(path, text)
    source = connections.warehouse("source")
    source.execute("DROP TABLE IF EXISTS payments")
    source.execute(
        "CREATE TABLE payments (payment_id VARCHAR(40), loan_id VARCHAR(40), amount NUMERIC(18, 2), "
        "method VARCHAR(20), paid_at TIMESTAMP, updated_at TIMESTAMP)"
    )
    source.insert_rows("payments", PAYMENT_COLUMNS, [[row[c] for c in PAYMENT_COLUMNS] for row in data["payments"]])
    return data
