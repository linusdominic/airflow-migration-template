CREATE SCHEMA IF NOT EXISTS {{ staging }};
CREATE SCHEMA IF NOT EXISTS {{ core }};

CREATE TABLE IF NOT EXISTS {{ core }}.payments (
    payment_id   VARCHAR(40),
    loan_id      VARCHAR(40),
    amount       NUMERIC(18, 2),
    method       VARCHAR(20),
    paid_at      TIMESTAMP,
    updated_at   TIMESTAMP
) {{ dialect.table_options(dist_key="loan_id", sort_key="paid_at") }};
