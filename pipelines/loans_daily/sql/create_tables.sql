-- Idempotent: safe to run on every execution.
CREATE SCHEMA IF NOT EXISTS {{ staging }};
CREATE SCHEMA IF NOT EXISTS {{ core }};
CREATE SCHEMA IF NOT EXISTS {{ marts }};

CREATE TABLE IF NOT EXISTS {{ core }}.loan_versions (
    loan_id        VARCHAR(40),
    customer_id    VARCHAR(40),
    product        VARCHAR(40),
    principal      NUMERIC(18, 2),
    rate           NUMERIC(6, 4),
    status         VARCHAR(20),
    state          VARCHAR(2),
    originated_at  DATE,
    updated_at     TIMESTAMP
) {{ dialect.table_options(dist_key="loan_id", sort_key="originated_at") }};

CREATE TABLE IF NOT EXISTS {{ core }}.loans (
    loan_id        VARCHAR(40),
    customer_id    VARCHAR(40),
    product        VARCHAR(40),
    principal      NUMERIC(18, 2),
    rate           NUMERIC(6, 4),
    status         VARCHAR(20),
    state          VARCHAR(2),
    originated_at  DATE,
    updated_at     TIMESTAMP
) {{ dialect.table_options(dist_key="loan_id", sort_key="originated_at") }};

CREATE TABLE IF NOT EXISTS {{ marts }}.loan_portfolio_daily (
    as_of_date     DATE,
    product        VARCHAR(40),
    status         VARCHAR(20),
    loans          BIGINT,
    principal      NUMERIC(18, 2)
) {{ dialect.table_options(sort_key="as_of_date") }};
