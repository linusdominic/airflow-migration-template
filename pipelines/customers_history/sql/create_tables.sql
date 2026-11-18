CREATE SCHEMA IF NOT EXISTS {{ staging }};
CREATE SCHEMA IF NOT EXISTS {{ core }};

CREATE TABLE IF NOT EXISTS {{ core }}.dim_customer (
    customer_id  VARCHAR(40),
    segment      VARCHAR(40),
    state        VARCHAR(2),
    credit_band  VARCHAR(20),
    valid_from   DATE,
    valid_to     DATE,
    is_current   BOOLEAN
) {{ dialect.table_options(dist_key="customer_id", sort_key="valid_from") }};
