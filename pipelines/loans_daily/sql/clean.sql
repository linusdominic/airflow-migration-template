-- The tMap: type the raw text, tidy the values, and keep one row per loan.
-- If a loan appears twice in a file, the row with the latest updated_at wins.
DROP TABLE IF EXISTS {{ staging }}.loans_clean;

CREATE TABLE {{ staging }}.loans_clean AS
SELECT loan_id, customer_id, product, principal, rate, status, state, originated_at, updated_at
FROM (
    SELECT
        TRIM(loan_id)                         AS loan_id,
        TRIM(customer_id)                     AS customer_id,
        LOWER(TRIM(product))                  AS product,
        CAST(principal AS NUMERIC(18, 2))     AS principal,
        CAST(rate AS NUMERIC(6, 4))           AS rate,
        LOWER(TRIM(status))                   AS status,
        UPPER(TRIM(state))                    AS state,
        CAST(originated_at AS DATE)           AS originated_at,
        CAST(updated_at AS TIMESTAMP)         AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY TRIM(loan_id) ORDER BY CAST(updated_at AS TIMESTAMP) DESC) AS rn
    FROM {{ staging }}.loans_raw
) ranked
WHERE rn = 1;
