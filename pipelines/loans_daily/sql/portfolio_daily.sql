-- The portfolio as it stood at the end of the logical date: for each loan, the last version
-- that existed by then. Built from the versions table, so re-running this date next month
-- gives the same rows. The date's rows are replaced, never appended to.
DELETE FROM {{ marts }}.loan_portfolio_daily WHERE as_of_date = {{ logical_date | lit }};

INSERT INTO {{ marts }}.loan_portfolio_daily (as_of_date, product, status, loans, principal)
SELECT {{ logical_date | lit }}, product, status, COUNT(*), SUM(principal)
FROM (
    SELECT v.*, ROW_NUMBER() OVER (PARTITION BY v.loan_id ORDER BY v.updated_at DESC) AS rn
    FROM {{ core }}.loan_versions v
    WHERE v.updated_at < {{ interval_end }}
) as_of
WHERE rn = 1
GROUP BY product, status;
