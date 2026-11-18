-- core.loans holds the latest version of each loan. Only the loans in today's file can
-- have changed, so only those are rebuilt, from the versions table. Because it reads the
-- versions and not the file, loading an old file never puts old values back.
DELETE FROM {{ core }}.loans
WHERE loan_id IN (SELECT loan_id FROM {{ staging }}.loans_clean);

INSERT INTO {{ core }}.loans (loan_id, customer_id, product, principal, rate, status, state, originated_at, updated_at)
SELECT loan_id, customer_id, product, principal, rate, status, state, originated_at, updated_at
FROM (
    SELECT v.*, ROW_NUMBER() OVER (PARTITION BY v.loan_id ORDER BY v.updated_at DESC) AS rn
    FROM {{ core }}.loan_versions v
    WHERE v.loan_id IN (SELECT loan_id FROM {{ staging }}.loans_clean)
) latest
WHERE rn = 1;
