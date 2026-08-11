-- ===========================================================================
-- Verification for the PII controls.
--
-- WHY THIS FILE EXISTS
-- --------------------
-- This project has been bitten twice by controls that reported success and
-- did nothing:
--
--   * dbt accepted `table_properties` and `cluster_by`, wrote them into
--     target/manifest.json, reported PASS=63 ERROR=0, and applied neither.
--     Only DESCRIBE DETAIL revealed the tables had no properties at all.
--
--   * A SQL Alert was created against `update_id`, a UUID string, with the
--     condition `GREATER_THAN 0`. The API returned 200 and the alert would
--     never have fired.
--
-- A column mask is in exactly the same category: `ALTER TABLE ... SET MASK`
-- succeeds whether or not the mask does what was intended, and the failure
-- mode -- a PAN still readable -- is silent and invisible until it matters.
-- So the mask gets the same treatment as everything else here: assert it,
-- do not assume it.
--
-- Run after 01 and 02. Every query below states what a PASS looks like.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- CHECK 1: every mask is actually attached.
--
-- PASS: ten rows, one per masked column.
-- FAIL: fewer -- an ALTER silently did not take, or a table was rebuilt by
--       the pipeline afterwards, which DROPS THE MASK (see CHECK 6).
-- ---------------------------------------------------------------------------

SELECT
    table_schema,
    table_name,
    column_name,
    mask_name
FROM system.information_schema.column_masks
WHERE table_catalog = 'finguard'
ORDER BY table_schema, table_name, column_name;


-- ---------------------------------------------------------------------------
-- CHECK 2: the mask actually transforms the value.
--
-- CHECK 1 proves a mask is attached. It does not prove the mask masks --
-- a function returning its input unchanged would attach just as cleanly.
--
-- Interpreting this depends on who runs it:
--   * member of finguard_pci_privileged -> full PAN, is_masked = false.
--     This is the control working, not failing.
--   * anyone else -> ****-****-****-1234, is_masked = true.
--
-- Both are correct results. What is NOT correct is a non-privileged caller
-- seeing 16 digits.
-- ---------------------------------------------------------------------------

SELECT
    current_user()                                        AS running_as,
    is_account_group_member('finguard_pci_privileged')    AS is_privileged,
    card_number                                           AS card_number_as_seen,
    card_number LIKE '****%'                              AS is_masked,
    length(card_number)                                   AS visible_length
FROM finguard.silver.transactions
LIMIT 5;


-- ---------------------------------------------------------------------------
-- CHECK 3: the coverage query -- tagged as sensitive but NOT masked.
--
-- This is the check worth keeping. Everything above verifies what was built
-- today; this one catches what breaks later. A table added next month with a
-- card_number column, tagged by whoever wrote it and never masked, shows up
-- here.
--
-- PASS: zero rows.
-- FAIL: any row -- a column is classified sensitive and is readable in clear.
--
-- Worth scheduling as a SQL Alert alongside the four in scripts/create_alerts.py.
-- ---------------------------------------------------------------------------

WITH tagged AS (
    SELECT catalog_name, schema_name, table_name, column_name
    FROM system.information_schema.column_tags
    WHERE catalog_name = 'finguard'
      AND tag_name IN ('pii', 'pci')
),
masked AS (
    SELECT table_catalog, table_schema, table_name, column_name
    FROM system.information_schema.column_masks
    WHERE table_catalog = 'finguard'
)
SELECT
    count(*)                                     AS unmasked_sensitive_columns,
    concat_ws(', ', collect_list(
        concat(t.schema_name, '.', t.table_name, '.', t.column_name)
    ))                                           AS offending_columns
FROM tagged t
LEFT JOIN masked m
       ON  m.table_schema = t.schema_name
       AND m.table_name   = t.table_name
       AND m.column_name  = t.column_name
WHERE m.column_name IS NULL
HAVING count(*) > 0;


-- ---------------------------------------------------------------------------
-- CHECK 4: the inverse -- PAN-shaped columns nobody classified.
--
-- CHECK 3 only sees columns someone remembered to tag. This one finds columns
-- that look like cardholder data regardless of whether anyone classified
-- them, which is the gap that actually causes breaches: not the known
-- sensitive column left unmasked, but the one nobody thought about.
--
-- PASS: zero rows.
-- KNOWN FAIL AT TIME OF WRITING: finguard.bronze.transactions.value, which
--   holds the raw Kafka payload with the PAN inside a JSON string. A column
--   mask cannot mask a substring, and masking `value` wholesale would break
--   silver, which parses it. The real fix is tokenizing at the producer so
--   the PAN never reaches bronze. Recorded here rather than suppressed --
--   an exception that is documented is a decision; an exception that is
--   filtered out of the query is a lie.
-- ---------------------------------------------------------------------------

SELECT
    table_schema,
    table_name,
    column_name,
    'PAN-shaped column name, no mask, no tag' AS finding
FROM system.information_schema.columns c
WHERE c.table_catalog = 'finguard'
  AND c.table_schema NOT IN ('information_schema', 'security')
  AND (
        lower(c.column_name) LIKE '%card_number%'
     OR lower(c.column_name) LIKE '%pan%'
     OR lower(c.column_name) LIKE '%cardnum%'
  )
  AND NOT EXISTS (
        SELECT 1 FROM system.information_schema.column_masks m
        WHERE m.table_catalog = c.table_catalog
          AND m.table_schema  = c.table_schema
          AND m.table_name    = c.table_name
          AND m.column_name   = c.column_name
  )
ORDER BY table_schema, table_name;


-- ---------------------------------------------------------------------------
-- CHECK 5: masking must not have broken the fraud join.
--
-- The single most dangerous side effect of column masking. gold.fraud_card_alert
-- joins transactions to the watchlist on
--
--     transactions.card_number == fraud_watchlist.entity_id
--
-- If the mask applied to the *join input* rather than to the output
-- projection, every comparison would become '****-****-****-9904' = '4144...'
-- and the join would silently return zero rows. Fraud detection would stop,
-- the pipeline would report success, and the alert count would quietly go to
-- zero -- the exact failure class this project has documented three times.
--
-- Unity Catalog applies masks at projection time, so the join is evaluated on
-- the underlying value and this is expected to be fine. "Expected to be fine"
-- is precisely the phrase that preceded the last two silent failures, so it
-- is asserted rather than trusted.
--
-- PASS: alert_count unchanged from the pre-masking baseline of 375.
-- FAIL: zero, or any number materially below 375.
-- ---------------------------------------------------------------------------

SELECT
    count(*)                        AS alert_count,
    375                             AS baseline_before_masking,
    count(*) = 375                  AS unchanged,
    count(DISTINCT card_number)     AS distinct_cards_as_seen
FROM finguard.gold.fraud_card_alert;


-- ---------------------------------------------------------------------------
-- CHECK 6: masks survive a pipeline rebuild.
--
-- THE OPERATIONAL TRAP, and the reason this file has a sixth check.
--
-- Lakeflow Declarative Pipelines own silver.transactions and
-- gold.fraud_card_alert. A full refresh DROPS AND RECREATES those tables, and
-- a recreated table has no column masks -- the ALTER statements in
-- 01_pii_masking.sql apply to a table that no longer exists. The controls
-- disappear with no error, no log line, and no alert.
--
-- This is not hypothetical for this project: a full refresh of
-- bronze.transactions is already on the backlog to fix a stale checkpoint.
-- Running it un-remasks everything downstream.
--
-- MITIGATION: re-run 01_pii_masking.sql after ANY full refresh, and run this
-- check to confirm. The durable fix is to apply masks from the pipeline
-- itself so they are part of table creation rather than a manual follow-up.
--
-- PASS: masked_columns = 6.
-- ---------------------------------------------------------------------------

SELECT
    count(*)        AS masked_columns,
    10              AS expected,
    count(*) = 10   AS all_masks_present,
    CASE WHEN count(*) < 10
         THEN 'MASKS MISSING - was a pipeline full-refreshed? Re-run 01_pii_masking.sql'
         ELSE 'ok'
    END             AS remediation
FROM system.information_schema.column_masks
WHERE table_catalog = 'finguard';
