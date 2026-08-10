-- Operational queries against the ingestion metadata tables.
--
-- These are the questions a YAML-only metadata-driven pipeline cannot answer.
-- The configs describe what *should* happen; these tables record what did.

-- ---------------------------------------------------------------------------
-- 1. What is this platform supposed to be ingesting?
-- ---------------------------------------------------------------------------
SELECT source_name,
       target_table,
       ingestion_type,
       is_generated,
       column_count
FROM finguard.ops.source_registry
ORDER BY ingestion_type, source_name;


-- ---------------------------------------------------------------------------
-- 2. Coverage by ingestion pattern.
--    `framework_built` < `sources` means some are declared elsewhere
--    (managed ingestion pipelines), which is expected, not a fault.
-- ---------------------------------------------------------------------------
SELECT ingestion_type,
       count(*)                                     AS sources,
       sum(CASE WHEN is_generated THEN 1 ELSE 0 END) AS framework_built
FROM finguard.ops.source_registry
GROUP BY ingestion_type
ORDER BY sources DESC;


-- ---------------------------------------------------------------------------
-- 3. DRIFT: declared in config but no bronze table exists.
--
--    This is the query that justifies the registry existing. A source can be
--    committed, reviewed and merged, and still never produce a table -- because
--    the pipeline was not repointed, the volume was missing, or the update
--    failed silently. Grepping YAML cannot detect that; this join can.
-- ---------------------------------------------------------------------------
SELECT r.source_name,
       r.target_table,
       r.ingestion_type,
       CASE WHEN t.table_name IS NULL THEN 'MISSING' ELSE 'EXISTS' END AS bronze_table
FROM finguard.ops.source_registry r
LEFT JOIN system.information_schema.tables t
       ON t.table_catalog = 'finguard'
      AND t.table_schema  = 'bronze'
      AND t.table_name    = split_part(r.target_table, '.', 3)
ORDER BY bronze_table, r.source_name;


-- ---------------------------------------------------------------------------
-- 4. Most recent pipeline update: what registered, skipped, failed.
-- ---------------------------------------------------------------------------
WITH latest AS (
    SELECT run_id
    FROM finguard.ops.ingestion_audit
    ORDER BY run_timestamp DESC
    LIMIT 1
)
SELECT a.source_name,
       a.ingestion_type,
       a.status,
       a.detail,
       a.run_timestamp
FROM finguard.ops.ingestion_audit a
JOIN latest USING (run_id)
ORDER BY CASE a.status WHEN 'FAILED' THEN 0 WHEN 'SKIPPED' THEN 1 ELSE 2 END,
         a.source_name;


-- ---------------------------------------------------------------------------
-- 5. STALENESS: sources that have not registered successfully recently.
--
--    A source that stops appearing in the audit table is the failure mode
--    nobody notices -- no error is raised, data simply stops arriving.
-- ---------------------------------------------------------------------------
SELECT r.source_name,
       r.ingestion_type,
       max(a.run_timestamp)                                        AS last_seen,
       datediff(current_timestamp(), max(a.run_timestamp))         AS days_since,
       CASE
           WHEN max(a.run_timestamp) IS NULL                       THEN 'NEVER RAN'
           WHEN datediff(current_timestamp(), max(a.run_timestamp)) > 1 THEN 'STALE'
           ELSE 'OK'
       END                                                         AS health
FROM finguard.ops.source_registry r
LEFT JOIN finguard.ops.ingestion_audit a
       ON a.source_name = r.source_name
      AND a.status = 'REGISTERED'
GROUP BY r.source_name, r.ingestion_type
ORDER BY health, r.source_name;


-- ---------------------------------------------------------------------------
-- 6. Failure history: what has broken, and how often.
-- ---------------------------------------------------------------------------
SELECT source_name,
       count(*)              AS failure_count,
       max(run_timestamp)    AS most_recent,
       max_by(detail, run_timestamp) AS latest_error
FROM finguard.ops.ingestion_audit
WHERE status = 'FAILED'
GROUP BY source_name
ORDER BY failure_count DESC;
