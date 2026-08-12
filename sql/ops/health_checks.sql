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


-- ---------------------------------------------------------------------------
-- 7. Late-arrival window headroom.
--
--    Answers: is the incremental overlap window in fct_transactions still wide
--    enough, or is it about to start silently dropping rows?
--
--    WHY THIS NEEDS A QUERY RATHER THAN A GLANCE. The failure it guards
--    against produces no error. A transaction arriving after the overlap
--    window never matches the incremental filter -- it is not rejected, not
--    quarantined, not logged. The run is green and the row simply is not
--    there. The only warning available is watching how close observed
--    lateness gets to the window before that happens.
--
--    READ `headroom_days` AS THE ALERT COLUMN. It reaches zero *before* loss
--    becomes possible, which is what makes it actionable. `window_status =
--    'BREACHED'` does NOT mean data was lost -- rows that fell outside the
--    window are not in this table at all. It means the window is no longer
--    known to be sufficient, which is the last warning there is.
-- ---------------------------------------------------------------------------
SELECT transaction_date,
       transactions,
       late_transactions,
       late_pct,
       p95_delay_hours,
       max_delay_hours,
       max_delay_days,
       window_days,
       headroom_days,
       window_status
FROM finguard.marts.late_arrival_monitor
WHERE transaction_date > current_date() - INTERVAL 30 DAYS
ORDER BY transaction_date DESC;


-- ---------------------------------------------------------------------------
-- 8. Window tuning evidence.
--
--    The other half of the question. Check 7 asks whether the window is too
--    NARROW; this asks whether it is too WIDE -- reprocessing days of history
--    on every run to catch stragglers that all arrive within minutes.
--
--    If p99 across the whole period is a small number of hours, the window is
--    paying for lateness that does not occur. Reducing it is a real saving,
--    and unlike most tuning it can be justified from data rather than
--    asserted.
-- ---------------------------------------------------------------------------
SELECT round(percentile_approx(p50_delay_hours, 0.5), 3)  AS typical_p50_hours,
       round(percentile_approx(p95_delay_hours, 0.5), 3)  AS typical_p95_hours,
       round(max(p99_delay_hours), 3)                     AS worst_p99_hours,
       round(max(max_delay_hours), 3)                     AS worst_case_hours,
       max(max_delay_days)                                AS worst_case_days,
       max(window_days)                                   AS configured_window_days,
       sum(late_transactions)                             AS total_late,
       sum(transactions)                                  AS total_transactions,
       round(100.0 * sum(late_transactions) / nullif(sum(transactions), 0), 3)
                                                          AS overall_late_pct
FROM finguard.marts.late_arrival_monitor;
