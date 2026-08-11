-- Monitoring detectors: scheduled checks that page a human.
--
-- Companion to health_checks.sql, which answers *config-level* questions --
-- what is declared, what is missing, what went stale. These answer *runtime*
-- questions: did it run, is the data good, is state bounded, what did it cost.
--
-- ---------------------------------------------------------------------------
-- CONTRACT: EVERY DETECTOR RETURNS ZERO ROWS WHEN HEALTHY.
-- ---------------------------------------------------------------------------
-- Every query below binds to a Databricks SQL Alert with the same trivial
-- condition: `count > 0` means fire. That uniformity is deliberate.
--
--   * A returned row IS the alert body -- it carries the diagnosis, so the
--     notification says which table and by how much, not just "something
--     broke".
--   * A query that errors returns no rows and fails loudly at the alert level,
--     rather than a threshold silently evaluating false forever.
--
-- EVERY DETECTOR ALSO RETURNS A NUMERIC COLUMN FOR THE CONDITION TO BIND TO.
-- Databricks alert conditions are `<column> <op> <threshold>`, evaluated on
-- the first row. Binding a threshold to a string column -- `update_id > 0`,
-- comparing a UUID against a number -- produces an alert that is successfully
-- created, reports no error, and never fires.
--
-- That bug was made and caught here: the first version of the update-failed
-- alert bound to update_id. It was found by running each detector and checking
-- the bound column was numeric, not by reading the config. Hence the explicit
-- count columns below, which exist purely to give the condition a number.
--
-- The inverse convention -- returning a status column with 'OK' or 'ALERT' --
-- looks friendlier and is worse: the alert then needs a per-query condition on
-- a string value, and a typo in the status literal disables the alert with no
-- error anywhere.
--
-- SEVERITY. Encoded as a column so one routing rule can split channels.
--   PAGE   - act now, out of hours included.
--   TICKET - act today, in hours.
-- Anything below TICKET belongs on a dashboard, not in a notification.


-- ===========================================================================
-- 1. PAGE -- a pipeline update failed.
-- ===========================================================================
-- Lakeflow already emails on update failure, so this is not the only guard.
-- It exists because the native notification is fire-and-forget: it cannot be
-- queried, trended, or joined to what else was happening. This can.
--
-- Scoped to the last 24h so a historical failure does not alert forever.
SELECT
    count(*) OVER ()             AS failure_count,   -- numeric bind column
    'PAGE'                       AS severity,
    'pipeline_update_failed'     AS check_name,
    pipeline_name,
    update_id,
    event_timestamp,
    message                      AS detail
FROM finguard.ops.pipeline_runs
WHERE state = 'FAILED'
  AND event_timestamp > current_timestamp() - INTERVAL 24 HOURS
ORDER BY event_timestamp DESC;


-- ===========================================================================
-- 2. PAGE -- the watermark has stalled.
-- ===========================================================================
-- THE FAILURE MODE THAT PRODUCES NO ERROR.
--
-- A stalled watermark means event time has stopped advancing while the query
-- keeps reporting success. Every late-arriving row is silently dropped from
-- stateful operators, joins stop matching, and the pipeline is green the whole
-- time. Nothing in the native alerting surfaces this.
--
-- Two distinct conditions, deliberately kept in one detector because the
-- operational response is the same -- go look at why event time is not moving:
--
--   NEVER_ADVANCED - watermark still at the epoch. The operator has never seen
--                    a usable event-time value. Usually a null or unparseable
--                    timestamp column, or a source that has produced nothing.
--                    Present in this workspace right now.
--
--   STALLED        - watermark far behind wall clock. Either the source has
--                    genuinely gone quiet, or upstream is delivering stale
--                    event times.
--
-- The 6-hour threshold is chosen for a batch-triggered portfolio pipeline that
-- runs on demand. A continuously-running production stream would use minutes.
-- Stated rather than presented as a universal number.
WITH latest_per_flow AS (
    SELECT
        flow_name,
        operator_name,
        watermark,
        event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY flow_name ORDER BY event_timestamp DESC) AS rn
    FROM finguard.ops.stream_health
    WHERE watermark IS NOT NULL
)
SELECT
    'PAGE'            AS severity,
    'watermark_stalled' AS check_name,
    flow_name,
    operator_name,
    watermark,
    event_timestamp   AS last_seen,
    CASE
        WHEN watermark <= TIMESTAMP '1971-01-01' THEN 'NEVER_ADVANCED'
        ELSE 'STALLED'
    END               AS failure_kind,
    ROUND(
        (unix_timestamp(event_timestamp) - unix_timestamp(watermark)) / 3600.0, 1
    )                 AS watermark_lag_hours
FROM latest_per_flow
WHERE rn = 1
  AND (
        watermark <= TIMESTAMP '1971-01-01'
        OR watermark < event_timestamp - INTERVAL 6 HOURS
      )
ORDER BY watermark_lag_hours DESC;


-- ===========================================================================
-- 3. TICKET -- expectation pass rate degraded against its own baseline.
-- ===========================================================================
-- Compared against each expectation's own trailing history, not a fixed
-- threshold. A hardcoded "alert below 99%" is wrong in both directions: it is
-- noise for an expectation that has always run at 95%, and it is blind to one
-- that silently fell from 100% to 99.5%.
--
-- Requires at least 3 prior runs before it will fire, so a new expectation
-- does not alert on its own first observation.
WITH per_run AS (
    SELECT
        dataset,
        expectation_name,
        update_id,
        max(event_timestamp)                                    AS run_time,
        sum(passed_records)                                     AS passed,
        sum(failed_records)                                     AS failed
    FROM finguard.ops.expectation_results
    GROUP BY dataset, expectation_name, update_id
),
rates AS (
    SELECT
        dataset,
        expectation_name,
        update_id,
        run_time,
        passed,
        failed,
        CASE WHEN passed + failed = 0 THEN NULL
             ELSE passed / (passed + failed) END                AS pass_rate,
        ROW_NUMBER() OVER (PARTITION BY dataset, expectation_name
                           ORDER BY run_time DESC)              AS recency
    FROM per_run
    WHERE passed + failed > 0
),
baseline AS (
    SELECT
        dataset,
        expectation_name,
        avg(pass_rate)   AS baseline_rate,
        count(*)         AS baseline_runs
    FROM rates
    WHERE recency BETWEEN 2 AND 11      -- previous 10 runs, excluding current
    GROUP BY dataset, expectation_name
)
SELECT
    -- Numeric bind column: how far below baseline, in percentage points.
    ROUND((b.baseline_rate - r.pass_rate) * 100, 2) AS drop_pct_points,
    'TICKET'                     AS severity,
    'expectation_rate_degraded'  AS check_name,
    r.dataset,
    r.expectation_name,
    r.update_id,
    ROUND(r.pass_rate * 100, 2)  AS current_pct,
    ROUND(b.baseline_rate * 100, 2) AS baseline_pct,
    r.failed                     AS failed_records,
    r.run_time
FROM rates r
JOIN baseline b
  ON b.dataset = r.dataset
 AND b.expectation_name = r.expectation_name
WHERE r.recency = 1
  AND b.baseline_runs >= 3
  -- One full percentage point below baseline. Absolute rather than relative so
  -- it does not become hypersensitive as the baseline approaches 100%.
  AND r.pass_rate < b.baseline_rate - 0.01
ORDER BY (b.baseline_rate - r.pass_rate) DESC;


-- ===========================================================================
-- 4. TICKET -- streaming state is growing without bound.
-- ===========================================================================
-- Watermarks bound state only if event time advances and rows actually age
-- out. When that breaks, state grows until the job OOMs -- and the run before
-- the OOM looks completely healthy.
--
-- Compares the most recent state size against the same flow's own trailing
-- median rather than a fixed row count, because the right absolute size
-- differs per operator.
WITH recent AS (
    SELECT
        flow_name,
        operator_name,
        num_rows_total,
        event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY flow_name ORDER BY event_timestamp DESC) AS rn
    FROM finguard.ops.stream_health
    WHERE num_rows_total IS NOT NULL
      AND operator_name IS NOT NULL
),
stats AS (
    SELECT
        flow_name,
        percentile_approx(num_rows_total, 0.5) AS median_rows,
        count(*)                               AS observations
    FROM recent
    WHERE rn BETWEEN 2 AND 21
    GROUP BY flow_name
)
SELECT
    -- Numeric bind column: multiple of the trailing median.
    ROUND(r.num_rows_total / s.median_rows, 2) AS growth_multiple,
    'TICKET'                 AS severity,
    'state_growth_unbounded' AS check_name,
    r.flow_name,
    r.operator_name,
    r.num_rows_total         AS current_state_rows,
    s.median_rows            AS baseline_median_rows,
    r.event_timestamp
FROM recent r
JOIN stats s ON s.flow_name = r.flow_name
WHERE r.rn = 1
  AND s.observations >= 5
  AND s.median_rows > 0
  -- 3x the trailing median. Streaming state is naturally spiky, so a tight
  -- multiplier would fire on every legitimate burst.
  AND r.num_rows_total > s.median_rows * 3
ORDER BY r.num_rows_total / s.median_rows DESC;


-- ===========================================================================
-- 5. DIAGNOSTIC -- state store partition count vs configured shuffle partitions.
-- ===========================================================================
-- Not an alert. A standing check for a real defect found in this workspace:
-- dedupeWithinWatermark reports 200 state store instances while the pipeline
-- configures spark.sql.shuffle.partitions = 16.
--
-- Stateful operators pin their partition count into the checkpoint when it is
-- first created. Changing shuffle.partitions afterwards cannot apply, because
-- state is physically laid out across the original partition count -- Spark
-- keeps the old value rather than silently corrupting state. So the tuning is
-- inert for this operator until the checkpoint is rebuilt.
--
-- This is exactly the class of problem that only shows up if you look: no
-- error, no warning, and a config file that reads as though it took effect.
SELECT
    'DIAGNOSTIC'                  AS severity,
    'state_partitions_vs_config'  AS check_name,
    flow_name,
    operator_name,
    max(num_state_instances)      AS state_store_instances,
    16                            AS configured_shuffle_partitions,
    'Checkpoint pins partition count; requires fresh checkpoint to change'
                                  AS note
FROM finguard.ops.stream_health
WHERE operator_name IS NOT NULL
  AND num_state_instances IS NOT NULL
GROUP BY flow_name, operator_name
HAVING max(num_state_instances) <> 16
ORDER BY state_store_instances DESC;


-- ===========================================================================
-- 6. COST -- DBU and dollars per pipeline update.
-- ===========================================================================
-- Attribution and trend, deliberately not anomaly detection. Outlier
-- flagging needs a baseline distribution; at 52 usage records for this
-- pipeline any z-score would be fitting noise. Attribution needs almost no
-- history and answers the question that actually matters: what does a run
-- cost, and is that going up.
--
-- This is also the independent check on the optimization work. The claims in
-- docs/performance_optimization.md rest on file counts and bytes; DBU per run
-- is a separate measurement that can corroborate or contradict them.
--
-- list_prices is time-ranged (price_end_time IS NULL = current price), so the
-- join must filter for the active row or usage multiplies across every
-- historical price.
SELECT
    u.usage_metadata.dlt_pipeline_id                       AS pipeline_id,
    date_trunc('DAY', u.usage_start_time)                  AS usage_day,
    round(sum(u.usage_quantity), 3)                        AS dbus,
    round(sum(u.usage_quantity * p.pricing.default), 4)    AS est_usd,
    count(*)                                               AS usage_records
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
       ON p.sku_name = u.sku_name
      AND p.currency_code = 'USD'
      AND p.price_end_time IS NULL
WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
  AND u.usage_start_time > current_timestamp() - INTERVAL 30 DAYS
GROUP BY 1, 2
ORDER BY usage_day DESC, dbus DESC;
