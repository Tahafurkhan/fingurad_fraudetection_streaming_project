-- Distribution monitoring: the fifth pillar.
--
-- WHY THIS IS THE GAP THAT MATTERED MOST
-- --------------------------------------
-- The five pillars of data observability are freshness, volume, schema,
-- lineage and distribution. This platform covered four:
--
--   freshness  -> dbt source freshness, watermark stall detector
--   volume     -> the bronze = silver + quarantine reconciliation
--   schema     -> contract-alignment tests across four definitions
--   lineage    -> ops.v_table_lineage and the impact views
--   distribution -> NOTHING
--
-- Every quality rule here is row-local: is transaction_id present, is amount
-- positive. Not one asks whether the SHAPE of today's data resembles
-- yesterday's.
--
-- For a fraud platform that is precisely the wrong gap to leave. A fraud
-- pattern shift is a distribution change in which every individual row is
-- valid: amounts move, geography concentrates, a channel spikes. Row-level
-- validation passes on all of it, the pipeline stays green, and the aggregate
-- has changed underneath.
--
-- WHY RATIOS AND NOT ANOMALY DETECTION
-- ------------------------------------
-- No ML here, deliberately. At this volume a learned baseline would be fitted
-- to a handful of days and would produce confident nonsense. A ratio against a
-- trailing window is crude, explainable, and honest about being crude -- and
-- when it fires, the reason is visible in the numbers rather than inside a
-- model. Revisit when there is a season's worth of history.
--
-- WHY EVERY CHECK RETURNS A NUMERIC FIRST COLUMN
-- ----------------------------------------------
-- Databricks alerts bind to a numeric column and compare it against a
-- threshold. Binding to a string produces an alert that never fires, silently
-- -- documented in scripts/create_alerts.py. Each query below therefore leads
-- with the number the alert watches.

-- ---------------------------------------------------------------------------
-- 1. Amount distribution shift.
--
--    Compares today against the trailing 7 days on median and p95. The median
--    catches a change in typical behaviour; p95 catches the tail moving, which
--    is where fraud lives -- a fraudster testing a stolen card with small
--    amounts before a large one shifts the tail long before the median.
--
--    Median rather than mean throughout: transaction amounts are right-skewed,
--    and a single large legitimate transaction drags a mean enough to look
--    like a shift on its own.
-- ---------------------------------------------------------------------------
WITH baseline AS (
    SELECT
        percentile_approx(amount, 0.50) AS p50,
        percentile_approx(amount, 0.95) AS p95,
        count(*)                        AS rows_seen
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 8 DAYS
      AND transaction_timestamp <  current_timestamp() - INTERVAL 1 DAY
),
current_window AS (
    SELECT
        percentile_approx(amount, 0.50) AS p50,
        percentile_approx(amount, 0.95) AS p95,
        count(*)                        AS rows_seen
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 1 DAY
)
SELECT
    -- The alert column: how far today's median has moved, as a multiple.
    -- Symmetric, so a halving reads the same magnitude as a doubling --
    -- otherwise a collapse in transaction size looks smaller than a spike and
    -- is under-alerted.
    round(
        greatest(
            c.p50 / nullif(b.p50, 0),
            b.p50 / nullif(c.p50, 0)
        ), 3
    )                                    AS median_shift_multiple,
    round(
        greatest(
            c.p95 / nullif(b.p95, 0),
            b.p95 / nullif(c.p95, 0)
        ), 3
    )                                    AS p95_shift_multiple,
    round(b.p50, 2)                      AS baseline_median,
    round(c.p50, 2)                      AS current_median,
    round(b.p95, 2)                      AS baseline_p95,
    round(c.p95, 2)                      AS current_p95,
    b.rows_seen                          AS baseline_rows,
    c.rows_seen                          AS current_rows
FROM baseline b
CROSS JOIN current_window c
-- Both windows must hold enough rows for a percentile to mean anything.
-- Without this the check fires every time ingestion is quiet, which is the
-- classic way a distribution monitor gets muted in its first week.
WHERE b.rows_seen >= 50
  AND c.rows_seen >= 50
  AND greatest(c.p50 / nullif(b.p50, 0), b.p50 / nullif(c.p50, 0)) > 2.0;


-- ---------------------------------------------------------------------------
-- 2. Categorical mix shift -- country, channel, transaction type.
--
--    A fraud ring operating from one geography, or a compromised channel,
--    shows up as a category's SHARE changing rather than as any row being
--    invalid. Share, not count: overall volume moving up and down is normal
--    and would make raw counts alert constantly.
--
--    UNPIVOTed into one result so a single alert covers all three dimensions.
--    Three near-identical alerts would triple the maintenance for no extra
--    signal.
-- ---------------------------------------------------------------------------
WITH baseline AS (
    SELECT 'country' AS dimension, country AS value, count(*) AS n
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 8 DAYS
      AND transaction_timestamp <  current_timestamp() - INTERVAL 1 DAY
    GROUP BY country
    UNION ALL
    SELECT 'payment_channel', payment_channel, count(*)
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 8 DAYS
      AND transaction_timestamp <  current_timestamp() - INTERVAL 1 DAY
    GROUP BY payment_channel
    UNION ALL
    SELECT 'transaction_type', transaction_type, count(*)
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 8 DAYS
      AND transaction_timestamp <  current_timestamp() - INTERVAL 1 DAY
    GROUP BY transaction_type
),
current_window AS (
    SELECT 'country' AS dimension, country AS value, count(*) AS n
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 1 DAY
    GROUP BY country
    UNION ALL
    SELECT 'payment_channel', payment_channel, count(*)
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 1 DAY
    GROUP BY payment_channel
    UNION ALL
    SELECT 'transaction_type', transaction_type, count(*)
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 1 DAY
    GROUP BY transaction_type
),
baseline_share AS (
    SELECT dimension, value,
           n / sum(n) OVER (PARTITION BY dimension) AS share
    FROM baseline
),
current_share AS (
    SELECT dimension, value, n,
           n / sum(n) OVER (PARTITION BY dimension) AS share
    FROM current_window
)
SELECT
    -- Alert column: percentage points of share change. Points rather than a
    -- ratio, because a category going from 0.1% to 0.3% is a 3x ratio and
    -- almost certainly noise, while 30% to 45% is 15 points and material.
    round(100 * abs(c.share - coalesce(b.share, 0)), 2) AS share_change_points,
    c.dimension,
    c.value,
    round(100 * coalesce(b.share, 0), 2)                AS baseline_pct,
    round(100 * c.share, 2)                             AS current_pct,
    c.n                                                 AS current_rows,
    CASE WHEN b.value IS NULL THEN 'NEW_VALUE' ELSE 'SHIFT' END AS change_type
FROM current_share c
LEFT JOIN baseline_share b
       ON b.dimension = c.dimension
      AND b.value     = c.value
WHERE c.n >= 20
  AND (
        -- A meaningful shift in an established category...
        100 * abs(c.share - coalesce(b.share, 0)) > 15
        -- ...or a value never seen before taking real share. A new country
        -- appearing with 5% of volume is worth a look even though no
        -- individual transaction is invalid.
        OR (b.value IS NULL AND c.share > 0.05)
      )
ORDER BY share_change_points DESC;


-- ---------------------------------------------------------------------------
-- 3. Alert-rate shift.
--
--    THE DETECTOR THAT WATCHES THE DETECTORS.
--
--    Fraud alerts stopping is indistinguishable from fraud stopping, and the
--    first is far more likely. If the watchlist join breaks, or the threshold
--    logic regresses, or the watermark excludes everything, alert volume goes
--    to zero and every other check stays green -- the pipeline ran, the data
--    arrived, the rules evaluated, and nothing fired.
--
--    Bidirectional deliberately: a spike means a rule has become too broad and
--    is about to drown the analysts, which damages the platform's credibility
--    as surely as silence does.
-- ---------------------------------------------------------------------------
WITH baseline AS (
    SELECT count(*) / 7.0 AS alerts_per_day
    FROM finguard.gold.high_value_transactions_alert
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 8 DAYS
      AND transaction_timestamp <  current_timestamp() - INTERVAL 1 DAY
),
current_window AS (
    SELECT count(*) AS alerts_today
    FROM finguard.gold.high_value_transactions_alert
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 1 DAY
),
volume AS (
    SELECT count(*) AS transactions_today
    FROM finguard.silver.transactions
    WHERE transaction_timestamp >= current_timestamp() - INTERVAL 1 DAY
)
SELECT
    round(
        greatest(
            c.alerts_today / nullif(b.alerts_per_day, 0),
            b.alerts_per_day / nullif(c.alerts_today, 0)
        ), 3
    )                                  AS alert_rate_multiple,
    c.alerts_today,
    round(b.alerts_per_day, 2)         AS baseline_alerts_per_day,
    v.transactions_today,
    CASE
        WHEN c.alerts_today = 0 AND b.alerts_per_day > 0
            THEN 'ALERTS STOPPED -- detection may be broken'
        WHEN c.alerts_today > b.alerts_per_day * 3
            THEN 'ALERT SPIKE -- rule too broad, or a real incident'
        ELSE 'shift'
    END                                AS diagnosis
FROM baseline b
CROSS JOIN current_window c
CROSS JOIN volume v
-- Only meaningful when transactions actually flowed. Zero alerts on zero
-- transactions is correct, and alerting on it would fire every quiet night.
WHERE v.transactions_today >= 50
  AND b.alerts_per_day > 0
  AND (
        c.alerts_today = 0
        OR c.alerts_today > b.alerts_per_day * 3
      );


-- ---------------------------------------------------------------------------
-- 4. Marts lag behind silver.
--
--    The ops-side twin of transform/tests/assert_marts_track_silver.sql. Both
--    exist on purpose: the dbt test gates the dbt run, this fires on a
--    schedule regardless of whether dbt ran at all.
--
--    That difference is the whole point. The failure this was written for was
--    a mart a month stale because the job never fired -- and a test that only
--    runs inside the job cannot catch the job not running.
-- ---------------------------------------------------------------------------
SELECT
    datediff(
        (SELECT max(transaction_timestamp) FROM finguard.silver.transactions),
        (SELECT max(transaction_timestamp) FROM finguard.marts.fct_transactions)
    )                                                          AS mart_lag_days,
    (SELECT max(transaction_timestamp) FROM finguard.silver.transactions)
                                                               AS silver_newest,
    (SELECT max(transaction_timestamp) FROM finguard.marts.fct_transactions)
                                                               AS mart_newest,
    (SELECT count(*) FROM finguard.silver.transactions)         AS silver_rows,
    (SELECT count(*) FROM finguard.marts.fct_transactions)      AS mart_rows
WHERE datediff(
        (SELECT max(transaction_timestamp) FROM finguard.silver.transactions),
        (SELECT max(transaction_timestamp) FROM finguard.marts.fct_transactions)
      ) > 1;
