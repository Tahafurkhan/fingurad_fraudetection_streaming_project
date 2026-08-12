-- Distribution monitoring as deployed views.
--
-- sql/ops/distribution_checks.sql holds the same logic as standalone queries
-- for interactive investigation. This file registers them as views, because an
-- alert needs a stable object to bind to -- a Databricks alert stores its own
-- copy of a query, so editing the file would leave the alert running the old
-- text with nothing reporting the divergence.
--
-- Views keep one definition. Change the view, every alert follows.
--
-- Each view returns rows ONLY when something is wrong, and leads with the
-- numeric column the alert compares against zero. An alert bound to a string
-- column never fires and reports no error -- see scripts/create_alerts.py.

CREATE SCHEMA IF NOT EXISTS finguard.ops;

-- ---------------------------------------------------------------------------
-- Amount distribution shift.
--
-- Median and p95 against a trailing 7-day baseline. Percentiles rather than a
-- mean: transaction amounts are right-skewed, so one large legitimate payment
-- moves a mean enough to look like a distribution change on its own.
--
-- The shift is expressed symmetrically -- max(a/b, b/a) -- so a halving reads
-- as the same magnitude as a doubling. Without that, a collapse in typical
-- transaction size registers as smaller than a spike and is under-alerted,
-- even though "amounts suddenly tiny" is exactly the signature of a card being
-- tested before a large fraudulent charge.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_amount_distribution_shift AS
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
    round(greatest(c.p50 / nullif(b.p50, 0), b.p50 / nullif(c.p50, 0)), 3)
                                         AS median_shift_multiple,
    round(greatest(c.p95 / nullif(b.p95, 0), b.p95 / nullif(c.p95, 0)), 3)
                                         AS p95_shift_multiple,
    round(b.p50, 2)                      AS baseline_median,
    round(c.p50, 2)                      AS current_median,
    round(b.p95, 2)                      AS baseline_p95,
    round(c.p95, 2)                      AS current_p95,
    b.rows_seen                          AS baseline_rows,
    c.rows_seen                          AS current_rows
FROM baseline b
CROSS JOIN current_window c
-- Minimum sample sizes. Without them this fires on every quiet night, which is
-- how a distribution monitor gets muted in its first week and never unmuted.
WHERE b.rows_seen >= 50
  AND c.rows_seen >= 50
  AND greatest(c.p50 / nullif(b.p50, 0), b.p50 / nullif(c.p50, 0)) > 2.0;


-- ---------------------------------------------------------------------------
-- Categorical mix shift across country, channel and transaction type.
--
-- A fraud ring in one geography, or a compromised channel, changes a
-- category's SHARE while every individual row stays valid. Share rather than
-- count, because overall volume rises and falls normally and raw counts would
-- alert on that alone.
--
-- Measured in percentage points, not ratios: 0.1% -> 0.3% is a 3x ratio and
-- almost certainly noise; 30% -> 45% is 15 points and material.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_categorical_mix_shift AS
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
    SELECT dimension, value, n / sum(n) OVER (PARTITION BY dimension) AS share
    FROM baseline
),
current_share AS (
    SELECT dimension, value, n,
           n / sum(n) OVER (PARTITION BY dimension) AS share
    FROM current_window
),
-- THE COLD-START GUARD, and it was added because the first run of this view
-- returned six rows on a healthy platform.
--
-- Every one said NEW_VALUE with baseline_pct = 0.0 -- India at 95.88%, PURCHASE
-- at 59.03%, and so on. Nothing had shifted. The baseline window was simply
-- empty, because all 825 transactions had arrived that day after a full
-- refresh, so every category legitimately present looked brand new.
--
-- Technically correct, operationally useless: it would page on every fresh
-- deployment, every backfill, and every restore. An empty baseline means
-- "cannot assess", not "everything changed", and a detector that cannot tell
-- those apart is one people mute in week one.
baseline_sufficiency AS (
    SELECT dimension, sum(n) AS baseline_rows
    FROM baseline
    GROUP BY dimension
)
SELECT
    round(100 * abs(c.share - coalesce(b.share, 0)), 2) AS share_change_points,
    c.dimension,
    c.value,
    round(100 * coalesce(b.share, 0), 2)                AS baseline_pct,
    round(100 * c.share, 2)                             AS current_pct,
    c.n                                                 AS current_rows,
    s.baseline_rows,
    CASE WHEN b.value IS NULL THEN 'NEW_VALUE' ELSE 'SHIFT' END AS change_type
FROM current_share c
JOIN baseline_sufficiency s
  ON s.dimension = c.dimension
LEFT JOIN baseline_share b
       ON b.dimension = c.dimension AND b.value = c.value
-- The JOIN above already drops dimensions with no baseline at all. The
-- threshold additionally requires enough baseline history for a share to be
-- meaningful: 100 rows spread across categories is the minimum at which a
-- 15-point move is signal rather than sampling noise.
WHERE s.baseline_rows >= 100
  AND c.n >= 20
  AND (
        100 * abs(c.share - coalesce(b.share, 0)) > 15
        -- A value never seen before taking real share. A new country at 5% of
        -- volume is worth investigating even though no row is invalid.
        OR (b.value IS NULL AND c.share > 0.05)
      );


-- ---------------------------------------------------------------------------
-- Alert-rate shift -- the detector that watches the detectors.
--
-- Fraud alerts stopping looks exactly like fraud stopping, and the first is
-- far more likely. If the watchlist join breaks, a threshold regresses, or a
-- watermark excludes everything, alert volume goes to zero while every other
-- check stays green: the pipeline ran, data arrived, rules evaluated, nothing
-- fired.
--
-- Bidirectional on purpose. A spike means a rule has become too broad and is
-- about to bury the analysts, which destroys trust in the platform as surely
-- as silence does.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_alert_rate_shift AS
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
    round(greatest(
        c.alerts_today / nullif(b.alerts_per_day, 0),
        b.alerts_per_day / nullif(c.alerts_today, 0)
    ), 3)                              AS alert_rate_multiple,
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
-- Zero alerts on zero transactions is correct, not a failure.
WHERE v.transactions_today >= 50
  AND b.alerts_per_day > 0
  AND (c.alerts_today = 0 OR c.alerts_today > b.alerts_per_day * 3);


-- ---------------------------------------------------------------------------
-- Marts lagging behind silver.
--
-- The ops twin of transform/tests/assert_marts_track_silver.sql. Both exist
-- deliberately: the dbt test gates the dbt run; this fires on a schedule
-- whether or not dbt ran.
--
-- That distinction is the entire reason this view exists. The failure it was
-- written for was a mart a month stale because the orchestration job was
-- paused and never fired -- and a test that only runs inside the job cannot
-- catch the job not running.
--
-- Compares event-time lag, not row counts. Counts legitimately differ: the
-- mart deduplicates on the natural key while silver retains at-least-once
-- redeliveries as the audit record.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_marts_lag AS
WITH state AS (
    SELECT
        (SELECT max(transaction_timestamp) FROM finguard.silver.transactions)
            AS silver_newest,
        (SELECT max(transaction_timestamp) FROM finguard.marts.fct_transactions)
            AS mart_newest,
        (SELECT count(*) FROM finguard.silver.transactions)    AS silver_rows,
        (SELECT count(*) FROM finguard.marts.fct_transactions) AS mart_rows
)
SELECT
    -- A null mart timestamp means the mart is empty, which is a failure that
    -- datediff would silently return null for. Coalesced to a large number so
    -- the alert's numeric comparison still fires.
    coalesce(datediff(silver_newest, mart_newest), 9999) AS mart_lag_days,
    silver_newest,
    mart_newest,
    silver_rows,
    mart_rows
FROM state
WHERE mart_newest IS NULL
   OR datediff(silver_newest, mart_newest) > 1;
