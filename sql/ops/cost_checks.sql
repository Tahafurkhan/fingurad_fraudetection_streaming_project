-- ===========================================================================
-- Cost detectors for FinGuard.
--
-- WHY THIS FILE EXISTS, AND WHY IT ALMOST DID NOT
-- ----------------------------------------------
-- When the observability layer was designed, cost anomaly detection was
-- rejected with the reasoning: "system.billing.usage is real, but with usage
-- this small the variance is noise."
--
-- That reasoning was half right and it was rejected for the wrong scope. It is
-- correct for the *pipeline*: FinGuard costs roughly $0.29/day, and a detector
-- on that number would fire on rounding. It is wrong for the *workspace*,
-- which is where money is actually lost. Measured on 11 August 2026:
--
--     30-day workspace spend                     $395.83
--     attributable to FinGuard                    $59.63
--     PREMIUM_SERVERLESS_REAL_TIME_INFERENCE     $239.35
--
-- A serverless model-serving endpoint unrelated to this project was burning a
-- flat 96 DBU/day floor -- every day, whether or not anything called it -- and
-- had been for weeks. That is scale-to-zero left disabled. It cost four times
-- what the entire fraud platform cost, and nothing in the workspace said so.
--
-- The lesson is about the *unit of analysis*, not about thresholds. Cost
-- monitoring scoped to the thing you are building will never find the thing
-- you forgot about, and the thing you forgot about is where runaway spend
-- lives. These detectors are therefore workspace-scoped by default.
--
-- CONVENTIONS (same as sql/ops/monitoring_checks.sql)
-- --------------------------------------------------
--   * Zero rows means healthy. A detector that returns rows when things are
--     fine cannot be attached to an alert.
--   * The FIRST column is always numeric, so a SQL Alert can bind its
--     condition to it. Binding GREATER_THAN 0 to a string column creates an
--     alert that is accepted, returns 200, and never fires -- that mistake was
--     already made once in this project.
--
-- PRICING NOTE
-- ------------
-- system.billing.list_prices is time-ranged. Joining without
-- `price_end_time IS NULL` multiplies usage by every historical price a SKU
-- ever had and silently inflates every figure. The filter is not optional.
--
-- These are LIST prices. Real invoices reflect committed-use discounts and
-- private rates, so every dollar figure here is an estimate for trend and
-- comparison, not an invoice. Stated rather than implied.
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 1. PAGE -- daily spend far above its own trailing baseline.
--
-- Compares yesterday against the median of the preceding two weeks, excluding
-- yesterday itself so a spike cannot raise the baseline it is measured against.
--
-- Median rather than mean: a single 400 DBU day would drag a mean upward and
-- suppress detection of the next spike. Median is unmoved by one outlier,
-- which is the entire reason to prefer it here.
--
-- Threshold 3x with a $5 floor. The floor matters more than the multiple: at
-- $0.30/day a 3x rise is $0.90 and worth nobody's attention, so a percentage
-- test alone produces exactly the noise that got cost detection rejected in
-- the first place. Requiring BOTH a multiple and an absolute floor is what
-- makes this alertable rather than annoying.
--
-- VERIFIED against real data: this would have fired on 2026-08-07 at 5.44x
-- ($41.36 against a $7.61 baseline).
-- ---------------------------------------------------------------------------

WITH daily AS (
    SELECT
        u.usage_date                                        AS usage_date,
        sum(u.usage_quantity * p.pricing.default)           AS usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
           ON p.sku_name       = u.sku_name
          AND p.currency_code  = 'USD'
          AND p.price_end_time IS NULL
    WHERE u.usage_date > current_date() - 30
    GROUP BY u.usage_date
),
baseline AS (
    SELECT
        percentile_approx(usd, 0.5) AS median_usd,
        count(*)                    AS baseline_days
    FROM daily
    -- Exclude the day under test. A spike must not inflate its own baseline.
    WHERE usage_date BETWEEN current_date() - 15 AND current_date() - 2
)
SELECT
    round(d.usd / nullif(b.median_usd, 0), 2)   AS spend_multiple,
    d.usage_date,
    round(d.usd, 2)                             AS usd_spent,
    round(b.median_usd, 2)                      AS baseline_usd,
    round(d.usd - b.median_usd, 2)              AS excess_usd
FROM daily d
CROSS JOIN baseline b
WHERE d.usage_date = current_date() - 1
  AND b.baseline_days >= 7
  AND b.median_usd > 0
  AND d.usd > b.median_usd * 3
  AND d.usd > 5.00;


-- ---------------------------------------------------------------------------
-- 2. TICKET -- always-on compute with no job or pipeline attached.
--
-- THE DETECTOR THAT WOULD HAVE SAVED $239.
--
-- The signal is not "expensive". It is "expensive with a nonzero floor every
-- single day, attached to nothing". A pipeline that runs daily produces spiky
-- usage with gaps. An endpoint with scale-to-zero disabled produces a flat
-- floor, because it bills for existing rather than for working.
--
-- min(daily) is therefore the discriminating statistic, not sum or max. A
-- workload that ever drops to zero is doing its job; one that never does is
-- being paid to wait.
--
-- Excluding rows carrying a dlt_pipeline_id or job_id removes everything this
-- project deliberately runs, leaving what nobody is watching.
--
-- VERIFIED: returns PREMIUM_SERVERLESS_REAL_TIME_INFERENCE at a 16 DBU floor
-- across 8 consecutive days, 1,358 DBU over the week.
-- ---------------------------------------------------------------------------

SELECT
    round(sum(daily_dbu), 1)                    AS dbu_last_7_days,
    sku_name,
    round(min(daily_dbu), 1)                    AS daily_floor_dbu,
    round(max(daily_dbu), 1)                    AS daily_peak_dbu,
    count(*)                                    AS days_with_usage,
    'Nonzero floor every day with no job or pipeline attached - check for '
    || 'scale-to-zero disabled on a serving endpoint or an idle warehouse'
                                                AS likely_cause
FROM (
    SELECT
        sku_name,
        usage_date,
        sum(usage_quantity) AS daily_dbu
    FROM system.billing.usage
    WHERE usage_date > current_date() - 8
      AND usage_metadata.dlt_pipeline_id IS NULL
      AND usage_metadata.job_id           IS NULL
    GROUP BY sku_name, usage_date
)
GROUP BY sku_name
HAVING min(daily_dbu) > 5      -- a real floor, not a rounding artifact
   AND count(*)      >= 6      -- sustained, not a two-day experiment
ORDER BY dbu_last_7_days DESC;


-- ---------------------------------------------------------------------------
-- 3. TICKET -- month-to-date spend on track to breach the monthly budget.
--
-- Databricks budgets are an ACCOUNT-level feature (accounts.cloud.databricks.com),
-- not reachable from a workspace API token -- both /api/2.0/budgets and
-- /api/2.1/budget-policies return 404 here. Rather than skip budget tracking,
-- the budget is expressed as a constant in this query.
--
-- That is a deliberate trade with a real advantage: the threshold lives in git
-- and changes through review, instead of being a number someone typed into a
-- console. The disadvantage is equally real -- this detects a breach, it
-- cannot prevent one. A true budget policy can block. State both.
--
-- Projection is linear on elapsed days. Crude, and correctly so: a smarter
-- forecast on 30 data points would be false precision. The question is only
-- "is this month heading somewhere unusual", and linear answers that.
--
-- MONTHLY_BUDGET_USD = 300. Chosen from measured history: the last 30 days
-- were $395.83, of which $239.35 was the idle endpoint. With that removed,
-- normal operation is around $156/month, so 300 leaves room for genuine work
-- while catching another always-on resource. Adjust deliberately, in a commit.
-- ---------------------------------------------------------------------------

WITH mtd AS (
    SELECT
        sum(u.usage_quantity * p.pricing.default)   AS usd_so_far,
        day(current_date())                         AS days_elapsed,
        day(last_day(current_date()))               AS days_in_month
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
           ON p.sku_name       = u.sku_name
          AND p.currency_code  = 'USD'
          AND p.price_end_time IS NULL
    WHERE u.usage_date >= date_trunc('MONTH', current_date())
)
SELECT
    round(usd_so_far / nullif(days_elapsed, 0) * days_in_month, 2)
                                        AS projected_month_usd,
    round(usd_so_far, 2)                AS spent_so_far_usd,
    300.00                              AS budget_usd,
    days_elapsed,
    days_in_month,
    round(
        (usd_so_far / nullif(days_elapsed, 0) * days_in_month) / 300.00 * 100, 1
    )                                   AS projected_pct_of_budget
FROM mtd
WHERE days_elapsed >= 5    -- projections from 2 days of data are meaningless
  AND usd_so_far / nullif(days_elapsed, 0) * days_in_month > 300.00;


-- ---------------------------------------------------------------------------
-- 4. DIAGNOSTIC -- cost attribution by project tag.
--
-- Not alertable; this is the question a dashboard answers.
--
-- custom_tags comes from the `tags:` blocks declared on the pipelines and job
-- in resources/*.yml. Before those existed, spend could be grouped by resource
-- id but not by project or environment -- and since dev and prod currently
-- publish to the same catalog, nothing else distinguished them.
--
-- Rows where project IS NULL are untagged spend. That bucket is the useful
-- one: it is everything running in this workspace that no declared resource
-- claims, which is exactly where the idle endpoint was hiding.
-- ---------------------------------------------------------------------------

SELECT
    coalesce(u.custom_tags['project'],     '(untagged)')  AS project,
    coalesce(u.custom_tags['environment'], '(untagged)')  AS environment,
    round(sum(u.usage_quantity), 2)                       AS dbus,
    round(sum(u.usage_quantity * p.pricing.default), 2)   AS est_usd,
    count(DISTINCT u.usage_date)                          AS active_days
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
       ON p.sku_name       = u.sku_name
      AND p.currency_code  = 'USD'
      AND p.price_end_time IS NULL
WHERE u.usage_date > current_date() - 30
GROUP BY 1, 2
ORDER BY est_usd DESC NULLS LAST;


-- ---------------------------------------------------------------------------
-- 5. DIAGNOSTIC -- unit economics: cost per 1,000 rows processed.
--
-- Absolute spend says nothing about efficiency. $8 is fine for a pipeline
-- moving millions of rows and terrible for one moving four thousand. Dividing
-- by rows produces a number that stays comparable as volume changes, which is
-- what makes it useful for judging whether an optimization actually helped.
--
-- At this project's volume the figure is dominated by fixed serverless
-- start-up cost rather than by per-row work, so it is a baseline to compare
-- against later, not a number to celebrate.
-- ---------------------------------------------------------------------------

WITH pipeline_cost AS (
    SELECT
        u.usage_metadata.dlt_pipeline_id                      AS pipeline_id,
        sum(u.usage_quantity * p.pricing.default)             AS usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices p
           ON p.sku_name       = u.sku_name
          AND p.currency_code  = 'USD'
          AND p.price_end_time IS NULL
    WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL
      AND u.usage_date > current_date() - 30
    GROUP BY 1
),
rows_moved AS (
    SELECT count(*) AS n FROM finguard.silver.transactions
)
SELECT
    round(c.usd / nullif(r.n, 0) * 1000, 4)   AS usd_per_1k_rows,
    c.pipeline_id,
    round(c.usd, 2)                           AS usd_30d,
    r.n                                       AS rows_in_silver
FROM pipeline_cost c
CROSS JOIN rows_moved r
ORDER BY usd_per_1k_rows DESC NULLS LAST;
