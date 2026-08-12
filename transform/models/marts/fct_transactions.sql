-- Transaction fact, incremental with point-in-time dimension joins.
--
-- Two things make this more than a copy of staging:
--
-- 1. It is incremental. Reprocessing every transaction on every run is
--    wasteful and, at real volume, impossible. `merge` with a unique key means
--    a late-arriving correction updates the existing row rather than
--    duplicating it.
--
-- 2. It joins the SCD2 dimensions *as of the transaction timestamp*, not as of
--    now. This is the whole point of the dimensional layer: a transaction from
--    last week is attributed to the customer risk profile and merchant risk
--    rating that were in force last week.

-- PHYSICAL LAYOUT. Clustered on the columns that appear in predicates:
-- customer_id for investigation lookups, transaction_date for reporting
-- ranges. Declared in the model rather than applied by an ALTER so the layout
-- is rebuilt with the table and shows up in code review -- an ALTER against a
-- live table is invisible to the repository and drifts silently.
--
-- The config key is `liquid_clustered_by`. dbt-databricks does NOT read
-- `cluster_by` -- and dbt silently accepts unknown config keys, so the wrong
-- name parses, lands in the manifest, and applies nothing while the run
-- reports success.
--
-- MEASURED HONESTLY: this table is currently a single 129KB file. Clustering
-- prunes files, and there is exactly one, so it prunes nothing. An earlier
-- benchmark appeared to show an 8-13% gain; that was warehouse warm-up, not
-- clustering, and the number is not claimed anywhere. The keys are correct for
-- the access pattern at scale and inert at this volume. See
-- docs/performance_optimization.md for the measurements.
--
-- MERGE COST. `incremental_predicates` bounds the target side of the merge.
-- Without it, MERGE scans the whole target table to find matches for the
-- incoming rows -- the merge gets more expensive every run as history
-- accumulates, even though the batch stays the same size. Restricting it to
-- the same window the source filter uses means the merge only touches recent
-- files, which is the difference between a merge that scales and one that
-- degrades.
--
-- WHY THERE IS NO incremental_predicates HERE ANY MORE.
--
-- There was. It bounded the target side to a few days:
--
--     DBT_INTERNAL_DEST.transaction_timestamp >= current_timestamp()
--         - interval 4 days
--
-- and it produced duplicate transaction_ids in production. The unique test
-- caught four:
--
--     TXN502842  transaction_timestamp 2026-07-13, loaded 2026-08-11
--     TXN502842  transaction_timestamp 2026-08-11, loaded 2026-08-12
--
-- Same business key, event times a month apart. The predicate meant MERGE only
-- considered target rows from the last four days as match candidates, so the
-- July copy was invisible and MERGE inserted rather than updated.
--
-- The cause is upstream and legitimate: bronze reads `startingOffsets:
-- earliest`, so a checkpoint reset replays the retention window and the same
-- transaction_id arrives again carrying a *different* event timestamp. The
-- model already documents this for the source-side dedup ("TXN266669 exists at
-- partition 1 offsets 146 and 392 with different event timestamps") -- the
-- error was assuming the target side could be bounded by event time when the
-- key it matches on is not event-time-correlated at all.
--
-- ANY time-bounded predicate is wrong here for that reason, not just a
-- too-narrow one: a redelivery can carry any event time within the retention
-- window, so no window short enough to help is also wide enough to be correct.
-- Widening it to cover retention would scan the whole table anyway, which is
-- what the predicate existed to avoid.
--
-- The cost of removing it is a full-target scan per merge, which grows with
-- history. That is a real scaling concern and the honest mitigation is liquid
-- clustering on transaction_date plus Delta's file skipping, not a predicate
-- that trades correctness for speed. Revisit if merge time becomes the
-- bottleneck; correctness first.
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='append_new_columns',
        liquid_clustered_by=['customer_id', 'transaction_date']
    )
}}

with source_transactions as (

    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    -- Only consider transactions newer than what has already been loaded.
    -- The overlap is deliberate: silver is fed by a watermarked stream, so a
    -- transaction can land after transactions with later event times. Without
    -- the overlap those stragglers would be skipped permanently. merge on
    -- transaction_id makes reprocessing the overlap idempotent.
    --
    -- The window comes from the `late_arrival_window_days` var rather than
    -- being written here, because marts.late_arrival_monitor computes headroom
    -- against the same number. A monitor reporting slack against a different
    -- window than the filter actually uses would be actively misleading.
    where transaction_timestamp >= (
        select coalesce(max(transaction_timestamp), timestamp '1900-01-01')
               - interval {{ var('late_arrival_window_days') }} days
        from {{ this }}
    )
    {% endif %}

),

transactions as (

    -- Deduplicate on the natural key.
    --
    -- Silver contains genuine duplicates: the same transaction_id appearing at
    -- different Kafka offsets. Verified in the data -- TXN266669 exists at
    -- partition 1 offsets 146 and 392 with different event timestamps. That is
    -- the expected consequence of at-least-once delivery combined with offset
    -- replay; bronze reads `startingOffsets: earliest`, so a rebuilt table
    -- re-reads the retention window.
    --
    -- Bronze and silver keep every copy deliberately -- they are the audit
    -- record, and discarding a physically delivered message there would lose
    -- the evidence that redelivery happened. The mart is where a transaction
    -- must mean one business event, so deduplication belongs here.
    --
    -- Highest kafka_offset wins: the most recently delivered copy is the one
    -- most likely to reflect any upstream correction. Ordering by offset
    -- rather than by event timestamp keeps the choice deterministic even when
    -- two copies share a timestamp.
    select * from (
        select
            *,
            row_number() over (
                partition by transaction_id
                order by kafka_partition, kafka_offset desc
            ) as _dedup_rank
        from source_transactions
    )
    where _dedup_rank = 1

),

customer_at_transaction_time as (

    select
        t.transaction_id,
        c.customer_sk,
        c.customer_segment,
        c.risk_score,
        c.risk_band,
        c.transaction_limit

    from transactions t
    left join {{ ref('dim_customer') }} c
           on c.customer_id = t.customer_id
          -- Point-in-time: the version in force when the transaction occurred.
          and t.transaction_timestamp >= c.valid_from
          and t.transaction_timestamp <  c.valid_to

),

merchant_at_transaction_time as (

    select
        t.transaction_id,
        m.merchant_sk,
        m.merchant_name,
        m.merchant_category,
        m.merchant_risk,
        m.is_blacklisted,
        m.requires_review

    from transactions t
    left join {{ ref('dim_merchant') }} m
           on m.merchant_id = t.merchant_id
          and t.transaction_timestamp >= m.valid_from
          and t.transaction_timestamp <  m.valid_to

)

select
    t.transaction_id,

    -- Dimension keys (surrogate, so they point at the correct version).
    c.customer_sk,
    m.merchant_sk,

    -- Date key. Conforms this fact to dim_date, which fct_alerts already used
    -- and this table did not -- so the two facts could not be sliced by the
    -- same calendar without one of them recomputing date parts inline. That is
    -- exactly the inconsistency a date dimension exists to prevent: two
    -- analysts deriving "is_weekend" separately and disagreeing about whether
    -- the week starts on Sunday.
    --
    -- Derived from transaction_timestamp (event time), never from
    -- dbt_loaded_at. A transaction that occurred on the 10th and arrived on the
    -- 13th belongs to the 10th; keying it by arrival would move revenue between
    -- days and make every daily total unreproducible.
    cast(date_format(t.transaction_timestamp, 'yyyyMMdd') as int) as date_key,

    -- Natural keys retained for traceability and ad-hoc querying.
    t.customer_id,
    t.merchant_id,

    -- Measures.
    t.amount,
    t.currency,

    -- Descriptive attributes as they were at transaction time.
    c.customer_segment,
    c.risk_score      as customer_risk_score,
    c.risk_band       as customer_risk_band,
    c.transaction_limit,
    m.merchant_name,
    m.merchant_category,
    m.merchant_risk,
    m.is_blacklisted  as merchant_blacklisted,
    m.requires_review as merchant_requires_review,

    t.transaction_type,
    t.payment_channel,
    t.device_id,
    t.transaction_city,
    t.transaction_country,
    t.is_international,
    t.transaction_status,

    -- Derived flags. Thresholds come from dbt_project.yml vars so the rule
    -- lives in one place rather than being restated per model.
    t.amount > {{ var('high_value_threshold') }}          as is_high_value,
    coalesce(t.amount > c.transaction_limit, false)       as exceeds_customer_limit,

    t.transaction_timestamp,
    t.transaction_date,

    -- ---------------------------------------------------------------
    -- LATE ARRIVAL MEASUREMENT
    --
    -- The 3-day overlap above handles late arrivals defensively: it
    -- reprocesses a window so stragglers are not skipped. What it never did
    -- was RECORD anything, which leaves two questions unanswerable.
    --
    --   1. Is 3 days the right window? It was chosen by judgement. If real
    --      lateness is under an hour it is wasteful; if anything exceeds
    --      3 days it is silently losing rows. Both cases look identical from
    --      outside -- a green run, a plausible row count.
    --
    --   2. Did we lose anything? A transaction arriving outside the window is
    --      not rejected or quarantined. It simply never matches the source
    --      filter, and no table anywhere records that it existed.
    --
    -- Measuring it costs three columns and makes the window an evidence-based
    -- setting instead of a guess. See marts.late_arrival_monitor for the
    -- aggregate and the alerting threshold.
    -- ---------------------------------------------------------------

    -- Event time to landing in silver. This is arrival lateness as the
    -- warehouse can observe it: the delay between the transaction occurring
    -- and this platform having it available to model.
    --
    -- silver_ingestion_timestamp rather than bronze: bronze is when the
    -- message was received, silver is when it became a usable typed row. The
    -- gap between them is this platform's own processing latency, which
    -- belongs in the SLA tables, not in a fact about the transaction.
    round(
        (unix_timestamp(t.silver_ingestion_timestamp)
         - unix_timestamp(t.transaction_timestamp)) / 3600.0,
        3
    ) as arrival_delay_hours,

    -- Whether this row arrived late enough to have needed the overlap window.
    -- A row is "late" when it landed on a later calendar day than it occurred.
    -- Day granularity rather than a fixed number of hours because the fact is
    -- reported daily: a transaction that crosses a day boundary is the one
    -- that changes an already-published daily total.
    t.silver_ingestion_timestamp::date > t.transaction_timestamp::date
        as is_late_arrival,

    -- THE ONE THAT MATTERS FOR TUNING THE WINDOW.
    --
    -- How close this row came to falling outside the 3-day overlap. A value
    -- approaching 3 means the window is about to start losing data silently.
    -- Exposed as a plain number so an alert can watch max() rather than
    -- someone remembering to check.
    --
    -- Rows that DID fall outside the window cannot appear here -- they never
    -- entered the model. That is the honest limit of this measurement: it
    -- shows the window narrowing, not what has already been lost. Bounding
    -- that requires reconciling against bronze, which is what the
    -- reconciliation check in ops does.
    round(
        datediff(t.silver_ingestion_timestamp, t.transaction_timestamp),
        0
    ) as arrival_delay_days,

    -- Kafka provenance, carried all the way to the mart.
    t.kafka_topic,
    t.kafka_partition,
    t.kafka_offset,

    current_timestamp() as dbt_loaded_at

from transactions t
left join customer_at_transaction_time c using (transaction_id)
left join merchant_at_transaction_time m using (transaction_id)
