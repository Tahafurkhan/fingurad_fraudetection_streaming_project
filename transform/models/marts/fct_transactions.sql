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
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='append_new_columns',
        liquid_clustered_by=['customer_id', 'transaction_date'],
        incremental_predicates=[
            "DBT_INTERNAL_DEST.transaction_timestamp >= "
            "current_timestamp() - interval 4 days"
        ]
    )
}}

with source_transactions as (

    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    -- Only consider transactions newer than what has already been loaded.
    -- The 3-day overlap is deliberate: silver is fed by a watermarked stream,
    -- so a transaction can land after transactions with later event times.
    -- Without the overlap those stragglers would be skipped permanently.
    -- merge on transaction_id makes reprocessing the overlap idempotent.
    where transaction_timestamp >= (
        select coalesce(max(transaction_timestamp), timestamp '1900-01-01')
               - interval 3 days
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

    -- Kafka provenance, carried all the way to the mart.
    t.kafka_topic,
    t.kafka_partition,
    t.kafka_offset,

    current_timestamp() as dbt_loaded_at

from transactions t
left join customer_at_transaction_time c using (transaction_id)
left join merchant_at_transaction_time m using (transaction_id)
