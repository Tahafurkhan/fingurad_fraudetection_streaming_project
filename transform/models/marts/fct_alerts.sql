-- Alert fact.
--
-- Sources from the streaming gold alerts rather than re-evaluating the rules.
-- The Lakeflow pipeline decides what is an alert; this model attaches
-- dimensional context and makes the result analysable over time.
--
-- The value added over the raw streaming tables is point-in-time attribution.
-- gold.high_value_transactions_alert joins silver.customers with spark.read,
-- which snapshots a continuously-updating table -- so its customer attributes
-- reflect whenever the stream happened to start, not the transaction. Joining
-- the SCD2 dimensions on the validity window fixes that for analysis, without
-- touching the operational path.

{{
    config(
        materialized='incremental',
        unique_key='alert_id',
        incremental_strategy='merge',
        file_format='delta'
    )
}}

with alerts as (

    select * from {{ ref('stg_alerts') }}

    {% if is_incremental() %}
    -- Overlap window: alerts can arrive slightly out of order relative to
    -- transaction time because the stream is watermarked. merge on alert_id
    -- makes reprocessing idempotent.
    where transaction_timestamp >= (
        select coalesce(max(transaction_timestamp), timestamp '1900-01-01')
               - interval 3 days
        from {{ this }}
    )
    {% endif %}

),

deduplicated as (

    -- The same transaction can match several watchlist entries, producing one
    -- alert row per match with the same alert_id. Keep the highest-risk match
    -- so alert_id stays a unique key.
    select * from (
        select
            *,
            row_number() over (
                partition by alert_id
                order by
                    case alert_risk_level
                        when 'CRITICAL' then 1
                        when 'HIGH'     then 2
                        when 'MEDIUM'   then 3
                        when 'LOW'      then 4
                        else 5
                    end,
                    alert_timestamp desc
            ) as _rank
        from alerts
    )
    where _rank = 1

),

with_customer as (

    select
        a.*,
        c.customer_sk,
        c.customer_segment,
        c.risk_score        as customer_risk_score,
        c.risk_band         as customer_risk_band,
        c.transaction_limit as customer_limit_at_time,
        c.city              as customer_city

    from deduplicated a
    left join {{ ref('dim_customer') }} c
           on c.customer_id = a.customer_id
          -- Point-in-time: the customer profile in force when the transaction
          -- occurred, not the current one.
          and a.transaction_timestamp >= c.valid_from
          and a.transaction_timestamp <  c.valid_to

),

with_merchant as (

    select
        a.*,
        m.merchant_sk,
        m.merchant_risk,
        m.is_blacklisted    as merchant_blacklisted

    from with_customer a
    left join {{ ref('dim_merchant') }} m
           on m.merchant_id = a.merchant_id
          and a.transaction_timestamp >= m.valid_from
          and a.transaction_timestamp <  m.valid_to

)

select
    alert_id,
    alert_type,
    transaction_id,

    -- Dimension keys.
    customer_sk,
    merchant_sk,
    cast(date_format(transaction_timestamp, 'yyyyMMdd') as int) as date_key,

    -- Natural keys for traceability.
    customer_id,
    merchant_id,

    transaction_amount,
    currency,
    transaction_limit,

    -- Context as it was at transaction time.
    customer_segment,
    customer_risk_score,
    customer_risk_band,
    customer_limit_at_time,
    customer_city,
    merchant_name,
    merchant_category,
    merchant_risk,
    merchant_blacklisted,

    -- Alert detail from the pipeline.
    alert_risk_level,
    reason_code,
    reason_description,
    watchlist_id,
    watch_type,

    transaction_type,
    payment_channel,
    transaction_city,
    transaction_country,
    is_international,
    transaction_status,

    -- How far a limit breach exceeded the limit. Null for watchlist alerts,
    -- which have no limit to compare against.
    case
        when transaction_limit is not null and transaction_limit > 0
        then round(transaction_amount / transaction_limit, 2)
    end as limit_breach_ratio,

    transaction_timestamp,
    alert_timestamp,

    -- Detection latency: how long the pipeline took to raise the alert after
    -- the transaction occurred. This is a real SLA metric, measurable because
    -- both timestamps are preserved.
    unix_timestamp(alert_timestamp) - unix_timestamp(transaction_timestamp)
        as detection_latency_seconds,

    current_timestamp() as dbt_loaded_at

from with_merchant
