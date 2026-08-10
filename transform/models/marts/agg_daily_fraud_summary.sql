-- Daily fraud summary.
--
-- The question this answers that the streaming layer cannot: "is our alerting
-- getting noisier?" A stream reports what is happening now; trend detection
-- needs history, and history is what the mart has.
--
-- Grain: one row per date per customer segment. Segment is included because an
-- alert-rate change concentrated in one segment means something different from
-- a uniform rise -- the first suggests a targeted attack, the second suggests
-- a rule that needs recalibrating.

{{ config(materialized='table') }}

with transactions as (

    select * from {{ ref('fct_transactions') }}

),

alerts as (

    select * from {{ ref('fct_alerts') }}

),

transaction_stats as (

    select
        transaction_date,
        customer_segment,
        count(*)                                          as transaction_count,
        count(distinct customer_id)                       as active_customers,
        count(distinct merchant_id)                       as merchants_used,
        sum(amount)                                       as total_amount,
        round(avg(amount), 2)                             as avg_amount,
        max(amount)                                       as max_amount,
        sum(case when is_international then 1 else 0 end) as international_count

    from transactions
    group by transaction_date, customer_segment

),

alert_stats as (

    select
        cast(transaction_timestamp as date)                     as transaction_date,
        customer_segment,
        count(*)                                                as alert_count,

        -- Alert types come from the pipeline, not recomputed here.
        sum(case when alert_type = 'FRAUD_WATCHLIST_MATCH' then 1 else 0 end)
            as watchlist_matches,
        sum(case when alert_type = 'HIGH_VALUE_TRANSACTION' then 1 else 0 end)
            as high_value_alerts,

        -- Severity as graded by the watchlist at match time.
        sum(case when alert_risk_level = 'CRITICAL' then 1 else 0 end) as critical_alerts,
        sum(case when alert_risk_level = 'HIGH' then 1 else 0 end)     as high_alerts,

        sum(case when merchant_blacklisted then 1 else 0 end)   as blacklisted_merchant_hits,
        sum(transaction_amount)                                 as amount_at_risk,

        -- Detection latency: how fast the streaming layer raised alerts. This
        -- is the pipeline's SLA, measurable because both timestamps survive
        -- into the mart.
        round(avg(detection_latency_seconds), 1)                as avg_detection_latency_sec,
        max(detection_latency_seconds)                          as max_detection_latency_sec

    from alerts
    group by 1, 2

)

select
    t.transaction_date,
    d.day_name,
    d.is_weekend,
    t.customer_segment,

    t.transaction_count,
    t.active_customers,
    t.merchants_used,
    t.total_amount,
    t.avg_amount,
    t.max_amount,
    t.international_count,

    -- Alerts may legitimately be absent on a quiet day, so coalesce rather
    -- than letting an inner join drop the row entirely -- a day with zero
    -- alerts is a meaningful data point, not a missing one.
    coalesce(a.alert_count, 0)               as alert_count,
    coalesce(a.watchlist_matches, 0)         as watchlist_matches,
    coalesce(a.high_value_alerts, 0)         as high_value_alerts,
    coalesce(a.critical_alerts, 0)           as critical_alerts,
    coalesce(a.high_alerts, 0)               as high_alerts,
    coalesce(a.blacklisted_merchant_hits, 0) as blacklisted_merchant_hits,
    coalesce(a.amount_at_risk, 0)            as amount_at_risk,

    -- Null rather than zero when there are no alerts: an absent latency is
    -- unknown, not instant.
    a.avg_detection_latency_sec,
    a.max_detection_latency_sec,

    -- The headline metric. Expressed per hundred transactions so a rate change
    -- is readable without mental arithmetic.
    round(
        100.0 * coalesce(a.alert_count, 0) / nullif(t.transaction_count, 0),
        2
    )                                        as alert_rate_pct

from transaction_stats t
left join alert_stats a
       on a.transaction_date = t.transaction_date
      and a.customer_segment = t.customer_segment
left join {{ ref('dim_date') }} d
       on d.full_date = t.transaction_date
