-- Merchant risk profile.
--
-- Merchants carry a declared risk rating from the source system. This model
-- computes an *observed* rating from actual transaction behaviour, so the two
-- can be compared.
--
-- That comparison is the point. A merchant rated LOW whose alert rate looks
-- like a HIGH merchant is either mis-rated or newly compromised, and neither
-- is visible from the declared rating alone. Surfacing the disagreement is
-- more useful than reporting either number by itself.

{{ config(materialized='table') }}

with transactions as (

    select * from {{ ref('fct_transactions') }}

),

alerts as (

    select
        merchant_id,
        count(*)                                                as alert_count,
        sum(case when alert_type = 'FRAUD_WATCHLIST_MATCH' then 1 else 0 end)
            as watchlist_alert_count,
        sum(case when alert_risk_level in ('HIGH','CRITICAL') then 1 else 0 end)
            as severe_alert_count,
        sum(transaction_amount)                                 as alerted_amount
    from {{ ref('fct_alerts') }}
    -- merchant_id is null for high-value alerts, which carry only the merchant
    -- name. Excluding them keeps this join keyed correctly rather than
    -- aggregating a null bucket.
    where merchant_id is not null
    group by merchant_id

),

merchant_activity as (

    select
        merchant_id,
        max(merchant_name)                            as merchant_name,
        max(merchant_category)                        as merchant_category,
        max(merchant_risk)                            as declared_risk,
        max(merchant_blacklisted)                     as is_blacklisted,

        count(*)                                      as transaction_count,
        count(distinct customer_id)                   as unique_customers,
        sum(amount)                                   as total_amount,
        round(avg(amount), 2)                         as avg_amount,
        max(amount)                                   as max_amount,
        round(stddev(amount), 2)                      as amount_stddev,

        sum(case when is_high_value then 1 else 0 end)          as high_value_count,
        sum(case when is_international then 1 else 0 end)        as international_count,
        sum(case when customer_risk_band = 'HIGH' then 1 else 0 end) as high_risk_customer_count,

        min(transaction_timestamp)                    as first_seen,
        max(transaction_timestamp)                    as last_seen

    from transactions
    group by merchant_id

),

combined as (

    select
        m.*,
        coalesce(a.alert_count, 0)           as alert_count,
        coalesce(a.watchlist_alert_count, 0) as watchlist_alert_count,
        coalesce(a.severe_alert_count, 0)    as severe_alert_count,
        coalesce(a.alerted_amount, 0)        as alerted_amount,

        round(
            100.0 * coalesce(a.alert_count, 0) / nullif(m.transaction_count, 0),
            2
        )                               as alert_rate_pct

    from merchant_activity m
    left join alerts a on a.merchant_id = m.merchant_id

)

select
    *,

    -- Observed risk, derived from behaviour rather than declaration.
    -- Thresholds are deliberately coarse: this is a triage signal pointing at
    -- merchants worth a human look, not a scoring model.
    case
        when is_blacklisted        then 'BLACKLISTED'
        when alert_rate_pct >= 50  then 'HIGH'
        when alert_rate_pct >= 20  then 'MEDIUM'
        else 'LOW'
    end as observed_risk,

    -- The actionable output: declared and observed disagree, with observed
    -- being worse. These are the merchants whose rating may be stale.
    case
        when is_blacklisted then false
        when declared_risk = 'LOW'
             and alert_rate_pct >= 20  then true
        when declared_risk = 'MEDIUM'
             and alert_rate_pct >= 50  then true
        else false
    end as risk_rating_understated

from combined
