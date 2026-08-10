-- Staging: unified alert stream.
--
-- The two streaming alert tables have different shapes because they detect
-- different things -- a watchlist match carries watchlist metadata, a limit
-- breach carries the limit that was breached. Downstream analysis mostly wants
-- "an alert happened", so this model unions them onto a common grain.
--
-- Nothing is recomputed here. The alerting rules live in the Lakeflow pipeline
-- and this model only reshapes what the pipeline already decided. That is the
-- boundary: streaming owns detection, dbt owns analysis of detections.

with watchlist_alerts as (

    select
        alert_id,
        alert_type,
        transaction_id,
        customer_id,
        merchant_id,
        amount                      as transaction_amount,
        currency,
        merchant_name,
        merchant_category,
        transaction_type,
        payment_channel,
        transaction_city,
        transaction_country,
        is_international,
        transaction_status,
        transaction_timestamp,
        alert_timestamp,

        -- Watchlist-specific context.
        risk_level                  as alert_risk_level,
        reason_code,
        reason_description,
        watchlist_id,
        watch_type,

        -- Not applicable to this alert type.
        cast(null as double)        as transaction_limit

    from {{ source('gold', 'fraud_card_alert') }}

),

limit_alerts as (

    select
        alert_id,
        alert_type,
        transaction_id,
        customer_id,

        -- gold.high_value_transactions_alert carries merchant_name but not
        -- merchant_id, because the stream-static join selected only the
        -- descriptive column. Left null rather than guessed at.
        cast(null as string)        as merchant_id,

        transaction_amount,
        currency,
        merchant_name,
        merchant_category,
        transaction_type,
        payment_channel,
        city                        as transaction_city,
        country                     as transaction_country,
        is_international,
        status                      as transaction_status,
        transaction_timestamp,
        alert_timestamp,

        -- A limit breach has no watchlist context; severity is implied by how
        -- far the amount exceeded the limit rather than by a risk grade.
        cast(null as string)        as alert_risk_level,
        cast(null as string)        as reason_code,
        cast(null as string)        as reason_description,
        cast(null as string)        as watchlist_id,
        cast(null as string)        as watch_type,

        transaction_limit

    from {{ source('gold', 'high_value_transactions_alert') }}

)

select * from watchlist_alerts
union all
select * from limit_alerts
