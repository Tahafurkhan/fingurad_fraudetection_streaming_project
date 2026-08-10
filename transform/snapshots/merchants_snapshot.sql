{% snapshot merchants_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='merchant_id',
        strategy='check',
        check_cols=['merchant_risk', 'is_blacklisted', 'merchant_category', 'merchant_city'],
        invalidate_hard_deletes=True
    )
}}

-- SCD2 history for merchants.
--
-- The business question this answers: when a merchant is blacklisted, which
-- transactions were authorised *before* that decision? Without history the
-- answer is unobtainable -- the current state overwrites the past, and every
-- transaction looks like it was made against a blacklisted merchant.
--
-- merchant_risk and is_blacklisted are the two attributes that feed fraud
-- scoring (weights 25 and 60 in the engine), so they are the ones worth
-- versioning. Category and city are included because a merchant changing
-- category is itself a fraud signal -- a "Grocery" that becomes "Jewellery"
-- overnight is worth a second look.
--
-- merchant_name is deliberately NOT watched: names get corrected for spelling
-- and formatting, and a new version per typo fix would bury the real signal.

select
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_city,
    merchant_country,
    merchant_risk,
    is_blacklisted,
    requires_review,
    silver_ingestion_timestamp

from {{ ref('stg_merchants') }}

{% endsnapshot %}
