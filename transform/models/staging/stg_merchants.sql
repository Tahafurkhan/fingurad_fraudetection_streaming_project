-- Staging: merchants.
--
-- Staging does renaming, light typing and nothing else. No joins, no business
-- logic, no filtering. That discipline is what keeps the layer boring enough to
-- be trustworthy -- if a number is wrong downstream, staging is not where you
-- look.
--
-- Silver has already deduplicated to one row per merchant_id, so this is
-- genuinely a projection.

select
    merchant_id,
    merchant_name,
    merchant_category,
    city                        as merchant_city,
    country                     as merchant_country,
    merchant_risk,
    is_blacklisted,
    requires_review,
    source_file,
    bronze_ingestion_timestamp,
    silver_ingestion_timestamp

from {{ source('silver', 'merchants') }}
