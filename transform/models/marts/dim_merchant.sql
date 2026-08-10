-- Merchant dimension, SCD2.
--
-- Built on the snapshot rather than on staging, so every historical version is
-- available for point-in-time joins. Querying "current state" means filtering
-- is_current, which is one predicate; querying "state as of a date" means a
-- range predicate on the validity window.

{{ config(materialized='table') }}

with snapshot_data as (

    select * from {{ ref('merchants_snapshot') }}

),

versioned as (

    select
        -- Surrogate key. The natural key alone is not unique in an SCD2 table
        -- -- a merchant with three versions has three rows sharing merchant_id
        -- -- so facts must join on the surrogate, not the natural key.
        {{ dbt_utils.generate_surrogate_key(['merchant_id', 'dbt_valid_from']) }}
            as merchant_sk,

        merchant_id,
        merchant_name,
        merchant_category,
        merchant_city,
        merchant_country,
        merchant_risk,
        is_blacklisted,
        requires_review,

        -- SCD2 cold start: the first version is backdated so it covers facts
        -- that predate the first snapshot run. See dim_customer for the full
        -- reasoning -- without this every historical transaction joins to a
        -- null dimension row.
        case
            when row_number() over (
                     partition by merchant_id order by dbt_valid_from
                 ) = 1
            then timestamp '1900-01-01 00:00:00'
            else dbt_valid_from
        end as valid_from,

        -- dbt leaves valid_to NULL on the current row. Coalescing to a far
        -- future date makes range joins uniform: `between valid_from and
        -- valid_to` works without a special case for the current version.
        coalesce(dbt_valid_to, timestamp '9999-12-31 23:59:59') as valid_to,

        dbt_valid_to is null as is_current,

        -- Version number per merchant, useful for demonstrating that history
        -- is actually accumulating.
        row_number() over (
            partition by merchant_id
            order by dbt_valid_from
        ) as version_number

    from snapshot_data

)

select * from versioned
