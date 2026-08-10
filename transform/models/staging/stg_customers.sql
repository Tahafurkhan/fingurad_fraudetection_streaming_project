-- Staging: customers.
--
-- Customers arrive by CDC from Postgres, so silver holds one row per observed
-- state rather than one row per customer. Staging keeps every version -- the
-- snapshot downstream is what turns them into effective-dated history.
--
-- Deduplicating here would destroy exactly the information SCD2 needs.

select
    customer_id,
    first_name,
    last_name,
    concat_ws(' ', first_name, last_name)   as customer_name,
    gender,
    age,
    city                                    as customer_city,
    state                                   as customer_state,
    country                                 as customer_country,
    annual_income,

    -- Mutable attributes. These are the columns the SCD2 snapshot watches:
    -- a change in any of them opens a new version.
    customer_segment,
    risk_score,
    transaction_limit,

    preferred_spending_min,
    preferred_spending_max,
    preferred_city,
    preferred_country,
    trusted_device_id,
    card_type,

    -- Card number is carried for joining to the fraud watchlist, which flags
    -- entity_id values. Masking belongs in the mart, not here, so the join key
    -- stays intact.
    card_number,

    email,
    account_open_date,
    silver_ingestion_timestamp

from {{ source('silver', 'customers') }}
