-- Customer dimension, SCD2.
--
-- The reason this project needs SCD2 at all: fraud alerts must be evaluated
-- against the risk profile that was current when the transaction happened.
-- A type-1 dimension would silently rewrite the basis of every historical
-- alert each time a customer's risk score changed.

{{ config(materialized='table') }}

with snapshot_data as (

    select * from {{ ref('customers_snapshot') }}

),

versioned as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}
            as customer_sk,

        customer_id,
        customer_name,
        first_name,
        last_name,

        -- Attributes that drive fraud evaluation, versioned.
        customer_segment,
        risk_score,
        transaction_limit,

        city,
        state,
        country,
        annual_income,
        card_type,

        -- PAN is masked here rather than in staging: staging keeps the full
        -- value so the fraud-watchlist join still works on entity_id, and the
        -- dimension exposes only what an analyst needs to identify a card.
        -- A column mask in Unity Catalog is the stronger control; this is
        -- defence in depth, not a substitute.
        concat('****-****-****-', right(card_number, 4)) as card_number_masked,

        email,
        account_open_date,

        -- Risk banding, computed once here so downstream models and dashboards
        -- do not each invent their own thresholds.
        case
            when risk_score >= 70 then 'HIGH'
            when risk_score >= 40 then 'MEDIUM'
            else 'LOW'
        end as risk_band,

        -- SCD2 cold start.
        --
        -- dbt stamps dbt_valid_from at the moment the snapshot first runs, not
        -- at the moment the record actually became true. Every row therefore
        -- starts life valid from "today", which breaks point-in-time joins for
        -- any fact that predates the first snapshot -- and the whole existing
        -- transaction history predates it.
        --
        -- The first version of each entity is backdated to the beginning of
        -- time so it covers all prior history. Genuine later versions keep
        -- their real dbt_valid_from, so change tracking from this point on is
        -- accurate. Without this the dimension is technically correct and
        -- practically useless: every historical fact joins to nothing.
        case
            when row_number() over (
                     partition by customer_id order by dbt_valid_from
                 ) = 1
            then timestamp '1900-01-01 00:00:00'
            else dbt_valid_from
        end as valid_from,

        coalesce(dbt_valid_to, timestamp '9999-12-31 23:59:59') as valid_to,
        dbt_valid_to is null as is_current,

        row_number() over (
            partition by customer_id
            order by dbt_valid_from
        ) as version_number

    from snapshot_data

)

select * from versioned
