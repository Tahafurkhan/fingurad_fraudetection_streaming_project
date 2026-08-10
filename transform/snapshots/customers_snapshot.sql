{% snapshot customers_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_segment', 'risk_score', 'city', 'transaction_limit'],
        invalidate_hard_deletes=True
    )
}}

-- SCD2 history for customers.
--
-- Why this exists: a fraud alert must be evaluated against the customer's risk
-- profile *at the time of the transaction*, not their current one. If a
-- customer's risk_score rises from 30 to 85 today, a transaction from last week
-- should still be judged against 30 -- otherwise every historical alert silently
-- rewrites itself whenever the profile changes, and the audit trail is
-- worthless.
--
-- Strategy is `check` rather than `timestamp`. The timestamp strategy needs a
-- reliable updated_at from the source; silver.customers has
-- silver_ingestion_timestamp, but that changes on every CDC re-ingest whether
-- or not anything meaningful changed. Using it would open a new version on
-- every pipeline run. `check` compares the watched columns directly, so a
-- version opens only when one of them actually differs.
--
-- check_cols is deliberately narrow -- the four attributes that drive fraud
-- evaluation. Watching every column would version on cosmetic changes (an
-- email correction, a name spelling fix) and bloat the dimension with rows
-- that carry no analytical meaning.
--
-- invalidate_hard_deletes closes a row when the customer disappears from the
-- source, rather than leaving it open forever as if still current.

select
    customer_id,
    customer_name,
    first_name,
    last_name,
    customer_segment,
    risk_score,
    transaction_limit,
    customer_city   as city,
    customer_state  as state,
    customer_country as country,
    annual_income,
    card_type,
    card_number,
    email,
    account_open_date,
    silver_ingestion_timestamp

from {{ ref('stg_customers') }}

{% endsnapshot %}
