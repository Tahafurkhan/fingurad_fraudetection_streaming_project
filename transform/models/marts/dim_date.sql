-- Date dimension.
--
-- Generated rather than sourced, because no upstream system owns "what is a
-- calendar". Covers 2024-2027, which brackets the transaction history with
-- room to grow.
--
-- Why a date dimension rather than date functions in each query: fraud
-- analysis constantly slices by is_weekend, hour-of-day and month. Computing
-- those inline means every analyst reinvents them, and they disagree -- one
-- treats Sunday as day 1, another as day 7. Defining them once makes the
-- answers consistent.

{{ config(materialized='table') }}

with date_spine as (

    select explode(
        sequence(
            to_date('2024-01-01'),
            to_date('2027-12-31'),
            interval 1 day
        )
    ) as date_day

)

select
    -- Integer surrogate key in yyyyMMdd form. Readable in a fact table
    -- without a join, which matters when eyeballing raw rows.
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,

    date_day                                        as full_date,
    year(date_day)                                  as year,
    quarter(date_day)                               as quarter,
    month(date_day)                                 as month,
    date_format(date_day, 'MMMM')                   as month_name,
    day(date_day)                                   as day_of_month,
    dayofweek(date_day)                             as day_of_week,
    date_format(date_day, 'EEEE')                   as day_name,
    weekofyear(date_day)                            as week_of_year,

    -- Spark's dayofweek is 1=Sunday .. 7=Saturday.
    dayofweek(date_day) in (1, 7)                   as is_weekend,

    -- Month boundaries are common fraud signals: salary-day spikes at the
    -- start, subscription renewals at the end.
    date_day = date_trunc('month', date_day)        as is_month_start,
    date_day = last_day(date_day)                   as is_month_end

from date_spine
