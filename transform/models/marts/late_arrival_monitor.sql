-- Late-arrival profile, per day.
--
-- WHY THIS MODEL EXISTS
-- ---------------------
-- fct_transactions reprocesses a 3-day overlap window so that transactions
-- arriving out of order are not skipped. That number was chosen by judgement,
-- and until now nothing measured whether it was right.
--
-- Both ways of being wrong are invisible from outside:
--
--   too wide   -- every run reprocesses three days of history to catch
--                 stragglers that all arrive within an hour. Pure waste, and
--                 waste that grows with the table.
--
--   too narrow -- transactions arriving after the window never match the
--                 source filter. They are not rejected, not quarantined, not
--                 logged. They simply never appear, and the run is green.
--
-- This model turns the window from a guess into a setting with evidence behind
-- it, and gives the alert in ops something to watch.
--
-- WHAT "LATE" MEANS HERE
-- ----------------------
-- Event time to silver arrival: the transaction occurred, and some time later
-- this platform had a usable typed row for it. That delay is mostly upstream --
-- Kafka delivery, producer retries, network partitions, a consumer catching up
-- after an outage. It is NOT this platform's processing time, which lives in
-- ops.job_sla.
--
-- Distinguishing the two matters when the number moves: rising arrival delay
-- means something upstream is struggling, and no amount of tuning here will
-- change it.

{{
    config(
        materialized='table',
        file_format='delta'
    )
}}

with transactions as (

    select
        transaction_date,
        arrival_delay_hours,
        arrival_delay_days,
        is_late_arrival
    from {{ ref('fct_transactions') }}
    -- Rows predating the lateness columns have nulls; excluding them keeps the
    -- percentiles honest rather than treating "not measured" as "not late".
    where arrival_delay_hours is not null

),

daily as (

    select
        transaction_date,

        count(*)                                          as transactions,
        sum(case when is_late_arrival then 1 else 0 end)  as late_transactions,

        round(avg(arrival_delay_hours), 3)                as avg_delay_hours,

        -- Percentiles, not just the average.
        --
        -- Arrival delay is heavily right-skewed: nearly everything lands in
        -- seconds and a handful of retries land hours later. An average over
        -- that distribution is dragged around by the tail and describes no
        -- actual transaction. p50 says what normal looks like; p99 and max say
        -- what the overlap window has to survive.
        round(percentile_approx(arrival_delay_hours, 0.50), 3) as p50_delay_hours,
        round(percentile_approx(arrival_delay_hours, 0.95), 3) as p95_delay_hours,
        round(percentile_approx(arrival_delay_hours, 0.99), 3) as p99_delay_hours,
        round(max(arrival_delay_hours), 3)                     as max_delay_hours,

        max(arrival_delay_days)                                as max_delay_days

    from transactions
    group by transaction_date

)

select
    transaction_date,
    transactions,
    late_transactions,

    round(100.0 * late_transactions / nullif(transactions, 0), 2) as late_pct,

    avg_delay_hours,
    p50_delay_hours,
    p95_delay_hours,
    p99_delay_hours,
    max_delay_hours,
    max_delay_days,

    -- The overlap window this is measured against, from dbt_project.yml so the
    -- model and fct_transactions cannot disagree about what the window is.
    {{ var('late_arrival_window_days') }} as window_days,

    -- Headroom: how much slack is left before the window starts dropping rows.
    -- This is the number an alert should watch, because it is the one that
    -- goes to zero before data goes missing.
    {{ var('late_arrival_window_days') }} - max_delay_days as headroom_days,

    -- Three states rather than a boolean, because "fine" and "about to break"
    -- need different responses and a boolean collapses them.
    --
    -- BREACHED is the serious one and needs reading carefully: it means a row
    -- arrived at the very edge of the window. Rows that arrived PAST the
    -- window are not in this table at all -- they never entered
    -- fct_transactions. So BREACHED does not say "we lost data", it says "the
    -- window is no longer known to be sufficient", which is the last warning
    -- available before loss becomes possible.
    case
        when max_delay_days >= {{ var('late_arrival_window_days') }}
            then 'BREACHED'
        when max_delay_days >= {{ var('late_arrival_window_days') }} - 1
            then 'AT_RISK'
        else 'OK'
    end as window_status,

    current_timestamp() as dbt_loaded_at

from daily
