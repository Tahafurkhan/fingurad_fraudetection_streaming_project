-- Marts must not silently fall behind silver.
--
-- WHY THIS TEST EXISTS
-- --------------------
-- Found during a project audit: silver held 825 transactions with today's
-- timestamps while fct_transactions held 4,396 rows whose newest event was a
-- month old. Both tables were healthy by every existing check. dbt tests
-- passed, the pipeline was green, source freshness was fine -- because
-- freshness tests watch SOURCES arriving, and nothing watched the mart
-- tracking its source.
--
-- A stakeholder querying fct_transactions that day would have received last
-- month's numbers with no indication anything was stale. That is a worse
-- failure than an error: an error stops people, a plausible wrong number does
-- not.
--
-- WHAT IT ASSERTS
-- ---------------
-- The newest transaction in the mart is within one day of the newest in
-- silver. Not row counts -- those legitimately differ, because the mart
-- deduplicates on the natural key and silver retains at-least-once redeliveries
-- as the audit record. Comparing counts would fail constantly for correct
-- reasons, and a test that cries wolf gets removed.
--
-- Event-time lag is the honest measure: if silver has data the mart has never
-- seen, the mart is stale regardless of how many rows either holds.
--
-- WHY ONE DAY
-- -----------
-- The orchestration job runs daily at 02:00. A lag under one day means the
-- most recent scheduled run picked up what silver had. More than that means a
-- run was missed, failed, or the incremental filter is dropping rows -- all
-- worth investigating, none self-announcing.
--
-- Returns rows only when the assertion fails, per dbt's singular-test contract.

{{ config(severity = 'warn') }}

with silver_state as (

    select
        max(transaction_timestamp) as silver_max_event,
        count(*)                   as silver_rows
    from {{ source('silver', 'transactions') }}

),

mart_state as (

    select
        max(transaction_timestamp) as mart_max_event,
        count(*)                   as mart_rows
    from {{ ref('fct_transactions') }}

)

select
    s.silver_max_event,
    m.mart_max_event,
    s.silver_rows,
    m.mart_rows,
    datediff(s.silver_max_event, m.mart_max_event) as lag_days,
    concat(
        'fct_transactions is ',
        cast(datediff(s.silver_max_event, m.mart_max_event) as string),
        ' day(s) behind silver.transactions. Newest in silver: ',
        cast(s.silver_max_event as string),
        '; newest in mart: ',
        cast(m.mart_max_event as string),
        '. The daily job has not run, has failed, or the incremental filter ',
        'is excluding rows.'
    ) as failure_reason

from silver_state s
cross join mart_state m

-- Fails when the mart trails by more than a day.
--
-- The mart being AHEAD is impossible and not tested for: it reads from silver,
-- so it cannot contain an event silver has never seen.
--
-- A null mart_max_event means the mart is empty, which is also a failure --
-- datediff against null returns null, so it is checked explicitly rather than
-- passing by accident.
where m.mart_max_event is null
   or datediff(s.silver_max_event, m.mart_max_event) > 1
