-- Lineage export from Unity Catalog system tables.
--
-- WHY THIS FILE EXISTS
-- --------------------
-- Unity Catalog captures column-level lineage automatically for everything
-- this platform writes. It is genuinely useful and almost never used, because
-- it lives in a UI panel one table at a time: to answer "what breaks if this
-- column changes?" you click through each downstream object, one at a time,
-- and hope you did not miss a branch.
--
-- The data behind that UI is queryable. These views turn it into something a
-- person can answer a question with, and something an alert can be built on.
--
-- WHAT THIS IS FOR, CONCRETELY
-- ----------------------------
-- Three questions that come up for real:
--
--   1. Impact analysis. The Kafka contract adds 90 days' notice for a removed
--      field (TRD 6.1). To use that notice you must know what the field feeds.
--      Guessing from memory on a platform with four layers plus marts is how
--      a dashboard silently breaks a month later.
--
--   2. Orphan detection. A table nothing reads is either a mistake or a cost.
--      Both are worth knowing; neither announces itself.
--
--   3. GDPR erasure scope (DP-03). An erasure request needs every table
--      holding a subject's data, including derived copies nobody remembers
--      building. Lineage is the only complete answer.
--
-- REQUIREMENTS AND LIMITS
-- -----------------------
-- `system.access` must be enabled on the metastore. Where it is not, these
-- views fail to create and the remedy is a metastore admin, not a code change.
--
-- Retention is bounded (currently 90 days). Lineage is therefore evidence of
-- what HAS run recently, not a complete static graph: a quarterly job that has
-- not fired inside the window is invisible here. Treating this as the full
-- picture is the mistake to avoid.

CREATE SCHEMA IF NOT EXISTS finguard.ops;

-- ---------------------------------------------------------------------------
-- Table-level edges.
--
-- Deduplicated to distinct pairs. The raw system table records one row per
-- lineage *event*, so a table read hourly for a month produces hundreds of
-- identical edges -- fine as an audit trail, useless as a graph.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_table_lineage AS
SELECT DISTINCT
    source_table_full_name  AS source_table,
    target_table_full_name  AS target_table,
    -- Which layer each side belongs to. Makes the medallion rule in TRD 7.3
    -- checkable: bronze should feed silver, not gold or marts directly.
    split(source_table_full_name, '\\.')[1] AS source_layer,
    split(target_table_full_name, '\\.')[1] AS target_layer,
    entity_type             AS produced_by_type,
    max(event_time)         AS last_seen
FROM system.access.table_lineage
WHERE source_table_full_name IS NOT NULL
  AND target_table_full_name IS NOT NULL
  AND (source_table_full_name LIKE 'finguard.%'
       OR target_table_full_name LIKE 'finguard.%')
GROUP BY ALL;

-- ---------------------------------------------------------------------------
-- Column-level edges -- the ones that answer the impact question.
--
-- Table-level lineage says silver.transactions feeds gold. Column-level says
-- WHICH downstream columns read `currency`, which is what a schema change
-- actually threatens.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_column_lineage AS
SELECT DISTINCT
    source_table_full_name  AS source_table,
    source_column_name      AS source_column,
    target_table_full_name  AS target_table,
    target_column_name      AS target_column,
    max(event_time)         AS last_seen
FROM system.access.column_lineage
WHERE source_table_full_name LIKE 'finguard.%'
GROUP BY ALL;

-- ---------------------------------------------------------------------------
-- Impact analysis: everything downstream of a table, transitively.
--
-- Recursion is capped at 10 levels. The real graph is 5 deep (bronze -> silver
-- -> gold -> staging -> marts), so 10 is headroom rather than a limit -- but
-- the cap is the point: a lineage cycle would otherwise run until the query
-- is killed. Cycles should not occur; unbounded recursion against a table
-- populated by an external system is not a bet worth taking.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_downstream_impact AS
WITH RECURSIVE downstream (root_table, target_table, depth) AS (
    SELECT source_table, target_table, 1
    FROM finguard.ops.v_table_lineage

    UNION ALL

    SELECT d.root_table, l.target_table, d.depth + 1
    FROM downstream d
    JOIN finguard.ops.v_table_lineage l
      ON l.source_table = d.target_table
    WHERE d.depth < 10
)
SELECT
    root_table,
    target_table AS affected_table,
    min(depth)   AS hops
FROM downstream
-- Self-edges are excluded. Streaming tables read themselves: a stateful
-- operator such as the dedup on silver.transactions produces a lineage edge
-- from the table to itself. That is accurate as lineage and useless as impact
-- analysis -- "changing this table affects this table" is noise in a list
-- whose whole purpose is naming what ELSE breaks.
WHERE target_table <> root_table
GROUP BY root_table, target_table;

-- ---------------------------------------------------------------------------
-- Orphans: tables written by this platform that nothing has read.
--
-- Read as a QUESTION, not a verdict. A table appears here for three very
-- different reasons and only one is a problem:
--
--   - genuinely unused                     -> delete it, stop paying for it
--   - read only by BI tools outside UC     -> lineage cannot see that consumer
--   - read less often than the retention   -> a quarterly report is invisible
--                                             inside a 90-day window
--
-- Alerting on this directly would produce false positives of the second and
-- third kind, which is exactly how a detector gets ignored (TRD OB-07).
--
-- ENGINE-INTERNAL TABLES ARE EXCLUDED, and the exclusions were derived from
-- running this view rather than anticipated. The first version returned 15
-- rows, of which none were user tables:
--
--   __materialization_mat_<pipeline-id>_<name>_1   Lakeflow's internal
--                                                  materialisations of
--                                                  streaming tables
--   __<pipeline-id>_<name>_sink                    sink tables behind the
--                                                  email notifiers
--   event_log_<pipeline-id>                        per-pipeline event logs
--
-- All are created by the engine, read only by the engine, and correctly have
-- no lineage. Left in, they would have made every run of this view report 15
-- orphans that no one should act on -- burying any real finding under noise
-- that never changes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_orphan_tables AS
SELECT
    t.table_catalog || '.' || t.table_schema || '.' || t.table_name AS table_name,
    t.table_schema  AS layer,
    t.created       AS created_at
FROM system.information_schema.tables t
LEFT JOIN (
    SELECT DISTINCT source_table FROM finguard.ops.v_table_lineage
) l
  ON l.source_table = t.table_catalog || '.' || t.table_schema || '.' || t.table_name
WHERE t.table_catalog = 'finguard'
  AND t.table_schema NOT IN ('information_schema', 'ops', 'security')
  AND l.source_table IS NULL
  -- Engine internals, not user tables.
  --
  -- `_` is a single-character wildcard in LIKE, so matching a literal
  -- underscore needs an escape. ESCAPE '!' rather than the conventional
  -- backslash: in Spark SQL a backslash immediately before the closing quote
  -- escapes the quote itself, so ESCAPE '\' is an unterminated literal and
  -- the parser fails several lines later with a message pointing at the wrong
  -- place.
  AND t.table_name NOT LIKE '!_!_%' ESCAPE '!'
  AND t.table_name NOT LIKE 'event!_log!_%' ESCAPE '!'
  -- Views have no lineage of their own; their sources do.
  AND t.table_type <> 'VIEW';

-- ---------------------------------------------------------------------------
-- Layer-rule violations: edges that skip a layer.
--
-- TRD AR-01 says no layer may be skipped -- a consumer needing bronze data
-- reads it via silver. The rule exists because a direct bronze -> gold edge
-- bypasses every quality expectation and quarantine route in between, so the
-- alert is computed from unvalidated data while appearing to be governed.
--
-- Encoded as a query rather than left in prose: an architecture rule nothing
-- checks is a preference.
--
-- A NOTE ON WHAT THIS DELIBERATELY DOES *NOT* FLAG
-- ------------------------------------------------
-- The first version of this view also flagged silver -> marts, on the
-- reasoning that marts should be built from gold. Run against the real
-- lineage graph it immediately returned three rows:
--
--     silver.transactions -> marts.stg_transactions
--     silver.customers    -> marts.stg_customers
--     silver.merchants    -> marts.stg_merchants
--
-- All three are correct architecture, not violations. dbt staging models are
-- a conformance layer: they rename, cast and lightly clean silver on the way
-- into the dimensional model. Requiring them to route through gold would mean
-- passing dimension source data through a layer built for streaming alert
-- aggregates, which is the wrong shape entirely.
--
-- The rule was wrong, so the rule was changed rather than the finding being
-- explained away in a runbook. A detector that reports three known-good edges
-- every run is a detector people learn to ignore, and then it is not there for
-- the edge that matters (TRD OB-07).
--
-- What remains flagged is bronze -> anything-past-silver, which is the case
-- with real consequences: unvalidated, unparsed payloads feeding business
-- output. Currently zero such edges exist.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW finguard.ops.v_layer_rule_violations AS
SELECT
    source_table,
    target_table,
    source_layer,
    target_layer,
    'bronze feeds ' || target_layer || ' directly, bypassing silver quality rules'
        AS violation
FROM finguard.ops.v_table_lineage
WHERE source_layer = 'bronze'
  AND target_layer IN ('gold', 'marts', 'snapshots');
