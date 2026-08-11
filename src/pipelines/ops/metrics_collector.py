"""Materialize pipeline telemetry from the Lakeflow event log into ops tables.

WHY THIS EXISTS
---------------
The event log already contains everything needed to answer "is this pipeline
healthy?" -- 3,443 events for this pipeline at time of writing. Nobody reads it.
Observability is the data existing; monitoring is something looking at it on a
schedule and telling you when it is wrong. This module is the bridge.

Three reasons to materialize rather than query `event_log()` directly:

  1. Retention. The event log is scoped to the pipeline and disappears with it.
     A deleted-and-recreated pipeline loses all history, which is exactly when
     you most want to compare "before" against "after".

  2. Shape. The interesting metrics are buried in a doubly-encoded JSON string
     (see _stream_health below). Every consumer would otherwise repeat that
     decode, and get it subtly wrong.

  3. Cross-pipeline joins. Alerts need to join telemetry against billing and
     the update timeline. A TVF scoped to one pipeline cannot do that.

WHY PLAIN SPARK, NOT @dp.table
------------------------------
Same reasoning as framework/registry.py: a Lakeflow dataset cannot record its
own failure. If this collector were part of the pipeline graph, the run that
fails is the run that writes no diagnostics -- the failure that erases its own
evidence. It runs as a separate job task, after the pipeline, and is explicitly
allowed to fail without failing the run it observes.

INCREMENTAL BY WATERMARK
------------------------
Each table records the max event timestamp already collected and only reads
past it. Reruns are therefore safe and cheap; a backfill is just deleting rows
and running again.
"""

from __future__ import annotations

from typing import Any

CATALOG = "finguard"
OPS = f"{CATALOG}.ops"

RUNS_TABLE = f"{OPS}.pipeline_runs"
EXPECTATIONS_TABLE = f"{OPS}.expectation_results"
STREAM_HEALTH_TABLE = f"{OPS}.stream_health"

# Table properties mirror the rest of the project: these are append-mostly
# time-series tables that are always queried by recency, so clustering on the
# timestamp is the predicate that actually gets used.
_TBLPROPS = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def _props_sql() -> str:
    inner = ", ".join(f"'{k}' = '{v}'" for k, v in _TBLPROPS.items())
    return f"TBLPROPERTIES ({inner})"


def _ensure_tables(spark: Any) -> None:
    """Create the three ops tables if absent. Safe on every run."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
            pipeline_id     STRING    COMMENT 'Lakeflow pipeline id',
            pipeline_name   STRING,
            update_id       STRING    COMMENT 'One pipeline update (run)',
            state           STRING    COMMENT 'Terminal state: COMPLETED | FAILED | CANCELED',
            event_timestamp TIMESTAMP COMMENT 'When the terminal state was reached',
            message         STRING,
            collected_at    TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (event_timestamp)
        COMMENT 'One row per pipeline update terminal state, from the event log'
        {_props_sql()}
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {EXPECTATIONS_TABLE} (
            pipeline_id      STRING,
            update_id        STRING,
            flow_name        STRING    COMMENT 'Flow that evaluated the expectation',
            dataset          STRING    COMMENT 'Target dataset the expectation guards',
            expectation_name STRING,
            passed_records   BIGINT,
            failed_records   BIGINT,
            dropped_records  BIGINT    COMMENT 'Rows dropped by expect_or_drop in this batch',
            event_timestamp  TIMESTAMP,
            collected_at     TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (event_timestamp)
        COMMENT 'Per-expectation pass/fail counts per flow progress event'
        {_props_sql()}
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {STREAM_HEALTH_TABLE} (
            pipeline_id            STRING,
            update_id              STRING,
            flow_name              STRING,
            batch_id               BIGINT,
            watermark              TIMESTAMP COMMENT 'Event-time watermark after this batch',
            operator_name          STRING    COMMENT 'Stateful operator, NULL if stateless',
            num_state_instances    BIGINT    COMMENT 'State store instances; pinned at checkpoint creation',
            num_rows_total         BIGINT    COMMENT 'Rows currently held in state',
            num_rows_dropped_late  BIGINT    COMMENT 'Rows discarded for arriving past the watermark',
            trigger_duration_ms    BIGINT,
            num_bytes_outstanding  BIGINT    COMMENT 'Source backlog in bytes',
            event_timestamp        TIMESTAMP,
            collected_at           TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (event_timestamp)
        COMMENT 'Streaming watermark, state and latency metrics per batch'
        {_props_sql()}
        """
    )


def _watermark(spark: Any, table: str, pipeline_id: str) -> str:
    """Latest event timestamp already collected for this pipeline.

    Returned as a SQL literal so it can be inlined into the extraction query.
    The epoch default makes the first run a full backfill.
    """
    row = spark.sql(
        f"SELECT max(event_timestamp) AS m FROM {table} "
        f"WHERE pipeline_id = '{pipeline_id}'"
    ).collect()
    if row and row[0]["m"] is not None:
        return f"TIMESTAMP '{row[0]['m']}'"
    return "TIMESTAMP '1970-01-01 00:00:00'"


def _collect_runs(spark: Any, pipeline_id: str) -> int:
    """Terminal update states.

    Only terminal states are kept. INITIALIZING/RUNNING transitions are noise
    for monitoring -- what matters is whether the update finished and how it
    ended.
    """
    since = _watermark(spark, RUNS_TABLE, pipeline_id)
    df = spark.sql(
        f"""
        SELECT
            origin.pipeline_id                                  AS pipeline_id,
            origin.pipeline_name                                AS pipeline_name,
            origin.update_id                                    AS update_id,
            get_json_object(details, '$.update_progress.state') AS state,
            timestamp                                           AS event_timestamp,
            message                                             AS message,
            current_timestamp()                                 AS collected_at
        FROM event_log('{pipeline_id}')
        WHERE event_type = 'update_progress'
          AND get_json_object(details, '$.update_progress.state')
              IN ('COMPLETED', 'FAILED', 'CANCELED')
          AND timestamp > {since}
        """
    )
    return _append(df, RUNS_TABLE)


def _collect_expectations(spark: Any, pipeline_id: str) -> int:
    """Per-expectation results.

    The expectations array hangs off flow_progress events with status RUNNING,
    not COMPLETED -- results are reported as batches process, not at flow end.
    Assuming COMPLETED here yields an empty table with no error, which is the
    kind of silent-nothing bug this project has hit before.

    from_json + explode rather than repeated get_json_object: the array is
    variable length, so positional extraction cannot work.
    """
    since = _watermark(spark, EXPECTATIONS_TABLE, pipeline_id)
    df = spark.sql(
        f"""
        WITH raw AS (
            SELECT
                origin.pipeline_id AS pipeline_id,
                origin.update_id   AS update_id,
                origin.flow_name   AS flow_name,
                timestamp          AS event_timestamp,
                CAST(get_json_object(details, '$.flow_progress.data_quality.dropped_records')
                     AS BIGINT) AS dropped_records,
                from_json(
                    get_json_object(details, '$.flow_progress.data_quality.expectations'),
                    'array<struct<name:string,dataset:string,
                                  passed_records:bigint,failed_records:bigint>>'
                ) AS expectations
            FROM event_log('{pipeline_id}')
            WHERE event_type = 'flow_progress'
              AND get_json_object(details, '$.flow_progress.data_quality.expectations')
                  IS NOT NULL
              AND timestamp > {since}
        )
        SELECT
            pipeline_id,
            update_id,
            flow_name,
            e.dataset                AS dataset,
            e.name                   AS expectation_name,
            e.passed_records         AS passed_records,
            e.failed_records         AS failed_records,
            dropped_records,
            event_timestamp,
            current_timestamp()      AS collected_at
        FROM raw
        LATERAL VIEW explode(expectations) t AS e
        """
    )
    return _append(df, EXPECTATIONS_TABLE)


def _collect_stream_health(spark: Any, pipeline_id: str) -> int:
    """Watermark, state and latency metrics.

    THE DOUBLE DECODE. stream_progress.progress_json is a JSON *string* nested
    inside the details JSON. A direct path such as

        $.stream_progress.metrics.watermark

    returns NULL -- not an error, just silence, which is how this stays
    unnoticed. The value has to be pulled out as a string first and then parsed
    a second time. That is why this collector exists rather than leaving
    everyone to query the event log directly.

    Only stateOperators[0] is read. Every stateful flow here has exactly one
    stateful operator; exploding the array would be more correct in general and
    is noted as a limitation rather than pretended away.
    """
    since = _watermark(spark, STREAM_HEALTH_TABLE, pipeline_id)
    df = spark.sql(
        f"""
        WITH decoded AS (
            SELECT
                origin.pipeline_id AS pipeline_id,
                origin.update_id   AS update_id,
                origin.flow_name   AS flow_name,
                timestamp          AS event_timestamp,
                get_json_object(details, '$.stream_progress.progress_json') AS pj
            FROM event_log('{pipeline_id}')
            WHERE event_type = 'stream_progress'
              AND timestamp > {since}
        )
        SELECT
            pipeline_id,
            update_id,
            flow_name,
            CAST(get_json_object(pj, '$.batchId') AS BIGINT)   AS batch_id,
            CAST(get_json_object(pj, '$.eventTime.watermark')
                 AS TIMESTAMP)                                 AS watermark,
            get_json_object(pj, '$.stateOperators[0].operatorName')
                                                               AS operator_name,
            CAST(get_json_object(pj, '$.stateOperators[0].numStateStoreInstances')
                 AS BIGINT)                                    AS num_state_instances,
            CAST(get_json_object(pj, '$.stateOperators[0].numRowsTotal')
                 AS BIGINT)                                    AS num_rows_total,
            CAST(get_json_object(pj, '$.stateOperators[0].numRowsDroppedByWatermark')
                 AS BIGINT)                                    AS num_rows_dropped_late,
            CAST(get_json_object(pj, '$.durationMs.triggerExecution')
                 AS BIGINT)                                    AS trigger_duration_ms,
            CAST(get_json_object(pj, '$.sources[0].metrics.numBytesOutstanding')
                 AS BIGINT)                                    AS num_bytes_outstanding,
            event_timestamp,
            current_timestamp()                                AS collected_at
        FROM decoded
        WHERE pj IS NOT NULL
        """
    )
    return _append(df, STREAM_HEALTH_TABLE)


def _append(df: Any, table: str) -> int:
    """Append a batch, returning the row count. Skips the write when empty."""
    count = df.count()
    if count:
        df.write.mode("append").saveAsTable(table)
    return count


def collect(spark: Any, pipeline_ids: list[str]) -> dict[str, int]:
    """Collect telemetry for each pipeline. Returns rows written per table.

    Per-pipeline failures are contained: one unreadable pipeline (deleted, or
    permissions changed) must not stop collection for the others. Monitoring
    that goes all-dark because one source is missing is worse than monitoring
    that reports partial data.
    """
    _ensure_tables(spark)
    totals = {"pipeline_runs": 0, "expectation_results": 0, "stream_health": 0}

    for pipeline_id in pipeline_ids:
        try:
            totals["pipeline_runs"] += _collect_runs(spark, pipeline_id)
            totals["expectation_results"] += _collect_expectations(spark, pipeline_id)
            totals["stream_health"] += _collect_stream_health(spark, pipeline_id)
        except Exception as exc:  # noqa: BLE001 - see docstring
            print(f"[metrics_collector] pipeline {pipeline_id} failed: {exc}")

    return totals


def parse_pipeline_ids(argv: list[str]) -> list[str]:
    """Extract --pipeline-ids from argv as a list.

    Separate from main() so it can be tested without a Spark session. The job
    passes bundle-resolved pipeline ids, which keeps workspace-specific
    identifiers out of the source.
    """
    if "--pipeline-ids" not in argv:
        return []
    value = argv[argv.index("--pipeline-ids") + 1]
    return [p.strip() for p in value.split(",") if p.strip()]


def main() -> None:
    """Entry point for the Databricks job task."""
    import sys

    from pyspark.sql import SparkSession

    pipeline_ids = parse_pipeline_ids(sys.argv)
    if not pipeline_ids:
        raise ValueError(
            "No pipeline ids given. Pass --pipeline-ids with a "
            "comma-separated list."
        )

    spark = SparkSession.builder.getOrCreate()
    totals = collect(spark, pipeline_ids)
    print(f"[metrics_collector] rows written: {totals}")


if __name__ == "__main__":
    main()
