"""Job-level telemetry and SLA measurement.

WHAT THIS ADDS THAT metrics_collector.py DOES NOT
-------------------------------------------------
`metrics_collector.py` mines the *pipeline* event log. That covers everything
inside a Lakeflow update and nothing outside it -- which leaves a real blind
spot, because the orchestrating job does more than run pipelines. It runs dbt
snapshot, dbt run, dbt test, source freshness, and the metrics collection
itself.

A dbt test failure, a task that never started because an upstream one failed,
a run killed by its timeout, a run that never fired because its schedule was
paused: none of these appear in a pipeline event log, because no pipeline
update took place. OB-01 in the TRD says *every* run is queryable; without
this collector that claim is only true of pipeline runs.

The distinction has a concrete failure mode. If the scheduled job stops firing
entirely -- paused schedule, expired credential, permissions change -- the
pipeline event log stays quiet and every existing detector reports healthy.
Silence is indistinguishable from success when you only watch the thing that
did not run.

THE SLA TABLE
-------------
NFR-01 sets a 15-minute P95 from transaction event time to alert availability.
That number appeared in a design document and was never measured, which makes
it an aspiration rather than an SLA. `job_sla` records per-run latency so the
target can be reported against instead of asserted.

Latency is measured end-to-end and decomposed, because the total on its own
does not say what to fix:

    queue_seconds     scheduled -> started    (compute cold start, contention)
    execution_seconds started   -> finished   (the actual work)
    total_seconds     scheduled -> finished

See docs/observability.md.
"""

from __future__ import annotations

from typing import Any

CATALOG = "finguard"
OPS = f"{CATALOG}.ops"

JOB_RUNS_TABLE = f"{OPS}.job_runs"
JOB_TASKS_TABLE = f"{OPS}.job_task_runs"
SLA_TABLE = f"{OPS}.job_sla"

_TBLPROPS = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def _props_sql() -> str:
    inner = ", ".join(f"'{k}' = '{v}'" for k, v in _TBLPROPS.items())
    return f"TBLPROPERTIES ({inner})"


def _ensure_tables(spark: Any) -> None:
    """Create the three job-level ops tables if absent. Safe on every run."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {JOB_RUNS_TABLE} (
            job_id            BIGINT,
            job_name          STRING,
            run_id            BIGINT    COMMENT 'One execution of the job',
            run_type          STRING    COMMENT 'JOB_RUN | SUBMIT_RUN | WORKFLOW_RUN',
            trigger_type      STRING    COMMENT 'What started it: CRON | ONE_TIME | RETRY',
            result_state      STRING    COMMENT 'SUCCESS | FAILED | TIMEDOUT | CANCELED',
            state_message     STRING,
            start_time        TIMESTAMP,
            end_time          TIMESTAMP,
            duration_seconds  DOUBLE,
            collected_at      TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (start_time)
        COMMENT 'One row per orchestration job run, including runs with no pipeline update'
        {_props_sql()}
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {JOB_TASKS_TABLE} (
            job_id            BIGINT,
            run_id            BIGINT,
            task_key          STRING,
            task_type         STRING    COMMENT 'pipeline | dbt | spark_python',
            result_state      STRING,
            state_message     STRING,
            start_time        TIMESTAMP,
            end_time          TIMESTAMP,
            duration_seconds  DOUBLE,
            collected_at      TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (start_time)
        COMMENT 'Per-task outcomes. Identifies WHICH step failed, not just that the run did.'
        {_props_sql()}
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SLA_TABLE} (
            job_id             BIGINT,
            job_name           STRING,
            run_id             BIGINT,
            sla_name           STRING   COMMENT 'The NFR being measured, e.g. NFR-01',
            target_seconds     DOUBLE   COMMENT 'The documented target',
            queue_seconds      DOUBLE   COMMENT 'Scheduled -> started (cold start, contention)',
            execution_seconds  DOUBLE   COMMENT 'Started -> finished',
            total_seconds      DOUBLE   COMMENT 'Scheduled -> finished; compared against target',
            met                BOOLEAN,
            measured_at        TIMESTAMP,
            collected_at       TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (measured_at)
        COMMENT 'Per-run SLA measurement. Makes a stated target reportable.'
        {_props_sql()}
        """
    )


# NFR-01: 15 minutes, transaction event time to alert availability.
#
# Measured here as job latency, which is a DELIBERATE UNDER-MEASUREMENT and
# must not be read as the full figure. The job clock starts when the run is
# scheduled; the true end-to-end clock starts when the transaction occurred.
# Missing from this number:
#
#   - time the event waited in Kafka before the run was triggered
#     (bounded by the trigger interval -- the dominant term, per TRD 5.2)
#   - watermark delay on the stream-stream join (up to 5 minutes)
#
# A run comfortably inside 900s therefore does NOT prove the SLA is met. What
# this table gives is the controllable part: if execution alone approaches the
# target, no trigger interval can rescue it. Closing the gap properly needs
# event-time-to-alert-time measured per row in gold, which is FUT work.
NFR_01_TARGET_SECONDS = 900.0


def _watermark(spark: Any, table: str, column: str, job_id: int) -> str:
    """Latest value already collected for this job, as a SQL literal.

    Same incremental pattern as metrics_collector: each run reads only past
    what it already has, so reruns are cheap and a backfill is deleting rows
    and running again.
    """
    row = spark.sql(
        f"SELECT max({column}) AS m FROM {table} WHERE job_id = {job_id}"
    ).collect()
    if row and row[0]["m"] is not None:
        return f"TIMESTAMP '{row[0]['m']}'"
    return "TIMESTAMP '1970-01-01 00:00:00'"


# Page size for the Jobs API run listing.
#
# 25, NOT 100, AND THE DIFFERENCE IS NOT COSMETIC. With expand_tasks=True the
# API caps limit at 25 and rejects anything larger outright:
#
#   Invalid limit 100 - it has to be no more than 26.
#
# The first version passed 100. Every job raised, the per-job exception
# handler caught it as designed, and the collector reported success having
# written zero rows -- a monitoring component failing in exactly the silent way
# it exists to prevent. Found by checking the tables were populated rather than
# by trusting the run status, which is the only way this class of bug surfaces.
_PAGE_SIZE = 25

# How many runs to pull per job.
#
# The collector is watermarked, so a normal run only needs enough history to
# cover the gap since last collection. 200 is generous for a daily job while
# bounding a first run on a job with years of history.
_MAX_RUNS = 200


def _fetch_runs(job_id: int, limit: int = _MAX_RUNS) -> list[dict]:
    """Read recent runs from the Jobs API, newest first.

    The REST API rather than a system table: `system.lakeflow.job_run_timeline`
    exists but is not available on every workspace tier, and a monitoring
    component that only works on some tiers is a monitoring component that gets
    quietly dropped. The SDK is present on every Databricks runtime.

    The SDK's list_runs is a generator that pages transparently, so `limit`
    here is the page size, not a total. Capping the accumulated list is done
    explicitly below.
    """
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    runs = []
    for run in client.jobs.list_runs(
        job_id=job_id, limit=_PAGE_SIZE, expand_tasks=True
    ):
        runs.append(run.as_dict())
        if len(runs) >= limit:
            break
    return runs


def _ms_to_ts(value: Any) -> Any:
    """Epoch milliseconds to a datetime, or None.

    The Jobs API reports 0 for "not set" rather than omitting the field --
    end_time is 0 while a run is in flight. Mapping that to 1970 would put a
    running job's end before its start and produce negative durations.
    """
    if not value:
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).replace(tzinfo=None)


def _collect_job_runs(spark: Any, job_id: int) -> int:
    """Persist terminal job runs newer than the watermark."""
    since = _watermark(spark, JOB_RUNS_TABLE, "start_time", job_id)
    since_ms = spark.sql(f"SELECT unix_millis({since}) AS m").collect()[0]["m"]

    rows = []
    for run in _fetch_runs(job_id):
        start_ms = run.get("start_time") or 0
        if start_ms <= since_ms:
            continue

        state = run.get("state", {}) or {}
        result = state.get("result_state")
        # Skip runs still in flight. A run without a terminal state has no
        # duration and no outcome; collecting it would write a row that a
        # later collection would need to update, and these tables are
        # append-only by design.
        if not result:
            continue

        start_ts = _ms_to_ts(start_ms)
        end_ts = _ms_to_ts(run.get("end_time"))
        duration = (end_ts - start_ts).total_seconds() if (start_ts and end_ts) else None

        rows.append(
            (
                int(job_id),
                (run.get("run_name") or "")[:255],
                int(run.get("run_id") or 0),
                run.get("run_type"),
                (state.get("life_cycle_state") or None),
                result,
                (state.get("state_message") or "")[:1000],
                start_ts,
                end_ts,
                duration,
            )
        )

    if not rows:
        return 0

    df = spark.createDataFrame(
        rows,
        "job_id BIGINT, job_name STRING, run_id BIGINT, run_type STRING, "
        "trigger_type STRING, result_state STRING, state_message STRING, "
        "start_time TIMESTAMP, end_time TIMESTAMP, duration_seconds DOUBLE",
    ).selectExpr("*", "current_timestamp() AS collected_at")

    df.write.mode("append").saveAsTable(JOB_RUNS_TABLE)
    return len(rows)


def _collect_task_runs(spark: Any, job_id: int) -> int:
    """Persist per-task outcomes.

    Job-level state says a run failed; task-level state says which step. On a
    seven-task DAG that is the difference between "the pipeline is broken" and
    "dbt_test failed while ingestion succeeded" -- and those two have entirely
    different responses at 02:00.
    """
    since = _watermark(spark, JOB_TASKS_TABLE, "start_time", job_id)
    since_ms = spark.sql(f"SELECT unix_millis({since}) AS m").collect()[0]["m"]

    rows = []
    for run in _fetch_runs(job_id):
        for task in run.get("tasks", []) or []:
            start_ms = task.get("start_time") or 0
            if start_ms <= since_ms:
                continue

            state = task.get("state", {}) or {}
            result = state.get("result_state")
            if not result:
                continue

            # Which kind of task this is, inferred from which spec is present.
            task_type = next(
                (
                    kind
                    for key, kind in (
                        ("pipeline_task", "pipeline"),
                        ("dbt_task", "dbt"),
                        ("spark_python_task", "spark_python"),
                        ("notebook_task", "notebook"),
                        ("sql_task", "sql"),
                    )
                    if task.get(key)
                ),
                "unknown",
            )

            start_ts = _ms_to_ts(start_ms)
            end_ts = _ms_to_ts(task.get("end_time"))
            duration = (
                (end_ts - start_ts).total_seconds() if (start_ts and end_ts) else None
            )

            rows.append(
                (
                    int(job_id),
                    int(run.get("run_id") or 0),
                    task.get("task_key"),
                    task_type,
                    result,
                    (state.get("state_message") or "")[:1000],
                    start_ts,
                    end_ts,
                    duration,
                )
            )

    if not rows:
        return 0

    df = spark.createDataFrame(
        rows,
        "job_id BIGINT, run_id BIGINT, task_key STRING, task_type STRING, "
        "result_state STRING, state_message STRING, start_time TIMESTAMP, "
        "end_time TIMESTAMP, duration_seconds DOUBLE",
    ).selectExpr("*", "current_timestamp() AS collected_at")

    df.write.mode("append").saveAsTable(JOB_TASKS_TABLE)
    return len(rows)


def _collect_sla(spark: Any, job_id: int) -> int:
    """Measure NFR-01 per successful run.

    Only SUCCESS runs are measured. A failed run has no meaningful latency --
    it did not deliver the thing the SLA is about -- and including failures
    would corrupt the percentile in whichever direction the failure happened to
    fall. Failures are an availability concern (NFR-05) and are counted in
    job_runs.
    """
    since = _watermark(spark, SLA_TABLE, "measured_at", job_id)
    since_ms = spark.sql(f"SELECT unix_millis({since}) AS m").collect()[0]["m"]

    rows = []
    for run in _fetch_runs(job_id):
        state = run.get("state", {}) or {}
        if state.get("result_state") != "SUCCESS":
            continue

        start_ms = run.get("start_time") or 0
        end_ms = run.get("end_time") or 0
        if not (start_ms and end_ms) or start_ms <= since_ms:
            continue

        # setup_duration covers cluster acquisition; execution_duration is the
        # work itself. Splitting them matters because they have different
        # remedies -- a slow start is a compute problem, slow execution is a
        # query problem, and a single total hides which one you have.
        setup_ms = run.get("setup_duration") or 0
        exec_ms = run.get("execution_duration") or 0
        total_s = (end_ms - start_ms) / 1000.0

        rows.append(
            (
                int(job_id),
                (run.get("run_name") or "")[:255],
                int(run.get("run_id") or 0),
                "NFR-01",
                NFR_01_TARGET_SECONDS,
                setup_ms / 1000.0,
                exec_ms / 1000.0,
                total_s,
                bool(total_s <= NFR_01_TARGET_SECONDS),
                _ms_to_ts(start_ms),
            )
        )

    if not rows:
        return 0

    df = spark.createDataFrame(
        rows,
        "job_id BIGINT, job_name STRING, run_id BIGINT, sla_name STRING, "
        "target_seconds DOUBLE, queue_seconds DOUBLE, execution_seconds DOUBLE, "
        "total_seconds DOUBLE, met BOOLEAN, measured_at TIMESTAMP",
    ).selectExpr("*", "current_timestamp() AS collected_at")

    df.write.mode("append").saveAsTable(SLA_TABLE)
    return len(rows)


def parse_job_ids(argv: list[str]) -> list[int]:
    """Extract --job-ids from argv.

    Separate from main() so it can be tested without a Spark session or a
    workspace, matching the pattern in metrics_collector.py.
    """
    if "--job-ids" not in argv:
        return []
    value = argv[argv.index("--job-ids") + 1]
    return [int(p.strip()) for p in value.split(",") if p.strip()]


def collect(spark: Any, job_ids: list[int]) -> dict[str, int]:
    """Collect job telemetry. Returns rows written per table.

    Per-job failures are contained for the same reason as in
    metrics_collector: one deleted or permission-changed job must not take
    monitoring dark for the rest.
    """
    _ensure_tables(spark)
    totals = {"job_runs": 0, "job_task_runs": 0, "job_sla": 0}

    for job_id in job_ids:
        try:
            totals["job_runs"] += _collect_job_runs(spark, job_id)
            totals["job_task_runs"] += _collect_task_runs(spark, job_id)
            totals["job_sla"] += _collect_sla(spark, job_id)
        except Exception as exc:  # noqa: BLE001 - see docstring
            print(f"[job_metrics_collector] job {job_id} failed: {exc}")

    return totals


def main() -> None:
    """Entry point for the Databricks job task."""
    import sys

    from pyspark.sql import SparkSession

    job_ids = parse_job_ids(sys.argv)
    if not job_ids:
        raise ValueError("No job ids given. Pass --job-ids with a comma-separated list.")

    spark = SparkSession.builder.getOrCreate()
    totals = collect(spark, job_ids)
    print(f"[job_metrics_collector] rows written: {totals}")


if __name__ == "__main__":
    main()
