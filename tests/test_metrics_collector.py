"""Tests for the monitoring telemetry collector.

The collector is mostly SQL, and SQL correctness is verified against the real
warehouse rather than here -- a mock cannot tell you that
`$.stream_progress.metrics.watermark` returns NULL while
`$.stream_progress.progress_json` then `$.eventTime.watermark` returns a value.
That was found by querying, not by testing.

What these tests protect is the logic around the SQL, which is where silent
regressions hide: the incremental watermark that stops a rerun duplicating
every row, the per-pipeline error containment that stops one bad pipeline
blanking all monitoring, and the argument parsing.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pipelines.ops import metrics_collector as mc  # noqa: E402


class FakeRow(dict):
    """Row supporting `row["col"]` like a Spark Row."""


class FakeDF:
    def __init__(self, rows, recorder=None, name=""):
        self._rows = rows
        self._recorder = recorder
        self._name = name

    def collect(self):
        return self._rows

    def count(self):
        return len(self._rows)

    @property
    def write(self):
        return self

    def mode(self, _mode):
        return self

    def saveAsTable(self, table):
        if self._recorder is not None:
            self._recorder.append((table, len(self._rows)))


class FakeSpark:
    """Records SQL issued, and returns canned results per query pattern.

    Deliberately not a Spark session: these tests assert on the SQL text and
    control flow, not on execution.
    """

    def __init__(self, watermark=None, row_counts=None, fail_on=None):
        self.queries: list[str] = []
        self.writes: list[tuple[str, int]] = []
        self._watermark = watermark
        self._row_counts = row_counts or {}
        self._fail_on = fail_on

    def sql(self, query):
        self.queries.append(query)

        if self._fail_on and self._fail_on in query:
            raise RuntimeError("simulated pipeline failure")

        if query.strip().upper().startswith("SELECT MAX(EVENT_TIMESTAMP)"):
            return FakeDF([FakeRow(m=self._watermark)])

        if "CREATE TABLE" in query:
            return FakeDF([])

        # An extraction query: return however many rows the test asked for.
        for key, count in self._row_counts.items():
            if key in query:
                return FakeDF(
                    [FakeRow(i=i) for i in range(count)], self.writes, key
                )
        return FakeDF([], self.writes)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def test_parse_pipeline_ids_splits_comma_list():
    argv = ["prog", "--pipeline-ids", "abc,def,ghi"]
    assert mc.parse_pipeline_ids(argv) == ["abc", "def", "ghi"]


def test_parse_pipeline_ids_strips_whitespace_and_empties():
    """The bundle substitutes ids into a template, so stray separators happen."""
    argv = ["prog", "--pipeline-ids", " abc , ,def, "]
    assert mc.parse_pipeline_ids(argv) == ["abc", "def"]


def test_parse_pipeline_ids_absent_returns_empty():
    assert mc.parse_pipeline_ids(["prog"]) == []


# ---------------------------------------------------------------------------
# Incremental watermark
# ---------------------------------------------------------------------------


def test_watermark_defaults_to_epoch_on_empty_table():
    """First run must backfill, not silently collect nothing."""
    spark = FakeSpark(watermark=None)
    result = mc._watermark(spark, mc.RUNS_TABLE, "pid-1")
    assert result == "TIMESTAMP '1970-01-01 00:00:00'"


def test_watermark_uses_last_collected_timestamp():
    spark = FakeSpark(watermark="2026-08-11 04:00:00")
    result = mc._watermark(spark, mc.RUNS_TABLE, "pid-1")
    assert result == "TIMESTAMP '2026-08-11 04:00:00'"


def test_watermark_is_scoped_per_pipeline():
    """Two pipelines advance independently.

    Without the pipeline_id filter, a busy pipeline's watermark would suppress
    collection for a quiet one -- monitoring would go dark on the pipeline
    least likely to be noticed.
    """
    spark = FakeSpark(watermark="2026-08-11 04:00:00")
    mc._watermark(spark, mc.RUNS_TABLE, "pid-xyz")
    assert "pid-xyz" in spark.queries[0]


def test_extraction_queries_filter_on_watermark():
    """The watermark must reach the extraction query, not just be computed."""
    spark = FakeSpark(watermark="2026-08-11 04:00:00")
    mc._collect_runs(spark, "pid-1")
    extraction = [q for q in spark.queries if "event_log" in q][0]
    assert "timestamp > TIMESTAMP '2026-08-11 04:00:00'" in extraction


# ---------------------------------------------------------------------------
# Write behaviour
# ---------------------------------------------------------------------------


def test_empty_batch_writes_nothing():
    """An empty append still commits a Delta version; skipping avoids churn."""
    spark = FakeSpark(watermark=None, row_counts={})
    written = mc._collect_runs(spark, "pid-1")
    assert written == 0
    assert spark.writes == []


def test_rows_are_appended_not_overwritten():
    """Overwrite mode would erase history on every run."""
    spark = FakeSpark(watermark=None, row_counts={"update_progress": 5})
    count = mc._collect_runs(spark, "pid-1")
    assert count == 5
    assert spark.writes == [(mc.RUNS_TABLE, 5)]


# ---------------------------------------------------------------------------
# Error containment
# ---------------------------------------------------------------------------


def test_one_failing_pipeline_does_not_stop_the_others():
    """Partial monitoring beats no monitoring.

    A deleted pipeline, or one whose permissions changed, makes event_log()
    raise. If that propagated, a single stale id would blank every other
    pipeline's telemetry.
    """
    spark = FakeSpark(watermark=None, row_counts={"update_progress": 3},
                      fail_on="bad-pipeline")
    totals = mc.collect(spark, ["bad-pipeline", "good-pipeline"])
    assert totals["pipeline_runs"] == 3


def test_collect_creates_tables_before_reading():
    spark = FakeSpark(watermark=None)
    mc.collect(spark, ["pid-1"])
    creates = [q for q in spark.queries if "CREATE TABLE" in q]
    assert len(creates) == 3


# ---------------------------------------------------------------------------
# Query shape
# ---------------------------------------------------------------------------


def test_stream_health_double_decodes_progress_json():
    """THE bug this collector exists to encapsulate.

    progress_json is a JSON string nested inside the details JSON. A direct
    path returns NULL rather than erroring, so a regression here is silent --
    the table populates with NULL watermarks and every stall detector goes
    quiet. Assert both decode stages are present.
    """
    spark = FakeSpark(watermark=None)
    mc._collect_stream_health(spark, "pid-1")
    query = [q for q in spark.queries if "event_log" in q][0]
    assert "$.stream_progress.progress_json" in query
    assert "$.eventTime.watermark" in query


def test_expectations_read_from_running_not_completed_events():
    """Expectation results ride on RUNNING flow_progress events.

    Filtering for status COMPLETED yields an empty table with no error.
    """
    spark = FakeSpark(watermark=None)
    mc._collect_expectations(spark, "pid-1")
    query = [q for q in spark.queries if "event_log" in q][0]
    assert "data_quality.expectations" in query
    assert "'COMPLETED'" not in query


def test_runs_collects_only_terminal_states():
    spark = FakeSpark(watermark=None)
    mc._collect_runs(spark, "pid-1")
    query = [q for q in spark.queries if "event_log" in q][0]
    for state in ("COMPLETED", "FAILED", "CANCELED"):
        assert state in query
    assert "INITIALIZING" not in query


@pytest.mark.parametrize(
    "table",
    [mc.RUNS_TABLE, mc.EXPECTATIONS_TABLE, mc.STREAM_HEALTH_TABLE],
)
def test_tables_are_in_the_ops_schema(table):
    """Monitoring tables belong beside the other operational metadata."""
    assert table.startswith("finguard.ops.")
