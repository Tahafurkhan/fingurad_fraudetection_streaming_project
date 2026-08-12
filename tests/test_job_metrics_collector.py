"""Tests for the job-level telemetry collector.

Same approach as test_metrics_collector.py: exercise the pure logic -- argument
parsing, timestamp conversion, SLA arithmetic -- without a Spark session or a
workspace. The parts that talk to the Jobs API are not tested here; they are
verified by running the collector against a real job.

The timestamp handling gets disproportionate attention because it is where the
Jobs API's conventions bite. It reports "not set" as integer 0, not as null,
and a naive conversion turns that into 1970 -- putting an in-flight run's end
before its start and producing large negative durations that would silently
corrupt the SLA percentile.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))


@pytest.fixture
def collector():
    """Import the collector without the conftest pyspark stub.

    The module imports pyspark only inside functions, so it loads cleanly with
    no engine present.
    """
    from pipelines.ops import job_metrics_collector

    return job_metrics_collector


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def test_parses_a_single_job_id(collector):
    assert collector.parse_job_ids(["prog", "--job-ids", "12345"]) == [12345]


def test_parses_multiple_job_ids(collector):
    argv = ["prog", "--job-ids", "111,222,333"]
    assert collector.parse_job_ids(argv) == [111, 222, 333]


def test_tolerates_whitespace_and_trailing_commas(collector):
    """Bundle variable substitution can leave stray separators.

    A trailing comma from an empty substitution would otherwise raise
    ValueError on int(''), failing the task for a formatting artefact.
    """
    argv = ["prog", "--job-ids", " 111 , 222 ,"]
    assert collector.parse_job_ids(argv) == [111, 222]


def test_missing_flag_returns_empty(collector):
    """Returns empty rather than raising, so main() can give a clear message."""
    assert collector.parse_job_ids(["prog"]) == []


# ---------------------------------------------------------------------------
# Timestamp conversion
# ---------------------------------------------------------------------------


def test_epoch_millis_converts_to_utc(collector):
    # 2026-08-11T12:00:00Z
    assert collector._ms_to_ts(1786449600000) == datetime(2026, 8, 11, 12, 0, 0)


@pytest.mark.parametrize("empty", [0, None])
def test_unset_time_becomes_none_not_1970(collector, empty):
    """THE ONE THAT MATTERS.

    The Jobs API reports an unset end_time as integer 0, not as a missing
    field. Converting that literally yields 1970-01-01, which for an in-flight
    run puts the end before the start and produces a duration of roughly minus
    fifty years. That value would enter the SLA table and drag the percentile
    somewhere meaningless, with nothing obviously wrong on the surface.
    """
    assert collector._ms_to_ts(empty) is None


def test_converted_timestamps_are_naive(collector):
    """Naive UTC, matching the Delta TIMESTAMP columns.

    A tz-aware datetime and a naive one cannot be compared, and the watermark
    query does exactly that on every incremental run.
    """
    assert collector._ms_to_ts(1786449600000).tzinfo is None


# ---------------------------------------------------------------------------
# SLA target
# ---------------------------------------------------------------------------


def test_nfr_01_target_matches_the_documented_sla(collector):
    """900 seconds = the 15 minutes stated in TRD NFR-01.

    Asserted so the constant and the requirement cannot drift apart silently.
    If the SLA is renegotiated, this test is the reminder that the document
    changes too.
    """
    assert collector.NFR_01_TARGET_SECONDS == 900.0


def test_sla_tables_live_in_the_ops_schema(collector):
    """Ops tables stay out of the business schemas.

    Telemetry in gold would appear in the lineage graph as though it were a
    data product, and would land in the access grants analysts get.
    """
    for table in (collector.JOB_RUNS_TABLE, collector.JOB_TASKS_TABLE, collector.SLA_TABLE):
        assert table.startswith("finguard.ops.")


def test_page_size_respects_the_expand_tasks_api_cap(collector):
    """The Jobs API caps limit at 25 when expand_tasks=True.

    REGRESSION TEST FOR A REAL FAILURE. The first version passed limit=100.
    Every job raised `Invalid limit 100 - it has to be no more than 26`, the
    per-job exception handler caught it exactly as designed, and the collector
    exited SUCCESS having written zero rows.

    That is the worst shape a monitoring bug can take: the thing meant to tell
    you when something is wrong reported that everything was fine. It was found
    only by querying the tables afterwards, not from the run status.

    Asserted as a constant rather than trusting a comment, because the number
    is a remote API's constraint and nothing local would otherwise fail if it
    were changed back.
    """
    assert collector._PAGE_SIZE <= 25


def test_max_runs_is_bounded(collector):
    """A first run against a long-lived job must not page forever.

    The collector is watermarked, so steady-state runs fetch little. The cap
    matters on the first run against a job with years of history, where an
    unbounded generator would page until the task timed out.
    """
    assert 0 < collector._MAX_RUNS <= 1000


def test_collector_tables_do_not_collide_with_pipeline_collector(collector):
    """The two collectors must not write to the same tables.

    They run in the same job with different schemas and different watermark
    columns; a name collision would make one silently append rows the other
    cannot interpret.
    """
    from pipelines.ops import metrics_collector

    job_tables = {collector.JOB_RUNS_TABLE, collector.JOB_TASKS_TABLE, collector.SLA_TABLE}
    pipeline_tables = {
        metrics_collector.RUNS_TABLE,
        metrics_collector.EXPECTATIONS_TABLE,
        metrics_collector.STREAM_HEALTH_TABLE,
    }

    assert not (job_tables & pipeline_tables)
