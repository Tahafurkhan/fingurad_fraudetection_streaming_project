"""Spark fixtures for integration tests.

WHY THESE TESTS NEED THEIR OWN CONFTEST
---------------------------------------
The unit suite's conftest installs a *fake* pyspark via an autouse fixture, so
framework modules import without an engine. That is correct for testing YAML
parsing and code generation, and fatal here: these tests need real Catalyst to
execute real transformations.

`stub_pyspark` is autouse, so it cannot simply be ignored -- it would replace
`sys.modules["pyspark"]` before any test in this directory runs. The override
below shadows it for this package only. Directory-scoped conftest files take
precedence over their parent for fixtures of the same name, so redefining
`stub_pyspark` as a no-op here disables the stub without touching the unit
suite.

WHY A SEPARATE VIRTUALENV
-------------------------
`databricks-connect` replaces the `pyspark` package in site-packages with a
client that refuses to create a local session:

    RuntimeError: Only remote Spark sessions using Databricks Connect are
    supported.

The two cannot coexist in one interpreter. `.venv-test/` holds real pyspark and
is used only for tests; the global environment keeps databricks-connect for
notebook work. See the Makefile and docs/setup.md.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))


@pytest.fixture(autouse=True)
def stub_pyspark():
    """Disable the parent conftest's pyspark stub for this package.

    Same name as the parent fixture, so pytest resolves to this one for tests
    in this directory. A no-op returning None; nothing here consumes it.
    """
    return None


def _databricks_connect_active() -> bool:
    """True when the installed pyspark is the Connect client, not real Spark."""
    try:
        import pyspark.sql.session as _session
    except ImportError:
        return True
    return "Databricks Connect" in (_session.SparkSession.getOrCreate.__doc__ or "") or bool(
        os.environ.get("DATABRICKS_CONNECT_ACTIVE")
    )


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession, created once for the whole test session.

    Session-scoped because JVM startup dominates: roughly 10-15 seconds against
    a few hundred milliseconds per test. Per-test sessions would make the suite
    slow enough that people stop running it, and a suite nobody runs is worse
    than no suite.

    Configuration is chosen for determinism and speed, not realism:
      - local[2]           two threads; enough to exercise partitioning without
                           the scheduler overhead of more
      - shuffle.partitions 1  the default 200 spawns 200 tasks to shuffle a
                           handful of rows, which is pure overhead at this size
      - timezone UTC       timestamp assertions must not depend on the machine
      - ui.enabled false   no port binding, so parallel runs cannot collide
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:  # pragma: no cover
        pytest.skip("pyspark not installed -- run via .venv-test")

    # These are FORCED, not setdefault, and that distinction is the whole
    # reason this block has a comment.
    #
    # A developer machine that has ever run Spark outside a virtualenv carries
    # PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON and SPARK_HOME as permanent user
    # environment variables. All three are actively wrong here:
    #
    #   PYSPARK_PYTHON        points at the global interpreter, which does not
    #                         have pyspark installed (it has databricks-connect
    #                         instead). The JVM launches a worker with it and
    #                         the worker dies -- surfacing as an unhelpful
    #                         "FileNotFoundError: [WinError 2]" from
    #                         subprocess, several frames from the real cause.
    #
    #   SPARK_HOME            points at a standalone Spark install whose jars
    #                         may be a different version from the pyspark in
    #                         this venv. Mixed versions fail late and strangely.
    #
    # setdefault() respects those values and inherits the breakage. Assignment
    # overrides them for this process only; the developer's shell is untouched.
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # Let pyspark use the jars it shipped with rather than an external install.
    os.environ.pop("SPARK_HOME", None)

    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("finguard-integration")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        )
    except RuntimeError as exc:
        # The databricks-connect collision, reported with the remedy rather
        # than as a bare traceback -- this is the failure a new contributor
        # hits first.
        pytest.skip(
            f"No local Spark session ({exc}). These tests require real pyspark; "
            "databricks-connect shadows it. Run: make test-integration"
        )

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze_frame(spark):
    """Build a bronze-shaped DataFrame from raw JSON payload strings.

    Mirrors finguard.bronze.transactions: the payload stays an unparsed string
    in `value`, with the Kafka envelope alongside. Tests supply payloads as
    strings -- including deliberately malformed ones -- because that is exactly
    what bronze holds and what silver must cope with.
    """
    from datetime import datetime

    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    bronze_schema = StructType(
        [
            StructField("value", StringType()),
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
            StructField("ingestion_timestamp", TimestampType()),
        ]
    )

    def _build(payloads: list[str]):
        now = datetime(2026, 8, 11, 12, 0, 0)
        rows = [
            (payload, "topic_0", i % 6, 1000 + i, now, now)
            for i, payload in enumerate(payloads)
        ]
        return spark.createDataFrame(rows, schema=bronze_schema)

    return _build


@pytest.fixture
def valid_payload():
    """A factory for contract-conforming payloads, overridable per field.

    Defaults are a realistic transaction. Tests override only the field under
    test, so a test reads as a statement about one thing rather than a wall of
    JSON -- and adding a contract field later does not require editing every
    test.
    """
    import json

    def _build(**overrides) -> str:
        payload = {
            "transaction_id": "txn-0001",
            "customer_id": "cust-0001",
            "card_number": "4111111111111111",
            "merchant_id": "merch-0001",
            "merchant_name": "Acme Stores",
            "merchant_category": "grocery",
            "amount": 125.50,
            "currency": "INR",
            "transaction_type": "purchase",
            "payment_channel": "pos",
            "device_id": "dev-0001",
            "city": "Chennai",
            "country": "IN",
            "transaction_timestamp": "2026-08-11T12:00:00.000Z",
            "is_international": False,
            "status": "approved",
        }
        payload.update(overrides)
        # A field set to None is being tested as *absent* from the payload,
        # which is not the same as present-and-null. Removing it is what the
        # producer would actually emit.
        return json.dumps({k: v for k, v in payload.items() if v is not None})

    return _build
