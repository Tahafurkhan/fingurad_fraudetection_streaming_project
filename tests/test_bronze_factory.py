"""Table generation.

The closure-binding test is the important one here. Python closures capture the
variable, not its value, so generating N tables inside a loop body makes every
one of them read the last config. The failure is silent -- N tables are created,
all pointing at the same source -- and only shows up as data that looks
plausible but is wrong.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def stub_readers():
    """Replace the real readers with ones that record which config they saw."""
    import pipelines.framework.readers as readers

    original = dict(readers.READERS)

    class TaggedFrame:
        """Stands in for a DataFrame, tagged with its originating config."""

        def __init__(self, tag):
            self.tag = tag
            self.columns = None

        def select(self, *cols):
            frame = TaggedFrame(self.tag)
            frame.columns = list(cols)
            return frame

        def withColumn(self, name, _value):
            self.columns = (self.columns or []) + [name]
            return self

    readers.READERS["kafka"] = lambda s, d, cfg: TaggedFrame(
        f"kafka:{cfg.get('secret_key')}"
    )
    readers.READERS["autoloader"] = lambda s, d, cfg: TaggedFrame(
        f"autoloader:{cfg.get('path')}"
    )
    readers.READERS["rest_api"] = lambda s, d, cfg: TaggedFrame(
        f"rest:{cfg.get('url')}"
    )

    yield readers

    readers.READERS.clear()
    readers.READERS.update(original)


def test_each_table_reads_its_own_source(config_dir, stub_readers, stub_pyspark):
    """The late-binding regression test.

    If `build_bronze_table` ever stops taking cfg as a parameter, every
    generated table collapses onto one source and this fails.
    """
    from pipelines.framework import build_all, load_source_configs

    configs = load_source_configs(config_dir)
    registered_functions = {}

    import pipelines.framework.bronze_factory as factory

    original_table = factory.dp.table

    def capturing_table(**kwargs):
        def decorator(fn):
            registered_functions[kwargs["name"]] = fn
            return fn

        return decorator

    factory.dp.table = capturing_table
    try:
        build_all(configs, None, None)
    finally:
        factory.dp.table = original_table

    # Each registered function, when called, must read the source that its own
    # config named.
    transactions = registered_functions["finguard.bronze.transactions"]()
    watchlist = registered_functions["finguard.bronze.fraud_watchlist"]()
    merchants = registered_functions["finguard.bronze.merchants"]()

    assert transactions.tag.startswith("kafka:")
    assert "fraud_watchlist" in watchlist.tag
    assert "merchants" in merchants.tag

    # And crucially, they must differ from each other.
    assert watchlist.tag != merchants.tag


def test_every_config_produces_an_audit_row(config_dir, stub_readers):
    """The audit record must describe what was asked for, not just what worked."""
    from pipelines.framework import build_all, load_source_configs

    configs = load_source_configs(config_dir)
    built, results = build_all(configs, None, None)

    assert len(results) == len(configs)
    assert {r["status"] for r in results} <= {"REGISTERED", "SKIPPED", "FAILED"}

    statuses = {r["source_name"]: r["status"] for r in results}
    assert statuses["customers"] == "SKIPPED"
    assert statuses["merchants"] == "REGISTERED"
    assert len(built) == sum(1 for r in results if r["status"] == "REGISTERED")


def test_one_failure_does_not_stop_the_others(config_dir, stub_readers):
    """Losing every table because one config is bad is the worse outcome."""
    from pipelines.framework import build_all, load_source_configs

    configs = load_source_configs(config_dir)

    # Remove the kafka reader so the transactions source cannot be built.
    del stub_readers.READERS["kafka"]

    built, results = build_all(configs, None, None)
    statuses = {r["source_name"]: r["status"] for r in results}

    assert statuses["transactions"] == "FAILED"
    assert statuses["merchants"] == "REGISTERED"
    assert statuses["fraud_watchlist"] == "REGISTERED"
    assert statuses["customers"] == "SKIPPED"

    failure = next(r for r in results if r["status"] == "FAILED")
    assert failure["detail"], "a failure must record why"


def test_batch_sources_become_materialized_views(write_config, stub_readers):
    """A REST pull returns a full snapshot.

    Registering it as a streaming table would append the entire result set on
    every run, duplicating every row.
    """
    import pipelines.framework.bronze_factory as factory
    from pipelines.framework import build_all, load_source_configs

    directory = write_config(
        "api.yaml",
        "name: vendor\ntarget: cat.bronze.vendor\n"
        "ingestion:\n  type: rest_api\n  url: https://example.test/v1/items\n",
    )

    seen = {"table": [], "mv": []}
    factory.dp.table = lambda **k: (lambda f: (seen["table"].append(k["name"]), f)[1])
    factory.dp.materialized_view = lambda **k: (
        lambda f: (seen["mv"].append(k["name"]), f)[1]
    )

    build_all(load_source_configs(directory), None, None)

    assert seen["mv"] == ["cat.bronze.vendor"]
    assert seen["table"] == []


def test_unknown_ingestion_type_names_known_types(write_config, stub_readers):
    """An unhelpful error here costs real debugging time."""
    from pipelines.framework.readers import get_reader

    with pytest.raises(ValueError) as excinfo:
        get_reader("kinesis")

    message = str(excinfo.value)
    assert "kinesis" in message
    assert "known types" in message


# --- Physical layout reaches the runtime -----------------------------------


def _layout_config(**optimization):
    """A minimal kafka SourceConfig carrying an optimization block."""
    from pipelines.framework.source_config import Column, SourceConfig

    return SourceConfig(
        name="layout",
        target="cat.schema.layout",
        comment="",
        ingestion={"type": "kafka", "secret_scope": "s", "secret_key": "k"},
        columns=[Column(name="value"), Column(name="ts")],
        optimization=optimization,
    )


def test_layout_settings_reach_the_decorator(stub_readers, stub_pyspark):
    """Physical layout must be passed to @dp.table, not merely computed.

    A property resolved correctly and then dropped before registration is
    indistinguishable from having no property at all -- until someone checks
    DESCRIBE DETAIL in production and finds the table unoptimised.
    """
    from pipelines.framework.bronze_factory import build_bronze_table

    cfg = _layout_config(
        cluster_by=["ts"],
        table_properties={"delta.dataSkippingNumIndexedCols": 4},
    )
    build_bronze_table(cfg, spark=object(), dbutils=object())

    kwargs = stub_pyspark["kwargs"][-1]
    assert kwargs["cluster_by"] == ["ts"]
    assert kwargs["table_properties"]["delta.dataSkippingNumIndexedCols"] == "4"
    # The override rides alongside the framework defaults.
    assert kwargs["table_properties"]["delta.autoOptimize.autoCompact"] == "true"


def test_no_clustering_keys_passes_none_not_empty_list(stub_readers, stub_pyspark):
    """An empty cluster_by must not be forwarded as an empty list.

    `cluster_by=[]` reads as "cluster by nothing", which is not the same as
    declining to cluster. Passing None leaves the table unclustered.
    """
    from pipelines.framework.bronze_factory import build_bronze_table

    build_bronze_table(_layout_config(), spark=object(), dbutils=object())

    assert stub_pyspark["kwargs"][-1]["cluster_by"] is None


def test_boolean_property_is_lowercased_for_delta(stub_readers, stub_pyspark):
    """YAML booleans must reach Delta as "false", not Python's "False".

    str(False) is "False", which Delta does not parse as a boolean -- the
    property is stored but the setting silently does not take effect.
    """
    from pipelines.framework.bronze_factory import build_bronze_table

    cfg = _layout_config(table_properties={"delta.enableChangeDataFeed": False})
    build_bronze_table(cfg, spark=object(), dbutils=object())

    props = stub_pyspark["kwargs"][-1]["table_properties"]
    assert props["delta.enableChangeDataFeed"] == "false"
