"""Config loading and validation.

Validation failures must be loud and specific. A config that loads but is
subtly wrong produces a table that silently ingests nothing, which is the
hardest class of bug to notice.
"""

from __future__ import annotations

import pytest

KAFKA_CONFIG = """
name: test_kafka
target: cat.schema.table
ingestion:
  type: kafka
  secret_scope: scope
  secret_key: key
"""


def test_loads_shipped_configs(config_dir):
    """The configs in the repository must always parse."""
    from pipelines.framework import load_source_configs

    configs = load_source_configs(config_dir)

    assert len(configs) >= 4
    names = {c.name for c in configs}
    assert {"transactions", "fraud_watchlist", "merchants", "customers"} <= names


def test_example_files_are_ignored(config_dir):
    """Templates end in .yaml.example and must not be loaded.

    merchant_risk_api.yaml.example points at a non-existent vendor endpoint;
    loading it would fail the pipeline on first run.
    """
    from pipelines.framework import load_source_configs

    configs = load_source_configs(config_dir)

    assert "merchant_risk_api" not in {c.name for c in configs}


def test_managed_sources_are_not_generated(config_dir):
    """Managed CDC is declared as a pipeline spec, not built by the framework."""
    from pipelines.framework import load_source_configs

    configs = load_source_configs(config_dir)
    customers = next(c for c in configs if c.name == "customers")

    assert customers.managed is True
    assert customers.is_generated is False


@pytest.mark.parametrize(
    "content,expected_message",
    [
        (
            "name: x\ningestion:\n  type: kafka\n  secret_scope: s\n  secret_key: k\n",
            "missing required key 'target'",
        ),
        (
            "name: x\ntarget: schema.table\ningestion:\n  type: kafka\n"
            "  secret_scope: s\n  secret_key: k\n",
            "must be catalog.schema.table",
        ),
        (
            "name: x\ntarget: a.b.c\ningestion:\n  type: kafka\n",
            "requires secret_scope, secret_key",
        ),
        (
            "name: x\ntarget: a.b.c\ningestion:\n  type: autoloader\n  format: json\n",
            "requires path",
        ),
        (
            "name: x\ntarget: a.b.c\ningestion:\n  type: rest_api\n",
            "requires url",
        ),
        (
            "name: x\ntarget: a.b.c\ningestion:\n  path: /p\n",
            "missing 'type'",
        ),
    ],
)
def test_invalid_config_is_rejected(write_config, content, expected_message):
    """Every validation failure names the file and what is wrong with it."""
    from pipelines.framework import load_source_configs

    directory = write_config("bad.yaml", content)

    with pytest.raises(ValueError) as excinfo:
        load_source_configs(directory)

    assert expected_message in str(excinfo.value)
    assert "bad.yaml" in str(excinfo.value)


def test_two_part_target_rejected(write_config):
    """Unity Catalog is three-level.

    A two-part name resolves against whatever the pipeline default schema
    happens to be, so the table lands somewhere unintended rather than failing.
    """
    from pipelines.framework import load_source_configs

    directory = write_config(
        "t.yaml",
        "name: x\ntarget: bronze.tbl\ningestion:\n  type: kafka\n"
        "  secret_scope: s\n  secret_key: k\n",
    )

    with pytest.raises(ValueError, match="catalog.schema.table"):
        load_source_configs(directory)


def test_duplicate_source_names_rejected(write_config, tmp_path):
    """Two configs claiming the same name would silently shadow each other."""
    from pipelines.framework import load_source_configs

    write_config("a.yaml", KAFKA_CONFIG)
    (tmp_path / "b.yaml").write_text(
        KAFKA_CONFIG.replace("cat.schema.table", "cat.schema.other"), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="duplicate source names"):
        load_source_configs(tmp_path)


def test_missing_directory_raises(tmp_path):
    from pipelines.framework import load_source_configs

    with pytest.raises(FileNotFoundError):
        load_source_configs(tmp_path / "does_not_exist")


# --- Physical layout declarations ------------------------------------------
#
# Optimization settings live in config so a new source arrives with its file
# management already decided. These tests exist because a mis-declared layout
# fails at table-creation time, deep inside a pipeline update, with a message
# that does not name the config file.


def test_defaults_applied_when_no_optimization_block(write_config):
    """A source declaring nothing still gets the framework's file management.

    Small-file accumulation is the default failure mode of streaming ingestion,
    so compaction has to be opt-out rather than opt-in.
    """
    from pipelines.framework import load_source_configs

    directory = write_config("t.yaml", KAFKA_CONFIG)
    cfg = load_source_configs(directory)[0]

    assert cfg.table_properties["delta.autoOptimize.optimizeWrite"] == "true"
    assert cfg.table_properties["delta.autoOptimize.autoCompact"] == "true"
    assert cfg.table_properties["delta.enableChangeDataFeed"] == "true"
    assert cfg.cluster_by == []


def test_source_can_override_a_default_property(write_config):
    from pipelines.framework import load_source_configs

    directory = write_config(
        "t.yaml",
        KAFKA_CONFIG
        + "optimization:\n"
        "  table_properties:\n"
        "    delta.enableChangeDataFeed: false\n",
    )
    cfg = load_source_configs(directory)[0]

    # Overridden, while the untouched defaults survive.
    assert cfg.table_properties["delta.enableChangeDataFeed"] == "false"
    assert cfg.table_properties["delta.autoOptimize.autoCompact"] == "true"


def test_cluster_by_limited_to_four_columns(write_config):
    """Delta caps clustering keys at four; catch it at config load."""
    from pipelines.framework import load_source_configs

    directory = write_config(
        "t.yaml",
        KAFKA_CONFIG
        + "columns:\n"
        + "".join(f"  - {{name: c{i}}}\n" for i in range(5))
        + "optimization:\n"
        "  cluster_by: [c0, c1, c2, c3, c4]\n",
    )

    with pytest.raises(ValueError, match="at most 4"):
        load_source_configs(directory)


def test_cluster_by_must_reference_declared_columns(write_config):
    """A typo in a clustering key would otherwise fail at table creation.

    This is not hypothetical: clustering bronze.transactions on customer_id was
    rejected here because bronze keeps the Kafka payload unparsed, so
    customer_id does not exist until silver.
    """
    from pipelines.framework import load_source_configs

    directory = write_config(
        "t.yaml",
        KAFKA_CONFIG
        + "columns:\n  - {name: value}\n"
        "optimization:\n  cluster_by: [customer_id]\n",
    )

    with pytest.raises(ValueError, match="cluster_by references columns"):
        load_source_configs(directory)


def test_optimization_must_be_a_mapping(write_config):
    from pipelines.framework import load_source_configs

    directory = write_config("t.yaml", KAFKA_CONFIG + "optimization: [a, b]\n")

    with pytest.raises(ValueError, match="must be a mapping"):
        load_source_configs(directory)
