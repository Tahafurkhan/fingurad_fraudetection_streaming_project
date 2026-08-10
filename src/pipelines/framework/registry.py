"""Operational metadata for the ingestion framework.

The YAML configs say what *should* be ingested. They cannot say whether a
source actually ran, how much it moved, or that it quietly stopped three days
ago. That is what these two tables are for:

  finguard.ops.source_registry   one row per source, refreshed from the YAML
                                 on every pipeline update. Answers "what is
                                 this platform supposed to be ingesting?"

  finguard.ops.ingestion_audit   one row per source per pipeline update.
                                 Answers "did it run, and what happened?"

The split matters. Structural definitions stay in git as YAML, where they get
version history and code review. Runtime state lives in Delta, where it can be
queried, dashboarded and alerted on. Putting source definitions in a table
would lose the diff; putting run history in YAML is impossible.

Both tables are written with plain Spark rather than @dp.table, because they
describe the pipeline rather than being part of its dataflow -- a Lakeflow
dataset cannot record its own failure.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .source_config import SourceConfig

REGISTRY_TABLE = "finguard.ops.source_registry"
AUDIT_TABLE = "finguard.ops.ingestion_audit"


def _ensure_tables(spark: Any) -> None:
    """Create the ops tables if absent. Safe to call on every update."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
            source_name      STRING  COMMENT 'Logical source name from config',
            target_table     STRING  COMMENT 'Fully qualified bronze table',
            ingestion_type   STRING  COMMENT 'kafka | autoloader | rest_api | managed_cdc',
            is_generated     BOOLEAN COMMENT 'Built by this framework, vs declared elsewhere',
            column_count     INT     COMMENT 'Columns declared in config (0 = pass through)',
            config_json      STRING  COMMENT 'Full ingestion block, for audit/debug',
            registered_at    TIMESTAMP
        )
        USING DELTA
        COMMENT 'Source registry, refreshed from config/sources/*.yaml on each pipeline update'
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            run_id           STRING  COMMENT 'Groups all sources in one pipeline update',
            source_name      STRING,
            target_table     STRING,
            ingestion_type   STRING,
            status           STRING  COMMENT 'REGISTERED | SKIPPED | FAILED',
            detail           STRING  COMMENT 'Error message when status = FAILED',
            run_timestamp    TIMESTAMP
        )
        USING DELTA
        COMMENT 'One row per source per pipeline update'
        """
    )


def sync_registry(spark: Any, configs: list[SourceConfig]) -> None:
    """Replace the registry with the current config set.

    A full replace rather than a merge: the registry mirrors the YAML exactly,
    so a source deleted from config should disappear here too. Keeping a stale
    row would be worse than useless -- it would claim the platform ingests
    something it no longer does.
    """
    _ensure_tables(spark)

    now = datetime.now(timezone.utc)
    rows = [
        (
            cfg.name,
            cfg.target,
            cfg.ingestion_type,
            cfg.is_generated,
            len(cfg.columns),
            json.dumps(cfg.ingestion, sort_keys=True),
            now,
        )
        for cfg in configs
    ]

    if not rows:
        return

    df = spark.createDataFrame(
        rows,
        "source_name STRING, target_table STRING, ingestion_type STRING, "
        "is_generated BOOLEAN, column_count INT, config_json STRING, "
        "registered_at TIMESTAMP",
    )
    df.write.mode("overwrite").saveAsTable(REGISTRY_TABLE)


def record_run(spark: Any, run_id: str, results: list[dict[str, Any]]) -> None:
    """Append one audit row per source for this pipeline update."""
    if not results:
        return

    _ensure_tables(spark)

    now = datetime.now(timezone.utc)
    rows = [
        (
            run_id,
            r["source_name"],
            r["target_table"],
            r["ingestion_type"],
            r["status"],
            r.get("detail"),
            now,
        )
        for r in results
    ]

    df = spark.createDataFrame(
        rows,
        "run_id STRING, source_name STRING, target_table STRING, "
        "ingestion_type STRING, status STRING, detail STRING, "
        "run_timestamp TIMESTAMP",
    )
    df.write.mode("append").saveAsTable(AUDIT_TABLE)
