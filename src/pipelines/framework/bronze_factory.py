"""Build bronze streaming tables from source configs.

The important mechanic here is the closure in `build_bronze_table`. `@dp.table`
registers a function with the Lakeflow runtime, so generating N tables means
creating N distinct function objects. Defining the function inside a helper --
rather than in the loop body directly -- binds `cfg` per call and avoids the
classic late-binding bug where every generated table ends up reading the last
config in the list.
"""

from __future__ import annotations

from typing import Any

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .readers import BATCH_TYPES, get_reader
from .source_config import SourceConfig


def _project(df: DataFrame, cfg: SourceConfig) -> DataFrame:
    """Apply the configured column list plus the standard bronze audit column.

    An empty column list means "keep the source shape as-is", which is the
    right default for a raw layer whose schema is not yet known.
    """
    if cfg.columns:
        selected = [
            F.expr(c.expr).alias(c.name) if c.expr else F.col(c.name)
            for c in cfg.columns
        ]
        df = df.select(*selected)

    # Every bronze table carries when the row landed. Downstream layers compare
    # this against event time to measure ingestion lag.
    return df.withColumn("ingestion_timestamp", F.current_timestamp())


def build_bronze_table(cfg: SourceConfig, spark: Any, dbutils: Any) -> None:
    """Register one bronze dataset for `cfg`.

    Streaming sources become streaming tables (append-only, incremental).
    Batch sources -- currently REST pulls -- become materialised views that are
    fully recomputed each update, which is the correct semantic for a reference
    dataset fetched as a complete snapshot.
    """
    reader = get_reader(cfg.ingestion_type)
    is_batch = cfg.ingestion_type in BATCH_TYPES

    if is_batch:

        @dp.materialized_view(name=cfg.target, comment=cfg.comment)
        def _bronze_view() -> DataFrame:
            return _project(reader(spark, dbutils, cfg.ingestion), cfg)

        _bronze_view.__name__ = f"bronze_{cfg.name}"
        return

    @dp.table(name=cfg.target, comment=cfg.comment)
    def _bronze_table() -> DataFrame:
        return _project(reader(spark, dbutils, cfg.ingestion), cfg)

    # Give the registered function a readable name; without this every
    # generated table shows up as `_bronze_table` in logs and error messages.
    _bronze_table.__name__ = f"bronze_{cfg.name}"


def build_all(
    configs: list[SourceConfig], spark: Any, dbutils: Any
) -> tuple[list[str], list[dict[str, Any]]]:
    """Register every generated source.

    Returns (built_table_names, per_source_results). Results carry a status for
    every config -- including the ones skipped -- so the audit table records
    what the platform was asked to do, not only what succeeded.

    A failure to register one source does not abort the others. Losing every
    table because one config is malformed is a worse outcome than losing one,
    and the failure is recorded rather than swallowed.
    """
    built: list[str] = []
    results: list[dict[str, Any]] = []

    for cfg in configs:
        entry = {
            "source_name": cfg.name,
            "target_table": cfg.target,
            "ingestion_type": cfg.ingestion_type,
        }

        if not cfg.is_generated:
            results.append(
                {
                    **entry,
                    "status": "SKIPPED",
                    "detail": "declared outside this framework (managed ingestion)",
                }
            )
            continue

        try:
            build_bronze_table(cfg, spark, dbutils)
        except Exception as exc:  # noqa: BLE001 - recorded, then re-surfaced in logs
            results.append({**entry, "status": "FAILED", "detail": str(exc)[:500]})
            print(f"  !! {cfg.name}: registration failed: {exc}")
            continue

        built.append(cfg.target)
        results.append({**entry, "status": "REGISTERED", "detail": None})

    return built, results
