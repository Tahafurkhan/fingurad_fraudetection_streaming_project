"""Load and validate bronze source configs.

Config lives in `config/sources/*.yaml`, one file per source. Keeping the
declarations out of Python is what makes adding a source a config change rather
than a code change -- the bronze generator loops over whatever it finds here.

Validation is deliberately strict and fails at import time. A malformed config
should break the pipeline update immediately with a clear message, rather than
producing a table that silently ingests nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# Ingestion types the bronze generator knows how to build. `managed_cdc` is
# intentionally absent: those pipelines are declared as specs, not code.
GENERATED_TYPES = {"kafka", "autoloader", "rest_api"}

# Delta properties applied to every generated bronze table unless a source
# overrides them. These are defaults rather than hard-coded values because the
# right answer differs per source, but the right *default* does not.
#
# optimizeWrite adds a shuffle before writing so each partition produces one
# appropriately-sized file instead of many small ones. autoCompact then runs a
# compaction pass after a write that leaves too many small files behind.
#
# Both matter specifically for streaming ingestion: every micro-batch commits
# its own files, so a table fed by a 10-second trigger accumulates thousands of
# tiny files a day. Measured on this project before these were set,
# bronze.fraud_watchlist held 19 files for 93 rows -- roughly 5KB per file
# against a Delta target of 128MB+. Every query paid the per-file open cost and
# the metadata overhead for essentially no data.
DEFAULT_TABLE_PROPERTIES: dict[str, str] = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    # Change Data Feed makes row-level changes readable by downstream
    # consumers without diffing snapshots. It is what lets a silver model read
    # only what changed rather than rescanning bronze, and it is required for
    # any consumer wanting insert/update/delete semantics rather than
    # append-only. It costs storage: change files are written alongside data.
    "delta.enableChangeDataFeed": "true",
    # Let Delta size files based on the table's actual rewrite pattern rather
    # than a fixed target. Appropriate here because these tables are written by
    # streaming appends and rewritten by compaction, which have different
    # optimal file sizes.
    "delta.tuneFileSizesForRewrites": "true",
}


@dataclass(frozen=True)
class Column:
    """One output column. `expr` is a SQL expression; None means pass through."""

    name: str
    expr: str | None = None


@dataclass(frozen=True)
class SourceConfig:
    name: str
    target: str
    comment: str
    ingestion: dict[str, Any]
    columns: list[Column] = field(default_factory=list)
    managed: bool = False
    # Physical-layout declarations, kept next to the source definition so a new
    # source arrives with its file management already decided rather than
    # inheriting whatever the default happens to be.
    optimization: dict[str, Any] = field(default_factory=dict)

    @property
    def ingestion_type(self) -> str:
        return self.ingestion["type"]

    @property
    def is_generated(self) -> bool:
        """True when the bronze generator should build a table for this source."""
        return not self.managed and self.ingestion_type in GENERATED_TYPES

    @property
    def cluster_by(self) -> list[str]:
        """Liquid clustering keys, empty when the source declares none.

        Empty is a legitimate answer. Clustering only pays off when a table has
        enough files for pruning to skip some; declaring keys on a table that
        fits in one file adds metadata and buys nothing.
        """
        return list(self.optimization.get("cluster_by", []))

    @property
    def table_properties(self) -> dict[str, str]:
        """Delta properties for this table: defaults with per-source overrides.

        Values are normalised to strings because Delta stores properties as
        strings. Booleans need explicit handling: YAML parses `false` into a
        Python bool, and `str(False)` is "False" with a capital F, which Delta
        does not recognise as falsey. The property would be set to an
        unparseable value and the setting would silently not take effect.
        """
        props = dict(DEFAULT_TABLE_PROPERTIES)
        for key, value in self.optimization.get("table_properties", {}).items():
            props[key] = str(value).lower() if isinstance(value, bool) else str(value)
        return props


def _validate(cfg: dict[str, Any], path: Path) -> None:
    for key in ("name", "target", "ingestion"):
        if key not in cfg:
            raise ValueError(f"{path.name}: missing required key '{key}'")

    if "type" not in cfg["ingestion"]:
        raise ValueError(f"{path.name}: ingestion is missing 'type'")

    # Unity Catalog is three-level; a two-part name would silently resolve
    # against whatever the pipeline default schema happens to be.
    if cfg["target"].count(".") != 2:
        raise ValueError(
            f"{path.name}: target '{cfg['target']}' must be "
            "catalog.schema.table"
        )

    kind = cfg["ingestion"]["type"]
    required = {
        "kafka": ("secret_scope", "secret_key"),
        "autoloader": ("path", "format"),
        "rest_api": ("url",),
    }.get(kind, ())

    missing = [k for k in required if k not in cfg["ingestion"]]
    if missing:
        raise ValueError(
            f"{path.name}: ingestion type '{kind}' requires {', '.join(missing)}"
        )

    optimization = cfg.get("optimization", {})
    if not isinstance(optimization, dict):
        raise ValueError(f"{path.name}: 'optimization' must be a mapping")

    cluster_by = optimization.get("cluster_by", [])
    if not isinstance(cluster_by, list):
        raise ValueError(f"{path.name}: 'cluster_by' must be a list of columns")

    # Delta caps liquid clustering at four keys. Failing here beats failing at
    # table-creation time with a less specific message from the engine.
    if len(cluster_by) > 4:
        raise ValueError(
            f"{path.name}: cluster_by accepts at most 4 columns, got "
            f"{len(cluster_by)}"
        )

    # Clustering keys must be columns the table actually produces. A typo would
    # otherwise surface as a table-creation failure well after config load.
    declared = {c["name"] for c in cfg.get("columns", [])}
    if declared:
        unknown = [c for c in cluster_by if c not in declared]
        if unknown:
            raise ValueError(
                f"{path.name}: cluster_by references columns not in this "
                f"source's column list: {unknown}"
            )


def load_source_configs(config_dir: str | Path) -> list[SourceConfig]:
    """Load every source config, sorted by name for deterministic ordering."""
    directory = Path(config_dir)
    if not directory.is_dir():
        raise FileNotFoundError(f"source config directory not found: {directory}")

    configs: list[SourceConfig] = []
    for path in sorted(directory.glob("*.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        _validate(raw, path)

        columns = [
            Column(name=c["name"], expr=c.get("expr"))
            for c in raw.get("columns", [])
        ]
        configs.append(
            SourceConfig(
                name=raw["name"],
                target=raw["target"],
                comment=raw.get("comment", ""),
                ingestion=raw["ingestion"],
                columns=columns,
                managed=raw.get("managed", False),
                optimization=raw.get("optimization", {}),
            )
        )

    names = [c.name for c in configs]
    duplicates = {n for n in names if names.count(n) > 1}
    if duplicates:
        raise ValueError(f"duplicate source names in config: {sorted(duplicates)}")

    return configs
