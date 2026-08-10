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

    @property
    def ingestion_type(self) -> str:
        return self.ingestion["type"]

    @property
    def is_generated(self) -> bool:
        """True when the bronze generator should build a table for this source."""
        return not self.managed and self.ingestion_type in GENERATED_TYPES


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
            )
        )

    names = [c.name for c in configs]
    duplicates = {n for n in names if names.count(n) > 1}
    if duplicates:
        raise ValueError(f"duplicate source names in config: {sorted(duplicates)}")

    return configs
