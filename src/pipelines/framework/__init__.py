"""Metadata-driven ingestion framework for the bronze layer."""

from .bronze_factory import build_all, build_bronze_table
from .registry import AUDIT_TABLE, REGISTRY_TABLE, record_run, sync_registry
from .source_config import SourceConfig, load_source_configs

__all__ = [
    "SourceConfig",
    "load_source_configs",
    "build_all",
    "build_bronze_table",
    "sync_registry",
    "record_run",
    "REGISTRY_TABLE",
    "AUDIT_TABLE",
]
