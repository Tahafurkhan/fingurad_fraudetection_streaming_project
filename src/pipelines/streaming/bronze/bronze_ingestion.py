# Bronze layer entry point.
#
# This single file replaces the previous one-file-per-source approach
# (finguard_bronze.py, fraud_watchlist_bronze.py). Every bronze table is built
# from a YAML declaration in config/sources/, so onboarding a new source is a
# config change with no Python edit.
#
# `spark` and `dbutils` are injected as globals by the Databricks runtime and
# are passed explicitly into the framework so the framework itself stays
# testable outside a cluster.

import sys
import uuid
from pathlib import Path


# Lakeflow runs pipeline files through exec() rather than importing them as
# modules, so `__file__` does not exist here. The project root is located by
# walking up from the current working directory until the marker directories
# appear, with an explicit fallback to the known workspace path.
#
# This is the kind of assumption that only breaks at runtime: the same code
# resolves fine under a normal import, which is why it has to be verified on a
# real pipeline update rather than locally.
def _find_project_root() -> Path:
    candidates = [Path.cwd(), *Path.cwd().parents]
    for candidate in candidates:
        if (candidate / "src" / "pipelines" / "framework").is_dir():
            return candidate
    # Fallback: the pipeline's configured root_path, two levels above this file.
    return Path(
        "/Workspace/Users/tahafurkhan@gmail.com/finguard_project"
    )


_ROOT = _find_project_root()
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from pipelines.framework import (  # noqa: E402
    build_all,
    load_source_configs,
    record_run,
    sync_registry,
)

_CONFIG_DIR = _ROOT / "config" / "sources"

_configs = load_source_configs(_CONFIG_DIR)
_built, _results = build_all(_configs, spark, dbutils)  # noqa: F821 - runtime globals

print(f"bronze: registered {len(_built)} table(s) from {len(_configs)} config(s)")
for _target in _built:
    print(f"  - {_target}")

_skipped = [r for r in _results if r["status"] == "SKIPPED"]
if _skipped:
    print("  skipped (declared outside this framework):")
    for _r in _skipped:
        print(f"  - {_r['target_table']}")

_failed = [r for r in _results if r["status"] == "FAILED"]
if _failed:
    print(f"  {len(_failed)} source(s) FAILED to register:")
    for _r in _failed:
        print(f"  - {_r['source_name']}: {_r['detail']}")

# Operational metadata. Wrapped because observability must never be the reason
# ingestion fails -- if the ops tables are unavailable, the pipeline should
# still ingest and simply log that bookkeeping was skipped.
try:
    sync_registry(spark, _configs)  # noqa: F821
    record_run(spark, str(uuid.uuid4()), _results)  # noqa: F821
    print("  ops: registry synced, run recorded")
except Exception as _exc:  # noqa: BLE001
    print(f"  ops: metadata write skipped ({_exc})")
