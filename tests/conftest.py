"""Shared fixtures.

The ingestion framework is importable without a Spark session, which is what
makes these tests possible in CI. `pyspark` is stubbed rather than installed:
the tests exercise config loading, validation and registration logic, none of
which need a real engine. Installing PySpark to test YAML parsing would make CI
minutes slower for no additional coverage.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))


@pytest.fixture(autouse=True)
def stub_pyspark(monkeypatch):
    """Install a fake pyspark so framework modules import cleanly.

    Autouse because every test in this suite imports the framework, and a
    partially-imported module would leak between tests.
    """
    registered: dict[str, list[str]] = {"table": [], "materialized_view": []}

    def _table(**kwargs):
        def decorator(fn):
            registered["table"].append(kwargs.get("name"))
            return fn

        return decorator

    def _materialized_view(**kwargs):
        def decorator(fn):
            registered["materialized_view"].append(kwargs.get("name"))
            return fn

        return decorator

    fake_dp = types.SimpleNamespace(table=_table, materialized_view=_materialized_view)

    pyspark_mod = types.ModuleType("pyspark")
    pyspark_mod.pipelines = fake_dp

    class FakeColumn:
        """Minimal Column stand-in.

        Needs .alias() because the factory calls it on expression columns.
        Returning a bare string here would make the test fail on the stub
        rather than on the code under test.
        """

        def __init__(self, name):
            self.name = name

        def alias(self, new_name):
            return FakeColumn(new_name)

        def __repr__(self):
            return f"Column({self.name})"

    sql_mod = types.ModuleType("pyspark.sql")
    sql_mod.DataFrame = object
    functions_mod = types.SimpleNamespace(
        col=lambda name: FakeColumn(name),
        expr=lambda expression: FakeColumn(expression),
        current_timestamp=lambda: FakeColumn("current_timestamp"),
    )
    sql_mod.functions = functions_mod

    monkeypatch.setitem(sys.modules, "pyspark", pyspark_mod)
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", fake_dp)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions_mod)

    # Drop any previously imported framework modules so each test re-imports
    # against this stub.
    for name in list(sys.modules):
        if name.startswith("pipelines"):
            del sys.modules[name]

    return registered


@pytest.fixture
def config_dir() -> Path:
    """The real config directory, so tests fail if a shipped config breaks."""
    return PROJECT_ROOT / "config" / "sources"


@pytest.fixture
def write_config(tmp_path):
    """Write a YAML config into a temp directory and return the directory."""

    def _write(filename: str, content: str) -> Path:
        (tmp_path / filename).write_text(content, encoding="utf-8")
        return tmp_path

    return _write
