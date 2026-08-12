"""Assert that the four definitions of the transaction contract agree.

THE PROBLEM THIS FILE EXISTS TO SOLVE
-------------------------------------
The same contract is written down in four places, because Lakeflow's execution
model prevents the pipeline files from importing a shared constant (each is
exec'd into a uniquely-named synthetic module, so there is no stable name to
import from):

  1. src/producer/schema.py                              -- producer JSON Schema
  2. src/pipelines/transforms/transactions.py            -- the shared module
  3. src/pipelines/streaming/silver/fingurad_silver.py   -- silver model
  4. src/pipelines/streaming/silver/transactions_quarantine.py

Four copies of one contract drift. That is not a hypothetical: adding a field
upstream means editing all four, and forgetting one produces NO ERROR. The
pipeline stays green, `from_json` returns null for the field the stale schema
does not mention, and the data is quietly wrong. Row counts are unchanged, so
the usual monitoring says nothing.

These tests are the only mechanism that makes such a drift loud. They parse the
model files as text rather than importing them -- the models call `spark` at
module scope and cannot be imported outside Lakeflow -- and compare what they
declare against the shared module.

Text parsing is a compromise, and an honest one: it is brittle if someone
reformats the models heavily, but it is the only way to check files that cannot
be imported. A failure here means "these files disagree", which is exactly the
signal wanted.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_MODEL = PROJECT_ROOT / "src" / "pipelines" / "streaming" / "silver" / "fingurad_silver.py"
QUARANTINE_MODEL = (
    PROJECT_ROOT / "src" / "pipelines" / "streaming" / "silver" / "transactions_quarantine.py"
)


@pytest.fixture(scope="module")
def transforms():
    """The shared module.

    Imported directly rather than through the conftest pyspark stub: this
    module needs the real pyspark *types* (StructType and friends) to build
    PAYLOAD_SCHEMA. It never creates a session, so importing it is cheap and
    needs no engine.
    """
    import sys

    sys.path.insert(0, str(PROJECT_ROOT / "src"))
    try:
        from pipelines.transforms import transactions
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"pyspark types unavailable: {exc}")
    return transactions


def _model_source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Expectations vs drop rules
# ---------------------------------------------------------------------------


def test_silver_expectations_match_shared_drop_rules(transforms):
    """`@dp.expect_or_drop` decorators must match DROP_RULES exactly.

    THE LOAD-BEARING TEST FOR THE NO-REFACTOR DECISION. The silver model was
    deliberately left calling its own inline logic rather than the shared
    module, because it is working production code against live Kafka and
    rewriting it carries regression risk.

    The cost of that decision is two expressions of the same rules. This test
    is what makes the cost affordable: the moment they disagree, CI fails.
    Without it, the tests would verify one set of rules while the pipeline
    enforced another -- a green suite proving nothing about production.
    """
    source = _model_source(SILVER_MODEL)
    declared = dict(
        re.findall(
            r'@dp\.expect_or_drop\(\s*["\'](\w+)["\']\s*,\s*["\']([^"\']+)["\']', source
        )
    )

    assert declared, "No expect_or_drop decorators found -- did the model change shape?"

    def _norm(rules: dict[str, str]) -> dict[str, str]:
        return {k: " ".join(v.split()).upper() for k, v in rules.items()}

    assert _norm(declared) == _norm(transforms.DROP_RULES), (
        "Silver model expectations and transforms.DROP_RULES disagree. "
        "Both define which rows are dropped; update them together."
    )


def test_silver_flag_expectations_match_shared_flag_rules(transforms):
    """`@dp.expect` (flag, not drop) must match FLAG_RULES.

    Kept separate from the drop-rule test because confusing the two severities
    is the actual failure mode: promoting `amount > 0` from expect to
    expect_or_drop would silently start deleting legitimate reversals.
    """
    source = _model_source(SILVER_MODEL)
    declared = dict(
        re.findall(
            r'@dp\.expect\(\s*["\'](\w+)["\']\s*,\s*["\']([^"\']+)["\']', source
        )
    )

    def _norm(rules: dict[str, str]) -> dict[str, str]:
        return {k: " ".join(v.split()).upper() for k, v in rules.items()}

    assert _norm(declared) == _norm(transforms.FLAG_RULES)


def test_quarantine_drop_rules_match_shared_module(transforms):
    """The quarantine model's rule set must match the silver drop rules.

    These two are two halves of one partition: silver keeps the rows that pass,
    quarantine keeps the rows that fail. If the rule sets differ, rows can
    satisfy neither -- and vanish with no record in either table. That is the
    exact failure DQ-06 reconciliation is meant to make impossible.
    """
    source = _model_source(QUARANTINE_MODEL)
    block = re.search(r"_DROP_RULES\s*=\s*\{(.*?)\}", source, re.DOTALL)
    assert block, "_DROP_RULES not found in the quarantine model"

    declared = dict(re.findall(r'["\'](\w+)["\']\s*:\s*["\']([^"\']+)["\']', block.group(1)))

    def _norm(rules: dict[str, str]) -> dict[str, str]:
        return {k: " ".join(v.split()).upper() for k, v in rules.items()}

    assert _norm(declared) == _norm(transforms.DROP_RULES), (
        "Quarantine model and transforms.DROP_RULES disagree. Rows failing a "
        "rule one knows and the other does not would be lost entirely."
    )


# ---------------------------------------------------------------------------
# Payload schema
# ---------------------------------------------------------------------------


def _struct_fields(source: str, variable: str) -> list[str]:
    """Field names from a StructType literal assigned to `variable`."""
    block = re.search(rf"{variable}\s*=\s*StructType\(\s*\[(.*?)\]\s*\)", source, re.DOTALL)
    if not block:
        return []
    return re.findall(r'StructField\(\s*["\'](\w+)["\']', block.group(1))


def test_silver_payload_schema_matches_shared_module(transforms):
    """The silver model's inline schema must match PAYLOAD_SCHEMA.

    Order is asserted, not just membership. `delta.dataSkippingNumIndexedCols`
    is set to 16 so statistics cover every column through
    transaction_timestamp, which liquid clustering on that column requires.
    Inserting a field ahead of it pushes it past the budget and the pipeline
    fails with DELTA_CLUSTERING_COLUMN_MISSING_STATS -- a failure whose cause
    is far from its symptom.
    """
    source = _model_source(SILVER_MODEL)
    declared = _struct_fields(source, "schema")
    expected = [f.name for f in transforms.PAYLOAD_SCHEMA.fields]

    assert declared == expected, (
        "Silver model payload schema and transforms.PAYLOAD_SCHEMA differ. "
        "Field ORDER matters here -- see dataSkippingNumIndexedCols."
    )


def test_quarantine_payload_schema_matches_shared_module(transforms):
    source = _model_source(QUARANTINE_MODEL)
    declared = _struct_fields(source, "_PAYLOAD_SCHEMA")
    expected = [f.name for f in transforms.PAYLOAD_SCHEMA.fields]

    assert declared == expected


def test_producer_contract_matches_shared_module(transforms):
    """Producer JSON Schema and consumer Spark schema must agree field-for-field.

    The end-to-end contract check (TRD TS-02, RISK-01). The producer and the
    consumer are two definitions of one wire format living in different files,
    and they drift silently -- a field added to the producer and not to the
    consumer is simply never read, with no error anywhere.

    This is the closest thing to schema-registry enforcement available without
    a registry, which is why RISK-01 remains open in the TRD rather than being
    marked mitigated.
    """
    import sys

    sys.path.insert(0, str(PROJECT_ROOT / "src" / "producer"))
    import schema as producer_schema

    producer_fields = set(producer_schema.TRANSACTION_SCHEMA["properties"])
    consumer_fields = {f.name for f in transforms.PAYLOAD_SCHEMA.fields}

    assert producer_fields == consumer_fields, (
        f"Producer emits {producer_fields - consumer_fields or '{}'} that silver "
        f"does not read; silver expects {consumer_fields - producer_fields or '{}'} "
        "that the producer does not send."
    )


def test_producer_required_fields_are_the_drop_rules(transforms):
    """Producer `required` and consumer drop rules must describe one policy.

    A field the producer guarantees but the consumer does not check is an
    unenforced guarantee. A field the consumer drops on but the producer does
    not require means the producer can legitimately emit rows the consumer
    quarantines -- a quarantine spike caused by nothing being wrong.

    `amount` is intentionally excluded: it is required on the wire but is a
    FLAG rule, not a DROP rule, because zero and negative values are legitimate
    reversals (TRD 9.2). Asserting the difference pins that decision.
    """
    import sys

    sys.path.insert(0, str(PROJECT_ROOT / "src" / "producer"))
    import schema as producer_schema

    required = set(producer_schema.TRANSACTION_SCHEMA["required"])
    drop_guarded = {
        rule.split()[0] for rule in transforms.DROP_RULES.values()
    }

    flagged = {rule.split()[0] for rule in transforms.FLAG_RULES.values()}
    unguarded = required - drop_guarded - flagged

    assert not unguarded, (
        f"Producer requires {unguarded}, but silver neither drops nor flags on "
        "them. Either the guarantee is unenforced or the rule is missing."
    )
