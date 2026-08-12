"""Integration tests for the bronze -> silver transformation.

These execute real Catalyst against real DataFrames. Everything here is
transformation logic that behaves identically on a laptop and on Databricks --
`from_json`, null handling, SQL predicate evaluation. What is deliberately NOT
tested here is anything platform-specific: Unity Catalog grants, Lakeflow
expectation accounting, liquid clustering. Those need a real workspace and
belong in a post-deploy smoke test, not in a suite that gates every commit.

The distinction matters because it determines what a green run here actually
proves: that the logic is right, not that the deployment is.
"""

from __future__ import annotations

import json

import pytest

from pipelines.transforms import transactions as T

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_valid_payload_parses_all_contract_fields(spark, bronze_frame, valid_payload):
    """Every field in the contract survives the bronze -> silver parse."""
    parsed = T.parse_payload(bronze_frame([valid_payload()]))
    row = parsed.collect()[0]

    assert row.transaction_id == "txn-0001"
    assert row.customer_id == "cust-0001"
    assert row.amount == 125.50
    assert row.currency == "INR"
    assert row.is_international is False
    # Timestamp arrives as an ISO string and must land as a real timestamp,
    # not a string -- every downstream window depends on it.
    assert row.transaction_timestamp is not None
    assert row.transaction_timestamp.year == 2026


def test_kafka_envelope_is_preserved_for_replay(spark, bronze_frame, valid_payload):
    """Offset coordinates survive parsing.

    Not incidental plumbing: TRD RS-03 requires reprocessing from an arbitrary
    source position, and a row whose offset was dropped during parsing cannot
    be traced back to the message that produced it.
    """
    parsed = T.parse_payload(bronze_frame([valid_payload()]))
    row = parsed.collect()[0]

    assert row.kafka_topic == "topic_0"
    assert row.kafka_offset == 1000
    assert row.kafka_partition == 0
    assert row.bronze_ingestion_timestamp is not None


def test_unparseable_payload_yields_all_nulls_not_an_error(spark, bronze_frame):
    """Malformed JSON must not kill the batch.

    This is the poison-pill case. `from_json` in PERMISSIVE mode returns nulls
    rather than raising, which is what lets one bad message be quarantined
    instead of halting ingestion for every well-formed message behind it.

    The test pins that behaviour deliberately: if someone later switches to
    FAILFAST, a single malformed payload would stop the pipeline, and this
    test is what tells them before production does.
    """
    parsed = T.parse_payload(bronze_frame(["{ this is not json at all"]), keep_raw=True)
    row = parsed.collect()[0]

    assert row.transaction_id is None
    assert row.amount is None
    # The raw string is the only evidence of what actually arrived.
    assert row.raw_payload == "{ this is not json at all"


def test_unknown_fields_are_ignored_not_fatal(spark, bronze_frame, valid_payload):
    """An added upstream field must not break the consumer.

    This is the additive half of the schema-evolution clause (TRD 6.1): the
    producer may add optional fields at will, with no notice, because the
    consumer projects explicitly and ignores what it does not know. If this
    test fails, that contract term is not actually safe and the notice period
    has to change.
    """
    payload = json.loads(valid_payload())
    payload["brand_new_upstream_field"] = "surprise"
    parsed = T.parse_payload(bronze_frame([json.dumps(payload)]))
    row = parsed.collect()[0]

    assert row.transaction_id == "txn-0001"
    assert "brand_new_upstream_field" not in parsed.columns


def test_removed_field_becomes_null_and_is_caught_by_rules(
    spark, bronze_frame, valid_payload
):
    """The breaking half of schema evolution.

    A field that stops arriving does not error -- it becomes null. For a
    mandatory field that null is caught by the drop rules and the row is
    quarantined, which is the mechanism that turns a silent upstream removal
    into a visible quarantine spike. This is why removal carries a 90-day
    notice period and addition carries none.
    """
    parsed = T.parse_payload(bronze_frame([valid_payload(customer_id=None)]))
    assert parsed.collect()[0].customer_id is None

    assert T.select_valid(parsed).count() == 0
    assert T.select_quarantined(parsed).count() == 1


# ---------------------------------------------------------------------------
# Quality routing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "missing_field,expected_rule",
    [
        ("transaction_id", "valid_transaction_id"),
        ("customer_id", "valid_customer_id"),
        ("card_number", "valid_card_number"),
        ("merchant_id", "valid_merchant_id"),
    ],
)
def test_each_mandatory_field_routes_to_quarantine_with_its_reason(
    spark, bronze_frame, valid_payload, missing_field, expected_rule
):
    """Each drop rule fires independently and names itself.

    The rule name is what an operator reads at 02:00, so it is asserted rather
    than just the row count -- a quarantine table that says a row failed but
    not why turns a five-minute diagnosis into an hour of guessing.
    """
    parsed = T.parse_payload(
        bronze_frame([valid_payload(**{missing_field: None})]), keep_raw=True
    )
    quarantined = T.select_quarantined(parsed).collect()

    assert len(quarantined) == 1
    assert expected_rule in quarantined[0].failed_rules


def test_multiple_failures_are_all_recorded(spark, bronze_frame, valid_payload):
    """A row failing several rules lists all of them, not just the first.

    Reporting only the first failure sends an operator round a fix-one-rerun
    loop, discovering the next problem each time.
    """
    parsed = T.parse_payload(
        bronze_frame([valid_payload(customer_id=None, merchant_id=None)]),
        keep_raw=True,
    )
    failed = T.select_quarantined(parsed).collect()[0].failed_rules

    assert "valid_customer_id" in failed
    assert "valid_merchant_id" in failed


def test_valid_rows_never_reach_quarantine(spark, bronze_frame, valid_payload):
    parsed = T.parse_payload(bronze_frame([valid_payload()]))

    assert T.select_valid(parsed).count() == 1
    assert T.select_quarantined(parsed).count() == 0


def test_quarantine_retains_raw_payload_for_replay(spark, bronze_frame, valid_payload):
    """The rejected message is kept verbatim.

    Without it a quarantined row records that something failed but not what
    arrived, and the row cannot be reprocessed after the upstream fix.
    """
    payload = valid_payload(customer_id=None)
    parsed = T.parse_payload(bronze_frame([payload]), keep_raw=True)
    row = T.select_quarantined(parsed).collect()[0]

    assert row.raw_payload == payload
    assert row.kafka_offset == 1000


# ---------------------------------------------------------------------------
# The reconciliation invariant -- DQ-06
# ---------------------------------------------------------------------------


def test_every_row_lands_in_exactly_one_path(spark, bronze_frame, valid_payload):
    """total == accepted + quarantined, for a mixed batch.

    THE MOST IMPORTANT TEST IN THIS FILE. It is the arithmetic that makes data
    loss impossible to miss (TRD DQ-06, FR-10). If a row satisfies neither
    branch it has vanished silently -- the pipeline stays green, the row count
    is quietly lower, and nobody asks why. A partition of the input is the only
    thing that rules that out.
    """
    payloads = [
        valid_payload(transaction_id="txn-a"),
        valid_payload(transaction_id="txn-b"),
        valid_payload(transaction_id=None),
        valid_payload(customer_id=None),
        "{ malformed",
    ]
    parsed = T.parse_payload(bronze_frame(payloads), keep_raw=True)
    total, accepted, quarantined = T.reconciliation_counts(parsed)

    assert total == 5
    assert accepted == 2
    assert quarantined == 3
    assert total == accepted + quarantined


def test_malformed_payload_is_quarantined_not_dropped(spark, bronze_frame):
    """Unparseable JSON must be captured, not silently discarded.

    Every field is null, so it fails every drop rule -- which is precisely how
    it ends up quarantined with its raw payload intact rather than vanishing.
    """
    parsed = T.parse_payload(bronze_frame(["not json"]), keep_raw=True)

    assert T.select_valid(parsed).count() == 0
    assert T.select_quarantined(parsed).count() == 1


def test_empty_batch_holds_the_invariant(spark, bronze_frame):
    """An empty input must not error. Real pipelines see empty batches often."""
    parsed = T.parse_payload(bronze_frame([]), keep_raw=True)
    total, accepted, quarantined = T.reconciliation_counts(parsed)

    assert (total, accepted, quarantined) == (0, 0, 0)


# ---------------------------------------------------------------------------
# Flag rules -- counted, not dropped
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("amount", [0.0, -50.0])
def test_non_positive_amount_is_flagged_but_retained(
    spark, bronze_frame, valid_payload, amount
):
    """`amount > 0` flags without removing. TRD 9.2.

    The distinction is deliberate and worth protecting with a test: zero and
    negative amounts occur legitimately as reversals and adjustments.
    Quarantining them would delete real transactions and under-report fraud
    exposure. If someone later "tightens" this into a drop rule, this test
    fails and explains why it should not be.
    """
    parsed = T.parse_payload(bronze_frame([valid_payload(amount=amount)]))
    flagged = T.flag_violations(parsed).collect()[0]

    assert T.select_valid(parsed).count() == 1  # retained
    assert "valid_amount" in flagged.flagged_rules  # but visible


def test_positive_amount_is_not_flagged(spark, bronze_frame, valid_payload):
    parsed = T.parse_payload(bronze_frame([valid_payload(amount=99.99)]))
    assert T.flag_violations(parsed).collect()[0].flagged_rules == ""


def test_null_amount_does_not_swallow_the_row(spark, bronze_frame, valid_payload):
    """The IS NOT TRUE guard, tested directly.

    A null amount makes `amount > 0` evaluate to NULL, not false. Under plain
    negation `NOT NULL` is NULL, and WHERE treats NULL as false -- so the row
    would be dropped by the expectations and simultaneously missed by the
    quarantine filter, vanishing with no record anywhere.

    This is the trap the `IS NOT TRUE` formulation exists to avoid, and the
    only test that would catch its removal.
    """
    parsed = T.parse_payload(bronze_frame([valid_payload(amount=None)]))
    flagged = T.flag_violations(parsed).collect()[0]

    assert "valid_amount" in flagged.flagged_rules
    # Still accounted for -- amount is a flag rule, so the row stays in silver.
    assert T.select_valid(parsed).count() == 1
