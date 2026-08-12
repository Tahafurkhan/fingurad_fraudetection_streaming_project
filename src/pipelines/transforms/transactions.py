"""Transaction payload schema, quality rules, and the silver/quarantine split.

This module is the single definition of three things that were previously
written out three times each -- once in the producer's JSON Schema, once in
the silver model, once in the quarantine model:

  1. the payload shape          -> PAYLOAD_SCHEMA
  2. the mandatory-field rules  -> DROP_RULES
  3. how a row is split between silver and quarantine

Triplication was not carelessness; it was forced by Lakeflow's execution model
(see the package docstring). Moving the definitions into an importable module
removes the fork without fighting that model.

WHY A DRIFT IS A SILENT FAILURE, NOT A LOUD ONE
-----------------------------------------------
When these definitions disagree, nothing errors. `from_json` returns null for
fields the schema does not mention, the row passes every IS NOT NULL rule that
still matches, and the pipeline stays green while the new field is quietly
dropped. The row count is unchanged; only the content is wrong. That is why
`tests/test_producer_schema.py` asserts the producer contract and
PAYLOAD_SCHEMA agree field-for-field, and why that test is worth more than its
size suggests.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# The payload contract
# ---------------------------------------------------------------------------

# Column order is load-bearing and must not be rearranged casually.
#
# `delta.dataSkippingNumIndexedCols` is set to 16 on finguard.silver.transactions
# so that statistics cover every column through transaction_timestamp -- which
# liquid clustering requires, because it clusters on that column. Inserting a
# field ahead of transaction_timestamp pushes it past the stats budget and the
# pipeline fails with DELTA_CLUSTERING_COLUMN_MISSING_STATS. Append new fields
# at the end, or raise the budget deliberately.
PAYLOAD_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("card_number", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_type", StringType()),
        StructField("payment_channel", StringType()),
        StructField("device_id", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("transaction_timestamp", TimestampType()),
        StructField("is_international", BooleanType()),
        StructField("status", StringType()),
    ]
)

# Rules whose failure removes a row from silver. Each is also the reason string
# recorded in the quarantine table, so the name is user-facing at 02:00 --
# "valid_customer_id" tells an operator what to look at; "rule_2" does not.
#
# These are IS NOT NULL checks only. A field being absent makes the record
# unusable: there is no customer to alert on, no card to block. Value-range
# checks belong in FLAG_RULES.
DROP_RULES: dict[str, str] = {
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_card_number": "card_number IS NOT NULL",
    "valid_merchant_id": "merchant_id IS NOT NULL",
    # Added after test_producer_required_fields_are_the_drop_rules found that
    # the producer guarantees this field and no consumer rule enforced it.
    #
    # This one is not merely another mandatory identifier -- it is the
    # event-time column. Every watermark in the project reads it: the dedup
    # below, the tumbling and sliding window aggregates, and the stream-stream
    # join behind fraud_card_alert. A null here does not cost one row; it
    # corrupts the windowing for the batch, because a row with no event time
    # cannot be placed in any window and cannot advance a watermark.
    #
    # Measured before adding: 825 of 825 rows had a non-null value. The
    # producer was honouring the guarantee. That is the absence of a violation,
    # not the presence of a control -- which is the whole point.
    "valid_transaction_timestamp": "transaction_timestamp IS NOT NULL",
}

# Rules that are recorded and counted but do NOT remove the row.
#
# `amount > 0` is here rather than in DROP_RULES deliberately. Zero and
# negative amounts occur legitimately as reversals and adjustments; quarantining
# them would delete real transactions and under-report fraud exposure. The rule
# earns its place through visibility -- a sudden spike in violations means an
# upstream defect, which a counted flag surfaces and a silent drop conceals.
FLAG_RULES: dict[str, str] = {
    "valid_amount": "amount > 0",
    # currency and status are guaranteed by the producer contract but are FLAG
    # rules, not DROP rules -- a deliberate asymmetry with the identifiers
    # above.
    #
    # The test that surfaced these asked "does the consumer enforce every
    # producer guarantee?" and the answer was no. The fix is not automatically
    # a drop rule, though: the question a drop rule answers is "is this row
    # unusable without the field?"
    #
    # Without customer_id there is no one to alert; without card_number there
    # is nothing to block. Those rows are genuinely unusable. A transaction
    # missing its currency is still a real transaction against a real card at a
    # real merchant, and dropping it would delete evidence of fraud to satisfy
    # a metadata rule -- the same reasoning that keeps `amount > 0` a flag
    # (TRD 9.2). It is recorded and counted instead, so a producer regression
    # shows up as a flag-rate spike rather than as silence.
    "valid_currency": "currency IS NOT NULL",
    "valid_status": "status IS NOT NULL",
}

# Kafka envelope columns carried through both paths so any row in either table
# can be traced back to the exact message that produced it, and replayed.
_ENVELOPE = [
    ("topic", "kafka_topic"),
    ("partition", "kafka_partition"),
    ("offset", "kafka_offset"),
    ("timestamp", "kafka_timestamp"),
    ("ingestion_timestamp", "bronze_ingestion_timestamp"),
]


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------


def parse_payload(bronze_df: DataFrame, *, keep_raw: bool = False) -> DataFrame:
    """Parse the raw Kafka `value` string into typed columns.

    Args:
        bronze_df: bronze.transactions, or any frame with `value` plus the
            envelope columns.
        keep_raw: retain the original payload string as `raw_payload`. The
            quarantine path needs it; silver does not.

    Returns a frame of the payload fields, the renamed envelope, and
    (optionally) raw_payload.

    WHY raw_payload MATTERS ON THE QUARANTINE PATH: when `from_json` cannot
    parse a message, every field comes back null and the parsed row says
    nothing about what actually arrived. The raw string is the only usable
    evidence of the failure.
    """
    projection = [F.from_json(F.col("value"), PAYLOAD_SCHEMA).alias("data")]
    if keep_raw:
        projection.append(F.col("value").alias("raw_payload"))
    projection += [F.col(src).alias(dst) for src, dst in _ENVELOPE]

    flattened = ["data.*"]
    if keep_raw:
        flattened.append("raw_payload")
    flattened += [dst for _, dst in _ENVELOPE]

    return bronze_df.select(*projection).select(*flattened)


def _passes_all_drop_rules() -> str:
    """SQL predicate that is true only when every drop rule holds."""
    return " AND ".join(f"({rule})" for rule in DROP_RULES.values())


def select_valid(parsed_df: DataFrame) -> DataFrame:
    """Rows that satisfy every drop rule -- the silver path.

    Kept as an explicit filter even though the Lakeflow model also declares
    `@dp.expect_or_drop` for the same rules. The decorators are what Lakeflow
    counts and reports; this function is what the tests can execute. They must
    agree, and `test_silver_expectations_match_shared_drop_rules` asserts it.

    `IS TRUE` rather than a bare predicate. The two differ only when the
    conjunction evaluates to NULL -- which happens the moment a value check
    joins the rule set, since `NULL > 0` is NULL rather than false. A bare
    predicate in WHERE treats NULL as false and would exclude the row here,
    while `select_quarantined` (using IS NOT TRUE) includes it: together they
    still partition the input. Writing both halves explicitly keeps that
    partition true by construction rather than by coincidence.
    """
    return parsed_df.where(f"({_passes_all_drop_rules()}) IS TRUE")


def select_quarantined(parsed_df: DataFrame) -> DataFrame:
    """Rows failing at least one drop rule, annotated with which ones.

    `IS NOT TRUE` rather than `NOT (...)`. Every current rule is IS NOT NULL,
    which is never itself null, so plain negation would work today. But add one
    value check -- say `amount > 0` -- and a null amount makes the conjunction
    null; `NOT NULL` is null; and WHERE treats null as false. That row would be
    dropped by the expectations and simultaneously missed here, vanishing with
    no record anywhere. Written defensively because the trap costs nothing to
    avoid and is invisible once sprung.
    """
    failed_rules = F.concat_ws(
        ",",
        *[
            F.when(F.expr(f"({rule}) IS NOT TRUE"), F.lit(name))
            for name, rule in DROP_RULES.items()
        ],
    )
    return parsed_df.where(f"({_passes_all_drop_rules()}) IS NOT TRUE").withColumn(
        "failed_rules", failed_rules
    )


def flag_violations(df: DataFrame) -> DataFrame:
    """Add a `flagged_rules` column naming any FLAG_RULES the row violates.

    Rows are not removed. An empty string means clean.
    """
    return df.withColumn(
        "flagged_rules",
        F.concat_ws(
            ",",
            *[
                F.when(F.expr(f"({rule}) IS NOT TRUE"), F.lit(name))
                for name, rule in FLAG_RULES.items()
            ],
        ),
    )


# Deduplication watermark for silver.transactions.
#
# The Kafka contract is at-least-once (TRD 6.1), so a producer retry or a
# replay after an uncommitted batch delivers the same transaction twice.
# Without dedup, one fraudulent transaction raises two alerts and every
# aggregate double-counts -- FR-11 requires this not happen.
#
# 10 minutes rather than a longer window: duplicates from at-least-once
# delivery arrive within seconds (a retry), not hours. The watermark bounds
# how much state the operator holds, and state is memory that never returns
# on an unbounded stream.
#
# Defined here so the model and the tests share one number. The model applies
# it -- see fingurad_silver.py -- because Lakeflow needs the streaming
# operator inside the decorated function.
DEDUP_WATERMARK = "10 minutes"
DEDUP_KEY = "transaction_id"


def reconciliation_counts(
    parsed_df: DataFrame,
) -> tuple[int, int, int]:
    """Return (total, accepted, quarantined) for one frame.

    Implements the DQ-06 invariant from the TRD: total == accepted +
    quarantined. If that does not hold, rows have gone somewhere neither table
    records, which is the failure mode quarantine exists to prevent. Used by
    tests; the production equivalent is a query against the two tables.
    """
    total = parsed_df.count()
    accepted = select_valid(parsed_df).count()
    quarantined = select_quarantined(parsed_df).count()
    return total, accepted, quarantined
