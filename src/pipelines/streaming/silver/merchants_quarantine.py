from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

# Quarantine for merchant rows that fail silver's drop rules.
#
# `expect_or_drop` removes bad rows and records a count in the event log. That
# tells you *how many* rows were dropped but not *which* ones or *why*, so the
# usual failure mode is noticing a count spike with no way to diagnose it.
#
# The standard pattern is to route failures to a parallel table rather than
# discard them: same source, inverted predicate. The two tables together
# account for every row that entered bronze, which means the reconciliation
#
#     bronze_count = silver_count + quarantine_count
#
# should hold, and a mismatch is itself a signal worth alerting on.
#
# Quarantine rows are never dropped -- this table has no expectations of its
# own. Applying quality rules to the quality-failure table would defeat the
# purpose.

# The drop rules from merchants_silver.py, expressed once so the two files
# cannot drift apart. Any row failing at least one of these is quarantined.
_DROP_RULES = {
    "valid_merchant_id": "merchant_id IS NOT NULL",
    "valid_merchant_risk": "merchant_risk IN ('LOW','MEDIUM','HIGH')",
    "blacklist_flag_present": "is_blacklisted IS NOT NULL",
}


@dp.table(
    name="finguard.silver.merchants_quarantine",
    comment=(
        "Merchant rows rejected by silver quality rules, with the failed rule "
        "recorded. Never dropped -- this is the diagnostic record."
    ),
)
def merchants_quarantine() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.merchants")

    # A row is quarantined when it does not pass every drop rule.
    #
    # The predicate is `IS NOT TRUE` rather than `NOT (...)`, and that
    # distinction is load-bearing. `merchant_risk IN (...)` returns NULL when
    # merchant_risk is NULL, so the conjunction is NULL, and `NOT NULL` is also
    # NULL -- a WHERE clause treats that as false. Such a row would be dropped
    # by the silver expectations and simultaneously missed by quarantine,
    # disappearing entirely. `IS NOT TRUE` collapses both FALSE and NULL to
    # true, so every non-passing row is captured.
    #
    # Verified against the warehouse: a row with NULL merchant_risk evaluates
    # NULL under `NOT (...)` and true under `IS NOT TRUE`.
    passes_all = " AND ".join(f"({rule})" for rule in _DROP_RULES.values())

    # Record which specific rules failed, so triage does not require rerunning
    # the predicates by hand.
    failed_rules = F.concat_ws(
        ",",
        *[
            F.when(~F.expr(rule), F.lit(name))
            for name, rule in _DROP_RULES.items()
        ],
    )

    return (
        bronze_df.where(f"({passes_all}) IS NOT TRUE")
        .select(
            F.col("merchant_id"),
            F.col("merchant_name"),
            F.col("merchant_category"),
            F.col("city"),
            F.col("country"),
            F.col("merchant_risk"),
            F.col("is_blacklisted"),
            F.col("_rescued_data"),
            F.col("source_file"),
            failed_rules.alias("failed_rules"),
            F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("quarantined_at"),
        )
    )
