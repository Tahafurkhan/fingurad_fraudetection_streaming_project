from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

# Quarantine for customer rows rejected by silver's drop rule.
#
# customer_silver.py drops rows with a null customer_id. For a CDC-fed table
# that is a more serious signal than it looks: customer_id is the primary key
# the Lakeflow managed ingestion pipeline uses to identify rows, so a null
# means either the source table has a row violating its own key constraint, or
# the CDC connector emitted a malformed change event. Both are worth seeing
# rather than silently discarding.
#
# The rule set is small because customer_silver.py only enforces one rule. The
# structure is the same as the other quarantine tables so all three can be
# queried uniformly.

_DROP_RULES = {
    "valid_customer_id": "customer_id IS NOT NULL",
}


@dp.table(
    name="finguard.silver.customers_quarantine",
    comment="Customer rows rejected by silver quality rules, with the failed rules recorded",
)
def customers_quarantine() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.customers")

    passes_all = " AND ".join(f"({rule})" for rule in _DROP_RULES.values())

    # `IS NOT TRUE` in SQL rather than a Column method: PySpark's Column has
    # no isNotTrue(), so the predicate is expressed as SQL text. It matters
    # that this is NOT plain `NOT (rule)` -- a rule evaluating to NULL would
    # make NOT NULL also NULL, which WHERE treats as false, so the row would be
    # dropped by the expectations and simultaneously missed here.
    failed_rules = F.concat_ws(
        ",",
        *[
            F.when(F.expr(f"({rule}) IS NOT TRUE"), F.lit(name))
            for name, rule in _DROP_RULES.items()
        ],
    )

    return bronze_df.where(f"({passes_all}) IS NOT TRUE").select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("email"),
        F.col("customer_segment"),
        F.col("risk_score"),
        F.col("card_number"),
        failed_rules.alias("failed_rules"),
        # `update_timestamp`, not `ingestion_timestamp`. The other bronze
        # tables are built by our own factory, which stamps an ingestion time
        # on every row. This one is not: it is written by the Lakeflow managed
        # CDC pipeline, which projects the source columns as-is and adds no
        # ingestion metadata. `update_timestamp` is the Postgres CDC cursor and
        # is the closest available provenance -- it says when the row changed
        # at source, not when Databricks saw it, so a quarantined row's arrival
        # time has to be read from `quarantined_at` below.
        F.col("update_timestamp").alias("source_update_timestamp"),
        F.current_timestamp().alias("quarantined_at"),
    )
