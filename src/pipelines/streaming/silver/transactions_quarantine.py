from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Quarantine for transactions rejected by silver's drop rules.
#
# fingurad_silver.py drops rows missing any of the four identifiers. The event
# log records how many were dropped but not which ones, so a spike in the drop
# count is visible without being diagnosable -- and for a fraud system, a
# sudden rise in malformed transactions is itself worth investigating. It could
# be an upstream schema change, a producer bug, or someone probing the API with
# deliberately malformed payloads.
#
# Rows land here with the specific failed rules recorded, plus the full Kafka
# envelope so any quarantined message can be traced to its exact offset and
# replayed after the upstream fix.
#
# This table applies no expectations of its own. Quality rules on the
# quality-failure table would defeat its purpose.

# Mirrors the schema in fingurad_silver.py. Duplicated deliberately rather than
# imported: the silver model is loaded by Lakeflow as a notebook, not as an
# importable module, so a shared constant is not reachable from here. If the
# payload shape changes, both files change together.
_PAYLOAD_SCHEMA = StructType(
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

# The drop rules from fingurad_silver.py. A row failing any of these is
# quarantined rather than discarded.
_DROP_RULES = {
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_card_number": "card_number IS NOT NULL",
    "valid_merchant_id": "merchant_id IS NOT NULL",
    # Must stay in step with fingurad_silver.py. These two rule sets are the
    # two halves of one partition -- silver keeps what passes, this table keeps
    # what fails. A rule present in one and missing from the other creates rows
    # that satisfy neither, which vanish with no record in either table.
    # tests/test_contract_alignment.py asserts they match.
    "valid_transaction_timestamp": "transaction_timestamp IS NOT NULL",
}


@dp.table(
    name="finguard.silver.transactions_quarantine",
    comment=(
        "Transactions rejected by silver quality rules, with the failed rules "
        "and full Kafka provenance for replay."
    ),
)
def transactions_quarantine() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.transactions")

    parsed = bronze_df.select(
        F.from_json(F.col("value"), _PAYLOAD_SCHEMA).alias("data"),
        # The raw payload is kept verbatim. When from_json cannot parse a
        # message every field is null and the parsed row says nothing about
        # what actually arrived -- the raw string is the only usable evidence.
        F.col("value").alias("raw_payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    ).select("data.*", "raw_payload", "kafka_topic", "kafka_partition",
             "kafka_offset", "kafka_timestamp", "bronze_ingestion_timestamp")

    passes_all = " AND ".join(f"({rule})" for rule in _DROP_RULES.values())

    # `IS NOT TRUE` rather than `NOT (...)` for the filter. Every rule here is
    # an IS NOT NULL predicate, which is never itself NULL, so plain negation
    # would work today -- but the moment someone adds a value check such as
    # `amount > 0`, a NULL amount makes the conjunction NULL, and NOT NULL is
    # NULL, which WHERE treats as false. That row would be dropped by the
    # expectations and simultaneously missed here, vanishing with no record
    # anywhere. Writing it defensively now costs nothing and removes a trap.
    # Expressed as SQL text rather than a Column method: PySpark's Column has
    # no isNotTrue().
    failed_rules = F.concat_ws(
        ",",
        *[
            F.when(F.expr(f"({rule}) IS NOT TRUE"), F.lit(name))
            for name, rule in _DROP_RULES.items()
        ],
    )

    return parsed.where(f"({passes_all}) IS NOT TRUE").select(
        F.col("transaction_id"),
        F.col("customer_id"),
        F.col("card_number"),
        F.col("merchant_id"),
        F.col("amount"),
        F.col("currency"),
        F.col("transaction_timestamp"),
        failed_rules.alias("failed_rules"),
        F.col("raw_payload"),
        # Kafka coordinates: the quarantined message can be located and
        # replayed from exactly this offset once the upstream cause is fixed.
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("quarantined_at"),
    )
