from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

# Merchant master, cleaned.
#
# Deduplication is the reason this file is more than a projection. Each run of
# upload_merchants.py writes a NEW timestamped file containing the full 200-row
# catalogue, and Auto Loader appends every file it sees. Bronze therefore holds
# one row per merchant *per upload*. Without dedup, silver would report 400
# merchants after two uploads and every downstream join would fan out.
#
# Keeping the newest row per merchant_id is also what makes this a usable
# source for an SCD2 dimension later: bronze retains the full history of
# observed states, silver exposes the current one, and the dimension derives
# change history from bronze.


@dp.table(
    name="finguard.silver.merchants",
    comment="Cleaned merchant master; one current row per merchant_id",
)
# Identity must exist -- a merchant row without an id cannot be joined to a
# transaction, so it is useless rather than merely imperfect.
@dp.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
# Risk band drives fraud scoring (HIGH_RISK_MERCHANT, weight 25). An unknown
# value would silently score as not-high-risk, so constrain it.
@dp.expect_or_drop("valid_merchant_risk", "merchant_risk IN ('LOW','MEDIUM','HIGH')")
# Blacklist flag drives the heaviest signal (weight 60). Null is not safe to
# default -- treat it as a broken row.
@dp.expect_or_drop("blacklist_flag_present", "is_blacklisted IS NOT NULL")
# Warn-only: a missing name or category is inconvenient for reporting but does
# not break fraud evaluation, so the row is kept and the violation recorded.
@dp.expect("merchant_name_present", "merchant_name IS NOT NULL")
@dp.expect("merchant_category_present", "merchant_category IS NOT NULL")
# Auto Loader routes fields that do not match the inferred schema into
# _rescued_data. Non-null means the upstream file shape changed. Warn rather
# than drop: the row is still usable, but the drift needs to be visible.
@dp.expect("no_schema_drift", "_rescued_data IS NULL")
def merchants_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.merchants")

    standardised_df = bronze_df.select(
        # Join keys are upper-cased and trimmed. Case or whitespace drift in a
        # key is the classic cause of a silently empty join -- 'mer0001' and
        # 'MER0001 ' both fail to match 'MER0001'.
        F.upper(F.trim(F.col("merchant_id"))).alias("merchant_id"),
        F.trim(F.col("merchant_name")).alias("merchant_name"),
        # Category and risk are compared as literals downstream, so normalise
        # case here rather than relying on every consumer to remember.
        F.initcap(F.trim(F.col("merchant_category"))).alias("merchant_category"),
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.initcap(F.trim(F.col("country"))).alias("country"),
        F.upper(F.trim(F.col("merchant_risk"))).alias("merchant_risk"),
        F.col("is_blacklisted").cast("boolean").alias("is_blacklisted"),
        # Derived flag: the fraud engine treats HIGH risk and blacklisting as
        # separate signals, but "needs review" is the common downstream
        # question and computing it once here avoids repeating the rule.
        (
            (F.upper(F.trim(F.col("merchant_risk"))) == F.lit("HIGH"))
            | (F.col("is_blacklisted") == F.lit(True))
        ).alias("requires_review"),
        # Lineage: which file the row came from, and when each layer saw it.
        F.col("_rescued_data"),
        F.col("source_file"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )

    # Deduplicate within the stream. dropDuplicatesWithinWatermark bounds the
    # state store: without a watermark, Spark would retain every merchant_id
    # ever seen in order to detect duplicates, and state would grow without
    # limit.
    #
    # The 1-day watermark is chosen from the upload cadence, not arbitrarily --
    # merchant files land at most daily, so duplicates always arrive well
    # inside that window. A shorter watermark would let re-uploads through as
    # new rows.
    deduplicated_df = standardised_df.withWatermark(
        "bronze_ingestion_timestamp", "1 day"
    ).dropDuplicatesWithinWatermark(["merchant_id"])

    return deduplicated_df
