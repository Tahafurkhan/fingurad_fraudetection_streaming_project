from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


# The most extreme small-file case in the project: 93 rows across 17 files,
# roughly 5KB each, because each Auto Loader micro-batch commits its own file
# regardless of how little it contains.
#
# This is the clearest illustration of why compaction is not optional for
# streaming ingestion. The data is trivial; the file count is not. Every query
# opens 17 files, reads 17 footers, and plans 17 splits to scan 89KB.
#
# No clustering keys: with 93 rows there is nothing to prune, and entity_id --
# the column the gold join uses -- has near-unique values, which would make
# clustering churn on every write for no read benefit.
_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.tuneFileSizesForRewrites": "true",
}


@dp.table(
    name="finguard.silver.fraud_watchlist",
    comment="Cleaned fraud watchlist",
    table_properties=_TABLE_PROPERTIES,
)
def fraud_watchlist_silver() -> DataFrame:

    bronze_df=(spark.readStream.table("finguard.bronze.fraud_watchlist"))

    cleaned_df=bronze_df.select(
    F.upper(F.col("watchlist_id")).alias("watchlist_id"),
    F.col("watch_type"),
   F.upper(F.col("entity_id")).alias("entity_id"),
   F.upper(F.col("risk_level")).alias("risk_level"),
   F.upper(F.col("action")).alias("action"),
    F.col("reason_code"),
    F.col("reason_description"),
    F.col("status"),
    F.to_timestamp(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
    F.col("reported_by"),
    F.col("reported_source"),
    F.col("country"),
    F.col("city"),
    F.col("source_file"),
    F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return cleaned_df
