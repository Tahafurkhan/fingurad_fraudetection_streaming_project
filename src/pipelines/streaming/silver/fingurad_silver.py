
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import *


# Physical layout for the busiest table in the project.
#
# CLUSTERING. Silver is the first layer where business keys exist as real
# columns -- bronze keeps the Kafka payload as an unparsed string, so it can
# only cluster on the envelope. The keys here come from observed predicates:
# every gold join is on customer_id, and every time-bounded query filters
# transaction_timestamp.
#
# Liquid clustering rather than partitioning by date. At ~4,400 rows over one
# day, date partitioning would create a directory holding the entire table --
# no pruning benefit -- and at scale it produces the classic small-file
# explosion of one tiny file per partition per micro-batch. Clustering keys can
# also be changed later without rewriting history; partition columns cannot.
#
# FILE MANAGEMENT. optimizeWrite adds a shuffle before write so each micro-batch
# emits appropriately-sized files rather than one per task; autoCompact
# rewrites small files afterwards. Measured before these were set, this table
# held 31 files for 347KB -- about 11KB per file. Every query paid 31 file
# opens to read a third of a megabyte.
#
# CHANGE DATA FEED. Enabled so downstream consumers can read row-level changes
# instead of diffing snapshots. It costs storage: change files are written
# alongside data on every commit.
_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.tuneFileSizesForRewrites": "true",
    # Statistics drive file skipping, and Delta only collects them for the
    # first N columns.
    #
    # THIS SETTING IS COUPLED TO THE CLUSTERING KEYS BELOW. Liquid clustering
    # requires every clustering column to have stats, so the budget must cover
    # them. A first attempt set this to 12 to avoid computing min/max over wide
    # descriptive columns, and the pipeline failed:
    #
    #   [DELTA_CLUSTERING_COLUMN_MISSING_STATS] Couldn't find clustering
    #   column(s) 'transaction_timestamp' in stats schema
    #
    # transaction_timestamp is the 14th column, so a budget of 12 excluded a
    # key this table clusters on. Lowering the stats budget is a real
    # optimization -- min/max over long strings is write cost for no read
    # benefit -- but it silently constrains which columns can be clustered.
    #
    # 16 covers every column through transaction_timestamp while still
    # excluding the trailing Kafka envelope and audit columns that no predicate
    # filters on.
    "delta.dataSkippingNumIndexedCols": "16",
}


@dp.table(
    name="finguard.silver.transactions",
    comment="Parsed and cleaned transactions data",
    table_properties=_TABLE_PROPERTIES,
    cluster_by=["customer_id", "transaction_timestamp"],
)
@dp.expect_or_drop("valid_transaction_id","transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number","card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id","merchant_id IS NOT NULL")
@dp.expect("valid_amount","amount > 0")
def transactions_silver() -> DataFrame:
    # Project the columns this model needs at the point of read.
    #
    # Delta is columnar, so naming columns here means the unused ones are never
    # read off storage -- the pruning happens in the scan, not after it. Bronze
    # carries `timestampType` and `key`, neither of which silver uses, and a
    # bare `.table()` read would fetch both.
    #
    # Catalyst can often infer this pruning from the later select. Being
    # explicit makes it independent of whether the optimiser sees through the
    # from_json call, and documents the actual dependency: if someone adds a
    # column to bronze, this model does not silently start reading it.
    bronze_df = spark.readStream.table("finguard.bronze.transactions").select(
        "value", "topic", "partition", "offset", "timestamp", "ingestion_timestamp"
    )

    schema = StructType([
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
        StructField("status", StringType())
    ])

    tranformed_df = bronze_df.select(
        F.from_json(col("value"), schema).alias("data")
        ,F.col("topic").alias("kafka_topic")
        ,F.col("partition").alias("kafka_partition")
        ,F.col("offset").alias("kafka_offset")
        ,F.col("timestamp").alias("kafka_timestamp")
      , F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        F.col("data.*")
        ,F.col("kafka_topic")
        ,F.col("kafka_partition")
        ,F.col("kafka_offset")
        ,F.col("kafka_timestamp")
        ,F.col("bronze_ingestion_timestamp")
        ,F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return tranformed_df
