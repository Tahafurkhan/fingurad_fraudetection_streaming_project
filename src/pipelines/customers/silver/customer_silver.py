from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


# Clustered on customer_id: this is the CDC-fed table every gold model joins,
# and customer_id is the only predicate any of them use.
#
# It is also the table where clustering will matter first in practice. Bronze
# CDC appends one row per customer *per change*, so unlike the other silver
# tables this one grows without bound as customers are updated at source --
# 1,002 customers today, but one row per edit forever. That growth pattern is
# exactly what clustering is for.
#
# update_timestamp is deliberately NOT a second key. The gold models reduce to
# the latest row per customer with a window function rather than filtering on a
# timestamp range, so a clustering key on it would be maintained on every write
# and read by nothing.
_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    # CDF matters more here than elsewhere: this table already represents a
    # change stream, and downstream SCD2 snapshots consume its transitions.
    "delta.enableChangeDataFeed": "true",
    "delta.tuneFileSizesForRewrites": "true",
    # 22 columns, most of them descriptive. Statistics on the leading 12 cover
    # customer_id and the numeric attributes that get filtered; collecting
    # min/max over the remaining free-text columns is write cost with no
    # matching read benefit.
    "delta.dataSkippingNumIndexedCols": "12",
}


@dp.table(
    name="finguard.silver.customers",
    comment="Parsed and cleaned customer data",
    table_properties=_TABLE_PROPERTIES,
    cluster_by=["customer_id"],
)
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
def customers_silver() -> DataFrame:
    bronze_df=spark.readStream.table("finguard.bronze.customers")

    transformed_df=bronze_df.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("gender"),
        F.col("age"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("annual_income"),
        F.col("customer_segment"),
        F.to_date(F.col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"),
        F.col("risk_score"),
        F.col("preferred_spending_min"),
        F.col("preferred_spending_max"),
        F.col("preferred_city"),
        F.col("preferred_country"),
        F.col("trusted_device_id"),
        F.col("card_number"),
        F.col("card_type"),
        F.col("email"),
        F.col("transaction_limit"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return transformed_df
