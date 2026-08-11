from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dp.table(
    name="finguard.gold.high_value_transactions_alert",
    comment="Alert details where transaction has been performed with value higher than what is configured by customer"
)
def high_value_transactions_alert() -> DataFrame:
    transactions=spark.readStream.table("finguard.silver.transactions")

    # Reduce customers to the latest row per customer_id before joining.
    #
    # silver.customers is CDC-fed and accumulates one row per change, so a
    # direct join fans out once any customer is updated at source -- each
    # transaction would produce one alert per recorded version of that
    # customer. The comparison here is `amount > transaction_limit`, so a
    # fan-out would also emit duplicate alerts against stale limits.
    #
    # See gold/fraud_card_alert.py for the fuller note on why current-state
    # enrichment (rather than point-in-time) is the right choice for the
    # operational alerting path.
    customers=(
        spark.read.table("finguard.silver.customers")
        .withColumn(
            "_row_num",
            F.row_number().over(
                Window.partitionBy("customer_id").orderBy(
                    F.col("silver_ingestion_timestamp").desc()
                )
            ),
        )
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    joined_df=(transactions.join(customers,transactions.customer_id==customers.customer_id,"left")
                .filter(F.col("amount")>F.col("transaction_limit"))
                .select(
                    F.concat_ws("-",F.lit("ALERT"),F.col("transaction_id")).alias("alert_id"),
                    F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
                    F.current_timestamp().alias("alert_timestamp"),

                    transactions.transaction_id,
                    transactions.customer_id,
                    customers.email.alias("customer_email"),
                    F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias("customer_name"),
                    transactions.amount.alias("transaction_amount"),
                    customers.transaction_limit,
                    transactions.currency,
                    transactions.merchant_name,
                    transactions.merchant_category,
                    transactions.transaction_type,
                    transactions.payment_channel,
                    transactions.city,
                    transactions.country,
                    transactions.is_international,
                    transactions.transaction_timestamp,
                    transactions.status
                )
            )
    return joined_df
