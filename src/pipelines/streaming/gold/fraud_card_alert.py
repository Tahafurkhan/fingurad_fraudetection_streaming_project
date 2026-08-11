from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
name="finguard.gold.fraud_card_alert"
,comment="Fraud watchlist matches, enriched with the customer record current at ingest time"
)
def fraud_card_alert() -> DataFrame:
    transactions=spark.readStream.table("finguard.silver.transactions")
    fraud_watchlist=spark.readStream.table("finguard.silver.fraud_watchlist")

    # Customer enrichment is a stream-static join.
    #
    # `spark.read` re-reads the table at the start of every micro-batch, so
    # each batch sees the customer rows that exist at that moment. That is the
    # correct construct here -- the alternative, readStream, would require a
    # watermark on customers and would only ever see rows arriving *during*
    # this query, missing the entire existing customer base.
    #
    # The subtlety that made the original version wrong: silver.customers is
    # fed by Postgres CDC with `update_timestamp` as the cursor, so it
    # accumulates one row per customer *per change*, not one row per customer.
    # Joining it directly fans out -- a customer with three recorded changes
    # multiplies their transactions by three. Today the table happens to hold
    # exactly one row per customer, which is why the fault has not surfaced
    # yet; it appears the first time any customer attribute is updated at
    # source.
    #
    # Reducing to the latest row per customer_id keeps the join one-to-one.
    # Note what this deliberately does NOT attempt: evaluating the customer
    # profile as it stood at transaction time. A stream cannot look up
    # historical dimension versions without unbounded state. Point-in-time
    # attribution belongs in the dimensional layer, where dim_customer carries
    # SCD2 validity windows and fct_alerts joins on them -- see
    # transform/models/marts/fct_alerts.sql. This table is the operational
    # alerting path and enriches with current state, which is what an analyst
    # responding to a live alert needs.
    latest_customer = (
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
    customers = latest_customer

    transactions_with_watermark=transactions.withWatermark("transaction_timestamp", "5 minutes")
    fraud_watchlist_with_watermark=fraud_watchlist.withWatermark("effective_from", "5 minutes")

    fraud_detected=(
            transactions_with_watermark.join(
                    fraud_watchlist_with_watermark,
                    transactions_with_watermark.card_number==fraud_watchlist_with_watermark.entity_id,
                    "inner"
            ).join(
                customers,
                transactions_with_watermark.customer_id==customers.customer_id,
                "left"
            ).select(

                # Alert identification
            F.concat_ws("-", F.lit("FRAUD"), F.col("transaction_id"), F.col("watchlist_id")).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),

            # Transaction details
            transactions_with_watermark.transaction_id,
            transactions_with_watermark.customer_id,
            customers.email.alias("customer_email"),
            F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
            transactions_with_watermark.card_number,
            transactions_with_watermark.amount,
            transactions_with_watermark.currency,
            transactions_with_watermark.merchant_id,
            transactions_with_watermark.merchant_name,
            transactions_with_watermark.merchant_category,
            transactions_with_watermark.transaction_type,
            transactions_with_watermark.payment_channel,
            transactions_with_watermark.device_id,
            transactions_with_watermark.city.alias("transaction_city"),
            transactions_with_watermark.country.alias("transaction_country"),
            transactions_with_watermark.transaction_timestamp,
            transactions_with_watermark.is_international,
            transactions_with_watermark.status.alias("transaction_status"),

            # Fraud watchlist details
            F.col("watchlist_id"),
            F.col("watch_type"),
            F.col("risk_level"),
            F.col("action"),
            F.col("reason_code"),
            F.col("reason_description"),
            F.col("effective_from").alias("watchlist_effective_from"),
            F.col("reported_by"),
            F.col("reported_source"),
            fraud_watchlist_with_watermark.city.alias("watchlist_city"),
            fraud_watchlist_with_watermark.country.alias("watchlist_country")

            )


    )
    return fraud_detected
