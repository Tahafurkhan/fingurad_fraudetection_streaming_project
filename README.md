# FinGuard: Real-Time Fraud Detection Streaming Platform

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4?style=flat&logo=delta&logoColor=white)

## 🎯 Overview

FinGuard is an enterprise-grade, real-time fraud detection system built on Databricks Lakehouse Platform. It leverages **Spark Structured Streaming**, **Delta Lake**, **Apache Kafka**, and **Lakeflow Spark Declarative Pipelines** to detect fraudulent transactions in real-time with millisecond latency.

### Key Features

✅ **Real-time Stream Processing** - Process millions of transactions per second from Kafka  
✅ **Multi-layered Architecture** - Bronze → Silver → Gold medallion architecture  
✅ **Fraud Detection** - Match transactions against fraud watchlists with streaming joins  
✅ **High-Value Transaction Monitoring** - Detect anomalous transaction patterns  
✅ **Stateful Stream Processing** - Watermarking and windowed aggregations  
✅ **Automated Alerting** - Email notifications for detected fraud  
✅ **Customer Data Integration** - Lakeflow Connect for customer profile ingestion  

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
├──────────────────────────────┬──────────────────────────────────────────┤
│  Apache Kafka (Confluent)    │   Cloud Storage (Customer Data)          │
│  - Real-time transactions    │   - Customer profiles                    │
│  - Fraud watchlist updates   │   - Account information                  │
└──────────────────┬───────────┴────────────────┬─────────────────────────┘
                   │                            │
                   ▼                            ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (Raw Ingestion)                      │
├──────────────────────────────────────────────────────────────────────────┤
│  • finguard.bronze.transactions      - Kafka stream ingestion            │
│  • finguard.bronze.fraud_watchlist   - Watchlist stream ingestion        │
│  • finguard.bronze.customers         - Customer data via Lakeflow        │
│                                                                           │
│  Technology: Spark Structured Streaming + Kafka Consumer                 │
│  Format: Delta Lake with Auto Loader (for file-based sources)            │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleansed & Enriched)                     │
├──────────────────────────────────────────────────────────────────────────┤
│  • finguard.silver.transactions      - Parsed JSON, type casting         │
│  • finguard.silver.fraud_watchlist   - Validated watchlist entries       │
│  • finguard.silver.customers         - Cleaned customer profiles         │
│                                                                           │
│  Transformations:                                                        │
│    ✓ JSON parsing & schema enforcement                                   │
│    ✓ Data type casting & validation                                      │
│    ✓ Data quality expectations (@dp.expect_or_drop)                     │
│    ✓ Null handling & deduplication                                       │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Business Logic & Alerts)                  │
├──────────────────────────────────────────────────────────────────────────┤
│  📊 Fraud Detection Tables:                                              │
│    • fraud_card_alert                - Watchlist match alerts            │
│    • high_value_transactions_alert   - Anomaly detection                 │
│                                                                           │
│  📈 Analytics Tables:                                                    │
│    • transaction_count_by_minute              - Tumbling window          │
│    • transaction_count_by_minute_sliding_window - Sliding window (5m/1m) │
│                                                                           │
│  🔔 Alert Processing:                                                    │
│    • Fraud card alert email notifier                                     │
│    • High-value transaction email notifier                               │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Data Flow: End-to-End Pipeline

### 1️⃣ Bronze Layer: Real-Time Ingestion

#### **Transaction Stream (Kafka → Bronze)**
```python
# File: finguard_streaming/bronze/finguard_bronze.py

@dp.table(name="finguard.bronze.transactions")
def transactions_bronze():
    # Kafka connection secured via Databricks Secrets
    streaming_df = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("startingOffsets", "earliest")
        .load()
    
    # Raw Kafka message parsing
    return streaming_df.select(
        col("key").cast("string"),
        col("value").cast("string"),  # JSON payload
        col("topic"), col("partition"), col("offset"),
        col("timestamp"),
        current_timestamp().alias("ingestion_timestamp")
    )
```

**Key Features:**
- ✅ Secure Kafka SASL_SSL authentication
- ✅ At-least-once delivery guarantee
- ✅ Offset management for fault tolerance
- ✅ Ingestion timestamp tracking

#### **Customer Data (Cloud Storage → Bronze via Lakeflow Connect)**
```python
# Ingested via Lakeflow Connect managed pipeline
# File: finguard_customers_silver_ingestion/silver/customer_silver.py

# Bronze layer automatically created by Lakeflow Connect
# Source: S3/ADLS/GCS customer CSV/JSON files
```

---

### 2️⃣ Silver Layer: Data Transformation & Quality

#### **Transaction Silver**
```python
@dp.table(name="finguard.silver.transactions")
@dp.expect_or_drop("valid_transaction", "transaction_id IS NOT NULL")
def transactions_silver():
    bronze_df = spark.readStream.table("finguard.bronze.transactions")
    
    # Parse JSON payload
    parsed_df = bronze_df.select(
        from_json(col("value"), transaction_schema).alias("data")
    ).select("data.*")
    
    # Type casting & enrichment
    return parsed_df.select(
        col("transaction_id"),
        col("customer_id"),
        col("card_number"),
        col("amount").cast("decimal(10,2)"),
        col("merchant_name"),
        to_timestamp(col("transaction_timestamp")).alias("transaction_timestamp"),
        # ... additional fields
        current_timestamp().alias("silver_ingestion_timestamp")
    )
```

#### **Customer Silver with Data Quality**
```python
@dp.table(name="finguard.silver.customers")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers_silver():
    bronze_df = spark.readStream.table("finguard.bronze.customers")
    
    return bronze_df.select(
        col("customer_id"),
        col("first_name"), col("last_name"),
        col("email"),
        to_date(col("account_open_date")).alias("account_open_date"),
        col("transaction_limit"),
        col("preferred_spending_min"),
        col("preferred_spending_max"),
        # ... risk scores & preferences
    )
```

---

### 3️⃣ Gold Layer: Fraud Detection & Analytics

#### **🔴 Fraud Card Alert - Streaming Join with Watermarking**

The crown jewel of the pipeline: **Stream-to-stream join with watermarking** to detect fraud in real-time.

```python
# File: finguard_streaming/gold/fraud_card_alert.py

@dp.table(name="finguard.gold.fraud_card_alert")
def fraud_card_alert():
    # Read streaming sources
    transactions = spark.readStream.table("finguard.silver.transactions")
    fraud_watchlist = spark.readStream.table("finguard.silver.fraud_watchlist")
    customers = spark.read.table("finguard.silver.customers")  # Batch join
    
    # 🌊 WATERMARKING: Handle late-arriving data
    transactions_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")
    watchlist_wm = fraud_watchlist.withWatermark("effective_from", "5 minutes")
    
    # 🔗 STATEFUL STREAMING JOIN
    fraud_detected = (
        transactions_wm
            .join(watchlist_wm, 
                  transactions_wm.card_number == watchlist_wm.entity_id,
                  "inner")  # Inner join = only matches
            .join(customers,  # Enrich with customer details
                  transactions_wm.customer_id == customers.customer_id,
                  "left")
            .select(
                concat_ws("-", lit("FRAUD"), col("transaction_id")).alias("alert_id"),
                lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
                current_timestamp().alias("alert_timestamp"),
                # Transaction details
                col("transaction_id"), col("amount"), col("merchant_name"),
                # Customer details
                customers.email.alias("customer_email"),
                concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
                # Watchlist details
                col("risk_level"), col("reason_description")
            )
    )
    
    return fraud_detected
```

**🎯 Advanced Streaming Concepts Used:**
1. **Watermarking** (`withWatermark`): 
   - Allows up to 5 minutes of late data
   - Controls state storage size
   - Ensures timely join processing

2. **Stateful Stream-Stream Join**:
   - Maintains internal state for both streams
   - Only processes records within watermark boundary
   - Automatic state cleanup after watermark expires

3. **Hybrid Join** (streaming + batch):
   - Joins with static customer dimension table
   - No watermark needed for batch side

---

#### **📊 Window Aggregations - Sliding Window**

Monitor transaction volume patterns using overlapping time windows:

```python
# File: finguard_streaming/gold/transaction_count_by_minute_sliding_window.py

@dp.table(name="finguard.gold.transaction_count_by_minute_sliding_window")
def transaction_count_by_minute():
    transactions = spark.readStream.table("finguard.silver.transactions")
    
    # 🌊 Watermark: 5-minute tolerance
    transactions_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")
    
    # 📈 SLIDING WINDOW: 5-minute window, 1-minute slide
    transaction_count = (
        transactions_wm
            .groupBy(
                window("transaction_timestamp", "5 minutes", "1 minute")
            )
            .agg(count("*").alias("transaction_count"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("transaction_count")
            )
    )
    
    return transaction_count
```

**Window Types in Use:**
- **Tumbling Window** (`transaction_count_by_minute.py`): 
  - Non-overlapping 1-minute windows
  - `window("transaction_timestamp", "1 minute")`
  
- **Sliding Window** (`transaction_count_by_minute_sliding_window.py`):
  - 5-minute window sliding every 1 minute
  - Overlapping windows for smooth trend analysis
  - `window("transaction_timestamp", "5 minutes", "1 minute")`

---

## 🔔 Alert System Architecture

### **Email Notification Pipeline**

```python
# File: finguard_streaming/alert/fraud_card_alert_email_notifier.py

@dp.table(name="finguard.alert.fraud_card_email_sent_log")
def fraud_alert_notifier():
    alerts = spark.readStream.table("finguard.gold.fraud_card_alert")
    
    # Trigger email notification via external API
    def send_fraud_alert(batch_df, batch_id):
        for row in batch_df.collect():
            email_payload = {
                "to": row.customer_email,
                "subject": f"🚨 Fraud Alert: Transaction {row.transaction_id}",
                "body": f'''
                    Dear {row.customer_name},
                    
                    Suspicious transaction detected:
                    • Amount: ${row.amount}
                    • Merchant: {row.merchant_name}
                    • Risk Level: {row.risk_level}
                    
                    If you did not authorize this, please call us immediately.
                '''
            }
            send_email(email_payload)  # External SMTP/SendGrid API
            
    # Process alerts in micro-batches
    alerts.writeStream \
        .foreachBatch(send_fraud_alert) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
```

---

## 🔧 Technical Deep Dive

### **Watermarking Strategy**

Watermarking allows Spark Structured Streaming to:
1. **Track event time progress** (not processing time)
2. **Handle late-arriving data** within tolerance window
3. **Automatically evict old state** to prevent memory bloat

```python
# 5-minute watermark means:
# - Data arriving up to 5 minutes late will still be processed
# - State older than (max_event_time - 5 minutes) is discarded
transactions_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")
```

**Watermark Impact on Joins:**
- Stream-stream joins REQUIRE watermarks on both sides
- Join condition must include time constraint
- State is maintained only within watermark boundaries

### **Stateful Transformations**

The pipeline uses several stateful operations:

| Operation | Type | State Management |
|-----------|------|------------------|
| `groupBy().agg()` | Stateful aggregation | Maintains counts/sums per key |
| `stream.join(stream)` | Stateful join | Buffers both streams for join |
| `window()` | Stateful windowing | Stores data for window duration |
| `dropDuplicates()` | Stateful dedup | Tracks seen keys |

**State Store Backend**: RocksDB (default for Databricks)

---

## 📁 Project Structure

```
fingurad_fraudetection_streaming_project/
│
├── 📓 Notebooks/
│   ├── 01_kafka_streaming_test.ipynb     # Kafka connectivity testing
│   ├── 02_Setup_Secret_Scope.ipynb       # Secrets configuration
│   ├── 03_Send_Email.ipynb               # Email notification setup
│   └── 04_Autoloader_test.py.ipynb      # Auto Loader testing
│
├── 🏭 finguard_streaming/                # Main streaming pipeline
│   │
│   ├── bronze/                           # Raw ingestion layer
│   │   ├── finguard_bronze.py           # Transaction Kafka stream
│   │   └── fraud_watchlist_bronze.py    # Watchlist Kafka stream
│   │
│   ├── silver/                           # Cleansed data layer
│   │   ├── fingurad_silver.py           # Parsed transactions
│   │   └── fraud_watchlist_silver.py    # Validated watchlist
│   │
│   ├── gold/                             # Business logic layer
│   │   ├── fraud_card_alert.py          # Fraud detection (streaming join)
│   │   ├── high_value_transactions_alert.py  # Anomaly detection
│   │   ├── transaciton_count_by_minute.py   # Tumbling window
│   │   └── transaciton_count_by_minute_sliding_window.py  # Sliding window
│   │
│   └── alert/                            # Alert processing
│       ├── fraud_card_alert_email_notifier.py
│       └── high_value_transaction_email_notifier.py
│
├── 👥 finguard_customers_silver_ingestion/
│   └── silver/
│       └── customer_silver.py           # Customer data transformation
│
└── 🎲 fraud_watchlist_file_generator/
    └── fraud_watchlist_data_generator.py  # Test data generator
```

---

## 🚀 Setup & Deployment

### **Prerequisites**

```
Databricks Runtime: DBR 14.3 LTS or higher (with Spark 3.5+)

Required Libraries:
- Delta Lake 3.0+
- Kafka 3.x client libraries
- databricks-sdk
```

### **1. Secret Management**

Configure Kafka credentials and email SMTP settings:

```python
# Run: 02_Setup_Secret_Scope.ipynb

# Create secret scope
dbutils.secrets.createScope("finguard-scope")

# Add Kafka credentials
kafka_config = {
    "bootstrap_servers": "pkc-xxxxx.confluent.cloud:9092",
    "api_key": "YOUR_KAFKA_API_KEY",
    "api_secret": "YOUR_KAFKA_API_SECRET",
    "topic": "financial-transactions"
}

dbutils.secrets.put("finguard-scope", "kafka_connection_details", 
                     json.dumps(kafka_config))

# Add email credentials
dbutils.secrets.put("finguard-scope", "smtp_username", "alerts@finguard.com")
dbutils.secrets.put("finguard-scope", "smtp_password", "your_password")
```

### **2. Create Unity Catalog Schema**

```sql
-- Create catalog and schema
CREATE CATALOG IF NOT EXISTS finguard;
USE CATALOG finguard;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS alert;
```

### **3. Deploy Lakeflow Spark Declarative Pipeline**

Navigate to Databricks UI:
1. Go to **Lakeflow** → **Create Pipeline**
2. Configure pipeline settings:
   - **Name**: `finguard-fraud-detection-pipeline`
   - **Storage**: `/pipelines/finguard`
   - **Target**: `finguard`
   - **Mode**: Continuous (for streaming)
3. Add library files from `finguard_streaming` directory
4. Click **Create** and **Start**

### **4. Configure Customer Data Ingestion (Lakeflow Connect)**

1. Navigate to **Lakeflow Connect** UI → **Create Connection**
2. Select source type:
   - **Cloud Storage** (S3, ADLS, GCS)
   - **SaaS** (Salesforce, Workday, HubSpot)
   - **Databases** (MySQL, PostgreSQL, SQL Server)
3. Configure connection:
   ```
   Source Path: s3://customer-data-bucket/profiles/
   File Format: CSV
   Destination: finguard.bronze.customers
   Trigger: Continuous
   ```
4. Start the ingestion pipeline

---

## 📊 Monitoring & Observability

### **Pipeline Metrics**

Access real-time metrics in Databricks Lakeflow UI:

- ✅ **Input Rate**: Records/sec from Kafka
- ✅ **Processing Rate**: Records/sec transformed
- ✅ **End-to-End Latency**: Source → Gold layer
- ✅ **Watermark Lag**: Current watermark vs. latest event time
- ✅ **State Memory Usage**: RocksDB state size

### **SQL Analytics Queries**

```sql
-- Real-time fraud alert monitoring
SELECT 
    alert_type,
    risk_level,
    COUNT(*) as alert_count,
    AVG(amount) as avg_transaction_amount
FROM finguard.gold.fraud_card_alert
WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY alert_type, risk_level;

-- Transaction volume trends (sliding window)
SELECT 
    window_start,
    window_end,
    transaction_count
FROM finguard.gold.transaction_count_by_minute_sliding_window
ORDER BY window_start DESC
LIMIT 60;
```

### **Databricks Dashboard Integration**

Create live dashboards in Lakeview:
1. Navigate to **Dashboards** → **Create Dashboard**
2. Add widgets querying Gold layer tables
3. Set refresh interval to 30 seconds
4. Share with stakeholders

---

## 🎓 Key Learnings & Best Practices

### **✅ DO**
- Use **watermarking** on all time-based joins and aggregations
- Implement **data quality checks** with `@dp.expect_or_drop`
- Use **Delta Lake** for ACID transactions on streaming data
- Set appropriate **trigger intervals** (processingTime vs availableNow)
- Monitor **state size growth** to prevent OOM errors

### **❌ DON'T**
- Don't use complete output mode with unbounded state
- Don't join streams without watermarks (infinite state growth)
- Don't skip checkpointing (required for fault tolerance)
- Don't use collect() on streaming DataFrames in production

---

## 📈 Performance Tuning

```python
# Optimize shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Optimize Kafka fetch size
.option("maxOffsetsPerTrigger", "1000000")  # Records per micro-batch
.option("minPartitions", "8")  # Kafka partition distribution

# Delta Lake optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

---

## 🤝 Contributing

Contributions are welcome! Please follow:
1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

---

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 👨‍💻 Author

**Taha Furkan**  
GitHub: [@Tahafurkhan](https://github.com/Tahafurkhan)

---

## 🙏 Acknowledgments

- Built on **Databricks Lakehouse Platform**
- Powered by **Apache Spark Structured Streaming**
- Data stored in **Delta Lake** format
- Stream ingestion via **Apache Kafka (Confluent Cloud)**

---

## 📞 Support

For questions or issues:
- Open a GitHub Issue
- Contact: tahafurkhan@gmail.com

---

**⭐ If this project helped you, please give it a star!**
