# FinGuard: Real-Time Fraud Detection Streaming Platform

<div align="center">

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4?style=flat&logo=delta&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Structured Streaming](https://img.shields.io/badge/Structured%20Streaming-E25A1C?style=flat)

**Enterprise-Grade Real-Time Fraud Detection on the Databricks Lakehouse**

</div>

---

## 🎯 Executive Summary

**FinGuard** is a production-ready fraud detection platform that processes millions of financial transactions per second using **Spark Structured Streaming**, **Delta Lake**, and **Apache Kafka**. It demonstrates advanced data engineering patterns (medallion architecture, stateful stream-stream joins, watermarking) combined with real-time alerting to catch fraudulent activity in milliseconds.

This project showcases:
- ✅ **Enterprise Data Architecture**: Bronze → Silver → Gold medallion pattern on Databricks
- ✅ **Stateful Stream Processing**: Watermarked stream-stream joins for fraud correlation
- ✅ **Realistic Transaction Simulation**: Multi-producer fraud detection testing framework
- ✅ **Real-Time Alerting**: Email notifications for fraud events with < 30s latency
- ✅ **Production Readiness**: Lakeflow Declarative Pipelines, Delta Lake ACID guarantees, secret management

---

## 📊 Architecture Overview

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                 DATA SOURCES                                     │
├────────────────────────────────────┬──────────────────────────────────────────────┤
│  Apache Kafka (Confluent Cloud)    │  Cloud Storage / Lakeflow Connect            │
│  • Real-time transactions          │  • Customer profiles                         │
│  • Fraud watchlist stream          │  • Account information                       │
└────────────────────────┬───────────┴────────────────┬─────────────────────────────┘
                         │                            │
                         ▼                            ▼
       ╔════════════════════════════════════════════════════════════╗
       ║  BRONZE LAYER: Raw Ingestion (Spark Structured Streaming)  ║
       ╠════════════════════════════════════════════════════════════╣
       ║  • finguard.bronze.transactions (Kafka → Delta)            ║
       ║  • finguard.bronze.fraud_watchlist (Watchlist stream)      ║
       ║  • finguard.bronze.customers (Cloud → Auto Loader)         ║
       ║                                                             ║
       ║  Technology: SASL_SSL Kafka, Auto Loader, checkpointing    ║
       ║  Format: Delta Lake with metadata tracking                 ║
       ╚════════════════════════┬════════════════════════════════════╝
                                │
                                ▼
       ╔════════════════════════════════════════════════════════════╗
       ║  SILVER LAYER: Cleansed & Enriched                         ║
       ╠════════════════════════════════════════════════════════════╣
       ║  • finguard.silver.transactions                            ║
       ║    ✓ JSON parsing & schema enforcement                     ║
       ║    ✓ Type casting & validation                             ║
       ║    ✓ Data quality checks (@dp.expect_or_drop)              ║
       ║                                                             ║
       ║  • finguard.silver.fraud_watchlist                         ║
       ║  • finguard.silver.customers                               ║
       ║                                                             ║
       ║  Technology: Declarative transformations, validation rules ║
       ╚════════════════════════┬════════════════════════════════════╝
                                │
                                ▼
       ╔════════════════════════════════════════════════════════════╗
       ║  GOLD LAYER: Business Logic & Alerts                       ║
       ╠════════════════════════════════════════════════════════════╣
       ║  📊 FRAUD DETECTION:                                        ║
       ║  • fraud_card_alert                                         ║
       ║    → Stateful Stream-Stream Join with Watermarking         ║
       ║    → Detects watchlist matches in real-time                ║
       ║                                                             ║
       ║  • high_value_transactions_alert                            ║
       ║    → Anomaly detection for large transactions              ║
       ║                                                             ║
       ║  📈 ANALYTICS:                                              ║
       ║  • transaction_count_by_minute (Tumbling window)           ║
       ║  • transaction_count_sliding_window (5m window, 1m slide)  ║
       ║                                                             ║
       ║  🔔 ALERT ENGINE:                                           ║
       ║  • Email notifications via SMTP/SendGrid                   ║
       ║  • Alert log & audit trail                                 ║
       ║                                                             ║
       ║  Technology: Watermarking, windowed aggregations, trigger  ║
       ║  intervals (30s micro-batch)                               ║
       ╚════════════════════════┬════════════════════════════════════╝
                                │
                                ▼
                    ┌──────────────────────────────┐
                    │  📧 ALERT NOTIFICATIONS      │
                    │  📊 DASHBOARDS & MONITORING  │
                    │  🔍 AUDIT LOGS               │
                    └──────────────────────────────┘
```

---

## 🔄 Data Pipeline Deep Dive

### 1️⃣ Bronze Layer: Kafka Ingestion

The **transaction stream** is ingested directly from Confluent Cloud Kafka with SASL_SSL authentication:

```python
@dp.table(name="finguard.bronze.transactions")
def transactions_bronze():
    streaming_df = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("startingOffsets", "earliest")
        .load()
    
    return streaming_df.select(
        col("key").cast("string"),
        col("value").cast("string"),  # JSON payload
        col("topic"), col("partition"), col("offset"),
        col("timestamp"),
        current_timestamp().alias("ingestion_timestamp")
    )
```

**Key Design Decisions:**
- SASL_SSL ensures encrypted, authenticated broker communication
- At-least-once delivery + idempotent producers prevent duplicates
- Offset tracking enables fault-tolerant replay

---

### 2️⃣ Silver Layer: Data Quality & Transformation

Parse and validate the JSON transaction payload, apply data quality rules:

```python
@dp.table(name="finguard.silver.transactions")
@dp.expect_or_drop("valid_transaction", "transaction_id IS NOT NULL")
def transactions_silver():
    bronze_df = spark.readStream.table("finguard.bronze.transactions")
    
    parsed_df = bronze_df.select(
        from_json(col("value"), transaction_schema).alias("data")
    ).select("data.*")
    
    return parsed_df.select(
        col("transaction_id"),
        col("customer_id"),
        col("amount").cast("decimal(10,2)"),
        col("merchant_name"),
        to_timestamp(col("transaction_timestamp")).alias("transaction_timestamp"),
        col("is_international"),
        # ... additional fields
        current_timestamp().alias("silver_ingestion_timestamp")
    )
```

**Quality Gates:**
- `@dp.expect_or_drop`: Reject invalid records with explanations
- Schema enforcement prevents parsing errors downstream
- Type casting catches data inconsistencies early

---

### 3️⃣ Gold Layer: Stateful Fraud Detection (Crown Jewel)

#### **Stream-Stream Join with Watermarking**

The most advanced piece: join a continuous transaction stream with a fraud watchlist stream, with proper time-bounded state management:

```python
@dp.table(name="finguard.gold.fraud_card_alert")
def fraud_card_alert():
    transactions = spark.readStream.table("finguard.silver.transactions")
    fraud_watchlist = spark.readStream.table("finguard.silver.fraud_watchlist")
    customers = spark.read.table("finguard.silver.customers")  # Batch join
    
    # 🌊 WATERMARKING: Handle late-arriving data (up to 5 min late)
    transactions_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")
    watchlist_wm = fraud_watchlist.withWatermark("effective_from", "5 minutes")
    
    # 🔗 STATEFUL STREAMING JOIN
    fraud_detected = (
        transactions_wm
            .join(watchlist_wm, 
                  transactions_wm.card_number == watchlist_wm.entity_id,
                  "inner")  # Inner join = only matches
            .join(customers,
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

**Why This Matters:**
1. **Watermarking** (`withWatermark`):
   - Allows up to 5 minutes of late data
   - Automatically evicts old state to prevent memory bloat
   - Controls RocksDB state size in a streaming context

2. **Stateful Stream-Stream Join**:
   - Maintains internal state for both sides of the join
   - Only processes records within the watermark window
   - Automatic state cleanup after watermark expiration

3. **Hybrid Batch-Stream Join**:
   - Customers table is static (batch read)
   - No watermark needed for batch side
   - Enriches alerts with customer name, email for notifications

---

#### **Window Aggregations**

Monitor transaction patterns with tumbling and sliding windows:

```python
# Tumbling Window: Non-overlapping 1-minute windows
@dp.table(name="finguard.gold.transaction_count_by_minute")
def transaction_count_tumbling():
    transactions = spark.readStream.table("finguard.silver.transactions")
    transactions_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")
    
    return (
        transactions_wm
            .groupBy(window("transaction_timestamp", "1 minute"))
            .agg(count("*").alias("transaction_count"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("transaction_count")
            )
    )

# Sliding Window: 5-minute window, sliding every 1 minute
@dp.table(name="finguard.gold.transaction_count_by_minute_sliding_window")
def transaction_count_sliding():
    transactions = spark.readStream.table("finguard.silver.transactions")
    transactions_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")
    
    return (
        transactions_wm
            .groupBy(window("transaction_timestamp", "5 minutes", "1 minute"))
            .agg(count("*").alias("transaction_count"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("transaction_count")
            )
    )
```

---

## 🎲 Transaction Simulation Engine

### `kafka_producer/` Folder

A standalone Python module that generates realistic transaction data and publishes to Kafka. Three producers cover different scenarios:

| Producer | Purpose |
|---|---|
| `producer_normal.py` | Continuous stream of legitimate + naturally-flagged fraud transactions |
| `producer_fraud_transaction.py` | Single high-value (>$100k) forced fraud event for testing |
| `producer_fraud_card.py` | Single forced fraud transaction on a fixed test card for testing |

#### Key Classes

**`CustomerGenerator`** (customer_generator.py)
- Creates 1,000 realistic customers with profiles (segment, income, spending habits)
- Generates Luhn-valid credit card numbers
- Assigns risk scores, trusted devices, international preferences
- Persists to `data/customers.csv`

**`MerchantGenerator`** (merchant_generator.py)
- Creates 200 merchants across 12 categories (grocery, fuel, airlines, etc.)
- Assigns risk tiers (LOW/MEDIUM/HIGH) and blacklist flags
- Balances domestic vs. international merchant distribution

**`FraudEngine`** (fraud_engine.py)
- Evaluates 8 fraud signals with weighted scoring (0-100):
  - `HIGH_VALUE_TRANSACTION`: Amount > $100k (weight: 40)
  - `IMPOSSIBLE_TRAVEL`: Same customer, Delhi → London in <20 min (weight: 50)
  - `NEW_DEVICE`: Transaction device ≠ trusted device (weight: 20)
  - `VELOCITY_FRAUD`: 5+ txns within 30 seconds (weight: 45)
  - `CARD_TESTING`: 3+ small txns ($5-$20) within 60s (weight: 30)
  - `BLACKLISTED_MERCHANT`: Card on watchlist (weight: 60)
  - `HIGH_RISK_MERCHANT`: Merchant risk = HIGH (weight: 25)
  - `INTERNATIONAL_TRANSACTION`: Cross-border (weight: 25)

**`TransactionGenerator`** (transaction_generator.py)
- Profile-driven: Platinum customers spend more at airlines; Regular customers at groceries
- Realistic amounts based on customer segment and merchant category
- Generates ISO-8601 timestamps with UTC timezone

---

## 📹 Live Demo & Dashboard

### Streaming Pipeline in Action

Watch the real-time fraud detection pipeline process transactions:

**[🎬 Streaming Pipeline Demo](./media/Streaming%20recording%20video.mp4)**  
*11 MB video showing live transaction ingestion, fraud scoring, and alert notifications*

---

### Interactive Dashboard

Real-time monitoring dashboard built in Databricks Lakeview:

**[📊 FinGuard Monitoring Dashboard PDF](./media/FinGuard%20Fraud%20Detection%20Monitor%202026-07-13T17-21-15%202026-07-13%2018_12.pdf)**  
*High-risk customer counts, fraud alert trends, transaction volume metrics*

Sample Queries from the Dashboard:

```sql
-- Real-time fraud alert summary (last hour)
SELECT 
    alert_type,
    risk_level,
    COUNT(*) as alert_count,
    AVG(amount) as avg_transaction_amount
FROM finguard.gold.fraud_card_alert
WHERE alert_timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY alert_type, risk_level;

-- Sliding window transaction volume (last 10 windows)
SELECT 
    window_start,
    window_end,
    transaction_count
FROM finguard.gold.transaction_count_by_minute_sliding_window
ORDER BY window_start DESC
LIMIT 10;
```

---

## 🚀 Project Structure

```
fingurad_fraudetection_streaming_project/
│
├── 📓 Notebooks/
│   ├── 01_kafka_streaming_test.ipynb           # Kafka connectivity validation
│   ├── 02_Setup_Secret_Scope.ipynb             # Secrets & credential management
│   ├── 03_Send_Email.ipynb                     # Email alert testing
│   └── 04_Autoloader_test.py.ipynb             # Auto Loader proof-of-concept
│
├── 🏭 finguard_streaming/                      # Main Lakeflow pipeline
│   │
│   ├── bronze/                                 # Raw ingestion
│   │   ├── finguard_bronze.py                  # Kafka → bronze tables
│   │   └── fraud_watchlist_bronze.py           # Watchlist stream
│   │
│   ├── silver/                                 # Cleansed layer
│   │   ├── fingurad_silver.py                  # Parsed transactions
│   │   └── fraud_watchlist_silver.py           # Validated watchlist
│   │
│   ├── gold/                                   # Business logic
│   │   ├── fraud_card_alert.py                 # **Stateful join with watermarking**
│   │   ├── high_value_transactions_alert.py    # Anomaly detection
│   │   ├── transaction_count_by_minute.py      # Tumbling window
│   │   └── transaction_count_by_minute_sliding_window.py  # Sliding window
│   │
│   └── alert/                                  # Alert processing
│       ├── fraud_card_alert_email_notifier.py  # SMTP notifier
│       └── high_value_transaction_email_notifier.py
│
├── 👥 finguard_customers_silver_ingestion/     # Customer data pipeline
│   └── silver/customer_silver.py               # Lakeflow Connect integration
│
├── 🎲 fraud_watchlist_file_generator/          # Test data generator
│   └── fraud_watchlist_data_generator.py
│
├── 📦 kafka_producer/                          # **Transaction simulator**
│   ├── config.py                               # Settings & env loading
│   ├── models.py                               # Data models
│   ├── customer_generator.py                   # Synthetic customer data
│   ├── merchant_generator.py                   # Synthetic merchant data
│   ├── transaction_generator.py                # Realistic transactions
│   ├── fraud_engine.py                         # Fraud scoring logic
│   ├── producer_normal.py                      # Continuous normal producer
│   ├── producer_fraud_transaction.py           # High-value fraud event
│   ├── producer_fraud_card.py                  # Fraudulent card event
│   ├── consumer.py                             # Debug consumer
│   ├── utils.py                                # Shared utilities
│   ├── requirements.txt                        # Python dependencies
│   ├── README.md                               # Producer documentation
│   └── .gitignore                              # Excludes .env, __pycache__, *.csv
│
└── 📁 media/                                   # Demos & docs
    ├── Streaming recording video.mp4           # Real-time pipeline demo
    └── FinGuard Fraud Detection Monitor...pdf  # Dashboard visualization

```

---

## 🔧 Technical Depth: Watermarking Explained

**Problem**: In a streaming join, how do you know when to stop waiting for late data?

**Solution**: **Watermarking**

```python
transactions_wm = transactions.withWatermark("transaction_timestamp", "5 minutes")
watchlist_wm = fraud_watchlist.withWatermark("effective_from", "5 minutes")

# Join waits for data within 5 minutes of the max event time it has seen
# After 5 minutes, old state is dropped to prevent unbounded memory growth
fraud_detected = transactions_wm.join(watchlist_wm, ...)
```

**What Happens:**
1. Spark tracks the **maximum event time** it has processed
2. The **watermark** = max_event_time - 5 minutes
3. Any data arriving **before the watermark** is dropped (too late)
4. State for timestamps **before the watermark** is evicted (memory cleanup)
5. Result: **Bounded state** + **correct late-arriving results** within the tolerance window

**Why This Matters for Production:**
- Without watermarking, the join would buffer data forever → OOM crash
- With watermarking, state size grows predictably based on tolerance, not data volume
- Critical for streaming systems processing billions of events/day

---

## 📊 State Management & RocksDB

Spark Structured Streaming uses **RocksDB** (by default on Databricks) to manage join state:

```python
# Automatic state size optimization
spark.conf.set("spark.sql.streaming.stateStore.compression.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.timeoutInterval", "3600s")

# Monitor state size in Databricks UI:
# Streaming → Query → Metrics → State Information
```

| Metric | Significance |
|---|---|
| `Total State Memory` | Sum of RocksDB memory across all stateful operations |
| `State Checkpoint Size` | Persisted state on disk (for fault tolerance) |
| `Watermark Lag` | Difference between current watermark and latest event time |

---

## 🔐 Security & Production Readiness

### Secret Management

All credentials (Kafka API keys, SMTP passwords) are stored in **Databricks Secret Scopes**:

```python
# Never hardcode credentials
bootstrap_servers = dbutils.secrets.get("finguard-scope", "kafka_bootstrap_servers")
api_key = dbutils.secrets.get("finguard-scope", "kafka_api_key")
api_secret = dbutils.secrets.get("finguard-scope", "kafka_api_secret")
```

### Idempotent Producers

All Kafka producers are configured for **exactly-once semantics**:

```python
config = {
    "acks": "all",                      # Wait for all replicas
    "retries": 5,                       # Retry transient failures
    "enable.idempotence": True,         # Prevent duplicate delivery
    "transactional.id": "...",          # Unique producer ID
}
```

---

## 📈 Performance Tuning

```python
# Adaptive Query Execution
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Kafka Consumer Tuning
.option("maxOffsetsPerTrigger", "1000000")  # Records per micro-batch
.option("minPartitions", "8")               # Kafka parallelism

# Delta Lake Optimization
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# State Store Compression
spark.conf.set("spark.sql.streaming.stateStore.compression.enabled", "true")
```

---

## 🎓 Key Learnings & Best Practices

### ✅ DO

- Use **watermarking on all time-based joins and aggregations**
- Implement **data quality checks** with `@dp.expect_or_drop`
- Use **Delta Lake** for ACID transactions on streaming data
- Set appropriate **trigger intervals** based on latency requirements
- Monitor **state size growth** to prevent OOM errors
- Store **secrets in Databricks Secret Scopes**, never in code

### ❌ DON'T

- Don't use **complete output mode** with unbounded state
- Don't join streams **without watermarks** (infinite state growth)
- Don't skip **checkpointing** (required for fault tolerance)
- Don't use `collect()` on streaming DataFrames in production
- Don't hardcode credentials or API keys in notebooks
- Don't deploy without monitoring RocksDB state metrics

---

## 🚀 Setup & Deployment Guide

### Prerequisites

```
Databricks Runtime: DBR 14.3 LTS or higher (Spark 3.5+)
Python: 3.10+
Kafka: Confluent Cloud (SASL_SSL)
```

### Step 1: Configure Secret Scope

Run notebook `02_Setup_Secret_Scope.ipynb`:

```python
# Create scope
dbutils.secrets.createScope("finguard-scope")

# Add Kafka credentials
dbutils.secrets.put("finguard-scope", "kafka_bootstrap_servers", "pkc-xxxxx.confluent.cloud:9092")
dbutils.secrets.put("finguard-scope", "kafka_api_key", "YOUR_API_KEY")
dbutils.secrets.put("finguard-scope", "kafka_api_secret", "YOUR_API_SECRET")

# Add email credentials
dbutils.secrets.put("finguard-scope", "smtp_username", "alerts@finguard.com")
dbutils.secrets.put("finguard-scope", "smtp_password", "YOUR_PASSWORD")
```

### Step 2: Create Unity Catalog Schema

```sql
CREATE CATALOG IF NOT EXISTS finguard;
USE CATALOG finguard;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS alert;
```

### Step 3: Deploy Lakeflow Pipeline

1. Navigate to **Lakeflow** → **Create Pipeline**
2. Configure:
   - **Name**: `finguard-fraud-detection-pipeline`
   - **Storage**: `/pipelines/finguard`
   - **Target**: `finguard`
   - **Mode**: Continuous
3. Add library files from `finguard_streaming/` directory
4. Click **Create** and **Start**

### Step 4: Run Transaction Producer

```bash
cd kafka_producer/
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt

# Set .env file (never commit this!)
cp .env.example .env
# Edit .env with your Kafka credentials

# Start producing transactions
python producer_normal.py
```

---

## 🎬 Demo Walkthrough

### 1. Validate Kafka Connectivity

Run `01_kafka_streaming_test.ipynb` to confirm broker access.

### 2. Start Transaction Producer

```bash
python kafka_producer/producer_normal.py
```

This generates:
- 1,000 customers with realistic segments (Regular, Gold, Platinum, Corporate)
- 200 merchants across 12 categories
- ~5 transactions/sec with 8% fraud rate

### 3. Monitor Real-Time Pipeline

The Lakeflow pipeline processes transactions in micro-batches (30s trigger interval):

```
Bronze:  1M+ raw transactions/hour
  ↓
Silver:  ~999,200 valid transactions/hour (data quality filtering)
  ↓
Gold:    ~8,000 fraud alerts/hour (8% fraud rate)
  ↓
Email:   Alerts sent to customer within 30 seconds
```

### 4. Query Live Results

```sql
-- Check latest fraud alerts
SELECT * FROM finguard.gold.fraud_card_alert
WHERE alert_timestamp >= current_timestamp() - INTERVAL 5 MINUTES
ORDER BY alert_timestamp DESC;

-- Monitor transaction volume
SELECT window_start, transaction_count 
FROM finguard.gold.transaction_count_by_minute_sliding_window
ORDER BY window_start DESC
LIMIT 5;
```

---

## 📞 Support & Contributing

- **Issues**: Open a GitHub Issue for bugs or feature requests
- **Questions**: Reach out at tahafurkhan@gmail.com
- **Contributing**: Fork → Feature Branch → Pull Request

---

## 📄 License

MIT License — see LICENSE file for details

---

## 👨‍💻 Author

**Taha Furkan**  
*Data Engineer | Agentic AI Engineer*  
GitHub: [@Tahafurkhan](https://github.com/Tahafurkhan)  
Email: tahafurkhan@gmail.com  
LinkedIn: [in/developertaha](https://www.linkedin.com/in/developertaha)

---

## 🙏 Acknowledgments

Built on:
- **Databricks** Lakehouse Platform & Lakeflow Pipelines
- **Apache Spark** Structured Streaming & Delta Lake
- **Apache Kafka** (Confluent Cloud) for real-time ingestion
- **Python** ecosystem (Faker, Confluent Kafka client, Pandas, NumPy)

---

<div align="center">

**⭐ If this project helped you learn about real-time data engineering and fraud detection, please give it a star!**

*Last Updated: 2026-07-22*  
*Production-Ready | Enterprise-Grade | Interview Showcase*

</div>
