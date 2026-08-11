# FinGuard — Real-Time Fraud Detection on Databricks

A card-fraud detection platform built on Databricks Lakeflow Declarative
Pipelines, Unity Catalog and Delta Lake. Transactions stream from Kafka,
reference data arrives as files, and customer master data is captured from
Postgres by CDC — all through a metadata-driven ingestion framework where
onboarding a new source is a YAML file rather than a code change.

**Scale, stated plainly:** this runs at roughly 5 transactions/second from a
simulator on serverless compute. It is built to demonstrate architecture and
patterns at portfolio cost, not to prove throughput. The design scales; the
current load deliberately does not.

---

## Architecture

```
                    ┌─────────────────────────────────────────┐
   Confluent Kafka  │                                         │
   (transactions) ──┤                                         │
                    │            BRONZE                       │
   UC Volume        │   generated from config/sources/*.yaml  │
   (watchlist,    ──┤   raw payloads + Kafka envelope kept    │
    merchants)      │                                         │
                    │                                         │
   Neon Postgres    │                                         │
   (customers)    ──┤   managed CDC, pk + cursor column       │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │            SILVER                       │
                    │   explicit per-source cleaning          │
                    │   quality expectations, typed schema    │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │            GOLD                         │
                    │   stream-stream join (watermarked)      │
                    │   tumbling + sliding window aggregates  │
                    │   fraud alerts → email sink             │
                    └─────────────────────────────────────────┘

                    ┌─────────────────────────────────────────┐
                    │            OPS                          │
                    │   source_registry  — what should run    │
                    │   ingestion_audit  — what actually ran  │
                    └─────────────────────────────────────────┘
```

### Ingestion patterns

Four genuinely different mechanisms, which matters more than the source count:

| Source | Pattern | Target | Notes |
|---|---|---|---|
| Transactions | Kafka (SASL_SSL) | `bronze.transactions` | 6 partitions; envelope metadata preserved for replay |
| Fraud watchlist | Auto Loader (JSON) | `bronze.fraud_watchlist` | Schema inference, `_rescued_data` for drift |
| Merchants | Auto Loader (CSV) | `bronze.merchants` | 200 rows; risk rating and blacklist flag |
| Customers | Lakeflow managed CDC | `bronze.customers` | Postgres; `customer_id` pk, `update_timestamp` cursor |

A REST reader exists and is tested (pagination, retry, auth) but ships as a
template — see `config/sources/merchant_risk_api.yaml.example` — because there
is no live vendor endpoint to point it at.

---

## The ingestion framework

Bronze tables are not written per source. They are generated:

```yaml
# config/sources/merchants.yaml
name: merchants
target: finguard.bronze.merchants
ingestion:
  type: autoloader
  path: /Volumes/finguard/source/merchants/source_data/
  format: csv
  options:
    header: "true"
columns:
  - {name: merchant_id}
  - {name: merchant_risk}
  - {name: source_file, expr: "_metadata.file_path"}
```

That file is the entire merchants source. No Python.

```
src/pipelines/framework/
  source_config.py    load + validate YAML, fail fast with the filename
  readers.py          one reader per ingestion type + dispatch table
  bronze_factory.py   generate one dataset per config
  registry.py         operational metadata
```

**Adding a source** — one YAML file.
**Adding a source type** — one reader function, one dispatch entry.

### Where it deliberately stops

Metadata-driven design pays off where logic is uniform and becomes a liability
where it is genuinely per-source. Bronze is uniform: read, project, timestamp.
Silver is not — the watchlist needs specific fields uppercased and a
`dd-MMM-yyyy HH:mm:ss` timestamp parsed; transactions need a JSON envelope
parsed against an explicit schema.

So silver stays explicit Python. Encoding that logic in YAML would mean
inventing a transformation DSL strictly worse than SQL.

### Config in git, state in Delta

| | Where | Why |
|---|---|---|
| Structural — what sources exist | YAML in git | Needs diffs, review, env promotion |
| Operational — what ran, how it went | Delta tables | Written at runtime; queryable |

The query that justifies the ops layer:

```sql
-- declared in config but no table exists
SELECT r.source_name,
       CASE WHEN t.table_name IS NULL THEN 'MISSING' ELSE 'EXISTS' END
FROM finguard.ops.source_registry r
LEFT JOIN system.information_schema.tables t
       ON t.table_schema = 'bronze'
      AND t.table_name = split_part(r.target_table, '.', 3)
```

This caught a real gap — `merchants` configured and merged, but no table,
because a pipeline had not been repointed after a refactor. Grepping YAML
cannot detect that.

Six operational queries: `sql/ops/health_checks.sql`.

---

## Stateful streaming

**Stream-stream join with watermarks on both sides** —
`gold/fraud_card_alert.py` joins the transaction stream against the fraud
watchlist stream, watermarked five minutes on each. The watermark bounds
retained state; without one the join accumulates forever.

**Windowed aggregation** — tumbling and sliding one-minute counts for velocity
detection.

**Quality expectations** — `expect_or_drop` on required identifiers,
`expect` on amount, with violations recorded in the event log.

**Fraud scoring** — the producer implements 8 weighted signals: impossible
travel, velocity, card testing, new device, blacklisted merchant, high-risk
merchant, high value, international.

---

## Repository layout

```
config/sources/        source declarations (the registry)
src/
  pipelines/
    framework/         metadata-driven ingestion framework
    streaming/         bronze entry point, silver, gold, alerts
    customers/         customer silver ingestion
  producer/            Kafka simulator: generators, fraud engine, producers
resources/             pipeline specs
sql/ops/               operational queries
notebooks/             exploration and setup notebooks
docs/                  architecture, challenges, interview prep
```

---

## Documentation

| Document | Contents |
|---|---|
| [Setup](docs/setup.md) | Running this on a fresh machine, plus troubleshooting |
| [Metadata-driven ingestion](docs/metadata_driven_ingestion.md) | Framework design, closure binding, batch vs streaming |
| [Performance optimization](docs/performance_optimization.md) | Every Spark/Delta optimization applied, in three honest tiers: measured, correct-but-unmeasurable, deliberately rejected |
| [Engineering challenges](docs/engineering_challenges.md) | 15 real failures — error text, cause, fix, optimization — including one measured negative result |
| [Interview preparation](docs/interview_preparation.md) | Scenario-driven Q&A: streaming, Kafka, dimensional modeling, dbt, system design, behavioural |

The dbt lineage DAG is generated rather than committed (it is build output).
To browse it:

```bash
cd transform && source ./set_env.sh
dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
```

---

## Running it

**Prerequisites:** Python 3.10+, a Databricks workspace with Unity Catalog, a
Confluent Cloud cluster.

```bash
python -m venv .venv && .venv\Scripts\activate
pip install -r src/producer/requirements.txt
pip install databricks-cli

databricks configure --token --host https://<workspace>.cloud.databricks.com
```

Copy `.env.example` to `.env` and `src/producer/.env.example` to
`src/producer/.env`, then fill in values. Both are gitignored.

Store Kafka connection details in the secret scope:

```python
# notebooks/exploration/02_Setup_Secret_Scope.ipynb
```

Generate and upload merchant data:

```bash
cd src/producer && python upload_merchants.py
```

Start producing transactions:

```bash
python producer_normal.py          # normal traffic
python producer_fraud_card.py      # watchlist-matching cards
```

---

## Known gaps

Stated deliberately — an accurate list is more useful than an impressive one.

- **Stream-static join bug.** `gold/fraud_card_alert.py` reads
  `silver.customers` with `spark.read`, snapshotting a table that is itself
  continuously updating. Customer attribute changes after stream start are not
  reflected in alerts. Fix ties into SCD2 and point-in-time joins.
- **No dimensional layer.** SCD2 dimensions, star schema and dbt marts are
  planned, not built.
- **No formal test suite.** Verification scripts exist for the framework;
  they are not yet pytest with CI.
- **Deployment is a sync script, not a bundle.** It only uploads, so deleted
  files persist remotely — which caused a duplicate-table failure. Asset
  Bundles are the correct fix.
- **No quarantine table.** Rows dropped by expectations disappear rather than
  being routed somewhere inspectable.
- **No measured performance numbers.** Liquid clustering with before/after
  benchmarks is planned.

---

## Stack

Databricks Lakeflow Declarative Pipelines (`pyspark.pipelines`) · Unity Catalog ·
Delta Lake · Spark Structured Streaming · Auto Loader · Confluent Kafka ·
Neon Postgres (CDC source) · Python 3.13
