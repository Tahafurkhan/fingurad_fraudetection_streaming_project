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

**What makes this different from a tutorial build:** the platform monitors
itself, and on its first run that monitoring found two real defects in this
project — a 606-hour stale watermark and a Spark setting that was silently inert
on stateful operators. Both are documented rather than quietly fixed, including
the one that contradicts a claim made elsewhere in these docs.

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
                    │   quarantine tables for rejected rows   │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │            GOLD                         │
                    │   stream-stream join (watermarked)      │
                    │   tumbling + sliding window aggregates  │
                    │   fraud alerts → email sink             │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────┐
                    │            MARTS (dbt)                  │
                    │   SCD2 dim_customer, dim_merchant       │
                    │   fct_transactions (point-in-time)      │
                    │   fct_alerts, aggregates                │
                    └─────────────────────────────────────────┘

    ┌──────────────────────────┐   ┌──────────────────────────┐
    │          OPS             │   │       SECURITY           │
    │  source_registry         │   │  mask_pan / mask_email   │
    │  ingestion_audit         │   │  10 column masks         │
    │                          │   │  28 classification tags  │
    │  pipeline_runs      ─┐   │   │  3-group access model    │
    │  expectation_results ├─  │   │                          │
    │  stream_health      ─┘   │   │  verified by 6 assertions│
    │   → 4 SQL Alerts         │   │                          │
    │   → 6-panel dashboard    │   │                          │
    └──────────────────────────┘   └──────────────────────────┘
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

---

## Stateful streaming

**Stream-stream join with watermarks on both sides** —
`gold/fraud_card_alert.py` joins the transaction stream against the fraud
watchlist stream, watermarked five minutes on each. The watermark bounds
retained state; without one the join accumulates forever.

**Windowed aggregation** — tumbling and sliding one-minute counts for velocity
detection.

**Quality expectations with quarantine** — `expect_or_drop` on required
identifiers, `expect` on amount. Rejected rows are routed to quarantine tables
with the failed rules and full Kafka coordinates, so a rejected message can be
traced to its offset and replayed after the upstream fix.

**Fraud scoring** — the producer implements 8 weighted signals: impossible
travel, velocity, card testing, new device, blacklisted merchant, high-risk
merchant, high value, international.

---

## Dimensional layer (dbt)

| Model | Type | Notes |
|---|---|---|
| `dim_customer` | SCD2 | Versioned on the attributes that drive fraud evaluation |
| `dim_merchant` | SCD2 | Declared vs observed risk |
| `dim_date` | Generated | 2024–2027 |
| `fct_transactions` | Incremental merge | Point-in-time dimension joins |
| `fct_alerts` | Incremental | Detection latency measured, not assumed |
| `agg_*` | Table | Daily fraud summary, merchant risk profile |

**Point-in-time joins are the reason the layer exists.** A transaction from
last week is attributed to the customer risk profile that was in force last
week, not to today's:

```sql
left join {{ ref('dim_customer') }} c
       on c.customer_id = t.customer_id
      and t.transaction_timestamp >= c.valid_from
      and t.transaction_timestamp <  c.valid_to
```

Boundary rule: **Lakeflow owns bronze and silver, dbt owns marts.** Nothing is
defined in both places.

---

## Governance

Unity Catalog column masking, applied and verified:

```sql
SELECT card_number FROM finguard.silver.transactions LIMIT 1;
-- before:  4144 88** **** 9904
-- after:   ****-****-****-9904
```

| | Before | After |
|---|---|---|
| Column masks | 0 | **10** |
| Classification tags | 0 | **28 column + 10 table** |
| Access model | `account users BROWSE` | 3 groups, no human write grants |

The mask branches on `is_account_group_member('finguard_pci_privileged')` and
**fails closed** — a missing group means everyone sees the masked value.

The verification query that searches for PAN-shaped columns *regardless of
tagging* found two leaks immediately after the first six masks were confirmed
working: `bronze.customers` (CDC lands the card on file before silver runs) and
`snapshots.customers_snapshot` (dbt-written, and it retains every historical
version forever). See [data governance](docs/data_governance.md).

**Not claimed: PCI compliance.** This implements Requirement 3.3 display
masking. It does not implement 3.5 at-rest protection, which needs
tokenization — `bronze.transactions.value` still holds PANs inside raw JSON.

---

## Observability

The platform monitors itself from the pipeline event log:

| Table | Contents |
|---|---|
| `ops.pipeline_runs` | 49 rows — update outcomes |
| `ops.expectation_results` | 501 rows — per-expectation pass/fail |
| `ops.stream_health` | 585 rows — watermarks, state size, batch latency |

Four scheduled SQL Alerts (2 hourly PAGE, 2 daily TICKET) and a six-panel
dashboard. Severity drives cadence: a PAGE check that runs daily is not a page;
a TICKET check that runs every ten minutes is a mailing list.

**Two real defects found on the first run** — see Known gaps below.

---

## Repository layout

```
config/sources/        source declarations (the registry)
src/
  pipelines/
    framework/         metadata-driven ingestion framework
    streaming/         bronze entry point, silver, gold, alerts
    customers/         customer silver ingestion
    ops/               event-log telemetry collector
  producer/            Kafka simulator: generators, fraud engine, producers
transform/             dbt project: staging, marts, snapshots
resources/             Asset Bundle: pipeline and job specs
scripts/               alert + dashboard provisioning (idempotent)
sql/
  ops/                 operational queries, monitoring detectors
  governance/          PII masking, grants, verification
tests/                 42 pytest tests
docs/                  architecture, challenges, governance, interview prep
```

---

## Documentation

| Document | Contents |
|---|---|
| [Setup](docs/setup.md) | Running this on a fresh machine, plus troubleshooting |
| [Engineering challenges](docs/engineering_challenges.md) | 17 real failures — error text, root cause, fix, interview angle — including one measured negative result |
| [Data governance](docs/data_governance.md) | PII masking, classification, access model, and the two leaks the verification found |
| [Observability & monitoring](docs/observability.md) | Event-log telemetry, detectors, alerting and runbook |
| [Performance optimization](docs/performance_optimization.md) | 19 optimizations in three honest tiers: measured, correct-but-unmeasurable, deliberately rejected |
| [Metadata-driven ingestion](docs/metadata_driven_ingestion.md) | Framework design, closure binding, batch vs streaming |
| [Interview preparation](docs/interview_preparation.md) | Scenario-driven Q&A: streaming, Kafka, dimensional modeling, dbt, system design |

The dbt lineage DAG is generated rather than committed (it is build output):

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

# The Go CLI, not the legacy Python databricks-cli -- `bundle` requires it.
# https://docs.databricks.com/dev-tools/cli/install.html
databricks configure --token --host https://<workspace>.cloud.databricks.com
```

Copy `.env.example` to `.env` and `src/producer/.env.example` to
`src/producer/.env`, then fill in values. Both are gitignored.

```bash
# Deploy pipelines and the orchestration job
databricks bundle validate -t dev
databricks bundle deploy   -t dev

# Governance (create the three account groups first -- see data_governance.md)
databricks sql -f sql/governance/01_pii_masking.sql
databricks sql -f sql/governance/02_grants.sql
databricks sql -f sql/governance/03_pii_verification.sql

# Monitoring
python scripts/create_alerts.py
python scripts/create_dashboard.py

# Produce transactions
cd src/producer && python upload_merchants.py
python producer_normal.py          # normal traffic
python producer_fraud_card.py      # watchlist-matching cards
```

---

## Known gaps

Stated deliberately — an accurate list is more useful than an impressive one.

**Found by this project's own monitoring:**

- **The `fraud_card_alert` watermark is 606 hours stale.** The stream-stream
  join stopped advancing event time on 18 June while continuing to report
  success on every run. Late-arriving records have been dropped silently since.
  Remediation needs a checkpoint reset via full refresh, pending confirmation
  the Kafka topic still holds the source data.
- **`shuffle.partitions: 16` does not apply to stateful operators.** They pin
  partition count into the checkpoint at creation; the `symmetricHashJoin` runs
  800. This contradicts a claim in
  [performance_optimization.md](docs/performance_optimization.md) — recorded
  here rather than quietly corrected there.

**Security:**

- **`bronze.transactions.value` retains PANs in clear** — 4,432 rows hold the
  raw Kafka payload with `card_number` inside JSON. A column mask cannot reach
  a substring. Real fix is tokenization at the producer.
- **Masks do not survive a Lakeflow full refresh.** A recreated table has no
  masks, silently. Re-run `01_pii_masking.sql` after any refresh; CHECK 6
  detects the gap.

**Correctness:**

- **Stream-static join snapshots a changing table.** `fraud_card_alert` reads
  `silver.customers` with `spark.read`. CDC fan-out is fixed, but customer
  attribute changes after stream start are still not reflected in alerts.
  Point-in-time attribution exists only in the dimensional layer.
- **Detection latency reads as ~6 hours** in `fct_alerts`, an artifact of
  triggered-mode runs against stale data rather than a real end-to-end SLA.

**Engineering:**

- **CI has never executed.** The workflow is committed but no pull request has
  ever been opened, and `dbt-build` is gated on `pull_request`. It also
  references a `ci` target that `transform/profiles.yml` does not define, so it
  would fail if run today.
- **`dev` and `prod` bundle targets write to the same catalog.** The variable is
  threaded correctly and set to `finguard` in both, so the isolation mechanism
  exists but is defeated.
- **Test coverage is 3 of 33 source files** — 42 tests covering config loading,
  bronze factory and the metrics collector. `fraud_engine.py` is 123 lines of
  seeded, deterministic logic and is untested.
- **No fraud-detection ground truth.** The producer generates fraud with known
  reasons and discards that label at the Kafka boundary, so precision and recall
  cannot be computed.
- **No backfill mechanism.** Reprocessing a bounded window is not expressible;
  the only primitive is full refresh.
- **No measured query-latency improvement.** Deliberately not claimed — at this
  data volume the workload is latency-dominated and a clustering benchmark
  showed warehouse warm-up rather than a real gain.

---

## Stack

Databricks Lakeflow Declarative Pipelines (`pyspark.pipelines`) · Unity Catalog ·
Delta Lake · Spark Structured Streaming · Auto Loader · Confluent Kafka ·
Neon Postgres (CDC source) · dbt · Databricks Asset Bundles · Python 3.13
