# Interview Preparation — FinGuard

Answers grounded in what this project actually does. Where something is not
built yet, that is stated rather than papered over. Claiming a capability you
cannot demonstrate is the fastest way to lose an interview; saying "not yet,
and here is why" is not.

**Rule for using this document:** never claim a number you have not measured.
This project runs at ~5 transactions/second against a 2X-Small serverless
warehouse. That is a portfolio scale, and saying so plainly is more credible
than an invented throughput figure.

---

## Part 1 — Project overview

### "Walk me through this project."

> FinGuard is a real-time card fraud detection platform on Databricks. It
> ingests transactions from Kafka, reference data from cloud storage, and
> customer master data from Postgres via CDC, then runs a medallion
> architecture — bronze, silver, gold — with stateful stream processing to
> raise fraud alerts.
>
> The part I would point at is the ingestion framework. Bronze tables are not
> hand-written per source; they are generated from YAML declarations. Adding a
> source is a config file, not code. On top of that there is an operational
> metadata layer that records what each pipeline update actually did, so
> configuration drift is detectable by query rather than by noticing missing
> data.

Keep it to about thirty seconds. Let them choose the thread to pull.

### "What is the actual scale?"

> Roughly five transactions per second from a simulator, on serverless
> compute. It is built to demonstrate the patterns, not the volume. The
> architecture — partitioning, watermarking, incremental ingestion — is what
> scales; the current load is deliberately small so the whole thing runs at
> portfolio cost.

Answering this honestly and immediately builds more trust than any number.

---

## Part 2 — Architecture

### "Why medallion?"

Bronze preserves raw payloads with Kafka envelope metadata (topic, partition,
offset, timestamp) so any offset range can be replayed and any row traced to
its source. Silver applies schema and quality rules. Gold holds business
aggregates and alerts.

The concrete payoff: when the JSON parse in silver had a bug, bronze still had
every raw message. Fix the code, rebuild silver, no data loss. If parsing
happened at ingest, that data would be gone.

### "Why is customers ingested differently from transactions?"

Different data, different semantics:

| Source | Mechanism | Why |
|---|---|---|
| Transactions | Kafka | High-frequency immutable events; need ordering and replay |
| Fraud watchlist | Auto Loader | Files arriving in a volume; incremental discovery |
| Merchants | Auto Loader (CSV) | Reference data delivered as file drops |
| Customers | Lakeflow managed CDC | Mutable rows in Postgres; need change capture, not snapshots |

Customers is the interesting one. Attributes like `risk_score` and
`customer_segment` change over time. The pipeline uses `customer_id` as the
primary key and `update_timestamp` as a cursor, so changes are re-ingested
rather than lost — which is the prerequisite for building SCD2 history.

### "Why not put everything on Kafka?"

Kafka is right for events and wrong for reference data. A merchant catalogue of
200 rows refreshed daily does not need a topic, partitions, or consumer
offsets. Conversely, polling a REST endpoint for transactions would throw away
ordering, replay, and exactly-once — the properties that make streaming worth
the complexity.

The framework supports a REST reader for reference data specifically, and
transactions deliberately stay on Kafka.

---

## Part 3 — The metadata-driven framework

This is the strongest part of the project. Expect follow-ups.

### "Explain the metadata-driven ingestion."

> Each bronze source is declared in a YAML file: ingestion type, target table,
> column list, type-specific options. At pipeline start, the framework loads
> every config, validates it, and generates one table definition per source.
> Adding a source is a config file with no Python change. Adding a new source
> *type* is one reader function plus one dispatch entry.

### "Show me the tricky part."

The closure binding. `@dp.table` registers a *function* that the runtime calls
later. Generating N tables in a loop is the classic trap:

```python
for cfg in configs:                      # WRONG
    @dp.table(name=cfg.target)
    def _t():
        return reader(spark, dbutils, cfg.ingestion)
```

Python closures capture the variable, not its value. By the time the runtime
invokes those functions the loop has finished, so every table reads the *last*
config. You get three tables all ingesting the same source.

The fix is binding per call by passing config as a parameter:

```python
def build_bronze_table(cfg, spark, dbutils):
    reader = get_reader(cfg.ingestion_type)

    @dp.table(name=cfg.target, comment=cfg.comment)
    def _bronze_table():
        return _project(reader(spark, dbutils, cfg.ingestion), cfg)
```

I tested this explicitly with stubbed readers, asserting each registered table
reads its own source.

### "Where does metadata-driven stop being a good idea?"

> At silver. Bronze is uniform — read, project, timestamp — so generating it
> from config removes duplication. Silver is genuinely per-source: the fraud
> watchlist needs specific fields uppercased and a `dd-MMM-yyyy HH:mm:ss`
> timestamp parsed; transactions need a JSON envelope parsed against an
> explicit schema. Encoding that in YAML means inventing a transformation DSL
> that is strictly worse than SQL.
>
> So the boundary is deliberate: config-driven where the logic is uniform,
> explicit where it is business logic.

Knowing where a pattern *stops* applying is what separates having used it from
having understood it.

### "YAML or a database for the config?"

> Both, at different layers. Structural definitions — what sources exist, their
> type and target — live in YAML in git, because they need diffs, code review
> and environment promotion. Operational state — what ran, when, with what
> outcome — lives in Delta tables, because it is written at runtime and needs
> to be queryable.
>
> Putting source definitions in a table loses version history: someone runs an
> UPDATE and production changes with no diff. Putting run history in YAML is
> impossible; the pipeline would have to commit to git on every update.

### "What does the operational layer buy you?"

The query that justifies it — declared in config versus actually built:

```sql
SELECT r.source_name,
       CASE WHEN t.table_name IS NULL THEN 'MISSING' ELSE 'EXISTS' END
FROM finguard.ops.source_registry r
LEFT JOIN system.information_schema.tables t
       ON t.table_schema = 'bronze'
      AND t.table_name = split_part(r.target_table, '.', 3)
```

This caught a real gap: `merchants` was configured, reviewed and merged, but no
table existed because the pipeline had not been repointed after a refactor.
Grepping YAML cannot detect that.

---

## Part 4 — Streaming

### "Explain watermarking."

A watermark tells Spark how late an event may arrive before it is dropped,
which bounds the state the engine must retain. Without one, a stream-stream
join accumulates state forever.

In `fraud_card_alert.py` both sides carry a five-minute watermark — the
transaction stream on `transaction_timestamp`, the watchlist on
`effective_from`. Five minutes is a trade: longer catches more genuinely late
events, shorter bounds memory. For fraud, alerting quickly matters more than
catching every straggler.

### "What happens to data later than the watermark?"

It is dropped from the stateful operation. It still lands in bronze — bronze is
append-only with no watermark — so it is recoverable by reprocessing. That is a
concrete reason to keep raw data rather than parse at ingest.

### "Stream-static versus stream-stream join?"

Stream-static joins a stream to a table read once. Stream-stream joins two
streams and requires watermarks on both so state can be evicted.

**There is a bug in this project worth discussing.** `fraud_card_alert.py`
does:

```python
customers = spark.read.table("finguard.silver.customers")
```

`finguard.silver.customers` is itself a continuously updating streaming table.
`spark.read` snapshots it at plan time, so customer attributes that change
after the stream starts are invisible to alerts. A customer whose risk score
rises is still evaluated against the old profile — for a fraud system, that is
wrong, not just stale.

The proper fix ties into SCD2: alerts should evaluate against the profile
current *at transaction time*, which needs dimensional history plus a
point-in-time join.

Volunteering a known bug in your own code, with the reasoning, reads as
stronger than claiming everything works.

---

## Part 5 — Failures and debugging

Draw on `docs/engineering_challenges.md`. Every item there really happened.

### "Tell me about a bug that was hard to find."

> Fraud alert emails were never sending, and nothing reported an error. The
> pipeline showed success on every batch.
>
> The cause was `dbutils().secrets().get(...)` — calling an object and an
> attribute as functions. That raises `TypeError`, but it sat inside a bare
> `except Exception` at module scope, so the exception was swallowed, the
> password became `None`, and every batch logged "key not available" and
> returned early.
>
> The lesson was about the `except`, not the typo. A pipeline that reports
> success while doing nothing is worse than one that crashes. Credential
> retrieval should fail loudly.

### "Tell me about a deployment that went wrong."

> Refactoring bronze to the config-driven framework, I hit six consecutive
> failed pipeline updates with four distinct causes.
>
> First, `NameError: __file__ is not defined` — Lakeflow `exec()`s pipeline
> files instead of importing them, so `__file__` does not exist. Worked
> locally, failed deployed.
>
> Second, `ImportError: cannot import name 'build_all' (unknown location)` —
> my sync tool uploaded modules as NOTEBOOK objects, which Python cannot
> import. And overwriting does not change an object's type; they had to be
> deleted and re-uploaded as files.
>
> Third, `Found duplicate table` — the sync only uploaded, never deleted, so
> superseded files lingered and collided with the new definitions.
>
> Fourth, a Kafka `SaslAuthenticationException` — a real credential problem,
> not code.
>
> The useful signal was that each failure was *different*. Failing the same way
> repeatedly means you are not learning; failing differently means you are
> making progress. The underlying lesson was that one-way file sync accumulates
> state, which is the argument for declarative deployment with Asset Bundles.

### "How do you handle a failure in one source?"

Registration failures are isolated per source. One malformed config records a
`FAILED` audit row and the other sources still build — losing every table
because one config is bad is a worse outcome. The ops writes themselves are
wrapped, because observability must never be the reason ingestion fails.

The real run demonstrated this: when Kafka auth failed, `merchants` and
`fraud_watchlist` completed normally while the eight flows genuinely dependent
on transactions were skipped. Blast radius bounded by dependency, not process.

---

## Part 6 — Questions to expect on gaps

Be straight about these.

### "Do you have tests?"

> Not a formal suite. I have verification scripts for the framework —
> validation rejection cases, closure binding, and the REST reader tested
> against a local HTTP server covering pagination, retry on 5xx, and auth
> failure. Turning those into pytest with CI is the next step.

### "How do you deploy?"

> Currently a Python script that pushes files to the workspace via the API.
> That is a weakness and I know why: it only uploads, so deleted files persist
> remotely, which caused the duplicate-table failure. Asset Bundles reconcile
> state rather than accumulating it, which is the correct fix.

### "What about data quality?"

> Lakeflow expectations in silver — `expect_or_drop` on required identifiers,
> `expect` on amount. Rows failing a drop rule are removed, and violations are
> recorded in the event log.
>
> What is missing is a quarantine table. Dropped rows currently disappear;
> they should be routed somewhere inspectable, because a spike in drops is a
> signal about upstream, not just noise to discard.

### "How would you scale this to 50 sources?"

> The framework already handles onboarding — 50 sources is 50 YAML files. Three
> things would need to change: per-source pipeline splitting so one failure has
> smaller blast radius; the ops layer needs alerting rather than just queries,
> since nobody watches a dashboard for 50 sources; and Asset Bundles for
> environment promotion.

---

## Part 7 — Things to avoid saying

**Do not claim throughput you have not measured.** "Millions of transactions
per second" against a 5 TPS simulator is checkable and will be checked.

**Do not say "production-ready."** Say what is built, what is not, and why.

**Do not hide the known bug.** The stream-static join is a better talking point
than a claim of perfection.

**Do not overstate the dbt work** until it exists. Marts, SCD2 and dimensional
modelling are planned, not built.

---

## Part 8 — Quick reference

| Concept | Where it lives |
|---|---|
| Medallion architecture | `src/pipelines/streaming/{bronze,silver,gold}` |
| Metadata-driven ingestion | `src/pipelines/framework/` |
| Closure binding | `bronze_factory.py:build_bronze_table` |
| Reader dispatch | `readers.py:READERS` |
| Config validation | `source_config.py:_validate` |
| Operational metadata | `registry.py`, `sql/ops/health_checks.sql` |
| Stream-stream join + watermarks | `gold/fraud_card_alert.py` |
| Tumbling / sliding windows | `gold/transaciton_count_by_minute*.py` |
| Quality expectations | `silver/fingurad_silver.py` |
| CDC ingestion | `resources/fingurad_ingestion.json` |
| Fraud scoring rules | `src/producer/fraud_engine.py` |

**Numbers you can defend:** 4 ingestion patterns; 4 bronze tables; 200
merchants (19 high-risk, 10 blacklisted); ~5 transactions/second; 6 Kafka
partitions; 5-minute watermarks; 8 weighted fraud signals.
