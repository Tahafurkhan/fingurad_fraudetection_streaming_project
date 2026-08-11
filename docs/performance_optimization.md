# Performance Optimization — Spark, Delta & Databricks

Every optimization applied to this project, with what it does, why it applies
here (or does not), and what was measured. Measurements were taken on
2026-08-11 against the live Unity Catalog tables.

**The rule this document follows:** an optimization is only claimed as a win if
it was measured. Where a technique is correct in principle but cannot be
demonstrated at this data volume, it says so. Where a technique was measured
and found not to help, that is recorded too.

That rule is not modesty. This project holds ~4,400 transactions. Most of what
matters at 4 billion rows is unmeasurable at 4,400, and an interviewer who
hears "liquid clustering gave me a 40% speedup" on a 129KB table knows
immediately that the number is noise.

---

## The three tiers

| Tier | Meaning | Count |
|---|---|---|
| **A — Measured** | Demonstrable effect at this data volume, with before/after numbers | 5 |
| **B — Correct, unmeasurable here** | Right setting for the access pattern; effect appears at scale | 9 |
| **C — Deliberately rejected** | Would be wrong for this workload, and why | 5 |

Tier C is the one that matters most in an interview. Applying every
optimization you know is not engineering; knowing which ones do not apply is.

---

## Headline result

The single measurable win across the whole project: **small-file compaction**.

```
                                     files          bytes
bronze.transactions            29 ->  1     377,976 -> 231,411   (-39%)
bronze.fraud_watchlist         19 ->  1      96,927 ->   9,328   (-90%)
bronze.merchants                2 ->  1      10,689 ->   6,890   (-36%)
silver.transactions            31 ->  1     347,290 -> 124,385   (-64%)
silver.fraud_watchlist         17 ->  1      89,414 ->  10,231   (-89%)
gold.fraud_card_alert           8 ->  1      99,252 ->  24,752   (-75%)
--------------------------------------------------------------------
TOTAL                         117 -> 17   1,372,550 -> 760,366   (-44.6%)
```

**85% fewer files. 44.6% less storage.** Same data, same row counts.

The storage reduction is the more interesting half and the part most people do
not predict. Compaction is usually explained as "fewer file handles to open,"
which is a read-path argument. But Parquet compresses *within* a row group: 19
small files each carry their own dictionary, their own column statistics, and
their own footer. Merging them into one file lets the dictionary encode across
all 93 rows at once. That is where 90% went.

---

# Tier A — Measured

## A1. Auto-compaction and optimized writes

**What it does.** `optimizeWrite` inserts a shuffle before writing so each
partition emits one appropriately-sized file instead of one file per task.
`autoCompact` runs a compaction pass after a write that leaves too many small
files behind.

**Why it applies here.** This is the defining failure mode of streaming
ingestion. Every micro-batch commits its own files regardless of how little
data it carries. `bronze.fraud_watchlist` held **19 files for 93 rows** — about
5KB per file, against a Delta target measured in hundreds of megabytes.

**Where it is set.** Framework default for every generated bronze table
([source_config.py](../src/pipelines/framework/source_config.py)), explicit on
each silver and gold table, and project-wide for the dbt marts
([dbt_project.yml](../transform/dbt_project.yml)).

```python
DEFAULT_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.tuneFileSizesForRewrites": "true",
}
```

**Measured.** See the headline table. 117 files → 17, 44.6% less storage.

**The subtlety worth knowing.** Setting these properties did **not** compact
the existing files. `autoCompact` acts on new writes only; historical
fragmentation needs an explicit `OPTIMIZE`. Verified directly — after applying
the properties, `bronze.fraud_watchlist` still reported 19 files. Turning on
auto-compaction and assuming yesterday's small files disappear is a common and
wrong assumption.

**Interview angle.** *"How do you handle the small-file problem in streaming?"*
The complete answer has three parts: prevent new ones (`optimizeWrite`), clean
up as you go (`autoCompact`), and fix the existing backlog (`OPTIMIZE`). Most
candidates give only the first two.

---

## A2. Shuffle partition sizing

**What it does.** `spark.sql.shuffle.partitions` sets how many partitions a
shuffle produces. The default is **200**.

**Why it applies here.** 200 partitions for 4,400 rows is **22 rows per
partition**. Spark schedules 200 tasks, each doing essentially nothing, and
task scheduling overhead dominates actual work. It also feeds directly into the
small-file problem: each shuffle partition can produce its own output file.

**Where it is set.** [resources/finguard_pipeline.yml](../resources/finguard_pipeline.yml)

```yaml
spark.sql.shuffle.partitions: "16"   # main pipeline
spark.sql.shuffle.partitions: "8"    # customers pipeline
```

**Why 16 and not 200 or 1.** Sized for this data volume rather than copied from
a guide. The general rule is to target ~128MB per partition, not to pick a
fixed number — at production volume this should rise substantially. A value
chosen once and never revisited is the same mistake as leaving it at 200.

**Interview angle.** *"What's the first thing you'd check on a slow small Spark
job?"* This is usually it, and the reasoning — 200 is sized for a large cluster
and large data — matters more than the number.

---

## A3. Column pruning before shuffle

**What it does.** Naming columns at read time so Delta's columnar layout skips
the rest at the scan, before any shuffle moves them across the network.

**Why it applies here.** `silver.customers` has 22 columns. The gold alert
models use six. The window function that deduplicates customers shuffles by
`customer_id` — so every unread column is also a column not moved over the
network during that shuffle.

**Where it is set.** [fraud_card_alert.py](../src/pipelines/streaming/gold/fraud_card_alert.py),
[high_value_transactions_alert.py](../src/pipelines/streaming/gold/high_value_transactions_alert.py),
[fingurad_silver.py](../src/pipelines/streaming/silver/fingurad_silver.py)

```python
spark.read.table("finguard.silver.customers").select(
    "customer_id", "first_name", "last_name",
    "email", "transaction_limit", "silver_ingestion_timestamp",
)
```

**Honest caveat.** Catalyst often infers this pruning already. Being explicit
makes it independent of whether the optimizer sees through the `from_json`
call, and documents the dependency: adding a column to bronze does not silently
make this model start reading it.

---

## A4. Broadcast join for the customer dimension

**What it does.** `F.broadcast()` ships the small side of a join to every
executor, turning a shuffle join into a local hash lookup.

**Why it applies here.** Without it, Spark shuffles **both** sides by
`customer_id` on every micro-batch — the transaction stream *and* the whole
customer table. In a micro-batch pipeline that shuffle is paid again on every
trigger, not once.

1,002 customers reduced to one row each, six columns — comfortably under the
10MB default `autoBroadcastJoinThreshold`.

**Where it is set.** Both gold alert tables.

```python
customers = F.broadcast(latest_customer)
```

**When this becomes wrong — and it will.** Broadcasting is a bet on size. The
dataset is collected to the driver and shipped to every executor, so a
dimension that outgrows the threshold turns this hint into an OOM rather than a
speedup. At a few million customers this must be removed. **A broadcast hint is
a statement about size that stops being true silently.**

**Interview angle.** *"When would you broadcast?"* The complete answer includes
when you would stop. Candidates who only know that broadcasting is fast get
caught by the follow-up.

---

## A5. Merge predicates on incremental models

**What it does.** `incremental_predicates` bounds the *target* side of a dbt
`MERGE`.

**Why it applies here.** Without it, `MERGE` scans the entire target table to
find matches for incoming rows. The batch stays the same size but the merge
gets slower every run as history accumulates — a pipeline that degrades
silently over months.

**Where it is set.** [fct_transactions.sql](../transform/models/marts/fct_transactions.sql)

```python
incremental_predicates=[
    "DBT_INTERNAL_DEST.transaction_timestamp >= "
    "current_timestamp() - interval 4 days"
]
```

**Why 4 days.** The source filter uses a 3-day lookback for late-arriving data;
the target predicate must be at least as wide or the merge would miss rows the
source offers. One day of margin.

**The trap.** If a genuinely old row arrives outside this window, the merge will
not find its existing match and will insert a duplicate. The predicate is a
performance/correctness trade, and it is only safe because the uniqueness test
on `transaction_id` would catch a violation.

---

# Tier B — Correct, unmeasurable at this volume

These are configured correctly and verified as *applied*. Their effect requires
more data than this project has.

## B1. Liquid clustering

**Applied to:** `silver.transactions` (`customer_id, transaction_timestamp`),
`silver.customers` (`customer_id`), `bronze.transactions` (`timestamp,
partition`), both gold alert tables, `marts.fct_transactions`,
`marts.fct_alerts`.

**Verified applied:**
```
fct_transactions  clusteringColumns = ["customer_id","transaction_date"]
fct_alerts        clusteringColumns = ["customer_id","alert_timestamp"]
```

**Why not measured.** Clustering works by letting a query skip files. After
compaction these tables are **one file each**. There is nothing to skip.

This was tested rigorously rather than assumed. An earlier benchmark measured
an 8–13% apparent improvement on `fct_transactions`; `DESCRIBE DETAIL` showed
the table was a single 129KB file, so the difference was warehouse warm-up. A
5-million-row synthetic table was built to get a fair test and compacted to one
file as well. **No number is claimed.**

**Why clustering rather than partitioning.** Partitioning by date at this
volume would create a directory holding the entire table. At scale it produces
the classic small-file explosion — one tiny file per partition per micro-batch.
Clustering keys can also be redefined later without rewriting history;
partition columns cannot.

**Key selection is constrained by the layer.** `bronze.transactions` clusters on
`timestamp, partition` rather than `customer_id` because bronze deliberately
keeps the Kafka payload as an unparsed string — `customer_id` does not exist as
a column until silver parses the JSON. The config validator enforces this:

```
ValueError: transactions.yaml: cluster_by references columns not in this
source's column list: ['customer_id']
```

That error was produced by this project's own validation during development,
not hypothesised.

---

## B2. The stats/clustering coupling — two real failures

Liquid clustering requires every clustering column to have statistics, and
Delta collects stats only for the first N columns (`delta.dataSkippingNumIndexedCols`,
default 32). **Lowering that budget silently constrains which columns can be
clustered.**

This broke the build twice.

**Failure 1 — `fct_alerts`.** Clustering on `alert_timestamp`, a ~60-column
table:

```
[DELTA_CLUSTERING_COLUMN_MISSING_STATS] Liquid clustering requires clustering
columns to have stats. Couldn't find clustering column(s) 'alert_timestamp'
in stats schema
```

`alert_timestamp` sits at roughly column 52, past the default 32.

**Failure 2 — `silver.transactions`.** Self-inflicted. I set
`dataSkippingNumIndexedCols: 12` as an optimization — stats over long strings
are write cost with no read benefit — and that excluded `transaction_timestamp`
at column 14, a column the table clusters on.

**Fix.** Raise the budget to cover the clustering keys: 64 for `fct_alerts`, 16
for `silver.transactions`.

**The alternative, and why it was rejected.** Reordering the select to move
clustering keys into the first 32 columns is free but makes column order
load-bearing — a trap for the next person who reorders for readability.

**Interview angle.** *"What's the catch with liquid clustering?"* This coupling
is a genuinely non-obvious one, and having hit it from both directions — a
column too far right, and a stats budget set too tight — is worth more than
reciting that clustering exists.

---

## B3. Change Data Feed

**Applied to:** all bronze and silver tables.

Makes row-level changes readable without diffing snapshots, so a downstream
consumer can read only what changed. It matters most on `silver.customers`,
which already represents a change stream and feeds SCD2 snapshots.

**It is not free.** Change files are written alongside data on every commit —
storage and write cost for a capability nothing currently reads in this
project. Enabled deliberately because retrofitting it requires a rewrite, and
the consumers are planned.

---

## B4. Bounded micro-batches

**`maxOffsetsPerTrigger: 50000`** (Kafka),
**`maxFilesPerTrigger: 100`** (Auto Loader).

At ~5 records/second the steady-state batch is far below these caps, so they
never bind in normal operation. They exist for the first run after a checkpoint
reset: with `startingOffsets: earliest` and no limit, Spark plans **one batch
over the entire retention window**. The symptom is a job that appears hung and
then dies on memory — which reads like a cluster sizing problem and is actually
an unbounded first batch.

**Insurance against replay, not a throughput throttle.**

---

## B5. RocksDB state store

```yaml
spark.sql.streaming.stateStore.providerClass: >-
  org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
```

The default keeps all streaming state on the JVM heap, so state competes with
execution memory and large state appears as GC pressure then OOM. RocksDB
spills to local disk and keeps only hot data in memory.

This project has three stateful operators — the stream-stream join in
`fraud_card_alert`, `dropDuplicatesWithinWatermark` in `merchants_silver`, and
the windowed aggregations. State is watermark-bounded but not trivial.

**Note:** changing the state store provider requires a fresh checkpoint. It
cannot be swapped on a running query.

---

## B6. Adaptive Query Execution

```yaml
spark.sql.adaptive.enabled: "true"
spark.sql.adaptive.coalescePartitions.enabled: "true"
spark.sql.adaptive.skewJoin.enabled: "true"
```

On by default in current runtimes — declared explicitly so the intent is
visible in review rather than inherited.

`coalescePartitions` merges small post-shuffle partitions at runtime, which is
the safety net for the shuffle-partition setting being wrong in either
direction. `skewJoin` splits oversized partitions from hot keys — directly
relevant to fraud data, where a high-volume merchant or a card under attack
produces exactly that skew.

**Do not claim credit for enabling AQE.** It is the default. Explain what it
does instead.

---

## B7. Auto Loader schema hints

```yaml
schema_hints: "merchant_id STRING, is_blacklisted BOOLEAN"
```

Type inference is right for the sample and wrong for the data. `merchant_id`
inferred as an integer breaks the moment an ID arrives with a leading zero;
`is_blacklisted` inferred from a file where every value is `"false"` can come
back as a string. Hints pin the columns that matter and leave inference on for
descriptive fields where it is harmless.

---

## B8. Statistics column budget

Stats drive file skipping and cost write time to compute. The default of 32
spends that budget on whatever columns happen to come first — including wide
free-text columns like `reason_description`, where min/max is write cost with
zero read benefit.

Set to 16 on `silver.transactions`, 12 on `silver.customers`, 64 on
`fct_alerts`. Each value covers that table's clustering keys and predicate
columns — see **B2** for what happens when it does not.

---

## B9. Deletion vectors

Already enabled. Deletes and updates write a marker rather than rewriting the
whole file. Worth knowing rather than claiming — it was on by default in this
runtime.

---

# Tier C — Deliberately rejected

## C1. Partitioning by date

**Rejected.** 4,400 rows over ~1 day means a partition holding the entire
table, or — at higher volume — one small file per partition per micro-batch.
This is the over-partitioning failure mode: directory count grows while no file
is large enough to be worth pruning.

Liquid clustering supersedes it, and its keys can change without rewriting
history.

**Where partitioning still wins:** a low-cardinality column with stable query
patterns where you need physical file layout guarantees for an external
consumer.

---

## C2. Clustering the small dimensions

**Rejected on** `silver.merchants` (200 rows, one 6KB file),
`silver.fraud_watchlist` (93 rows), `dim_merchant`, `dim_customer`.

Clustering prunes files. One file prunes nothing. Declaring keys would add
metadata and clustering maintenance for exactly zero benefit.

The optimization that *does* apply to a table this size is the opposite one:
small enough to **broadcast**, so joins against it need no shuffle at all.

**Small dimensions get broadcast; large facts get clustered.** Applying the
fact-table optimization to a 200-row dimension is a common and expensive
mistake.

---

## C3. Z-ORDER

**Rejected.** Superseded by liquid clustering for new tables. Z-ORDER requires
a full rewrite (`OPTIMIZE ... ZORDER BY`) whenever the layout needs refreshing,
and the columns cannot be changed without another full rewrite.

Z-ORDER remains relevant for existing tables not yet converted to liquid
clustering — a migration consideration, not a design choice for new tables.

---

## C4. Caching streaming DataFrames

**Rejected.** `.cache()` / `.persist()` on a streaming DataFrame is meaningless
— each micro-batch processes new data, so there is nothing to reuse. It is a
common wrong answer in interviews because caching is genuinely useful in batch
Spark where a DataFrame is referenced multiple times in one job.

---

## C5. Query-time benchmarking at this scale

**Attempted, and rejected as a measurement method.**

Three queries of different shapes, five runs each, after warming the warehouse:

```
count silver.transactions      median 1649 ms
filter by customer             median 1641 ms
join alerts to customers       median 1557 ms
```

All three land at ~1.6s regardless of shape. A full table count, a filtered
aggregate, and a two-table join should not take the same time — this is
serverless round-trip latency, not data processing. The queries are so small
that the data work is invisible next to the API overhead.

**Conclusion:** query timing cannot demonstrate anything at this data volume.
File counts and byte sizes are objective, reproducible, and unaffected by
warehouse state. That is why every Tier A claim in this document is a physical
measurement rather than a timing.

---

# The bug this work exposed

Worth its own section because it is the best interview story here.

**dbt silently ignored every optimization config.**

The first implementation used `cluster_by` and `table_properties`. dbt parsed
them cleanly, wrote them into `target/manifest.json`, and reported `PASS=63
WARN=0 ERROR=0`. Everything looked correct.

`DESCRIBE DETAIL` told a different story:

```
fct_transactions: files=1 cluster=[]
    props={}
```

**Nothing had been applied.** The cause: dbt-databricks reads
`liquid_clustered_by` and `tblproperties`. It does not read `cluster_by` or
`table_properties` — and **dbt accepts unknown config keys without complaint**,
storing them in the manifest and applying nothing.

The fix was a rename. The lesson is the verification method: the run result and
the manifest both said success. Only checking the warehouse revealed the truth.

```python
# WRONG -- parses, appears in manifest, applies nothing, run succeeds
config(cluster_by=['customer_id'], table_properties={...})

# RIGHT
config(liquid_clustered_by=['customer_id'], tblproperties={...})
```

**Interview angle.** *"How do you know an optimization actually took effect?"*
The answer is not "the job succeeded." Every optimization in this document was
verified against `DESCRIBE DETAIL` and `SHOW TBLPROPERTIES` after deployment,
because this exact failure — configuration accepted, silently discarded, run
green — is invisible any other way.

It is the same class of bug as the swallowed `TypeError` that stopped fraud
emails sending for weeks, and the `NOT (rules)` NULL predicate that made
quarantined rows vanish. **Silent success is this project's recurring enemy.**

---

# Verification method

Every claim reproduced by:

1. **Baseline** — `DESCRIBE DETAIL` + `SHOW TBLPROPERTIES` for all 14 tables,
   recorded to JSON before any change.
2. **Apply** — config in code, deployed via pipeline update and `dbt build
   --full-refresh --no-partial-parse`.
3. **Verify applied** — re-query the warehouse. Never trust the run result.
4. **Measure** — re-capture layout, diff against baseline.
5. **Report honestly** — including what did not move.

Guarded by 25 pytest tests, 8 covering the optimization config surface
specifically — including one that catches a real bug: YAML parses `false` into
a Python bool, and `str(False)` is `"False"`, which Delta does not recognise as
a boolean. The property would be set to an unparseable value and the setting
would silently not take effect.

---

# Quick reference

| Optimization | Tier | Where |
|---|---|---|
| optimizeWrite / autoCompact | A | All layers |
| Shuffle partitions (200→16) | A | [finguard_pipeline.yml](../resources/finguard_pipeline.yml) |
| Column pruning | A | Gold joins, silver parse |
| Broadcast join | A | Both gold alert tables |
| Merge predicates | A | [fct_transactions.sql](../transform/models/marts/fct_transactions.sql) |
| Liquid clustering | B | Silver, gold, marts |
| Stats column budget | B | silver.transactions (16), fct_alerts (64) |
| Change Data Feed | B | Bronze + silver |
| maxOffsetsPerTrigger | B | [transactions.yaml](../config/sources/transactions.yaml) |
| maxFilesPerTrigger | B | Auto Loader sources |
| RocksDB state store | B | Pipeline config |
| AQE + skew join | B | Pipeline config |
| Schema hints | B | [merchants.yaml](../config/sources/merchants.yaml) |
| Date partitioning | C | Rejected — over-partitioning |
| Clustering small dims | C | Rejected — nothing to prune |
| Z-ORDER | C | Rejected — superseded |
| Caching streams | C | Rejected — meaningless |
| Query benchmarking | C | Rejected — latency-dominated |

### Numbers you can defend

| Metric | Value |
|---|---|
| Total files before → after | 117 → 17 (**-85%**) |
| Total bytes before → after | 1,372,550 → 760,366 (**-44.6%**) |
| Largest single reduction | bronze.fraud_watchlist, 19 files → 1 (-90% bytes) |
| Shuffle partitions | 200 → 16 |
| Broadcast side | 1,002 rows × 6 columns |
| Tables with clustering applied | 7 |
| pytest tests | 25 passing (8 on optimization config) |
| dbt nodes | 63 passing |
| Query timing improvement | **Not claimed** — latency-dominated at this scale |

---

## Sources

- [Use liquid clustering for tables — Databricks](https://docs.databricks.com/aws/en/tables/clustering)
- [Debunking 8 data layout myths: why Liquid Clustering outperforms partitioning — Databricks](https://www.databricks.com/blog/debunking-8-data-layout-myths-why-liquid-clustering-outperforms-partitioning)
- [How to Choose Between Liquid Clustering and Partitioning with Z-Order — Canadian Data Guy](https://www.canadiandataguy.com/p/optimizing-delta-lake-tables-liquid)
- [Optimize data file layout — Databricks](https://docs.databricks.com/aws/en/delta/optimize)
- [Configure Structured Streaming trigger intervals — Databricks](https://docs.databricks.com/aws/en/structured-streaming/triggers)
- [Configure Auto Loader for production workloads — Databricks](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/production)
- [What is stateful streaming? — Databricks](https://docs.databricks.com/aws/en/structured-streaming/stateful-streaming)
- [Best Practices for Streaming in Production — Databricks Blog](https://www.databricks.com/blog/streaming-production-collected-best-practices)
- [Adaptive Query Execution — Apache Spark](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- [dbt-databricks configurations — dbt Developer Hub](https://docs.getdbt.com/reference/resource-configs/databricks-configs)
