# Interview Preparation — FinGuard

A scenario-driven preparation guide built on what this project actually does.
Every code reference is real, every number is measured, and every gap is
stated rather than papered over.

**How interviewers actually score you.** Across published rubrics the pattern
is consistent: candidates are rejected far more often for *how* they reason
than for what they know. The modeling round is the loop-decider — it appears in
roughly a third of all DE loops and more than half at senior level — and the
single most common rejection reason is failing to state the grain before
drawing tables. In behavioural rounds, the biggest disqualifier is not a weak
story; it is saying "we" so consistently that the interviewer cannot tell what
*you* did.

**The one rule for this document.** Never claim a number you have not
measured. This project runs ~5 transactions/second against a 2X-Small
serverless warehouse. Saying that plainly is more credible than an invented
throughput figure, and an interviewer who catches one inflated number will
discount everything else you said.

---

## Contents

| Part | Focus |
|---|---|
| 1 | The 60-second walkthrough and scale question |
| 2 | Architecture: why each decision was made |
| 3 | Metadata-driven ingestion (strongest section) |
| 4 | Streaming: watermarks, joins, state |
| 5 | Kafka scenarios (10 verbatim production questions) |
| 6 | Dimensional modeling (the round that gates offers) |
| 7 | dbt: incremental, snapshots, the traps |
| 8 | System design: fraud detection at real scale |
| 9 | Debugging war stories (STAR format) |
| 10 | Gaps — answering honestly about what is missing |
| 11 | The trap table |
| 12 | Quick reference |

---

## Part 1 — Opening

### "Walk me through this project."

Keep it to thirty seconds. Give them threads to pull, do not pull them
yourself.

> FinGuard is a real-time card fraud detection platform on Databricks. It
> ingests transactions from Kafka, reference data from cloud storage, and
> customer master data from Postgres via CDC, then runs a medallion
> architecture with stateful stream processing to raise fraud alerts, and a
> dbt dimensional layer on top for analysis.
>
> The two parts I would point at: bronze tables are generated from YAML
> declarations rather than hand-written per source, so adding a source is a
> config file and not code. And there is a deliberate boundary between the
> streaming layer and the dbt layer — streaming owns detection, dbt owns
> historical analysis — which I can explain if it is useful.

That last sentence is bait. The boundary question is where the strongest
material lives (Part 8).

### "What is the actual scale?"

> Roughly five transactions per second from a simulator, on serverless
> compute. It is built to demonstrate the patterns, not the volume. About
> 4,400 transactions, 1,002 customers, 200 merchants end to end.
>
> The architecture is what scales — watermarking, incremental ingestion,
> partition-aware replay. The load is deliberately small so the whole thing
> runs at portfolio cost. I would rather show you a correct 5 TPS pipeline
> than claim a number I never measured.

Answering this immediately and honestly buys credibility for everything after
it. Candidates who inflate here get probed for the rest of the interview.

### "What would you do differently if you started over?"

> Three things. I would use Databricks Asset Bundles from day one instead of a
> file-sync script — my sync only uploaded and never deleted, which caused a
> duplicate-table failure that cost me an afternoon. I would put secret
> scanning in CI before the first commit rather than after leaking
> credentials. And I would write the closure-binding test before writing the
> loop that generates tables, because that bug is invisible until it is in
> production.

Naming a real, specific mistake reads as far stronger than "I'd add more
tests."

---

## Part 2 — Architecture

### "Why medallion?"

Bronze preserves raw payloads with the full Kafka envelope — topic, partition,
offset, timestamp — so any offset range can be replayed and any row traced to
source. Silver applies schema and quality rules. Gold holds alerts and
aggregates.

The concrete payoff, from this project: when the JSON parse in silver had a
bug, bronze still had every raw message. Fix the code, rebuild silver, no data
loss. If parsing happened at ingest, that data would be gone permanently.

**The follow-up to be ready for — "isn't bronze just a waste of storage?"**

> It is storage traded for recoverability, and the trade is usually
> overwhelmingly worth it. Raw JSON in a compressed Delta table is cheap;
> re-requesting a week of history from an upstream you do not control is
> sometimes impossible. Kafka retention is finite — mine is set to a window,
> not forever — so once a message ages out of the topic, bronze is the only
> copy. The question is not whether bronze costs storage, it is what your
> recovery story is without it.

### "Why is customers ingested differently from transactions?"

Different data, different semantics:

| Source | Mechanism | Why this and not something else |
|---|---|---|
| Transactions | Kafka | High-frequency immutable events; need ordering, replay, offset-level recovery |
| Fraud watchlist | Auto Loader (JSON) | Files arriving in a volume; need incremental discovery without re-listing |
| Merchants | Auto Loader (CSV) | Reference data delivered as file drops |
| Customers | Lakeflow managed CDC | *Mutable rows* in Postgres; need change capture, not snapshots |

Customers is the interesting one and the one they will probe. Attributes like
`risk_score`, `customer_segment` and `transaction_limit` change over time. The
pipeline uses `customer_id` as the primary key and `update_timestamp` as the
CDC cursor, so changes are re-ingested rather than overwritten — which is the
prerequisite for SCD2 history downstream.

**"Why not just snapshot the customers table nightly?"**

> A nightly snapshot gives you daily granularity and silently loses any change
> that happens and reverts within the same day. For fraud that matters: a
> customer whose risk score spikes at 10am and is manually reset at 4pm looks
> unchanged in a nightly snapshot, and the transactions in between get scored
> against a profile that was not actually in effect. CDC captures the
> transition; snapshots capture only the endpoints.

### "Why not put everything on Kafka?"

Kafka is right for events and wrong for reference data. A merchant catalogue of
200 rows refreshed daily does not need a topic, partitions, or consumer
offsets — it needs a file drop and an incremental read. Conversely, polling a
REST endpoint for transactions would throw away ordering, replay, and offset
semantics, which are the properties that make streaming worth its complexity.

The framework supports a REST reader specifically for reference data
([readers.py:58](../src/pipelines/framework/readers.py#L58)), and transactions
deliberately stay on Kafka. The reader even refuses to materialise an empty
result over existing data — a REST endpoint returning `[]` because of an auth
failure should not silently truncate a table.

---

## Part 3 — Metadata-driven ingestion

This is the strongest section of the project. Expect the deepest follow-ups
here, and welcome them.

### "Explain the metadata-driven ingestion."

> Each bronze source is declared in YAML: ingestion type, target table, column
> list, type-specific options. At pipeline start the framework loads every
> config, validates it, and generates one table definition per source. Adding
> a source is a config file with no Python change. Adding a new source *type*
> is one reader function plus one line in a dispatch table.

### "Show me the hard part."

The closure binding. This is the single best technical story in the project
because it is a bug that unit tests catch and code review usually does not.

`@dp.table` registers a *function* that the runtime invokes later. Generating N
tables in a loop is the classic trap:

```python
for cfg in configs:                      # WRONG
    @dp.table(name=cfg.target)
    def _t():
        return reader(spark, dbutils, cfg.ingestion)
```

Python closures capture the *variable*, not its value. By the time the runtime
calls those functions the loop has finished, so every table reads the **last**
config. You get three tables all ingesting the same source — and each one
succeeds, so nothing fails. You discover it when someone notices the merchant
table contains transactions.

The fix is binding per call by passing config as a function parameter, so each
invocation gets its own scope
([bronze_factory.py](../src/pipelines/framework/bronze_factory.py)):

```python
def build_bronze_table(cfg: SourceConfig, spark, dbutils) -> None:
    reader = get_reader(cfg.ingestion_type)

    @dp.table(name=cfg.target, comment=cfg.comment)
    def _bronze_table() -> DataFrame:
        return _project(reader(spark, dbutils, cfg.ingestion), cfg)
    _bronze_table.__name__ = f"bronze_{cfg.name}"
```

I test this explicitly with stubbed readers, asserting each registered table
reads its own source rather than all reading the last one.

**If they push: "what's the alternative fix?"**

> A default argument — `def _t(cfg=cfg)` — binds at definition time and also
> works. I prefer the function-parameter version because the binding is
> structural rather than relying on a Python evaluation-order subtlety that
> the next reader has to know. Both are correct; one is more obvious.

### "Where does metadata-driven stop being a good idea?"

Knowing where a pattern *stops* applying is what separates having used it from
having understood it. This question is a senior-level filter.

> At silver. Bronze is uniform — read, project, timestamp — so generating it
> from config removes real duplication. Silver is genuinely per-source: the
> fraud watchlist needs specific fields uppercased and a
> `dd-MMM-yyyy HH:mm:ss` timestamp parsed; transactions need a JSON envelope
> parsed against an explicit schema. Encoding that in YAML means inventing a
> transformation DSL that is strictly worse than the SQL it replaces — no type
> checking, no IDE support, no debugger, and a config file that is now a
> programming language nobody else knows.
>
> So the boundary is deliberate: config-driven where the logic is uniform,
> explicit code where it is business logic.

### "YAML or a database for the config?"

> Both, at different layers, and the split is about what each medium is good
> at.
>
> Structural definitions — what sources exist, their type and target — live in
> YAML in git, because they need diffs, code review, and environment
> promotion. Operational state — what ran, when, with what outcome — lives in
> Delta tables, because it is written at runtime and needs to be queryable.
>
> Putting source definitions in a table loses version history: someone runs an
> UPDATE and production changes with no diff, no reviewer, no revert. Putting
> run history in YAML is impossible — the pipeline would have to commit to git
> on every update.

### "What does the operational layer actually buy you?"

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
Grepping YAML cannot detect that — the YAML was correct. Only comparing intent
against reality finds it.

The ops writes are wrapped in their own error handling, because observability
must never become the reason ingestion fails.

---

## Part 4 — Streaming

### "Explain watermarking. Not the definition — what does it actually guarantee?"

The definition is easy and everyone has it. The distinction that separates
candidates:

> A watermark tells Spark how late an event may arrive before it is dropped,
> which is what bounds the state the engine retains. Without one, a
> stream-stream join accumulates state forever and the job eventually dies.
>
> The thing people get wrong is treating it as a guarantee. It is a
> **heuristic threshold, not a contract**. Spark computes the watermark as
> max-event-time-seen minus the delay, and it advances based on data it has
> *already observed*. Records later than the threshold are dropped from the
> stateful operation, but records arriving before it are not guaranteed to be
> processed either — it is best-effort. If you need a hard guarantee that no
> event is lost, the watermark is the wrong tool; you need a replayable source
> and a reprocessing path, which is what bronze provides here.

In [fraud_card_alert.py](../src/pipelines/streaming/gold/fraud_card_alert.py)
both sides carry a five-minute watermark — the transaction stream on
`transaction_timestamp`, the watchlist on `effective_from`. Five minutes is a
trade: longer catches more genuinely late events and holds more state, shorter
bounds memory and alerts faster. For fraud, alerting quickly matters more than
catching every straggler, and the stragglers are still recoverable from bronze.

### "What happens to data later than the watermark?"

> It is dropped from the stateful operation — the join, the aggregation. It
> still lands in bronze, which is append-only and has no watermark, so it is
> fully recoverable by reprocessing. That is a concrete argument for keeping
> raw data rather than parsing at ingest: the watermark is a latency decision
> in the serving path, not a data-retention decision.

### "Stream-static versus stream-stream join — when do you use which?"

Stream-static joins a stream to a table that is re-read per micro-batch.
Stream-stream joins two streams and requires watermarks on both so state can be
evicted.

**This project uses both, and the reasoning is the interesting part.**

The fraud match is stream-stream: transactions against the fraud watchlist,
because both are event streams where either side may arrive first.

Customer enrichment is stream-static, and there is a subtle bug in the obvious
implementation that is worth volunteering:

```python
customers = spark.read.table("finguard.silver.customers")   # fans out
```

`silver.customers` is CDC-fed, so it accumulates **one row per customer per
change**, not one row per customer. Joining it directly fans out — a customer
with three recorded changes multiplies their transactions by three, producing
duplicate alerts evaluated against stale limits. Today the table happens to
hold exactly one row per customer, which is precisely why the fault had not
surfaced: it appears the first time any customer attribute is updated at
source.

The fix reduces to the latest row per customer before joining:

```python
latest_customer = (
    spark.read.table("finguard.silver.customers")
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(
            F.col("silver_ingestion_timestamp").desc())))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
```

**The follow-up that separates senior candidates — "shouldn't the alert use the
profile as it was at transaction time?"**

> Yes, for analysis. No, for alerting, and the split is deliberate.
>
> A stream cannot look up historical dimension versions without unbounded
> state — you would have to retain every version of every customer forever.
> So point-in-time attribution belongs in the dimensional layer, where
> `dim_customer` carries SCD2 validity windows and `fct_alerts` joins on them.
>
> The streaming table is the operational path: an analyst responding to a live
> alert needs the customer's *current* contact details and *current* risk
> profile, not the profile from three weeks ago. Two consumers, two
> correctness definitions, two tables.

That answer demonstrates you understand that "correct" depends on the consumer,
which is a senior-level distinction.

### "Why `dropDuplicatesWithinWatermark` instead of `dropDuplicates`?"

> `dropDuplicates` on a stream retains state for every key it has ever seen —
> unbounded growth, and eventually the job dies. `dropDuplicatesWithinWatermark`
> (Spark 3.5+) bounds that state by the watermark, so it deduplicates within a
> window and evicts older keys.
>
> The trade is explicit: duplicates arriving further apart than the watermark
> will not be caught. In `merchants_silver.py` I use a one-day watermark on
> `merchant_id`, because a merchant record redelivered more than a day apart
> is a genuinely different event worth keeping.

### "Your pipeline emits alerts. How do you guarantee each alert is sent once?"

This is where most candidates say "exactly-once" and get caught.

> I do not guarantee it, and I would be cautious of anyone who claims they do
> over email.
>
> `foreachBatch` is **at-least-once** by default. If the batch succeeds in
> sending emails but the driver dies before the offset commit, the batch
> replays and those emails send twice. Delta writes can be made idempotent
> with `txnAppId` and `txnVersion`, which makes the *table* write
> exactly-once — but an external side effect like SMTP has no such mechanism.
>
> The honest answer for this project is that the email sink is at-least-once
> and a duplicate fraud alert is an acceptable failure mode — far better than
> a missed one. If duplicates mattered, the fix is a dedupe table keyed on
> `alert_id` written transactionally in the same batch, checked before
> sending.

---

## Part 5 — Kafka scenarios

These come up verbatim in experienced-engineer interviews. Each is a
production symptom, not a definition.

### "Consumer lag is climbing but CPU and memory look fine. Diagnose it."

> I would work through it in stages rather than guessing.
>
> First, is lag rising on *one* partition or all of them? One partition means
> a hot key — everything hashing to the same partition — or a size skew, or a
> slow leader broker. All partitions means the consumer simply cannot keep up.
>
> Second, compare producer rate to consumer rate. If producers write 10k/s and
> we consume 8k/s, lag grows even though every component is healthy. That is a
> capacity problem, not a bug.
>
> Third, check for a rebalance loop — grep the logs for `rebalance` and
> `re-join`. A consumer stuck rebalancing looks idle on CPU because it is not
> processing, and lag grows the whole time.
>
> Fourth, blocking I/O in the poll loop. Low CPU with high lag is the
> signature of waiting on something — a downstream API or database — not of
> compute saturation.

### "Consumers stopped processing, but offsets keep committing."

> That is `enable.auto.commit=true` with a broken processing path. Auto-commit
> advances the offset on a schedule regardless of whether your business logic
> succeeded, so you get silent data loss: the messages are marked consumed and
> never processed.
>
> Fix is `enable.auto.commit=false` and commit manually *after* processing
> succeeds. That moves you from "possible silent loss" to "possible
> duplicates," which is the right trade almost always — duplicates are
> detectable and fixable, silent loss is neither.

### "Consumers restart constantly during peak traffic."

> Batches get bigger under load, processing per batch takes longer, and the
> next `poll()` misses `max.poll.interval.ms`. Kafka concludes the consumer is
> dead and triggers a rebalance. The rebalance pauses consumption, lag grows,
> the next batch is bigger still — it is self-reinforcing.
>
> Three levers: raise `max.poll.interval.ms`, lower `max.poll.records` so each
> batch is smaller, or move processing off the poll thread. I would reach for
> `max.poll.records` first because it addresses the cause rather than raising
> the threshold for detecting a real hang.

### "After a restart, some messages processed twice. Bug or expected?"

> Expected. Kafka is at-least-once by default. The consumer polls offsets
> 100–150, processes them, and dies before committing; on restart it resumes
> from the last committed offset and reprocesses.
>
> The engineering answer is not to eliminate duplicates but to make processing
> idempotent — upserts keyed on a business key, or a processed-ID table. In
> this project, `fct_transactions` deduplicates on `transaction_id` in the
> mart precisely because the bronze layer can contain the same transaction
> twice. And it did: I found `TXN266669` at partition 1, offsets 146 *and*
> 392 — 17 duplicate transaction_ids that a dbt uniqueness test caught.

That last detail is a real finding from this project and lands much harder than
the textbook answer.

### "One consumer in the group is slower than the rest."

> The diagnostic that separates the two cases: after a rebalance, does the lag
> follow the *partition* or the *consumer*?
>
> If lag stays with the same partition, it is a data problem — hot key, large
> messages, slow leader. If lag follows the same consumer instance, it is that
> instance — GC pauses, a stale code version, different config, or noisy
> neighbours on the host.

### "You added more consumers and throughput didn't improve."

> Parallelism in a consumer group is capped by partition count. Six partitions
> means at most six active consumers; the seventh sits idle holding no
> assignment.
>
> If partitions are not the limit, the bottleneck is downstream — a database
> or API that is now receiving the same total request rate from more clients.
> Adding consumers to a downstream-bound pipeline just spreads the same
> queue across more processes.

### "A single message crashes the consumer, forever."

> Poison pill. The record throws, the offset never commits, the restart
> re-reads the same record. The partition is now permanently blocked.
>
> Fix: bounded retry with backoff, then route to a dead-letter queue and
> commit the offset to unblock the partition. Log the topic, partition and
> offset so the bad record is recoverable. This project's equivalent is the
> quarantine tables — rows failing quality rules are written to
> `transactions_quarantine` with their full Kafka coordinates rather than
> being dropped, so they can be inspected and replayed from exactly that
> offset.

### "Does Kafka guarantee global ordering?"

The trap question. The answer is no, and confidently saying so is the signal.

> Only **per partition**. There is no global ordering across a topic, and any
> design that assumes it is wrong at more than one partition.
>
> If you need ordering for a specific entity — all events for one card, in
> order — you partition by that key so they land on the same partition. The
> cost is that a hot key creates a hot partition, which is the skew problem
> from earlier. Ordering and even distribution are in direct tension; you pick
> per use case.

---

## Part 6 — Dimensional modeling

The highest-signal round. The most common rejection is not a wrong schema — it
is starting to draw before stating the grain.

### The opening move, every time

Before drawing anything, say out loud:

> "The fact table is one row per **completed card transaction**. Let me state
> that first because everything else follows from it."

That single sentence is scored. Candidates who skip it build fact tables that
mix grains and then have to discard them.

### "Walk me through your dimensional model."

> Grain first: `fct_transactions` is one row per transaction. `fct_alerts` is
> one row per alert — a different grain, deliberately a separate fact table,
> because one transaction can raise multiple alerts and cramming both into one
> table would mix grains and double-count amounts.
>
> Conformed dimensions: `dim_customer`, `dim_merchant`, `dim_date`, shared by
> both facts using the same surrogate keys, so cross-process analysis works.
>
> `dim_customer` and `dim_merchant` are SCD2, built from dbt snapshots with
> the `check` strategy, carrying `valid_from` / `valid_to` / `is_current`.
>
> `transaction_id` stays on the fact as a **degenerate dimension** — it has no
> descriptive attributes, so a `dim_transaction` table would be a join to
> retrieve a column that is already there.

Volunteering "degenerate dimension" unprompted is a strong senior signal.

### "Why surrogate keys instead of just customer_id?"

> Because with a natural key, history rewrites itself. If the fact stores
> `customer_id` and I join to the current dimension row, then a customer
> upgraded from Silver to Gold makes *every historical transaction* look like
> it was made by a Gold customer. Last year's segment analysis silently
> changes.
>
> The surrogate key pins each fact to a specific *version* of the dimension.
> That is the entire point of SCD2 — without surrogate keys you have the
> storage cost of history and none of the benefit.

### "How do you handle a late-arriving fact against an SCD2 dimension?"

A senior-level question with a specific expected answer.

> You join on the validity window, not on `is_current`:
>
> ```sql
> ON f.customer_id = d.customer_id
> AND f.transaction_timestamp >= d.valid_from
> AND f.transaction_timestamp <  d.valid_to
> ```
>
> That resolves the fact to the dimension version in effect when the
> transaction actually happened, regardless of when it arrived. Joining on
> `is_current` would attribute a three-week-old transaction to today's
> profile.

**And the cold-start problem, which I hit in this project:**

> When I first built this, all 4,396 fact rows came back with NULL customer
> attributes. The cause: `dbt_valid_from` on the first snapshot version was
> the date I *ran the snapshot*, but the transactions were from weeks earlier.
> Every fact fell before the first validity window and matched nothing.
>
> The fix is backdating the first version of each entity to a sentinel:
>
> ```sql
> case when row_number() over (
>          partition by customer_id order by dbt_valid_from) = 1
>     then timestamp '1900-01-01 00:00:00'
>     else dbt_valid_from
> end as valid_from
> ```
>
> The semantics are honest: we do not know when this customer's first version
> became true, only that it was in effect before we started observing.

This is a genuinely common production bug and having hit it is worth more than
having read about it.

### "What if a fact arrives for a customer that doesn't exist in the dimension?"

> An **inferred member**. You insert the dimension row with the natural key
> populated and the descriptive attributes null, point the fact at it, and
> update it when the real record arrives.
>
> The alternatives are both worse: dropping the fact loses a real transaction,
> and holding it in a staging area until the dimension appears means an
> upstream delay silently stalls the whole fact load.

### "Star or snowflake?"

> Star, and I would push back on snowflaking dimension hierarchies. The
> storage saving is negligible — a merchant category repeated 200 times is
> nothing — and the cost is a join on every query plus BI tools that handle
> flat dimensions better. Denormalise the hierarchy into the dimension.
>
> Snowflaking earns its place when a sub-entity is genuinely shared and
> genuinely large, or when it has its own independent change cadence.

### "Would you use One Big Table instead?"

> For this workload, no. OBT wins on query simplicity and single-table scan
> performance, and it loses precisely where this project needs to win: SCD2
> history. Flattening dimensions into the fact means either no history at all,
> or restating every historical row when a customer attribute changes.
>
> OBT is a good fit for an ML feature table or a serving layer built *from* a
> star schema, not as a replacement for one.

---

## Part 7 — dbt

### "How do incremental models actually work?"

> First run does a full `SELECT` and creates the table. Subsequent runs wrap
> the same `SELECT` in a `MERGE` and process only new rows, filtered by an
> `is_incremental()` block that references `{{ this }}` for the high-water
> mark.

### "What's the most common incremental bug you've seen?"

> Omitting `unique_key` on a merge strategy. Without it the merge has no join
> condition to match on, so every run appends instead of upserting and you
> accumulate duplicates silently — the model succeeds every time.
>
> The defence is pairing it with a uniqueness test on the same key. That is
> exactly how I found the 17 duplicate `transaction_id` values in this
> project: the model looked fine, the test failed, and the real cause turned
> out to be upstream Kafka replay rather than the merge itself.

### "What does `is_incremental()` return during `--full-refresh`?"

A trap question with a counter-intuitive answer.

> `false`. `--full-refresh` drops and rebuilds the table, so dbt deliberately
> skips the incremental filter to reprocess everything.
>
> The trap is putting logic in the `is_incremental()` block that is required
> for correctness rather than just for filtering. If you compute a column
> there, a full refresh produces a *different* table — and full refreshes are
> exactly when you are least likely to be watching closely.

### "How do dbt snapshots implement SCD2?"

> A snapshot compares the current source state against the stored version and
> writes a new row when it differs, maintaining `dbt_valid_from` /
> `dbt_valid_to`.
>
> Two strategies: `timestamp` uses an updated-at column and is cheaper and
> more reliable when you trust that column; `check` compares a named column
> list and is what you use when there is no dependable timestamp. I use
> `check` here.

### "What happens when a record is deleted at source?"

> By default, nothing — the snapshot simply stops seeing it, so the last
> version stays open with `dbt_valid_to` null, looking permanently current.
> That is wrong for anything counting active entities.
>
> dbt 1.9+ has `hard_deletes: new_record`, which writes a tombstone row
> marking the deletion, so you can distinguish "still current" from "deleted
> on this date." The older `invalidate_hard_deletes` just closed the window,
> which loses the fact that a deletion occurred.

### "Where do you draw the line between streaming and dbt?"

The architectural question this project is designed to answer:

> Streaming owns detection, dbt owns analysis, and the boundary is latency
> versus historical correctness.
>
> Gold streaming tables produce alerts in near-real-time using current-state
> enrichment, because an analyst responding to a live alert needs current
> contact details. dbt marts read *from* those gold tables and attach
> point-in-time SCD2 dimensions, compute detection latency, and build the
> aggregates analysts query.
>
> Putting SCD2 point-in-time joins in the stream would need unbounded state.
> Putting alert detection in dbt would add batch latency to a decision that
> has to be fast. Each layer does what it is actually good at.

---

## Part 8 — System design

### "Design a fraud detection system for 5,000 transactions/second with a 100ms decision budget."

Do not start drawing. Start with the budget, because the budget forces every
subsequent decision.

> First, where does the 100ms go? Roughly: network in and out, feature
> lookup, model inference, decision write. Feature lookup is the part that
> kills you — if features come from a warehouse query, you have already blown
> the budget before scoring.
>
> So the architecture splits by latency requirement, not by technology:
>
> **Synchronous path (inside the budget).** Transaction arrives at the payment
> gateway, features are fetched from an in-memory store keyed by card and
> customer, the model scores, approve or decline returns. Single-digit
> milliseconds for the lookup, which is why this is Redis or equivalent and
> not a warehouse.
>
> **Streaming path (seconds).** Kafka consumes the same transaction and
> computes windowed aggregates — transaction count in the last five minutes,
> distinct merchants in the last hour, velocity by device — writing them back
> into the feature store so the *next* transaction's synchronous lookup has
> them. This is where my project's watermarked windowed aggregations live.
>
> **Batch path (hours to days).** Historical features, model retraining, label
> joining once chargebacks arrive weeks later.
>
> The hard problem across all three is **training/serving skew**. If a feature
> is computed one way in the batch training pipeline and another way in the
> streaming path, the model scores against a distribution it never saw. That
> is what a feature store solves — a single definition of each feature served
> to both paths. It is the failure mode I would raise unprompted, because it
> is the one that silently degrades model accuracy without any alert firing.

**Then name your failure modes before they ask.** Interviewers explicitly score
whether you surface at least two unprompted:

> Two that would worry me most. First, the feature store going stale — if the
> streaming path stops updating, the synchronous lookups keep succeeding
> against old values, so fraud detection quietly degrades with no error
> anywhere. I would alert on feature freshness, not just on job success.
>
> Second, unbounded state in the streaming aggregations. Without watermarks
> the state store grows until the job dies, usually at peak traffic when you
> can least afford it.

### "Batch or streaming — how do you choose?"

> Default to batch, and make streaming justify itself. Streaming costs more in
> operational complexity, in state management, and in the class of bugs
> available to you — watermarks, late data, rebalances.
>
> The question is whether a decision changes based on freshness. Fraud
> declines: yes, obviously — a five-minute-old decision is worthless. A daily
> merchant risk report: no, and building it on streaming buys nothing but
> pager duty.
>
> In this project transactions stream because detection is latency-bound, and
> the dbt marts run in batch because nobody needs a dimensional aggregate in
> under a second.

### "Lambda or Kappa?"

> Kappa if you can — one code path, no reconciliation between a batch and a
> streaming implementation of the same logic drifting apart.
>
> Lambda's real cost is maintaining two implementations that must agree. Its
> real benefit is that reprocessing history through a batch engine is often
> cheaper and simpler than replaying a very long stream.
>
> This project is closer to Kappa: Kafka is the replayable log, bronze keeps
> raw payloads, so reprocessing means rebuilding from bronze rather than
> maintaining a separate batch path.

---

## Part 9 — Debugging stories (STAR)

Use "I" not "we." Interviewers cannot score contributions they cannot
attribute. Name the tradeoff you made, not just the outcome.

### "Tell me about a bug that was hard to find."

> **Situation.** Fraud alert emails were not being delivered, but the pipeline
> reported success on every batch. Nothing in the event log indicated failure.
>
> **Task.** Find out why a path that reported healthy was doing nothing.
>
> **Action.** I traced the notifier and found `dbutils().secrets().get(...)` —
> calling an object and an attribute as functions. That raises `TypeError`
> immediately, but it sat inside a bare `except Exception` at module scope, so
> the exception was swallowed, the password became `None`, and every batch hit
> a `if APP_PASSWORD is None: return` guard and exited quietly.
>
> I fixed the call to construct `DBUtils` from the active session, and moved
> secret resolution *inside* the batch function — at module scope the
> credential is captured in a closure that gets serialised to executors, which
> is a second problem hiding behind the first.
>
> **Result.** Alerts deliver. But the lesson I took was about the `except`,
> not the typo: a pipeline that reports success while doing nothing is worse
> than one that crashes. I now treat any bare `except` around credential
> retrieval as a defect regardless of whether it is currently firing.

### "Tell me about a deployment that went wrong."

> **Situation.** Refactoring bronze ingestion to the config-driven framework,
> I hit six consecutive failed pipeline updates.
>
> **Action.** Four distinct root causes, each one further than the last.
> `NameError: __file__ is not defined` — Lakeflow `exec()`s pipeline files
> rather than importing them, so `__file__` does not exist; it worked locally
> and failed deployed. Then `ImportError: cannot import name 'build_all'
> (unknown location)` — my sync uploaded modules as NOTEBOOK objects, which
> Python cannot import, and overwriting does not change an object's type so
> they had to be deleted and re-uploaded. Then `Found duplicate table` — the
> sync only uploaded and never deleted, so superseded files lingered and
> collided. Then a Kafka `SaslAuthenticationException`, which was a real
> credential problem rather than code.
>
> **Result.** All four fixed. The signal I paid attention to was that each
> failure was *different* — failing identically means you are not learning,
> failing differently means you are making progress.
>
> **What I changed permanently.** The underlying defect was one-way sync
> accumulating state, so I moved to Databricks Asset Bundles, which reconcile
> rather than accumulate. Deleting a file locally now removes it remotely.

That last line matters. Interviewers listen for whether you changed a *system*
or just fixed an *instance*.

### "Tell me about a time you were wrong."

> I claimed liquid clustering would improve query performance on the
> transaction mart and set out to demonstrate it. I ran the benchmark
> properly — five runs per query, medians, before and after — and measured an
> 8–13% improvement.
>
> Then I checked `DESCRIBE DETAIL` and the table was a single 129KB file.
> Clustering had nothing to prune. I built a 5-million-row synthetic table to
> get a fair test and it compacted to one file as well. The 8–13% was
> warehouse warm-up, not clustering.
>
> I dropped the benchmark tables and wrote it up as a negative result. The
> honest conclusion is that liquid clustering is the right default for large
> Delta tables, and at this project's scale it is unmeasurable — the file
> layout has nothing to optimise. Reporting a number I could not defend would
> have been worse than reporting none.

This is one of the strongest available answers. Most candidates have no story
where they disproved their own claim.

### "Tell me about a security mistake."

> I committed live credentials to a public repository — a Confluent API key
> and secret, a Gmail app password, and a `print()` of a Databricks token that
> got saved into notebook output.
>
> The important part is what fixing it actually required. Deleting the values
> from the file does nothing: every prior commit still contains them and stays
> retrievable. The only real remedy is rotation at the provider, which I did.
>
> What I changed permanently: secret scanning in CI, and configuration read
> from `dbutils.widgets` or environment variables so no credential is ever
> literal in a file. Cheap to add, and it eliminates the entire class.

---

## Part 9b — Governance and security

The round most portfolio projects cannot answer at all. For a fraud platform it
is not optional: an interviewer will reach PCI within ten minutes.

### "How do you handle PII in a data platform?"

> The distinction that matters is between a *transformation* and a *control*.
>
> I originally masked the PAN inside a dbt model and wrote a comment calling it
> defence in depth. It wasn't. A mask applied in a model protects that model's
> output. Every other consumer reads the source table: notebooks, JDBC, Power
> BI, dbt itself. So it was a single layer, applied to the one table an analyst
> is least likely to open while working a live alert.
>
> The control belongs at the catalog boundary. A Unity Catalog column mask
> protects the column for every reader through every engine, including paths
> nobody thought about when writing the model:
>
> ```sql
> CREATE FUNCTION finguard.security.mask_pan(pan STRING) RETURN
>   CASE WHEN is_account_group_member('finguard_pci_privileged') THEN pan
>        WHEN pan IS NULL THEN NULL
>        WHEN length(pan) < 8 THEN '****'
>        ELSE concat('****-****-****-', right(pan, 4)) END;
> ```
>
> Three details are deliberate. NULL in, NULL out — otherwise a NULL PAN becomes
> the string with nothing after the dashes, which reads as a real empty card and
> breaks `IS NULL` downstream. The `length < 8` guard, because `right(pan, 4)`
> on a four-character value returns the whole thing. And the group check first,
> which is the only reason this is a function rather than a literal expression.
>
> It **fails closed**: `is_account_group_member()` returns false for a group
> that doesn't exist, so a missing group means everyone sees the mask. A control
> that fails open leaks the first time someone is onboarded in a hurry.

### "How do you know the masking works?"

This is the question that separates candidates. The answer is not "I applied it."

> Six assertions in `sql/governance/03_pii_verification.sql`, because this
> project has been bitten three times by controls that reported success and did
> nothing — dbt configs silently ignored, a SQL alert bound to a UUID string,
> the v1 alerts API that creates inert alerts. `ALTER TABLE ... SET MASK`
> succeeds whether or not the mask does what you intended.
>
> The check that mattered searches for PAN-shaped column names **regardless of
> whether anyone tagged them** — written on the assumption that my first pass
> had missed something. Run right after I'd applied and verified six masks, it
> found two more tables leaking in clear:
>
> - `bronze.customers` — my threat model was "transactions carry PANs." But the
>   customer master carries the card on file, and CDC lands it in bronze before
>   silver runs.
> - `snapshots.customers_snapshot` — dbt writes it, so it sat outside my mental
>   model of "tables the pipeline owns." It's the worst one to miss: an SCD2
>   snapshot keeps every historical version forever, so it accumulates PANs
>   that exist nowhere else.
>
> A check that validates what you built confirms your assumptions. A check that
> hunts for what you missed finds bugs.

### "Did masking break anything?"

> That was the risk I was most worried about. `fraud_card_alert` joins on the
> masked column — `transactions.card_number == fraud_watchlist.entity_id`.
>
> If Unity Catalog applied masks to join *inputs* rather than to the output
> projection, every comparison becomes a masked string compared to a real PAN,
> the join returns zero rows, fraud detection silently stops, and the pipeline
> reports success. That is the same silent-failure shape as everything else in
> my challenges document.
>
> UC applies masks at projection time, so it's fine — but "expected to be fine"
> is exactly the phrase that preceded my last three silent failures, so I
> asserted it: 375 alerts before, 375 after. `fct_transactions` 4,396 before and
> after.

### "Is this PCI compliant?"

**Say no.** Claiming compliance is the fastest way to fail the round.

> No. It implements Requirement 3.3 — masking on display, with access limited to
> a documented business need, which I express as group membership. It does not
> implement 3.5, at-rest protection, which needs tokenization or managed-key
> encryption. The bytes are still clear text on storage; the mask is an
> access-time transformation, so anyone reading the Delta files outside Unity
> Catalog still sees the PAN.
>
> There's also a specific residual exposure I'd rather state than have found:
> `bronze.transactions.value` holds the raw Kafka payload with the PAN inside a
> JSON string, across 4,432 rows. A column mask can't reach a substring, and
> masking the whole column breaks silver, which parses it. The real fix is
> tokenizing at the producer so the PAN never lands.

### "What breaks your masking?"

> A full refresh. Lakeflow drops and recreates tables, and a recreated table has
> no masks — no error, no log line, no alert. The `ALTER` statements applied to
> a table that no longer exists.
>
> That's not hypothetical for me: I have a `bronze.transactions` full refresh on
> the backlog to fix a stale checkpoint, and running it would un-mask everything
> downstream. CHECK 6 detects the gap; the durable fix is applying masks from
> the pipeline itself so they're part of table creation rather than a manual
> follow-up.

---

## Part 9c — When your own monitoring finds your bugs

The strongest available material, because almost no portfolio can demonstrate it.

### "What did your monitoring actually catch?"

> Two real defects, on its first run against real data.
>
> **A 606-hour stale watermark.** `fraud_card_alert`'s stream-stream join stopped
> advancing event time on 18 June and reported success on every run since.
> Records arriving beyond the frozen watermark are treated as late and dropped
> silently — which is *correct behaviour* for a watermarked join. The pipeline
> was doing exactly what it was told. For a fraud alerting path that means alerts
> that should have fired didn't, and nothing was red.
>
> The mechanism is worth explaining: a join's watermark is the **minimum** across
> its inputs. One quiet source freezes the whole operator even while the other
> side flows normally.
>
> **`shuffle.partitions: 16` was inert on stateful operators.** Telemetry showed
> the `symmetricHashJoin` running 800 partitions against a configured 16.
> Stateful operators pin partition count into the checkpoint at creation, because
> state is partitioned by key and changing the count would redistribute every
> key. The config applies to stateless shuffles and to checkpoints created after
> the change — it cannot apply retroactively.
>
> That one contradicts a claim in my own optimization document. I recorded the
> correction in the README's known gaps rather than quietly editing the older
> doc.

**Why this answer works:** it demonstrates the monitoring is real. Anyone can
build a dashboard. Having it find something you didn't know was broken — in your
own project, contradicting your own documentation — is the thing that cannot be
faked.

### "How do you monitor a Databricks pipeline?"

> Event-log telemetry into three ops tables, then alerts on those rather than on
> task exit codes. Exit codes can't see the failures that matter — every serious
> bug in this project returned success.
>
> One implementation detail worth knowing: `stream_progress.progress_json` is
> **doubly encoded** — a JSON string nested inside the details JSON. The obvious
> query returns NULL rather than erroring:
>
> ```sql
> get_json_object(details, '$.stream_progress.metrics.watermark')  -- NULL
> ```
>
> A monitoring table built on that path fills with NULL watermarks, every stall
> detector goes quiet, and the monitoring looks healthy precisely because it's
> broken. I found it by dumping the raw payload instead of trusting the path
> that should have worked.

### "How do you avoid alert fatigue?"

> Severity drives cadence, not just labelling. A PAGE check that runs daily isn't
> a page; a TICKET check that runs every ten minutes is a mailing list. I have
> two hourly PAGE alerts and two daily TICKET alerts.
>
> Every detector returns **zero rows when healthy**, and `empty_result_state` is
> set to OK — left at UNKNOWN, a recovered pipeline never clears. I also
> disabled notify-on-recovery: recovery mail doubles volume and trains people to
> skim.
>
> And I proved delivery rather than assuming it — created a throwaway alert
> scheduled two minutes out, watched it reach TRIGGERED with a
> `last_evaluated_at` timestamp, confirmed the email, then deleted it.


---

## Part 9d — Cost management

Increasingly asked at senior level, and most portfolio projects have no answer.

### "How do you control cost on a data platform?"

> Four layers, and I'd separate visibility from control up front because most
> answers conflate them.
>
> **Attribution** — tags on every declared resource (`project`, `environment`,
> `cost_center`, `owner`) so spend groups by project rather than by resource id.
> That matters here because my dev and prod bundle targets currently publish to
> the same catalog, so nothing else distinguishes them in billing.
>
> **Detection** — three detectors: daily spend against a trailing median, idle
> compute, and a monthly budget projection.
>
> **Compute controls** — triggered rather than continuous pipelines, warehouse
> auto-stop at 10 minutes, `maxOffsetsPerTrigger` to bound replay, and a job
> timeout so a hung task can't hold serverless compute open indefinitely.
>
> **And the honest part** — all of that detects, none of it prevents. Nothing
> blocks a runaway job mid-flight. Real prevention needs account-level budget
> policies that can enforce, and those aren't reachable from a workspace token —
> `/api/2.0/budgets` returns 404. So my budget is a constant in a SQL query.
> That has one advantage, the threshold lives in git and changes through review,
> and one real limitation, which is that it can't stop anything.

### "How do you find waste?"

The strongest answer available, because it's a real finding and it starts with
being wrong.

> I'd originally rejected cost anomaly detection when I built my monitoring —
> the reasoning was that at my spend level the variance is noise. Then I
> actually measured the workspace: $395 over 30 days, of which my entire fraud
> platform was $59. A serverless model-serving endpoint I'd forgotten about was
> burning $239 — four times the whole project.
>
> It was a flat 96 DBU/day, every single day. That's scale-to-zero left
> disabled: it bills for existing, not for working.
>
> My reasoning had been right about the pipeline and wrong about the workspace.
> Monitoring scoped to the thing you're building cannot see the thing you forgot
> about, and the thing you forgot about is where the money goes. All my
> detectors are workspace-scoped now.

Then the technical discriminator, which is what separates this from a story:

> The signal isn't size, it's **shape**. A big number might be a legitimate
> heavy job. What identifies waste is a nonzero floor every single day — real
> workloads are spiky and touch zero, an idle resource never does. So the
> statistic is `min(daily_dbu)`, not `sum` or `max`. And I filter *out* rows
> carrying a pipeline or job id, which leaves exactly what nobody is watching —
> the inverse of how my original cost panel was written.

### "How do you avoid a cost alert that cries wolf?"

> Require both a multiple and an absolute floor. My spend-spike detector needs
> 3x the trailing median **and** more than $5. The floor is what makes it
> alertable — at $0.30/day a 3x rise is 90 cents, and a percentage test alone
> reproduces exactly the noise that made me reject the idea in the first place.
>
> I also compare against the **median**, not the mean, and exclude the day under
> test from its own baseline. A single 400 DBU day would drag a mean up and
> suppress detection of the next spike.

### "Why is your cost alert daily when your pipeline alerts are hourly?"

A good interviewer asks this. It tests whether you understand your data source
rather than just your thresholds.

> Billing data lands with a lag of a few hours. An hourly cost check re-reads
> the same incomplete day and pages repeatedly about one event. Cadence should
> match how fast the underlying signal can actually change, not how urgent the
> topic feels. I run it at 07:30 so the previous day has settled.

### "What does your pipeline actually cost?"

> $59.63 over 30 days for the whole platform, at roughly 5 transactions/second.
> Per pipeline it's $7.94 for the main streaming pipeline and $1.12 for customer
> CDC, with the rest on the SQL warehouse.
>
> I also track cost per 1,000 rows, because absolute spend says nothing about
> efficiency — $8 is fine for millions of rows and terrible for four thousand.
> At my volume that figure is dominated by fixed serverless start-up cost rather
> than per-row work, so I treat it as a baseline to compare against later rather
> than a number to be proud of.
>
> One caveat I'd state: these are list prices from `system.billing.list_prices`,
> so they exclude committed-use discounts. It's an estimate for trend and
> comparison, not an invoice.


---

## Part 10 — Answering honestly about gaps

Being straight here is worth more than a fabricated capability. Every one of
these has a "here is what I would do" attached.

### "Do you have tests?"

> Two suites. 42 pytest tests covering the framework — config validation
> rejection cases, closure binding, the REST reader against a local HTTP
> server exercising pagination, retry on 5xx, and auth failure. PySpark is
> stubbed so they run offline in CI without a cluster.
>
> And dbt tests on the dimensional layer — uniqueness, not-null,
> relationships, accepted values.
>
> **What is missing, and I would say this before being asked.** Those 42 tests
> cover 3 of 33 source files: config loading, the bronze factory, and the
> metrics collector. `fraud_engine.py` is 123 lines of seeded, deterministic,
> pure business logic — the single most testable file in the repository — and
> it has no tests. That is the first gap I would close.
>
> There is also no integration testing against a real cluster. The pytest
> suite proves framework logic; it cannot prove a Lakeflow pipeline builds.

### "How do you deploy?"

> Databricks Asset Bundles with dev and prod targets, plus a GitHub Actions
> workflow that runs the pytest suite and validates the bundle on every push.
>
> It started as a Python file-sync script, which I replaced specifically
> because it only uploaded and never deleted — that caused a duplicate-table
> failure when superseded files lingered in the workspace.

### "What about data quality?"

> Lakeflow expectations in silver — `expect_or_drop` on required identifiers —
> plus quarantine tables so dropped rows are inspectable rather than gone.
> Rows land in `transactions_quarantine` with the specific failed rules and
> full Kafka coordinates, so any quarantined message can be replayed from its
> exact offset after an upstream fix.
>
> One detail I would highlight: the quarantine filter uses
> `(rules) IS NOT TRUE` rather than `NOT (rules)`. With plain negation, a rule
> evaluating to NULL makes `NOT NULL` also NULL, which `WHERE` treats as
> false — so the row gets dropped by the expectations *and* missed by the
> quarantine, disappearing with no record anywhere. Every rule today is an
> `IS NOT NULL` check that can never itself be NULL, so it does not currently
> matter; it matters the moment someone adds `amount > 0`.

That is a genuinely subtle SQL point and demonstrates thinking about failure
modes that have not happened yet.

### "What's missing that you'd build next?"

> Four things, in priority order.
>
> **Backfill as a designed capability.** This is the biggest hole. Right now the
> only reprocessing primitive is full refresh — all or nothing. A real platform
> gets asked "the fraud rule was wrong for three weeks, recompute those alerts,"
> and I can't express that. The property that makes it work is idempotency, and
> `fct_transactions` already has it via merge on `transaction_id`; the streaming
> layer doesn't. It also ties directly to my stale-watermark defect, where late
> records were dropped since 18 June — backfill is the remedy.
>
> **Ground truth for detection quality.** My producer generates fraud with known
> reasons and discards that label at the Kafka boundary, so I cannot compute
> precision or recall. The platform detects fraud and nobody can say whether
> it's any good. Carrying `is_fraud` and `fraud_reasons` into bronze fixes it.
>
> **A schema registry.** I'm already on Confluent. Today the Kafka payload is
> parsed against a hardcoded schema in two places, so a producer-side field
> addition silently returns null rather than failing. A registry enforces
> compatibility at publish time instead of parsing defensively downstream.
>
> **Reconciliation.** I have `ingestion_audit` recording what ran, but nothing
> that ties the numbers out — Kafka offsets consumed versus bronze rows versus
> silver rows, with the deltas explained. Bronze has 4,432 and silver 4,413;
> something should assert *why* those 19 differ rather than leaving it to
> inspection.

---

## Part 11 — The trap table

Questions where the intuitive answer is wrong.

| Question | Wrong answer | Correct answer |
|---|---|---|
| Does a watermark guarantee late data is processed? | Yes, within the threshold | No — it is a heuristic bound on state, best-effort only |
| Does Kafka guarantee ordering? | Yes | Per partition only; never globally |
| Is `foreachBatch` exactly-once? | Yes | At-least-once; needs `txnAppId`/`txnVersion` for idempotent Delta writes, and external sinks have no equivalent |
| What does `is_incremental()` return during `--full-refresh`? | `true` | `false` — full refresh deliberately skips the filter |
| Does dbt snapshot handle source deletions? | Yes | No, by default — needs `hard_deletes: new_record` (1.9+) |
| Does removing a secret from a file fix a leak? | Yes | No — git history retains it; only rotation works |
| Does overwriting a workspace file change its object type? | Yes | No — NOTEBOOK stays NOTEBOOK; delete and re-upload |
| Do more consumers mean more throughput? | Yes | Only up to partition count, and only if downstream keeps up |
| Is `NOT (predicate)` the inverse of the predicate? | Yes | Not with NULLs — use `IS NOT TRUE` |
| Should the fact table join `is_current` dimension rows? | Yes | No — join the validity window for point-in-time correctness |
| Is a lone identifier a dimension? | Yes | No — degenerate dimension, keep it on the fact |
| Does liquid clustering always help? | Yes | Not on small tables — one file has nothing to prune |
| Is `dropDuplicates` safe on a stream? | Yes | No — unbounded state; use `dropDuplicatesWithinWatermark` |
| Does CDC give one row per entity? | Yes | No — one row per *change*; dedupe before joining or you fan out |
| Does `shuffle.partitions` apply to stateful operators? | Yes | No — partition count is pinned into the checkpoint at creation |
| Does a green dbt run mean configs were applied? | Yes | No — dbt accepts unknown config keys silently; verify with `DESCRIBE DETAIL` |
| Does a column mask survive a pipeline full refresh? | Yes | No — the table is recreated without masks, silently |
| Does masking a join key break the join? | Yes | No — UC masks at projection time, not on join inputs (but assert it) |
| Is masking the same as encryption? | Yes | No — PCI 3.3 display vs 3.5 at-rest; masked bytes are still clear on storage |
| Does an HTTP 200 mean an alert will fire? | Yes | No — a string-bound condition or the v1 API creates inert alerts |
| Is high total spend the sign of waste? | Yes | No — a nonzero daily *floor* is; totals catch legitimate heavy jobs too |
| Can you join `list_prices` without filtering? | Yes | No — it is time-ranged; omit `price_end_time IS NULL` and you multiply by every historical price |
| Should cost alerts run as often as pipeline alerts? | Yes | No — billing lands hours late, so hourly checks re-page on one event |

---

## Part 12 — Quick reference

| Concept | Where it lives |
|---|---|
| Medallion architecture | [src/pipelines/streaming/](../src/pipelines/streaming/) |
| Metadata-driven ingestion | [src/pipelines/framework/](../src/pipelines/framework/) |
| Closure binding fix | [bronze_factory.py](../src/pipelines/framework/bronze_factory.py) |
| Reader dispatch | [readers.py](../src/pipelines/framework/readers.py) |
| Config validation | [source_config.py](../src/pipelines/framework/source_config.py) |
| Operational metadata | [registry.py](../src/pipelines/framework/registry.py) |
| Stream-stream join + watermarks | [fraud_card_alert.py](../src/pipelines/streaming/gold/fraud_card_alert.py) |
| Stream-static join (dedup fix) | [high_value_transactions_alert.py](../src/pipelines/streaming/gold/high_value_transactions_alert.py) |
| Quarantine + NULL semantics | [transactions_quarantine.py](../src/pipelines/streaming/silver/transactions_quarantine.py) |
| SCD2 snapshots | [transform/snapshots/](../transform/snapshots/) |
| Point-in-time join | [fct_alerts.sql](../transform/models/marts/fct_alerts.sql) |
| SCD2 cold-start backdating | [dim_customer.sql](../transform/models/marts/dim_customer.sql) |
| Incremental merge + dedup | [fct_transactions.sql](../transform/models/marts/fct_transactions.sql) |
| Deployment | [databricks.yml](../databricks.yml), [resources/](../resources/) |

### Numbers you can defend

Measured on 2026-08-11. Never quote a number not on this list.

| Metric | Value |
|---|---|
| Transactions (silver) | 4,413 |
| Transactions (mart, deduplicated) | 4,396 |
| Duplicate `transaction_id` found by dbt test | 17 |
| Customers | 1,002 |
| Merchants | 200 (19 high-risk, 10 blacklisted) |
| Fraud alerts (gold) | 375 |
| Alerts in mart with point-in-time dims | 353 |
| High-value alerts | 3 |
| Ingestion patterns | 4 (Kafka, Auto Loader JSON/CSV, CDC, REST) |
| pytest tests | 42 passing |
| dbt tests | 50 passing |
| Kafka partitions | 6 |
| Watermark | 5 minutes (fraud), 1 day (merchant dedup) |
| Throughput | ~5 transactions/second (simulator) |
| **Governance** | |
| Column masks applied | 10 |
| Classification tags | 28 column + 10 table |
| PAN as seen unprivileged | `****-****-****-9904` |
| Alert count before/after masking | 375 / 375 (unchanged) |
| **Observability** | |
| `ops.pipeline_runs` rows | 49 |
| `ops.expectation_results` rows | 501 |
| `ops.stream_health` rows | 585 |
| Scheduled SQL Alerts | 4 (2 hourly PAGE, 2 daily TICKET) |
| Watermark lag found | 606.3 hours |
| State partitions vs configured | 800 vs 16 |
| **Cost** | |
| 30-day workspace spend | $395.83 |
| Attributable to FinGuard | $59.63 |
| Idle endpoint waste found | $239.35 |
| Main pipeline 30-day cost | $7.94 |
| Monthly budget / projection | $300 / $484.71 |
| **Optimization** | |
| Files before / after compaction | 117 -> 17 (-85%) |
| Bytes before / after | 1,372,550 -> 760,366 (-44.6%) |

### Things not to say

- **Never claim unmeasured throughput.** "Millions per second" against a 5 TPS
  simulator is checkable and will be checked.
- **Never say "production-ready."** Say what is built, what is not, and why.
- **Never claim exactly-once** without naming the mechanism that provides it.
- **Never say "we" when you mean "I."** Unattributable contributions cannot be
  scored.
- **Never blame a stakeholder or a tool** for a failure. It is the single
  biggest behavioural disqualifier.
- **Never say "it went well"** as a result. Quantify it, even roughly.

---

## Sources

Research underpinning the scenario questions and scoring guidance:

**Streaming and Spark**
- [Watermarks in Apache Spark Structured Streaming — Damavis](https://blog.damavis.com/en/watermarks-in-apache-spark-structured-streaming/)
- [Feature deep dive: Watermarking in Structured Streaming — Databricks](https://databricks.com/blog/feature-deep-dive-watermarking-apache-spark-structured-streaming)
- [Multiple stateful operators in Structured Streaming — Databricks](https://www.databricks.com/blog/multiple-stateful-operators-structured-streaming)
- [Designing Robust Stream–Stream Joins with Watermarks](https://medium.com/@soni97divaker/designing-robust-stream-stream-joins-with-watermarks-in-databricks-structured-streaming-67f3c27c2509)
- [100 Spark Scenario Based Interview Questions 2026](https://www.jobswithscala.com/blog/100-spark-scenario-based-interview-questions-and-answers/)

**Kafka**
- [Kafka Consumer Scenario-Based Questions for Experienced Engineers](https://medium.com/@javalearners/kafka-consumer-scenarios-based-interview-questions-for-exprienced-engineers-48aac972e6d2)
- [Kafka Topic Scenario-Based Questions](https://medium.com/@javalearners/kafka-topic-scenarios-based-interview-questions-for-experienced-engineers-7de199813860)
- [50 Kafka Interview Questions for Data Engineers (2026) — Datavidhya](https://datavidhya.com/blog/kafka-data-engineering-interview-questions/)

**Dimensional modeling**
- [10 Data Modeling Problems That Gate Senior DE Offers — DataExpert](https://dataexpert.medium.com/10-data-modeling-problems-that-gate-senior-de-offers-67be8712ce3f)
- [Data Modeling Interview Questions (Rubric-Scored) — DataDriven](https://datadriven.io/data-modeling-interview-questions)
- [50 Data Modeling Interview Questions for DEs — Datavidhya](https://datavidhya.com/blog/data-modeling-interview-questions/)

**dbt**
- [50 dbt Interview Questions for Data Engineers (2026) — Datavidhya](https://datavidhya.com/blog/dbt-data-engineering-interview-questions/)
- [hard_deletes config — dbt Developer Hub](https://docs.getdbt.com/reference/resource-configs/hard-deletes)

**System design**
- [Design a Real-Time Fraud Detection System](https://medium.com/@bugfreeai/tiktok-mle-system-design-interview-design-a-real-time-fraud-detection-system-749cea63ffa5)
- [Real-Time Fraud Detection: Latency, Features & Scale — Redis](https://redis.io/blog/real-time-fraud-detection/)
- [Batch vs Streaming for Data Engineering Interviews](https://medium.com/@dharmatejasamudrala/i-spent-80-hours-studying-batch-vs-streaming-heres-what-actually-matters-for-data-engineering-ada17d00635c)

**Databricks and Delta**
- [Use liquid clustering for tables — Databricks docs](https://docs.databricks.com/aws/en/tables/clustering)
- [Debunking 8 data layout myths — Databricks](https://www.databricks.com/blog/debunking-8-data-layout-myths-why-liquid-clustering-outperforms-partitioning)
- [Liquid Clustering vs Partitioning with Z-Order — Canadian Data Guy](https://www.canadiandataguy.com/p/optimizing-delta-lake-tables-liquid)

**Interview process and behavioural**
- [Data Engineering Interview Prep (2026): Rounds, Questions, Rubric — DataDriven](https://datadriven.io/data-engineer-interview-prep)
- [Ultimate Guide to Behavioral Data Engineer Interviews — DataExpert](https://www.dataexpert.io/blog/ultimate-guide-behavioral-data-engineer-interviews)
- [5 Red Behaviour Flags During Data Engineering Interviews — Data Gibberish](https://www.datagibberish.com/p/5-red-behaviour-flags-during-data-engineering-interviews)
