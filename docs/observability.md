# Observability and Monitoring

How this platform reports on itself: what is collected, what is alerted on,
what was deliberately not built, and what the numbers actually were.

Structured in the same three tiers as
[performance optimization](performance_optimization.md) — **measured**,
**correct but unmeasurable here**, **deliberately rejected** — because the
value of a monitoring design is mostly in knowing which of those three any
given piece belongs to.

---

## The distinction that organises this document

**Observability** is the data being available. **Monitoring** is something
looking at it on a schedule and telling you when it is wrong.

Databricks gives you the first for free. This pipeline had **3,443 event log
records** before any of this work started — watermarks, state sizes,
expectation results, per-batch latency, all of it already there. None of it was
being read.

That gap is the entire subject. A platform can be fully observable and
completely unmonitored, and from the outside those look identical right up
until something breaks quietly.

---

## What this found

The monitoring was not built and then tested against hypothetical failures. It
was built, pointed at the existing workspace, and immediately returned real
defects that had been present and invisible.

### 1. The stream-stream join watermark is 606 hours behind

```
flow_name                      operator            watermark             lag
finguard.gold.fraud_card_alert symmetricHashJoin   2026-06-18T12:26:00Z  606.3h
finguard.silver.merchants      dedupeWithinWatermark 2026-08-09T06:32:48Z  46.1h
```

`fraud_card_alert` is the stream-stream join between transactions and the fraud
watchlist. Its event-time watermark last advanced **25 days ago**.

Every run since has reported success. Nothing failed. But a watermark that far
behind means the join has been silently discarding late-arriving records for
weeks — which, for a fraud alerting path, means alerts that should have fired
did not.

**This is the failure mode monitoring exists for.** There is no error, no
exception, no red run. The only way to see it is to look at event time and
compare it to wall clock, which is exactly what nothing was doing before.

### 2. Stateful operators run 200–800 partitions against a configured 16

```
flow_name                                        operator            instances  configured
finguard.gold.fraud_card_alert                   symmetricHashJoin   800        16
finguard.silver.merchants                        dedupeWithinWatermark 200      16
finguard.gold.transaciton_count_by_minute        stateStoreSave      200        16
finguard.gold.transaciton_count_by_minute_sliding stateStoreSave     200        16
```

[finguard_pipeline.yml](../resources/finguard_pipeline.yml) sets
`spark.sql.shuffle.partitions: "16"`, and
[performance_optimization.md](performance_optimization.md) documents that as an
applied optimization. For every stateful operator in the pipeline, **it is
not in effect**.

Stateful operators pin their partition count into the checkpoint when it is
first created. State is physically laid out across that many partitions, so
Spark cannot honour a later change without invalidating the state — it keeps
the original value instead. The setting applies to stateless shuffles only
until the checkpoint is rebuilt.

The join is at **800**, or 50× the configured value.

This is worth stating plainly because it corrects a claim made elsewhere in
this repository. The optimization was real, the config was right, and the
effect was partial in a way nothing surfaced. Config that reads as applied and
is silently inert is precisely what "monitor the actual runtime, not the
intended configuration" means.

### 3. Cost attribution works and is cheap to obtain

```
day          DBU     USD      usage_records
2026-08-11   0.835   $0.29    2
2026-08-10   3.384   $1.18    20
2026-07-13  10.417   $3.65    32
```

`system.billing.usage.usage_metadata.dlt_pipeline_id` attributes usage directly
to a pipeline, joinable to `system.billing.list_prices` for dollars.

---

## Tier A — Measured and working

Built, run against the live workspace, verified to return real values.

| # | Capability | Evidence |
|---|---|---|
| 1 | Update outcome history | 49 rows in `ops.pipeline_runs` |
| 2 | Expectation results per run | 501 rows; e.g. `valid_transaction_id` 4,413 passed / 0 failed |
| 3 | Watermark and state telemetry | 585 rows in `ops.stream_health` |
| 4 | Watermark stall detection | Fires on 2 flows, 606.3h and 46.1h |
| 5 | State-vs-config divergence | Fires on 4 operators |
| 6 | Failed-update detection | 29 distinct failed updates in 24h |
| 7 | Cost attribution | $3.65 / $1.18 / $0.29 by day |
| 8 | Alert delivery | 4 alerts, scheduled, email subscribed |

### The collector

[`src/pipelines/ops/metrics_collector.py`](../src/pipelines/ops/metrics_collector.py)
reads the event log and writes three Delta tables.

Three reasons to materialize rather than query `event_log()` directly:

1. **Retention.** The event log is scoped to the pipeline and disappears with
   it. A deleted-and-recreated pipeline loses all history — exactly when
   before/after comparison matters most.
2. **Shape.** The interesting metrics are behind a double JSON decode (below).
   Every consumer would otherwise repeat it and get it subtly wrong.
3. **Joins.** Alerts join telemetry to billing and the update timeline. A TVF
   scoped to one pipeline cannot.

It runs as a **separate job task, not a `@dp.table`**, for the same reason
[`registry.py`](../src/pipelines/framework/registry.py) does: a Lakeflow dataset
cannot record its own failure. A collector inside the graph writes nothing on
the run that fails — the run you most need evidence from.

The job task uses `run_if: ALL_DONE` rather than the default `ALL_SUCCESS`, for
the same reason, and `disable_auto_optimization: true` so monitoring never
fails the run it monitors.

### The double decode

The single most useful technical detail here.

`stream_progress` events contain a field called `progress_json` whose value is
**a JSON string nested inside the details JSON**. The natural path returns
nothing:

```sql
-- Returns NULL. Not an error -- silence.
get_json_object(details, '$.stream_progress.metrics.watermark')
```

```sql
-- Correct: extract the string, then parse it a second time.
get_json_object(
  get_json_object(details, '$.stream_progress.progress_json'),
  '$.eventTime.watermark')
```

The first form fails silently. A monitoring table built on it populates
normally with NULL watermarks, and every stall detector quietly matches
nothing — monitoring that appears healthy precisely because it is broken.

Behind that second decode: `eventTime.watermark`, `stateOperators[]` with
RocksDB metrics and `numRowsDroppedByWatermark`, `durationMs`, and
per-expectation `observedMetrics`.

A regression test asserts both decode stages are present in the query text,
because this failure has no runtime symptom.

### Detector design: zero rows means healthy

[`sql/ops/monitoring_checks.sql`](../sql/ops/monitoring_checks.sql). Every
detector returns **no rows when healthy**, so every alert binds the same
trivial condition.

- A returned row *is* the alert body — it carries the diagnosis, so the
  notification names the table and the magnitude.
- A query that errors returns nothing and fails loudly at the alert layer,
  instead of a threshold silently evaluating false forever.

The tempting inverse — return a `status` column of `'OK'` / `'ALERT'` — is
worse: each alert then needs its own string condition, and a typo in the
literal disables it with no error anywhere.

**Every detector also returns a numeric first column.** Databricks alert
conditions are `column op threshold`, evaluated numerically. This was learned
by getting it wrong: the first version of the update-failed alert bound to
`update_id`, a UUID string, comparing a UUID against `0`. The alert was created
successfully, reported no error, and would never have fired. Found by asserting
the bound column was numeric — not by reading the config back, which looked
fine.

### Severity drives cadence

| Severity | Checks | Schedule |
|---|---|---|
| **PAGE** | update failed, watermark stalled | hourly |
| **TICKET** | expectation degraded, state growth | daily 06:00 |
| **DIAGNOSTIC** | state partitions vs config | dashboard only |

A PAGE check that runs daily is not a page. A TICKET check that runs every ten
minutes is a mailing list. `notify_on_ok` is **false** — recovery mail doubles
volume and trains people to skim.

Thresholds compare against each metric's **own trailing baseline**, not fixed
numbers. A hardcoded "alert below 99%" is wrong in both directions: noise for
an expectation that has always run at 95%, blind to one that slipped from
100% to 99.5%.

### Alerts are code

[`scripts/create_alerts.py`](../scripts/create_alerts.py) is idempotent —
matching on `display_name` and updating rather than duplicating. Alerts are
workspace state, so without this they are click-configured and invisible to
review, the same problem the Asset Bundle solved for the pipeline.

**Two API versions exist and they are not equivalent:**

| Endpoint | Schedule | Email subscriptions |
|---|---|---|
| `/api/2.0/sql/alerts` (v1) | ✗ | ✗ |
| `/api/2.0/alerts` (v2) | ✓ | ✓ |

The v1 endpoint accepts a create call, returns 200, and produces an alert that
looks correct in the response while never evaluating and notifying nobody. Both
were built on v1 first and had to be rebuilt. The v2 payload is also *not*
wrapped in `{"alert": {...}}`, and its list response key is `alerts`, not
`results` — reading the wrong key made every alert look new, so a rerun would
have silently duplicated all four. Caught by running `--dry-run` twice.

---

## Tier B — Correct, but unmeasurable at this scale

Implemented and structurally right; this data volume cannot demonstrate value.
Stated rather than quietly claimed.

| Technique | Why it cannot be shown here |
|---|---|
| **Expectation baseline drift** | Needs ≥3 prior runs per expectation. Present data is 100% pass across the board, so the detector has never had a real drop to catch. |
| **State growth detection** | Compares against a 20-observation trailing median. State here is bounded and tiny; the 3× multiplier has never been approached. |
| **Backlog / consumer lag** | `numBytesOutstanding` is collected and is `0` everywhere — a simulator producing 5 txn/sec never builds a backlog. The metric that matters most in production is structurally uninteresting here. |
| **Cost trend** | Three days of data. Enough to attribute, not enough to trend. |
| **Trigger duration as latency SLI** | Observed 164s and 176s on a 200-row table, almost entirely `rocksdbCommitFileSyncLatencyMs`. At this scale duration measures fixed overhead, not data volume — the same reason query timings were rejected in the optimization work. |

---

## Tier C — Deliberately rejected

### Prometheus / Grafana / OpenTelemetry

**Rejected.** Not for effort — because it duplicates a telemetry path that
already exists and makes things worse.

Databricks emits pipeline telemetry to the event log and `system.lakeflow`
regardless. Getting the same data into Prometheus means a metrics listener in
the driver, a push gateway, a Prometheus host, and a Grafana host. Then "did
`silver.transactions` run?" has two sources that can disagree, and debugging
moves from the pipeline to the pipe.

Concretely: **serverless compute has no host to scrape.** No node exporter, no
long-lived JVM between triggered runs, nothing that persists after the update.
A pull-based scraper has nothing to pull from most of the time.

**Where it is right:** self-managed Spark on Kubernetes or EMR, where no
system-tables equivalent exists and the collection layer must be built. That is
a real and common architecture. The answer changes with the platform — hosted
with a telemetry substrate versus self-managed without one — and the reasoning
matters more than the tool.

### SLO error budgets

**Rejected — but the SLIs were built.** The distinction is the point.

An error budget is arithmetic on a denominator: 99.9% monthly means 43 minutes
of allowed failure. That number governs something only when tied to a
consequence — someone is paged, a release freezes when the budget burns. Here
the denominator is a hand-started simulator against a job that is `PAUSED` and
`development: true`. A budget computed on that uptime measures a personal
calendar, not a system.

The **indicators** are legitimate and are implemented: freshness lag,
expectation pass rate, dropped-row ratio. What was skipped is the budget
accounting layer on top. A budget without a consequence is a number on a
dashboard.

### Cost anomaly detection

**Rejected — but cost attribution was built.** This was reconsidered after
initially rejecting the whole area, which was wrong.

*Attribution* answers "what did this run cost, and is it trending up." It needs
almost no history and it works — $3.65, $1.18, $0.29 above, joined through
`dlt_pipeline_id`.

*Anomaly detection* answers "is this run statistically unusual." It needs a
baseline distribution, and 52 usage records is not one; a z-score on that fits
noise. Collapsing the two and discarding both threw away the useful half.

Cost attribution also does something the file-count and byte measurements in
the optimization work cannot: it is an **independent** measure of whether that
work reduced compute.

### Paging via SMS or phone call

**Rejected — not buildable here, and documented rather than faked.**

Databricks does not send SMS or place calls. The industry pattern is two-hop:

```
detector → webhook → incident tool (PagerDuty/Opsgenie) → phone/SMS/Slack
```

The incident tool owns on-call rotation, escalation on non-acknowledgement, and
timezone handling — deliberately, since a data platform should not know who is
on call this week.

That requires a paid account and a webhook URL not available here. Building a
mock PagerDuty integration that has never fired would be worse than describing
the real design honestly. Email is what is actually wired, and it is verified.

---

## Tier A2 — Job-level telemetry and SLA measurement

Added after a gap review found OB-01 ("every pipeline run shall persist its
outcome") was only true of *pipeline* runs.

### The blind spot

`metrics_collector.py` mines the Lakeflow event log. That covers everything
inside a pipeline update and nothing outside one — which leaves out the dbt
tasks, a task skipped because an upstream one failed, a run killed by its
timeout, and a run that never fired at all.

The last case is the dangerous one. **If the scheduled job stops firing
entirely** — paused schedule, expired credential, permissions change — the
pipeline event log stays quiet and every existing detector reports healthy.
Silence is indistinguishable from success when the only thing you watch is the
thing that did not run.

### What was added

`src/pipelines/ops/job_metrics_collector.py`, writing three tables:

| Table | Grain | Answers |
|---|---|---|
| `ops.job_runs` | One orchestration run | Did it run at all? How did it end? |
| `ops.job_task_runs` | One task within a run | *Which step* failed — ingestion or dbt? |
| `ops.job_sla` | One measurement per successful run | Did it meet NFR-01? |

Task-level grain matters on a seven-task DAG: "the pipeline is broken" and
"dbt_test failed while ingestion succeeded" have entirely different responses at
02:00.

### The SLA table, and what it deliberately does not measure

NFR-01 sets 15 minutes end-to-end. `ops.job_sla` records per-run latency against
that target, decomposed into queue time (cold start, contention) and execution
time — because a slow start is a compute problem and slow execution is a query
problem, and a single total hides which one you have.

**This is a deliberate under-measurement and must not be read as the full
figure.** The job clock starts when the run is *scheduled*; the SLA clock starts
when the *transaction occurred*. Missing:

- time the event waited in Kafka before the run triggered — the dominant term
- watermark delay on the stream-stream join — up to 5 minutes

So a run comfortably inside 900s does not prove the SLA is met. What it proves
is the controllable part: **if execution alone approaches the target, no trigger
interval can rescue it.** True end-to-end needs event-time-to-alert-time per row
(TRD FUT-06).

---

## Tier A3 — Lineage as a queryable graph

Unity Catalog captures column-level lineage automatically. It is genuinely
useful and almost never used, because it lives in a UI panel one table at a
time. `sql/ops/lineage_export.sql` turns it into five views.

| View | Answers |
|---|---|
| `v_table_lineage` | Deduplicated table-level edges |
| `v_column_lineage` | Which downstream columns read a given column |
| `v_downstream_impact` | Everything downstream, transitively, with hop count |
| `v_orphan_tables` | Tables nothing reads |
| `v_layer_rule_violations` | Edges that skip a medallion layer (TRD AR-01) |

**`v_downstream_impact` is the one that earns its keep.** The Kafka contract
gives 90 days' notice on a removed field. Using that notice means knowing what
the field feeds — and on a four-layer platform plus marts, guessing from memory
is how a dashboard silently breaks a month later. Measured on
`silver.transactions`: 12 downstream objects, 3 hops deep.

### Both new detectors were wrong on their first run

Recorded because the reflex is to trust a new detector and explain away the
data:

- **`v_layer_rule_violations` flagged three correct edges.** dbt staging models
  read silver directly, which is right — routing them through gold would push
  dimension source data through a layer built for streaming aggregates. **The
  rule was changed, not the finding explained.** A detector that reports
  known-good edges every run is one people stop reading.
- **`v_orphan_tables` returned 15 rows, none of them user tables** — all
  Lakeflow internals (`__materialization_mat_*`, `event_log_*`, sink tables).
  Excluded by pattern.

### The retention caveat

Lineage records what has *run recently* (currently 90 days), so it is evidence,
not a static graph. Two artefacts proved this immediately: a `marts_marts`
schema that no longer exists still appears, and `silver.transactions` shows as
its own downstream via the dedup operator's self-edge. A quarterly job outside
the window would be invisible entirely.

---

## What is not covered

- **The stall detector is proven to fire, not to clear.** It fired on a real
  stalled watermark. A live recovery has since been induced — the producer runs
  and the pipeline consumed a 164-message backlog to lag zero — but the detector
  clearing was not itself observed end-to-end.
- **`ops.job_sla` measures job latency, not end-to-end latency.** See Tier A2.
- **A job cannot see its own outcome.** `collect_job_metrics` runs last and
  reports on the run it belongs to, so its own result appears in the *next*
  run's collection. Inherent to a job observing itself; why the tables are
  append-only and watermarked.
- **Only `stateOperators[0]` is read.** Every stateful flow here has exactly one
  stateful operator. A flow with two would have the second silently ignored.
- **The collector is single-threaded over pipelines.** Fine for two; would need
  reworking for fifty.
- **`ops.pipeline_runs` has no retention policy.** It grows without bound.

---

## Running it

```bash
# Collect telemetry (also runs automatically in finguard_daily)
python src/pipelines/ops/metrics_collector.py --pipeline-ids <id1>,<id2>

# Create or update the alerts (idempotent)
python scripts/create_alerts.py --dry-run
python scripts/create_alerts.py
```

Detector queries for ad-hoc use: [`sql/ops/monitoring_checks.sql`](../sql/ops/monitoring_checks.sql).
Config-level checks (drift, staleness): [`sql/ops/health_checks.sql`](../sql/ops/health_checks.sql).

---

## Runbook

What to do when each alert fires. An alert that arrives at 2am without a next
action is noise with good intentions.

### PAGE — pipeline update failed

1. Identify the flow:
   ```sql
   SELECT pipeline_name, update_id, event_timestamp, message
   FROM finguard.ops.pipeline_runs
   WHERE state = 'FAILED' ORDER BY event_timestamp DESC LIMIT 5;
   ```
2. `message` names the failing flow. Common causes seen in this project:
   - `bronze.transactions` — Kafka checkpoint references dead-cluster
     partitions. Needs full refresh; **truncates existing rows**.
   - `DELTA_CLUSTERING_COLUMN_MISSING_STATS` — a clustering key sits beyond
     `dataSkippingNumIndexedCols`. Raise the budget or change the keys.
3. Repeated identical failures are one root cause, not many. 29 failures in 24h
   here are all the same checkpoint issue.

### PAGE — watermark stalled

1. Distinguish the two kinds:
   ```sql
   SELECT flow_name, operator_name, watermark, event_timestamp
   FROM finguard.ops.stream_health
   WHERE watermark IS NOT NULL ORDER BY event_timestamp DESC LIMIT 20;
   ```
2. **`NEVER_ADVANCED`** (watermark at epoch) — the operator has never seen a
   usable event-time value. Check the timestamp column is non-null and
   parseable, and that the source has produced anything at all.
3. **`STALLED`** (watermark far behind clock) — either the source went quiet,
   or upstream is delivering stale event times. Check the producer first.
4. Late data is already lost. The watermark cannot be rewound; recovery means
   fixing the source and, if the gap matters, reprocessing from bronze.

### TICKET — expectation rate degraded

1. Find which expectation and how far:
   ```sql
   SELECT dataset, expectation_name, update_id,
          sum(passed_records) passed, sum(failed_records) failed
   FROM finguard.ops.expectation_results
   GROUP BY 1,2,3 ORDER BY max(event_timestamp) DESC LIMIT 20;
   ```
2. A sharp drop usually means upstream schema or semantic change, not a code
   change here. Compare against the producer.
3. `expect_or_drop` violations are routed to the quarantine tables — inspect
   them for the actual bad rows rather than guessing.

### TICKET — state growth unbounded

1. Confirm the trend is real rather than one spiky batch:
   ```sql
   SELECT flow_name, operator_name, num_rows_total, event_timestamp
   FROM finguard.ops.stream_health
   WHERE operator_name IS NOT NULL ORDER BY event_timestamp DESC LIMIT 30;
   ```
2. Growing state with a stalled watermark is one problem, not two: rows are not
   ageing out because event time is not advancing. Fix the watermark.
3. Growing state with a healthy watermark means the watermark delay is too
   generous, or join keys are more diverse than assumed.

### DIAGNOSTIC — state partitions vs config

Not urgent, and not fixable in place. Stateful operators pin partition count at
checkpoint creation; changing `shuffle.partitions` cannot apply retroactively.
Realigning requires a full refresh, which rebuilds state from scratch — worth
scheduling deliberately, not doing in response to an alert.
