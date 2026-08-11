# Cost Management — Attribution, Detection and Control

**Scope:** cost attribution via tags, three spend detectors, budget tracking,
and the compute-level controls already in place.
**Status:** applied and verified against the live workspace, 11 August 2026.

---

## The finding that motivated this

Cost anomaly detection was originally **rejected** when the observability layer
was designed. The stated reasoning:

> `system.billing.usage` is real, but with usage this small the variance is
> noise.

That was half right, and it was rejected at the wrong scope. Measured:

| | 30-day spend |
|---|---|
| **Workspace total** | **$395.83** |
| Attributable to FinGuard | $59.63 |
| `PREMIUM_SERVERLESS_REAL_TIME_INFERENCE` | **$239.35** |

A serverless model-serving endpoint unrelated to this project had been burning
a **flat 96 DBU/day floor** — every day, whether or not anything called it —
for weeks. That is scale-to-zero left disabled. It cost **four times** the
entire fraud platform, and nothing in the workspace said so.

Daily DBU for that SKU:

```
2026-08-11    16.0        2026-08-06   342.5
2026-08-10    99.3        2026-08-05   246.0
2026-08-09    96.0        2026-08-04    96.0
2026-08-08    96.0        2026-08-03    96.0
2026-08-07   366.3        2026-08-02    96.0
```

The 96.0 repeating is the tell. Real workloads are spiky and touch zero; a
number that never moves is a resource being paid to wait.

### The actual lesson

It is about the **unit of analysis**, not about thresholds.

The original reasoning was correct for the pipeline — FinGuard costs about
$0.29/day, and a detector on that fires on rounding. It was wrong for the
workspace, which is where money is actually lost.

> **Cost monitoring scoped to the thing you are building will never find the
> thing you forgot about, and the thing you forgot about is where runaway spend
> lives.**

Every detector here is therefore workspace-scoped by default.

---

## What was already in place

Cost *visibility* existed before this work; cost *control* did not.

| Control | Status | Where |
|---|---|---|
| Per-pipeline cost attribution | Present | `sql/ops/monitoring_checks.sql` detector 6 |
| Dashboard cost panel | Present | `scripts/create_dashboard.py` |
| Correct `list_prices` join | Present | filters `price_end_time IS NULL` |
| Triggered, not continuous | Present | `resources/finguard_pipeline.yml` |
| Warehouse auto-stop (10 min) | Present | workspace setting |
| Bounded micro-batches | Present | `maxOffsetsPerTrigger` |
| **Spend anomaly detection** | **Added** | `sql/ops/cost_checks.sql` |
| **Idle compute detection** | **Added** | `sql/ops/cost_checks.sql` |
| **Budget projection** | **Added** | `sql/ops/cost_checks.sql` |
| **Tag-based attribution** | **Added** | `resources/*.yml` |
| **Job timeout** | **Added** | `resources/finguard_job.yml` |

---

## Attribution: why tags

`system.billing.usage` carries `usage_metadata.dlt_pipeline_id` for free, so
per-pipeline cost was already answerable. What it could not answer:

- How much did **dev** cost versus **prod**?
- What does **this project** cost, as one number?
- Which spend belongs to **nobody**?

The third is the one that matters. Untagged spend is everything running in the
workspace that no declared resource claims — exactly where the idle endpoint
was hiding.

```yaml
tags:
  project: finguard
  environment: ${bundle.target}
  cost_center: data-engineering
  owner: tahafurkhan@gmail.com
```

`${bundle.target}` resolves to `dev` or `prod` at deploy time, so one
declaration tags each environment correctly. This matters more here than usual
because **both bundle targets currently publish to the same `finguard`
catalog**, so nothing else distinguishes them in billing.

---

## The detectors

All in `sql/ops/cost_checks.sql`, following the same conventions as the
pipeline detectors: **zero rows means healthy**, and the **first column is
numeric** so a SQL Alert can bind to it.

### 1. PAGE — daily spend spike

Compares yesterday against the **median** of the preceding two weeks, excluding
yesterday itself.

Two design choices carry the weight:

**Median, not mean.** A single 400 DBU day drags a mean upward and suppresses
detection of the next spike. Median is unmoved by one outlier.

**A multiple AND an absolute floor.** 3× with a $5 minimum. The floor matters
more: at $0.30/day a 3× rise is $0.90 and worth nobody's attention. A
percentage test alone produces exactly the noise that got cost detection
rejected in the first place.

Verified against real history — it would have fired on 2026-08-07:

```
spend_multiple  usage_date   usd_spent  baseline_usd  excess_usd
5.44            2026-08-07   41.36      7.61          33.75
```

### 2. TICKET — idle compute

**The detector that would have saved $239.**

The signal is not "expensive". It is *expensive with a nonzero floor every
single day, attached to nothing*.

`min(daily_dbu)` is the discriminating statistic — not `sum`, not `max`. A
workload that ever drops to zero is doing its job. One that never does is being
paid to wait.

Filtering out rows carrying `dlt_pipeline_id` or `job_id` removes everything
this project deliberately runs, leaving what nobody is watching.

Verified — currently returns:

```
dbu_last_7_days  sku_name                                floor  peak    days
1358.1           PREMIUM_SERVERLESS_REAL_TIME_INFERENCE  16.0   366.3   8
```

### 3. TICKET — budget projection

Databricks budgets are an **account-level** feature
(`accounts.cloud.databricks.com`), not reachable from a workspace API token —
both `/api/2.0/budgets` and `/api/2.1/budget-policies` return 404 here. Rather
than skip budget tracking, the budget is a constant in the query.

**That trade has a real advantage and a real cost, and both should be stated.**
Advantage: the threshold lives in git and changes through review, instead of
being a number someone typed into a console. Cost: this *detects* a breach, it
cannot *prevent* one. A true budget policy can block.

`MONTHLY_BUDGET_USD = 300`, chosen from measured history rather than picked:
the last 30 days were $395.83, of which $239.35 was the idle endpoint. Without
it, normal operation is about $156/month, so 300 leaves room for real work
while catching another always-on resource.

Currently firing:

```
projected_month_usd  spent_so_far  budget  days_elapsed  pct_of_budget
484.71               169.98        300.00  11            159.7
```

Projection is linear on elapsed days. Crude, and correctly so — a smarter
forecast on 30 data points would be false precision.

### 4 & 5. DIAGNOSTIC — attribution and unit economics

Not alertable. Cost by project tag, and cost per 1,000 rows processed.

Unit economics matter because absolute spend says nothing about efficiency: $8
is fine for a pipeline moving millions of rows and terrible for one moving four
thousand. At this volume the figure is dominated by fixed serverless start-up
cost rather than per-row work, so it is a **baseline to compare against later**,
not a number to celebrate.

---

## Cadence: why cost pages run daily

The cost PAGE alert runs **daily at 07:30**, while pipeline PAGE alerts run
hourly. That looks inconsistent and is deliberate.

**Billing data lands with a lag of a few hours.** An hourly cost check re-reads
the same incomplete day and pages repeatedly about one event. Cadence should
match how fast the underlying signal can actually change, not how urgent the
topic feels.

07:30 rather than 06:00 for the same reason — it gives the previous day's
billing time to settle before the comparison runs.

---

## Verified state

Seven alerts, no duplicates, every bound column numeric:

| Alert | Binds | Cadence | Currently |
|---|---|---|---|
| PAGE pipeline update failed | `failure_count` | hourly | TRIGGERED (17) |
| PAGE watermark stalled | `watermark_lag_hours` | hourly | TRIGGERED (606.3) |
| PAGE daily spend spike | `spend_multiple` | daily 07:30 | OK (0 rows) |
| TICKET expectation degraded | `drop_pct_points` | daily 06:00 | OK (0 rows) |
| TICKET state growth | `growth_multiple` | daily 06:00 | OK (0 rows) |
| TICKET idle compute | `dbu_last_7_days` | daily 06:00 | 1,358 DBU |
| TICKET budget projection | `projected_month_usd` | daily 06:00 | $484.71 |

Dashboard: 8 panels, all returning data, single instance (idempotent).

---

## What this does not do

**It detects, it does not prevent.** Every control here is a detector. Nothing
blocks a runaway job mid-flight. Real prevention needs account-level budget
policies with enforcement, which are not reachable from a workspace token.

**List prices, not invoice prices.** `list_prices` excludes committed-use
discounts and private rates, so every dollar figure is an estimate for trend
and comparison. Stated rather than implied.

**No tagging on the SQL warehouse.** The warehouse is a pre-existing shared
resource, not declared in the bundle, so it carries no project tag. Its $50.17
appears under `warehouse_id` attribution but not under `project=finguard`.

**Tags apply to future usage only.** `custom_tags` is stamped at usage time, so
historical rows stay untagged. The tag-based panel will look empty for
`project=finguard` until the next deploy runs — the `(untagged)` row currently
holds all $395.74.

---

## Interview angles

**"How do you control cost on a data platform?"**

> Four layers, and I'd separate visibility from control because most answers
> conflate them.
>
> Attribution first — tags on every declared resource, so spend groups by
> project and environment rather than by resource id. Then detection: a spend
> spike compared against a trailing median, idle compute found by looking for a
> nonzero daily floor, and a monthly budget projection. Then compute-level
> controls: triggered rather than continuous pipelines, warehouse auto-stop,
> bounded micro-batches, a job timeout.
>
> And then the honest part — all of that detects, none of it prevents. Real
> prevention needs account-level budget policies that can block, which I can't
> reach from a workspace token.

**"How do you find waste?"**

The strongest answer available, because it is a real finding:

> By looking for a nonzero floor rather than a high total. My workspace was
> spending $395 a month and I assumed my pipeline was most of it — it was $59.
> A serverless serving endpoint I'd forgotten about was burning a flat 96
> DBU/day, which is scale-to-zero left disabled, and it cost four times the
> whole platform.
>
> What I'd emphasise is the mistake behind it: I'd originally rejected cost
> anomaly detection as noise, and I was right about the pipeline and wrong about
> the workspace. Monitoring scoped to what you built cannot see what you forgot
> about, and what you forgot about is where the money goes.

**"Why is your cost alert daily when your pipeline alerts are hourly?"**

A good interviewer will ask this, and it tests whether you understand your own
data:

> Billing data lands hours late. An hourly cost check re-reads the same
> incomplete day and pages repeatedly about one event. Cadence should match how
> fast the signal can change, not how urgent the topic feels.

---

## Files

| File | Purpose |
|---|---|
| `sql/ops/cost_checks.sql` | 5 detectors — 3 alertable, 2 diagnostic |
| `scripts/create_alerts.py` | Alert provisioning, idempotent |
| `scripts/create_dashboard.py` | Cost panels 7 and 8 |
| `resources/finguard_pipeline.yml` | Attribution tags on both pipelines |
| `resources/finguard_job.yml` | Attribution tags plus `timeout_seconds` |

## Open item

**The idle endpoint is still running** — 16 DBU today. It is outside this
project, so it is flagged rather than deleted: check Serving endpoints and
either enable scale-to-zero or remove it. The detector will keep reporting it
until then, which is the correct behaviour for a control that has found
something real.
