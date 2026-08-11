# Project Challenges & Optimization Report — FinGuard

**Project:** FinGuard — Real-Time Card Fraud Detection on Databricks
**Platform:** Lakeflow Declarative Pipelines · Unity Catalog · Delta Lake · Confluent Kafka · dbt
**Owner:** Taha Furkhan M
**Report date:** 11 August 2026
**Scale:** ~5 transactions/second from a simulator on serverless compute

---

## How to read this document

Every entry follows the same structure: **Description → Impact → Root Cause →
Metrics → Solution → Implementation → Result → Interview Angle.**

Three rules govern what is written here.

**Every number is measured.** Where a number could not be measured, the
document says so rather than estimating. The line "Query timing improvement:
**Not claimed**" in the results table is deliberate and is the most important
line in it.

**Business impact sections are omitted, not faked.** This is a solo portfolio
project. There is no revenue impact, no customer satisfaction score, no team
whose velocity slipped. Inventing those would undermine every real number here.
Where the standard report template asks for them, this document substitutes
what actually exists: **correctness impact** (did the platform produce wrong
answers), **cost impact** in measured DBU and dollars from `system.billing`, and
**detection impact** (would a real incident have been caught).

**A negative result is included.** Challenge 13 is an optimization that was
predicted, implemented, measured, and could not be demonstrated. It stays in
because a report containing only successes is a marketing document.

---

## Contents

| # | Challenge | Class | Severity |
|---|---|---|---|
| 1 | Clear-text PANs with no access control | Security / PCI | **Critical** |
| 2 | Silent secret failure — fraud alerts never sent | Silent failure | Critical |
| 3 | Stream-stream join watermark stalled 606 hours | Streaming correctness | Critical |
| 4 | dbt silently ignored every optimization config | Silent failure | High |
| 5 | Alert bound to a string column — would never fire | Silent failure | High |
| 6 | CDC fan-out in the stream-static join | Correctness | High |
| 7 | Credentials committed to a public repository | Security | High |
| 8 | NULL semantics silently swallowed quarantine rows | SQL correctness | High |
| 9 | 17 duplicate transactions from Kafka replay | Exactly-once | Medium |
| 10 | SCD2 cold start — every dimension attribute NULL | Dimensional modeling | High |
| 11 | `shuffle.partitions` inert on stateful operators | Streaming internals | Medium |
| 12 | Doubly-encoded JSON in the event log | Observability | Medium |
| 13 | Liquid clustering — a measured negative result | Optimization | — |
| 14 | Duplicate table definitions after refactor | Deployment drift | Medium |
| 15 | Liquid clustering requires stats on its own keys | Delta internals | Medium |
| 16 | Kafka authentication failure blocks the whole DAG | Blast radius | Medium |
| 17 | Cross-cluster Kafka migration invalidated checkpoints | Streaming state | Medium |

**The four themes.** These seventeen incidents collapse into four failure
modes, and the themes are worth more in an interview than any single anecdote:

1. **Silent failure** (1, 2, 4, 5, 12) — the operation reports success and does
   nothing. The most dangerous class, because monitoring built on exit codes
   cannot see it.
2. **Correctness under streaming semantics** (3, 6, 8, 9, 11) — the pipeline
   runs, produces output, and the output is wrong.
3. **State and deployment drift** (14, 15, 17) — what is deployed diverges from
   what is declared.
4. **Blast radius** (7, 16) — one failing component takes down more than it
   should.

---

# 1. CHALLENGE: Clear-text PANs with no access control

## 1.1 Description

The platform stored full 16-digit primary account numbers in clear text across
four tables, readable by any principal with `SELECT`.

```sql
SELECT card_number FROM finguard.silver.transactions LIMIT 1;
-- 4144 88** **** 9904   <- all 16 digits returned; redacted in this doc
```

Unity Catalog governance state at discovery, queried directly:

```sql
SELECT * FROM system.information_schema.column_masks WHERE table_catalog='finguard';
-- 0 rows
SELECT * FROM system.information_schema.column_tags  WHERE catalog_name='finguard';
-- 0 rows
SHOW GRANTS ON CATALOG finguard;
-- account users | BROWSE | CATALOG | finguard
```

Zero masks, zero classification tags, and a single catalog-wide `BROWSE` grant.

The project *believed* it had PAN protection. `dim_customer` contains:

```sql
concat('****-****-****-', right(card_number, 4)) as card_number_masked
```

with a comment describing it as "defence in depth" and noting that a Unity
Catalog mask "is the stronger control." The UC mask was never applied. So the
model-level masking was not a second layer — it was the only layer, applied to
the one table an analyst is *least* likely to query while responding to a live
alert.

## 1.2 Impact

**Severity: Critical.** For a fraud-detection platform this is not an
incomplete feature, it is a contradiction of the system's purpose.

- **Exposure:** 4,413 PANs in `silver.transactions`, 1,002 in
  `silver.customers`, 375 in `gold.fraud_card_alert`, plus every historical
  version in `snapshots.customers_snapshot`.
- **Correctness impact:** none — the data was correct, just unprotected.
- **Cost impact:** none.
- **Audit impact:** total. PCI-DSS Requirement 3.3 mandates PAN masking on
  display with access limited to those with a documented business need. Neither
  half existed.

## 1.3 Root Cause Analysis

Three compounding causes:

**Masking was treated as a modeling concern rather than a platform control.**
Applying `concat('****-...')` in a dbt model protects that model's output. It
does nothing for the source table, and every other consumer — notebook, JDBC,
Power BI, `dbt run` itself — reads the source.

**The threat model was scoped to transactions.** The initial protection plan
covered `silver.transactions` and `gold.fraud_card_alert`. It missed that the
*customer master* carries the card on file, and that CDC writes it to bronze
before silver exists.

**Nothing could answer "which columns are sensitive?"** With no classification
tags, the only audit method was reading every DDL by hand. That does not scale
past the tables you happen to remember.

## 1.4 Current Performance Metrics (before)

| Metric | Value |
|---|---|
| Column masks in catalog | 0 |
| Classification tags | 0 |
| Grants beyond blanket `BROWSE` | 0 |
| Tables with clear-text PAN | 6 (4 known, 2 undiscovered) |
| PAN rows exposed | 5,790+ |

## 1.5 Solution

A three-file, version-controlled governance layer under `sql/governance/`.
Committed rather than clicked into the workspace, for the same reason the Asset
Bundle replaced the sync script: workspace state that matters must be reviewable.

**Masking functions** in a dedicated `finguard.security` schema, so permission
to alter a control is separable from permission to read the data it protects:

```sql
CREATE OR REPLACE FUNCTION finguard.security.mask_pan(pan STRING)
COMMENT 'PCI-DSS 3.3 display masking...'
RETURN
    CASE
        WHEN is_account_group_member('finguard_pci_privileged') THEN pan
        WHEN pan IS NULL THEN NULL
        WHEN length(pan) < 8 THEN '****'
        ELSE concat('****-****-****-', right(pan, 4))
    END;
```

Three details in that function are load-bearing:

- **`is_account_group_member` first** — the privileged branch is the only reason
  this is a function rather than a literal expression.
- **NULL in, NULL out** — without it, a NULL PAN becomes the string
  `'****-****-****-'`, which reads as a real empty card and breaks `IS NULL`
  downstream.
- **`length < 8` guard** — `right(pan, 4)` on a 4-character value returns the
  whole value. The guard prevents a short value being revealed in full.

**Fails closed.** `is_account_group_member()` returns FALSE for a group that
does not exist rather than erroring. A missing group means everyone sees the
mask. A control that fails open is not a control.

## 1.6 Implementation

| File | Contents |
|---|---|
| `sql/governance/01_pii_masking.sql` | Functions, 10 `SET MASK` statements, classification tags |
| `sql/governance/02_grants.sql` | Three-group access model, no write grants to humans |
| `sql/governance/03_pii_verification.sql` | Six assertions that the controls actually work |

The grant model separates two questions that are usually conflated: *may you
read this table* (`finguard_fraud_analysts`) and *may you see the PAN in it*
(`finguard_pci_privileged`). Membership in the second grants no additional table
access. "Who can see PANs" is therefore answerable by listing one small group,
rather than by reasoning about the union of several grants.

One non-obvious grant is required:

```sql
GRANT EXECUTE ON FUNCTION finguard.security.mask_pan TO `finguard_fraud_analysts`;
```

Without `EXECUTE` on the masking function, `SELECT` **errors** rather than
returning masked data — and the error names a function the analyst never
referenced. The table grant looks right, the mask looks right, and the query
fails.

## 1.7 Results

Verified against the live workspace, not asserted:

| Check | Before | After |
|---|---|---|
| Column masks attached | 0 | **10** |
| PAN as seen by non-privileged caller | `4144 88** **** 9904` | `****-****-****-9904` |
| Email as seen | `taha@gmail.com` | `t***@gmail.com` |
| Tagged-sensitive-but-unmasked columns | n/a | **0** |
| Fraud alert count (regression check) | 375 | **375 — unchanged** |
| `fct_transactions` rows (regression check) | 4,396 | **4,396 — unchanged** |
| DDL idempotency | n/a | 28 statements, clean on 3 consecutive runs |

### The finding that justifies the verification file

`03_pii_verification.sql` CHECK 4 searches for PAN-shaped columns **regardless
of whether anyone tagged them**. Run immediately after the first six masks were
applied and confirmed working, it found two more leaks:

```
bronze.customers.card_number              5026 54** **** 1551   CLEAR
snapshots.customers_snapshot.card_number  5212 34** **** 1234   CLEAR
```

- **`bronze.customers`** was missed because the threat model said "transactions
  carry PANs." The customer master carries the card on file, and CDC lands it in
  bronze before silver runs.
- **`snapshots.customers_snapshot`** was missed because dbt writes it, so it sat
  outside the mental model of "tables the pipeline owns." It is also the worst
  one to miss: an SCD2 snapshot retains every historical version forever, so it
  accumulates PANs that exist nowhere else.

Both are now masked; total went from 6 to 10. **The check that found them was
written on the assumption the first pass was complete.**

The marts were checked by reading actual values rather than reasoning about
them: `stg_*` are VIEWs and inherit the source mask, and
`dim_customer.card_number_masked` was already derived. No action needed —
but verified, not assumed.

## 1.8 Residual Risk — stated, not hidden

**`bronze.transactions.value` still contains PANs in clear.** 4,432 rows hold
the raw Kafka payload with `card_number` inside a JSON string. A column mask
cannot mask a substring, and masking the column wholesale breaks silver, which
parses it. The real fix is tokenization at the producer so the PAN never lands.
This is recorded in CHECK 4's output rather than filtered out of the query — an
exception that is documented is a decision; an exception excluded from the check
is a lie.

**Masking is not encryption.** PCI-DSS 3.5 requires the PAN be rendered
unreadable *at rest*. These bytes are still clear text on storage; the mask is
an access-time transformation. Anyone reading the Delta files outside Unity
Catalog still sees the PAN. Calling this "PCI compliant" would be exactly the
kind of unmeasured claim this project avoids.

**Masks do not survive a full refresh.** Lakeflow drops and recreates tables on
full refresh, and a recreated table has no masks — silently, with no error and
no log line. This is not hypothetical: a `bronze.transactions` full refresh is
already on the backlog to fix a stale checkpoint, and running it would un-mask
everything downstream. CHECK 6 detects it; the durable fix is applying masks
from the pipeline itself.

## 1.9 Interview Angle

**Question this answers:** "How do you handle PII in a data platform?" — and its
follow-up, "how do you know it works?"

**What the interviewer is scoring:** whether you distinguish a *control* from a
*transformation*. Most candidates describe masking in a model. The senior answer
is that the control belongs at the catalog boundary, because a model protects
one output while a catalog mask protects the column through every engine that
reads it.

**The strongest thing to say:** not "I implemented masking" but "I implemented
masking, then wrote a check that assumed I'd missed something, and it found two
tables I had." Then explain *why* they were missed — threat model scoped to
transactions, and dbt-written tables outside the mental model of ownership.
That is a description of how security gaps actually happen.

**Expected follow-ups and the honest answers:**

- *"Is this PCI compliant?"* — No. It satisfies 3.3 display masking. It does not
  satisfy 3.5 at-rest protection, which needs tokenization. Say this plainly;
  claiming compliance is the fastest way to fail the round.
- *"What happens to the join?"* — UC applies masks at projection time, so joins
  evaluate the underlying value. Asserted in CHECK 5, not assumed: alert count
  is 375 before and after. Had masks applied to join inputs, the fraud join
  would have silently returned zero rows.
- *"What breaks this?"* — A full refresh. Covered above.

---

# 2. CHALLENGE: Silent secret failure — fraud alerts never sent

## 2.1 Description

The fraud alert email notifier ran successfully on every pipeline update and
sent nothing. No error, no exception, no failed task. The pipeline was green for
days while the alerting path — the actual product of a fraud detection system —
was dead.

## 2.2 Impact

**Severity: Critical.** A fraud platform that detects fraud and tells nobody has
the same business value as one that detects nothing. Every alert generated
during the affected window was written to Delta and never delivered.

## 2.3 Root Cause Analysis

The notifier read SMTP credentials from a secret scope. The scope lookup was
wrapped in exception handling that logged and continued, because a transient
secret-service failure should not kill a streaming query. The secret key name
was wrong. Every invocation took the failure path, logged to a stream nobody was
reading, and returned normally.

The root cause is not the typo. It is that **the failure path and the
no-work-to-do path were indistinguishable**: both returned normally, and the
task exit code was the only monitored signal.

## 2.4 Solution and Result

Fail fast on configuration errors, tolerate only transient ones. A missing
secret is a deployment error and must be loud; an SMTP timeout is transient and
may be retried. The distinction is the fix.

This class of failure is why `docs/observability.md` monitors *expectation pass
rates and watermark advancement* rather than task exit codes. Exit codes cannot
see this.

## 2.5 Interview Angle

**Question:** "Tell me about a bug that was hard to find."

**What is being scored:** whether you can articulate *silent failure* as a
category. The strong answer names the pattern — an error path that returns
normally is invisible to exit-code monitoring — and then generalises: this is
the same shape as Challenges 4, 5 and 12 in this document, which is why the
monitoring layer asserts on *behaviour* rather than on *completion*.

---

# 3. CHALLENGE: Stream-stream join watermark stalled for 606 hours

## 3.1 Description

`finguard.gold.fraud_card_alert` reported successful updates continuously while
its event-time watermark had not advanced since 18 June. Found by the
monitoring built in `docs/observability.md`, on its first run against real data.

```
flow_name              operator_name        watermark_lag_hours
fraud_card_alert       symmetricHashJoin    606.3
```

## 3.2 Impact

**Severity: Critical, and invisible before monitoring existed.**

A stalled watermark means the join stopped advancing event time. Records
arriving with timestamps beyond the frozen watermark are treated as late and
**dropped silently**. For a fraud alerting path, that means alerts that should
have fired did not — and nothing in the pipeline reported a problem, because
dropping late records is *correct behaviour* for a watermarked join. The
pipeline was doing exactly what it was told.

## 3.3 Root Cause Analysis

Under investigation. The stream-stream join watermarks both sides — transactions
on `transaction_timestamp`, watchlist on `effective_from`, five minutes each.
The join's watermark is the **minimum** of its inputs. If one input stops
producing records with advancing event times, the join's watermark freezes even
though the other side is flowing normally.

`silver.fraud_watchlist` was last written 25 hours ago while
`silver.transactions` was last written 684 hours ago — so the transaction side
is the stalled input, and the pipeline has not ingested new transactions since
mid-July.

## 3.4 Metrics

| Metric | Value |
|---|---|
| Watermark lag | 606.3 hours (~25 days) |
| Reported update state | SUCCESS, every run |
| Alerts produced during stall | 0 new |
| Detection method | `stream_progress` event log telemetry |

## 3.5 Solution — designed, not yet applied

Remediation requires a checkpoint reset via full refresh, which truncates and
rebuilds. That is a deliberate, destructive operation and is **pending
confirmation that the Kafka topic still holds the source data** — running it
against an empty topic would destroy 4,432 rows and replace them with nothing.

The detection is permanent regardless: `FinGuard PAGE - watermark stalled`
evaluates hourly and reached `TRIGGERED` at 06:35Z on its real schedule.

## 3.6 Interview Angle

**Question:** "What is a watermark, and what goes wrong with them?"

Most candidates can define a watermark. Far fewer can name the failure mode:
**a watermark that stops advancing produces silent data loss while the query
reports healthy**, and in a multi-input join the watermark is the minimum across
inputs, so one quiet source freezes the whole operator.

**The strongest framing:** "My own monitoring found a 25-day-stale watermark in
my own pipeline that had been reporting success the entire time." That is a
better answer than any hypothetical, because it demonstrates the monitoring
works — the point of observability is finding things you did not know were
broken.

---

# 4. CHALLENGE: dbt silently ignored every optimization config

## 4.1 Description

Liquid clustering and Delta table properties were configured across the dbt
marts. `dbt run` reported `PASS=63 ERROR=0`. The configs appeared in
`target/manifest.json`. **None of them were applied to any table.**

## 4.2 Root Cause Analysis

dbt-databricks reads `liquid_clustered_by` and `tblproperties`. The models used
`cluster_by` and `table_properties` — the names used by other adapters.

dbt **accepts unknown config keys without complaint**, stores them in the
manifest, and ignores them. There is no warning. The manifest containing the key
is what makes this convincing: the config is demonstrably *there*, just not
*applied*.

## 4.3 How It Was Found

Not from the run output, which was green. From `DESCRIBE DETAIL`:

```sql
DESCRIBE DETAIL finguard.marts.fct_transactions;
-- clusteringColumns: []
-- properties: {}
```

## 4.4 Solution and Result

Corrected to the adapter's actual key names, then **verified by reading table
metadata rather than trusting the run result**:

```
clusteringColumns  [["customer_id"],["transaction_timestamp"]]
delta.autoOptimize.optimizeWrite    true
delta.autoOptimize.autoCompact      true
delta.tuneFileSizesForRewrites      true
```

## 4.5 Interview Angle

**Question:** "How do you know your optimizations are working?"

**The answer that lands:** "Because I checked the table, not the exit code. I
had a dbt run report 63 passing models while applying zero of the configured
optimizations — the configs were in the manifest and silently ignored, because
dbt accepts unknown keys." Then generalise: a tool reporting success tells you
it did *something*, not that it did *what you asked*. This is the same class as
Challenges 1, 2, 5 and 12.

---

# 5. CHALLENGE: Alert bound to a string column — would never fire

## 5.1 Description

While building the monitoring layer, a SQL Alert was created with the condition
`update_id GREATER_THAN 0`. `update_id` is a UUID string. The API returned 200,
the alert appeared correctly configured in the UI, and it would never have
fired under any circumstance.

## 5.2 Root Cause Analysis

`evaluation.source.name` must name a column the query returns, and the
comparison is numeric. Binding to a string column is accepted at creation and
fails silently at evaluation.

This was caught by a verification script asserting that every alert's bound
column is numeric — written *before* the alerts, on the assumption that
something would go wrong.

## 5.3 Solution

Every detector in `sql/ops/monitoring_checks.sql` now returns an explicit
numeric column as its **first** column, and that is what the alert binds to:
`failure_count`, `watermark_lag_hours`, `drop_pct_points`, `growth_multiple`.

Two related defects were found the same way:

- **v1 vs v2 API.** `/api/2.0/sql/alerts` (v1) has no `schedule` field and no
  email subscriptions. Alerts created there exist, return 200, look correct —
  and never evaluate, notifying nobody. The working endpoint is
  `/api/2.0/alerts` (v2). Discovered by probing, not from documentation.
- **Broken idempotency.** The v2 list endpoint returns `{"alerts": [...]}`; the
  code read `{"results": [...]}`. Every alert looked new, and a rerun would have
  silently duplicated all four. Caught by running `--dry-run` twice.

## 5.4 Interview Angle

**Question:** "How do you test alerting?"

**The answer:** "By proving the alert fires, not by proving it was created." I
created a throwaway alert scheduled two minutes out, watched it reach
`TRIGGERED` with a `last_evaluated_at` timestamp, confirmed the email arrived,
then deleted it. Three separate bugs in this build — string binding, wrong API
version, broken idempotency — all returned HTTP 200.

---

# 6. CHALLENGE: CDC fan-out in the stream-static join

## 6.1 Description

`gold/fraud_card_alert.py` enriches transactions with customer data by joining
`silver.customers`. That table is fed by Postgres CDC using `update_timestamp`
as the cursor, so it accumulates **one row per customer per change**, not one row
per customer.

Joining it directly fans out: a customer with three recorded changes multiplies
their transactions by three.

## 6.2 Why It Had Not Surfaced

The table currently holds exactly one row per customer, because no customer
attribute has been updated at source yet. The fault is latent and would appear
on the first update — in production, silently inflating alert counts.

## 6.3 Solution

Reduce to the latest row per `customer_id` before joining:

```python
latest_customer = (
    spark.read.table("finguard.silver.customers")
    .select("customer_id", "first_name", "last_name", "email",
            "transaction_limit", "silver_ingestion_timestamp")
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("customer_id")
              .orderBy(F.col("silver_ingestion_timestamp").desc())))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
```

Two optimizations ride along, both deliberate:

- **Column pruning before the shuffle.** `silver.customers` has 22 columns; the
  join needs 6. Delta is columnar, so naming them means 16 columns are never
  read *and* never moved across the network during the `customer_id` shuffle.
- **Broadcast the reduced side.** 1,002 rows × 6 columns is far under the 10MB
  threshold. Without it, Spark shuffles *both* sides on every micro-batch. This
  is the highest-value hint available in a stream-static join, because the
  shuffle would otherwise be paid on every trigger rather than once.

The broadcast comment records when it becomes wrong: it is a bet on driver
memory, and at a few million customers it turns into an OOM. A broadcast hint is
a statement about size that stops being true silently.

## 6.4 Interview Angle

**Question:** "What is a stream-static join and what goes wrong?"

**Scored on:** whether you know the static side is re-read every micro-batch,
and whether you spot that a CDC-fed table is *not* a dimension table — it is a
change log, and joining it fans out. The stronger candidate also notes what this
deliberately does not attempt: point-in-time attribution. A stream cannot look
up historical dimension versions without unbounded state, so that belongs in
the dimensional layer, where `dim_customer` carries SCD2 validity windows.

---

# 7. CHALLENGE: Credentials committed to a public repository

## 7.1 Description

A Confluent API key and secret, and a Gmail app password, were committed in
`notebooks/exploration/02_Setup_Secret_Scope.ipynb` and pushed to a public
GitHub repository. A Databricks PAT was later pasted into a chat session.

## 7.2 Impact

**Severity: High.** Public exposure of live credentials for a Kafka cluster and
an email account.

## 7.3 The Critical Lesson

**Deleting the file does not remediate the exposure.** Git history retains
every version. A credential pushed publicly must be treated as compromised the
moment it lands, and **rotation at the provider is the only remedy** —
`git rm`, force-push, and history rewriting all fail to help, because the value
may already have been cloned or indexed.

## 7.4 Solution

- Rotation at Confluent and Google (the only real fix).
- `.env` files gitignored; all credentials read from them at runtime.
- Secrets in a Databricks secret scope, stored as a JSON blob rather than
  individual keys so rotating a cluster means updating one secret.
- **Gitleaks in CI with `fetch-depth: 0`** — a shallow clone would miss a secret
  added and later removed, which is exactly the case that matters.

## 7.5 Interview Angle

**Question:** "You find a credential in your repo. What do you do?"

The immediate discriminator: candidates who say "remove it and force-push" have
not thought it through. The correct first action is **rotate**, then clean
history, then add the control that prevents recurrence. Stating the order — and
why deletion is insufficient — is the whole answer.

---

# 8. CHALLENGE: NULL semantics silently swallowed quarantine rows

## 8.1 Description

The quarantine table captures rows dropped by silver's expectations. The natural
filter — `WHERE NOT (rule1 AND rule2 AND ...)` — is correct for the current
rules and becomes a trap the moment anyone adds a value check.

## 8.2 Root Cause

Every current rule is an `IS NOT NULL` predicate, which is never itself NULL, so
plain negation works today. Add `amount > 0`, and a NULL `amount` makes the
conjunction NULL. `NOT NULL` is NULL. `WHERE` treats NULL as false.

That row is **dropped by the expectations and simultaneously missed by the
quarantine** — it vanishes with no record anywhere.

## 8.3 Solution

```python
return parsed.where(f"({passes_all}) IS NOT TRUE")
```

`IS NOT TRUE` evaluates NULL as "not true" and captures the row. Expressed as
SQL text rather than a Column method because **PySpark's `Column` has no
`isNotTrue()`** — a small API-surface trap worth knowing.

## 8.4 Interview Angle

**Question:** "Explain three-valued logic in SQL."

Nearly everyone can recite that NULL comparisons yield NULL. Far fewer connect
it to a concrete data-loss bug. The answer that lands: "I wrote a quarantine
table for rows that fail quality rules, and realised the filter would silently
miss the exact rows it exists to catch — as soon as anyone added a non-null
predicate. Writing it defensively cost nothing and removed a trap for whoever
adds the next rule."

---

# 9. CHALLENGE: 17 duplicate transactions from Kafka replay

## 9.1 Description

`silver.transactions` contained genuine duplicates: the same `transaction_id` at
different Kafka offsets. Verified concretely — `TXN266669` exists at partition 1,
offsets 146 and 392, with different event timestamps.

## 9.2 Root Cause

At-least-once delivery combined with offset replay. Bronze reads
`startingOffsets: earliest`, so a rebuilt table re-reads the retention window.

## 9.3 Solution — and where deduplication belongs

Bronze and silver **keep every copy deliberately**. They are the audit record,
and discarding a physically delivered message there would destroy the evidence
that redelivery happened.

The mart is where a transaction must mean one business event, so deduplication
happens in `fct_transactions`:

```sql
row_number() over (
    partition by transaction_id
    order by kafka_partition, kafka_offset desc
) as _dedup_rank
```

Highest offset wins — the most recently delivered copy most likely reflects an
upstream correction. Ordering by offset rather than event timestamp keeps the
choice **deterministic** when two copies share a timestamp.

## 9.4 Interview Angle

**Question:** "How do you achieve exactly-once?"

The senior answer refuses the premise: you get at-least-once delivery plus
idempotent processing, and "exactly-once" is a property of the *end state*, not
of delivery. The follow-up that separates candidates is **where** to
deduplicate. Doing it in bronze feels tidy and destroys your audit trail. Layer
boundaries carry different guarantees: bronze is evidence, the mart is truth.

---

# 10. CHALLENGE: SCD2 cold start — every dimension attribute NULL

## 10.1 Description

After building `dim_customer` as SCD2 from a dbt snapshot, every point-in-time
join from `fct_transactions` returned NULL for every dimension attribute. The
dimension had 1,002 rows and joined to nothing.

## 10.2 Root Cause

dbt stamps `dbt_valid_from` at the moment the snapshot **first runs**, not when
the record actually became true. Every row therefore starts life valid from
"today" — and the entire existing transaction history predates the first
snapshot. A point-in-time join asks for the version in force at
`transaction_timestamp`, and no version existed then.

## 10.3 Solution

Backdate the first version of each entity to the beginning of time:

```sql
case
    when row_number() over (
             partition by customer_id order by dbt_valid_from
         ) = 1
    then timestamp '1900-01-01 00:00:00'
    else dbt_valid_from
end as valid_from
```

Genuine later versions keep their real `dbt_valid_from`, so change tracking from
this point forward is accurate. Without this the dimension is **technically
correct and practically useless**.

## 10.4 Interview Angle

**Question:** "Walk me through implementing SCD2."

The cold-start problem is what separates someone who has *built* one from
someone who has *read about* one. The tell is the phrase "technically correct
and practically useless" — every row is valid, the SCD2 mechanics are right, and
every historical join returns NULL.

---

# 11. CHALLENGE: `shuffle.partitions` inert on stateful operators

## 11.1 Description

`spark.sql.shuffle.partitions: 16` is configured on the pipeline and documented
in `performance_optimization.md` as reducing scheduling overhead. Telemetry
showed stateful operators running at **200–800 partitions** — the
`symmetricHashJoin` at 800, fifty times the configured value.

## 11.2 Root Cause

**Stateful operators pin their partition count into the checkpoint at creation.**
State is partitioned by key, and changing the partition count would redistribute
every key, invalidating existing state. Spark therefore honours the count in the
checkpoint and ignores the current config.

The setting applies to stateless shuffles and to any operator whose checkpoint
was created after the config change. It cannot apply retroactively.

## 11.3 Impact and Honest Correction

**This corrects a claim in this project's own optimization document.** The
config was right, the reasoning was right, and the effect was partial in a way
nothing surfaced until the monitoring measured it. Realigning requires a full
refresh to create fresh checkpoints.

Documented in `observability.md` and the README rather than quietly edited out
of `performance_optimization.md`.

## 11.4 Interview Angle

**Question:** "You set `shuffle.partitions` and nothing changed. Why?"

A genuinely hard question. The answer requires knowing that **streaming state is
partitioned and the partitioning is baked into the checkpoint** — which is also
why changing the state store provider requires a fresh checkpoint. The strong
version adds: "I found this because my monitoring compared configured against
actual, and it disagreed with my own documentation."

---

# 12. CHALLENGE: Doubly-encoded JSON in the event log

## 12.1 Description

Building stream telemetry required reading `stream_progress` events from
`event_log()`. The natural extraction returned NULL for every field:

```sql
get_json_object(details, '$.stream_progress.metrics.watermark')  -- NULL
```

No error. Just NULL.

## 12.2 Root Cause

`stream_progress.progress_json` is **a JSON string nested inside the details
JSON** — doubly encoded. One `get_json_object` pass reaches the string; the
payload requires a second decode.

```sql
WITH decoded AS (
    SELECT get_json_object(details, '$.stream_progress.progress_json') AS pj
    FROM event_log('...')
    WHERE event_type = 'stream_progress'
)
SELECT CAST(get_json_object(pj, '$.eventTime.watermark') AS TIMESTAMP) AS watermark
FROM decoded WHERE pj IS NOT NULL
```

## 12.3 Why This Belongs in a Failure Report

It is silent failure again, in its purest form. A monitoring table built on the
single-decode path fills with NULL watermarks, every stall detector goes quiet,
and **the monitoring appears healthy precisely because it is broken**. Found by
dumping the raw payload rather than trusting the path that "should" work.

## 12.4 Interview Angle

**Question:** "How do you monitor a Databricks pipeline?"

Naming `event_log()` and the `system.lakeflow` tables is a decent answer. Adding
"and the progress payload is doubly encoded, so the obvious query returns NULL
rather than erroring" demonstrates you have actually done it.

---

# 13. CHALLENGE: Liquid clustering — a measured negative result

## 13.1 Description

Liquid clustering was applied to `fct_transactions` on `customer_id` and
`transaction_date`, with a before/after benchmark run to demonstrate the gain.

**The benchmark appeared to show an 8–13% improvement. That result was not
real.**

## 13.2 Root Cause of the False Positive

`fct_transactions` is a single 129KB file. Clustering works by **skipping
files**, and there is exactly one file, so it can skip nothing. The measured
difference was SQL warehouse warm-up between the two runs.

## 13.3 What Was Done

The 8–13% figure is **not claimed anywhere in this project**. The clustering
keys remain configured, because they are correct for the access pattern and
become effective as the table grows past one file. The results table records:

> Query timing improvement: **Not claimed** — latency-dominated at this scale

## 13.4 Interview Angle

**Question:** "Tell me about an optimization that did not work."

This is the most valuable entry in the document. Most candidates cannot answer
it, because most portfolio projects claim wins nobody measured.

The answer demonstrates three things at once: you benchmark rather than assume;
you understand *why* clustering needs multiple files; and you are willing to
report a result you did not want. An interviewer who hears "I predicted a gain,
measured it, found it was warm-up, and removed the claim" learns more about how
you work than any success story.

**The generalisable lesson:** an optimization that cannot be measured at your
data volume should be documented as correct-but-unmeasurable, not presented as a
win. That is why this project's optimization document has three tiers.

---

# 14. CHALLENGE: Duplicate table definitions after refactor

## 14.1 Description

After restructuring the repository, a pipeline update failed with two files
defining `finguard.bronze.fraud_watchlist`.

## 14.2 Root Cause

Deployment was a sync script that **only uploaded**. Files deleted locally
persisted in the workspace indefinitely. The old file still defined the table,
the new file also defined it, and the pipeline saw both.

Nothing in the repository could reveal this — the local tree was correct.

## 14.3 Solution

Databricks Asset Bundles. The distinction is not convenience: a bundle
**reconciles** state while a sync script only adds to it. `bundle deploy`
removes what is no longer declared.

Pipeline source globs moved into `resources/finguard_pipeline.yml`, so the paths
became part of the diff. Previously they lived only in the workspace UI and were
invisible to code review — which is how they came to point at the old
directories after the restructure with nothing to reveal it.

## 14.4 Interview Angle

**Question:** "How do you deploy Databricks code?"

The discriminator is **reconciliation versus upload**. Anyone can describe
copying files. The senior point is that deployment state must converge on the
declaration, including deletions, or the workspace accumulates ghosts that no
code review can see.

---

# 15. CHALLENGE: Liquid clustering requires stats on its own keys

## 15.1 Description

The clustering build broke twice with an error stating a clustering column had
no statistics.

## 15.2 Root Cause

Delta collects statistics only for the first N columns (`dataSkippingNumIndexedCols`,
default 32). Liquid clustering requires **every clustering column to have
statistics**.

Two distinct failures:

1. `alert_timestamp` sat at roughly column 52 in a wide table — beyond the
   default budget.
2. **Self-inflicted:** setting `dataSkippingNumIndexedCols: 12` as an
   optimization excluded `transaction_timestamp` at column 14 from a table that
   clusters on it.

The second is the instructive one: **lowering the stats budget silently
constrains which columns can be clustered.** Two independently reasonable
optimizations that conflict.

## 15.3 Interview Angle

**Question:** "What are the limits of liquid clustering?"

Knowing that clustering depends on statistics, and that the stats budget is
itself tunable and interacts with it, is a level of Delta internals most
candidates do not reach. The strong version is the self-inflicted case: two
optimizations, each correct alone, that broke the build together.

---

# 16. CHALLENGE: Kafka authentication failure blocks the whole DAG

## 16.1 Description

A Kafka authentication failure halted the entire pipeline update, including
datasets with no Kafka dependency — customer CDC ingestion, merchant files,
watchlist processing.

## 16.2 Root Cause

All ingestion ran in a single pipeline. A failure anywhere failed the update.
Blast radius was defined by **convenience** — everything in one place — rather
than by **dependency**.

## 16.3 Solution

Customer silver ingestion split into its own pipeline
(`finguard_customers_pipeline`), because it is fed by Postgres CDC rather than
Kafka. A Kafka outage now cannot block customer updates.

The orchestration reflects this: `ingest_streaming` and `ingest_customers` are
independent tasks. `collect_metrics` depends on both with `run_if: ALL_DONE` —
because **the run worth diagnosing is the run that failed**, and a collector
that only executes on success gathers evidence for every case except the one
that matters.

## 16.4 Interview Angle

**Question:** "How do you decide what belongs in one pipeline?"

Answer on **blast radius and failure independence**, not on logical grouping.
The `run_if: ALL_DONE` detail is worth volunteering — it shows you have thought
about what monitoring does when things break, which is the only time monitoring
matters.

---

# 17. CHALLENGE: Cross-cluster Kafka migration invalidated checkpoints

## 17.1 Description

Migrating to a different Confluent cluster caused the stream to fail on startup.
Offsets stored in the checkpoint referenced a topic that no longer existed in
the new cluster.

## 17.2 Root Cause

A Structured Streaming checkpoint stores **committed offsets per topic
partition**, meaningful only relative to the cluster that issued them. Point the
same checkpoint at a different cluster and the stored offsets are nonsense.

## 17.3 The Tempting Wrong Fix

`failOnDataLoss: false` makes the error disappear. It also **permanently
disables the alarm for genuine data loss** — the stream will thereafter skip
silently past missing data forever.

The framework keeps it strict:

```python
reader.option("failOnDataLoss", str(cfg.get("fail_on_data_loss", True)).lower())
```

Real migrations are handled by resetting the checkpoint deliberately.

## 17.4 Interview Angle

**Question:** "What is in a streaming checkpoint, and when must you reset it?"

Reset is required for: changing Kafka cluster, changing the state store
provider, and changing stateful operator partitioning (Challenge 11). The
strongest addition is naming `failOnDataLoss: false` as the tempting wrong fix
and explaining why it is worse than the error — it converts a loud one-time
failure into permanent silence.

---

# RESULTS & ACHIEVEMENTS

## Performance improvements (measured)

The single measurable win at this data volume: **small-file compaction**.

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

**85% fewer files, 44.6% less storage, identical row counts.**

The storage reduction is the half most people do not predict. Compaction is
usually explained as "fewer file handles" — a read-path argument. But Parquet
compresses *within* a row group: 19 small files each carry their own dictionary,
column statistics and footer. Merging them lets the dictionary encode across all
93 rows at once. That is where the 90% went.

## Security posture

| Metric | Before | After |
|---|---|---|
| Column masks | 0 | 10 |
| Classification tags (column) | 0 | 28 |
| PAN visible to non-privileged caller | 16 digits | last 4 |
| Tagged-sensitive-but-unmasked | n/a | 0 |
| Secret scanning in CI | none | gitleaks, full history |

## Observability

| Metric | Value |
|---|---|
| Telemetry rows collected | 49 runs, 501 expectations, 585 stream-health |
| Scheduled alerts | 4 (2 hourly PAGE, 2 daily TICKET) |
| Alert delivery | verified end-to-end via throwaway alert |
| Dashboard panels | 6, all returning data |
| **Real defects found on first run** | **2** |

## Numbers deliberately not claimed

| Metric | Status |
|---|---|
| Query latency improvement | **Not claimed** — latency-dominated at this scale |
| Throughput at production volume | **Not claimed** — never tested above ~5 txn/sec |
| Cost savings from optimization | **Not claimed** — variance exceeds effect at this spend |
| PCI-DSS compliance | **Not claimed** — 3.3 display masking only, not 3.5 at-rest |

---

# LESSONS LEARNED

**1. Success is not a signal; verification is.** Five separate incidents here
returned HTTP 200 or a green run while doing nothing: dbt configs, the string-bound
alert, the v1 alert API, the secret lookup, the doubly-encoded event log. The
common remedy is asserting on *observable state* — `DESCRIBE DETAIL`, a
`TRIGGERED` status, a masked value read back — rather than on the absence of an
error.

**2. Write the check that assumes you failed.** The PII verification query that
searches for PAN-shaped columns *regardless of tagging* found two leaks
immediately after the first six masks were verified working. A check that only
validates what you built confirms your assumptions; a check that looks for what
you missed finds bugs.

**3. Layer boundaries carry different guarantees.** Bronze is evidence and must
keep duplicates. The mart is truth and must remove them. Detection belongs in
streaming; point-in-time attribution belongs in the dimensional layer. Most
correctness bugs here came from doing the right operation at the wrong layer.

**4. Configuration is not effect.** `shuffle.partitions: 16` was set correctly
and applied to 16 of 800 partitions on stateful operators. A setting in a config
file is a request, not a guarantee.

**5. Report the negative results.** The clustering benchmark showed a gain that
was warehouse warm-up. Removing that claim cost a nice number and bought
something worth more: every remaining number is defensible.

---

# APPENDIX

## Supporting documentation

| Document | Contents |
|---|---|
| [Performance optimization](performance_optimization.md) | 19 optimizations in three tiers: measured, correct-but-unmeasurable, rejected |
| [Observability & monitoring](observability.md) | Event-log telemetry, detectors, alerting, runbook |
| [Data governance](data_governance.md) | PII masking, classification, access model, verification |
| [Metadata-driven ingestion](metadata_driven_ingestion.md) | Framework design, closure binding, batch vs streaming |
| [Interview preparation](interview_preparation.md) | Scenario-driven Q&A across all rounds |

## Verification artifacts

| Artifact | Purpose |
|---|---|
| `sql/governance/03_pii_verification.sql` | 6 assertions on the masking controls |
| `sql/ops/monitoring_checks.sql` | 6 detectors, zero rows = healthy |
| `tests/` | 42 pytest tests |
| `.github/workflows/ci.yml` | gitleaks, ruff, pytest, bundle validate, dbt build |

## Known open items

- `bronze.transactions.value` retains PANs inside JSON — needs producer-side
  tokenization.
- Masks do not survive a Lakeflow full refresh — re-run
  `01_pii_masking.sql` after any refresh.
- Watermark stall remediation pending confirmation the Kafka topic retains data.
- CI has never executed: no pull request has been opened, and the `dbt-build`
  job is gated on `pull_request`.
