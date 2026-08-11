# Engineering Challenges & Optimization

Every entry is a failure that actually occurred in this project, with the real
error text, the diagnosis, the fix, and what makes it not happen again.
Nothing here is hypothetical, and one entry is a negative result — an
optimization I predicted, measured, and could not demonstrate.

**How to use this in an interview.** Each entry has an **Interview angle**
section naming the question it answers and what the interviewer is scoring.
The questions cluster around a small number of themes — silent failure, NULL
semantics, unbounded state, deployment state drift — and those themes are
worth more than the individual anecdotes.

---

## Contents

| # | Challenge | Class |
|---|---|---|
| 1 | Silent secret failure — fraud alerts never sent | Silent failure |
| 2 | `__file__` undefined in a Lakeflow pipeline | Runtime model |
| 3 | Python cannot import a NOTEBOOK object | Deployment |
| 4 | Duplicate table definitions after refactor | Deployment state drift |
| 5 | Kafka authentication failure blocks the DAG | Blast radius |
| 6 | Credentials committed to a public repository | Security |
| 7 | Producer and pipeline disagreed on topic name | Config drift |
| 8 | CDC fan-out in the stream-static join | Correctness |
| 9 | 17 duplicate transactions from Kafka replay | Exactly-once |
| 10 | SCD2 cold start — every dimension attribute NULL | Dimensional modeling |
| 11 | NULL semantics silently swallowed quarantine rows | SQL correctness |
| 12 | `isNotTrue()` does not exist in PySpark | API surface |
| 13 | Liquid clustering — a negative result | Optimization |
| 14 | Git Bash path mangling broke dbt connections | Tooling |
| 15 | Cross-cluster Kafka migration invalidated checkpoints | Streaming state |

---

## The deployment that failed six times

The pipeline update history for the refactor session:

```
11:25:54  FAILED   e4950eb9   NameError: __file__ is not defined
11:26:18  FAILED   8a30e497   (same, retry)
11:26:49  FAILED   f07b8f28   ImportError: cannot import name 'build_all'
11:27:40  FAILED   2da013a6   (same, retry)
11:29:08  FAILED   d0901d8f   DLTAnalysisException: Found duplicate table
11:32:00  FAILED   11cb2c4e   SaslAuthenticationException (Kafka creds)
```

Six consecutive failures, four distinct root causes. Each got further than the
last.

**That progression is the useful part.** A deployment that fails *differently*
each time is making progress; one that fails identically is not. It is worth
saying out loud in an interview, because it reframes a list of failures as a
debugging method.

---

## 1. Silent secret failure meant fraud alerts were never sent

**Problem.** The fraud email notifier ran without error and reported success on
every batch. No emails were ever delivered.

**Error.** None. That was the problem.

**Cause.** In `fraud_card_alert_email_notifier.py`:

```python
APP_PASSWORD = dbutils().secrets().get("finguard-scope", "gmail_api_key")
```

`dbutils` is an object, not a callable, and `secrets` is an attribute, not a
method. This raises `TypeError` immediately. But it sat inside a bare
`try/except Exception` at module scope, so the exception was swallowed,
`APP_PASSWORD` became `None`, and every batch hit:

```python
if APP_PASSWORD is None:
    print("Gmail API key not available, skipping email notifications")
    return
```

The pipeline reported healthy. The alerting path — the entire point of a fraud
detection system — was dead.

**Fix.** Construct `DBUtils` from the active session, inside the batch
function:

```python
from pyspark.dbutils import DBUtils
app_password = DBUtils(df.sparkSession).secrets.get(
    scope="finguard-scope", key="gmail_api_key"
)
```

**Optimization.** Three changes beyond the immediate fix:

- Secret resolution moved *inside* the batch function. At module scope the
  credential is captured in a closure that gets serialised to executors;
  inside, it stays on the driver. This is a security improvement, not just a
  correctness one.
- Failure is per-batch and logged with the batch id, so it surfaces in the
  event log rather than once at import where nobody looks.
- The empty-batch check short-circuits before any secret work, avoiding a
  secret fetch per micro-batch when there is nothing to send.

**Prevention.** Never wrap credential retrieval in a bare `except` that
degrades to a no-op. If a secret is required, failing loudly is correct.

**Interview angle.** *"Tell me about a bug that was hard to find."* The
interviewer is scoring whether you identify the real defect. The typo is
trivial; the `except` is the actual bug, and the lesson is that a pipeline
reporting success while doing nothing is worse than one that crashes.

---

## 2. `__file__` is not defined in a Lakeflow pipeline

**Problem.** The config-driven bronze entry point failed instantly on
deployment, though it worked locally.

**Error.**
```
File ".../bronze_ingestion.py", cell 1, line 19
NameError: name '__file__' is not defined
```

**Cause.** The file located the project root with:

```python
_SRC = Path(__file__).resolve().parents[3]
```

Lakeflow does not import pipeline files as modules. It compiles and runs them:

```python
exec(compiled_code, sys.modules['bronze_ingestion_py_6759526e'].__dict__)
```

`exec()` does not set `__file__`. The same code resolves fine under a normal
import — which is exactly why local testing missed it.

**Fix.** Walk up from the working directory looking for a marker, with an
explicit fallback:

```python
def _find_project_root() -> Path:
    for candidate in [Path.cwd(), *Path.cwd().parents]:
        if (candidate / "src" / "pipelines" / "framework").is_dir():
            return candidate
    return Path("/Workspace/Users/<user>/finguard_project")
```

**Optimization.** Long term this belongs in a Databricks Asset Bundle, where
the root is a deployment variable rather than something discovered at runtime.
Path discovery is a workaround for not having deployment configuration — now
addressed, since the project deploys via bundle.

**Prevention.** Assume nothing about the execution model of a managed runtime.
`__file__`, `__name__`, and the working directory all differ between import,
`exec()`, and notebook execution.

**Interview angle.** *"Have you hit something that worked locally but failed in
production?"* Scoring whether you understand that managed runtimes are not
plain Python.

---

## 3. Python cannot import a NOTEBOOK object

**Problem.** After fixing path resolution, the import itself failed.

**Error.**
```
ImportError: cannot import name 'build_all' from 'pipelines.framework'
(unknown location)
```

"unknown location" is the tell: Python resolved the *namespace* but found no
module file behind it.

**Cause.** Files were uploaded with `{'format': 'SOURCE', 'language':
'PYTHON'}`, which creates a **NOTEBOOK** workspace object. Notebooks are not
importable modules:

```
__init__.py        NOTEBOOK   <- not importable
bronze_factory.py  NOTEBOOK
readers.py         NOTEBOOK
```

**Fix.** Upload importable modules with `format: AUTO` so they remain plain
`FILE` objects.

**The non-obvious part:** overwriting does **not** change an existing object's
type. The notebooks had to be deleted first, then re-uploaded. An overwrite
returns 200 and changes nothing about the type.

**Optimization.** The distinction is now explicit in the deployment config:
anything under `framework/` is a file, pipeline entry points are notebooks.

**Prevention.** Verify with `workspace/list` after uploading. A 200 response
means the request succeeded, not that the right kind of object exists.

**Interview angle.** *"How do you debug an ImportError you have never seen?"*
The "unknown location" detail is the diagnostic — it distinguishes "module not
found" from "namespace found, no module behind it."

---

## 4. Duplicate table definitions after a refactor

**Problem.** With imports working, the pipeline failed during analysis.

**Error.**
```
DLTAnalysisException: Found duplicate table 'finguard.bronze.fraud_watchlist'
```

**Cause.** The refactor replaced two per-source files with one config-driven
entry point. The old files were deleted locally, but the sync tooling only
*uploads* — it never removes remote files absent from the source tree. The
pipeline glob matched three files:

```
bronze_ingestion.py         <- defines fraud_watchlist from config
fraud_watchlist_bronze.py   <- stale, defines the same table
finguard_bronze.py          <- stale, defines transactions
```

Two definitions of one table is a hard failure at analysis time — which is the
*good* outcome. A silent last-writer-wins would have been worse.

**Fix.** Delete the stale remote files and the superseded workspace tree.

**Optimization.** One-way sync is the underlying defect. Asset Bundles
reconcile state rather than accumulating it: deleting a file locally removes it
remotely. That is the real fix and is now in place; manual deletion was a
patch.

**Prevention.** Treat the workspace as deployment *output*, never as a place
where files accumulate. If the deploy mechanism cannot delete, it is not a
deploy mechanism.

**Interview angle.** *"What did you change permanently?"* — the highest-signal
behavioural follow-up. Deleting the files fixes the instance; moving to
declarative deployment fixes the class.

---

## 5. Kafka authentication failure blocks the whole DAG

**Problem.** With every code defect resolved, the pipeline reached `RUNNING`,
built two tables, then failed.

**Error.**
```
kafkashaded.org.apache.kafka.common.errors.SaslAuthenticationException:
Authentication failed.
```

**Cause.** Not a code defect. Confluent Cloud rejected the API key held in the
Databricks secret scope — credentials rotated at the provider without the
scope being updated.

The framework was working correctly. The error trace proves it reached a live
Kafka subscription:

```
Current Start Offsets: {KafkaV2[Subscribe[credit_card_transcations]]:
  {"0":733,"1":725,"2":723,"3":758,"4":741,"5":772}}
```

Config parsed, secret resolved, connection attempted, six partitions
discovered, offsets read. Only authentication failed.

**Blast radius.** One failed bronze table skipped eight downstream flows:

| Flow | Outcome |
|---|---|
| `bronze.merchants` | COMPLETED |
| `bronze.fraud_watchlist` | COMPLETED |
| `silver.fraud_watchlist` | COMPLETED |
| `silver.merchants` | COMPLETED |
| `bronze.transactions` | **FAILED** |
| `silver.transactions` | SKIPPED |
| `gold.fraud_card_alert` | SKIPPED |
| `gold.high_value_transactions_alert` | SKIPPED |
| `gold.transaciton_count_by_minute` | SKIPPED |
| `gold.…_sliding_window` | SKIPPED |

**Fix.** Update the secret scope with current Confluent credentials.

**Optimization.** The sources that did not depend on Kafka completed normally.
That is correct behaviour — a single upstream failure should not stop unrelated
ingestion — and it is why the framework isolates registration failures per
source rather than aborting the whole build.

**Prevention.** Credential rotation must update every consumer, not just the
provider. A secret scope holding a stale key is indistinguishable from a
correct one until something uses it.

**Interview angle.** *"How do you limit blast radius?"* The answer is that
blast radius should be bounded by *dependency*, not by process. Eight skipped
flows is correct when they genuinely need transactions; the improvement is
shrinking the dependency graph, not catching the error.

---

## 6. Credentials committed to a public repository

**Problem.** `02_Setup_Secret_Scope.ipynb` contained live credentials in
plaintext, committed and pushed to a public GitHub repository: a Confluent API
key and secret, a Gmail app password, and `print(api_token)` which wrote a
Databricks PAT into saved notebook output.

**Cause.** The notebook was written as a working script with values inline.

**Fix.** **Rotation, not redaction.** Removing the values from the file does
not remove them from git history — every prior commit still contains them and
remains retrievable indefinitely. The credentials must be invalidated at the
provider.

**Optimization.** Configuration read from `dbutils.widgets` or environment
variables so no secret is ever literal in a file. Notebook outputs stripped
before commit, since `print()` of a token persists in the `.ipynb` JSON.

**Prevention.** Secret scanning in CI (`gitleaks`, `trufflehog`) as a
pre-commit hook or pipeline gate. Cheap, and it eliminates the entire class.

**Interview angle.** *"Tell me about a security mistake."* Scoring whether you
know that deleting the file is not a fix. Candidates who say "I removed it from
the repo" fail this question.

---

## 7. Producer and pipeline disagreed on the topic name

**Problem.** Latent misconfiguration: transactions could be produced to a topic
nothing consumed.

**Cause.** Two names in two places:

| Location | Value |
|---|---|
| Secret scope / Confluent | `credit_card_transcations` (typo) |
| `config.py` default | `credit_card_transactions` (correct) |

With `TOPIC_NAME` unset, the producer defaults to the correctly spelled name,
writes to a topic the pipeline never subscribes to, and bronze stays empty. No
error anywhere — the producer reports successful delivery and the pipeline
reports zero new rows. Both are "working."

**Fix.** Pin `TOPIC_NAME` explicitly in `.env` to the name that actually exists.

**Optimization.** A default that differs from the production value is a trap. A
required setting with no default fails fast; a wrong default fails silently.
Config validation should reject an unset topic rather than substitute a guess.

**Interview angle.** *"What's wrong with default values in config?"* The
general principle: defaults are appropriate for *tuning* parameters and
dangerous for *identity* parameters. Getting a batch size wrong is slow;
getting a topic name wrong is silent.

---

## 8. CDC fan-out in the stream-static join

**Problem.** Customer enrichment in the gold alert tables would duplicate
alerts as soon as any customer record changed at source.

**Cause.** The original code:

```python
customers = spark.read.table("finguard.silver.customers")
```

`silver.customers` is fed by Postgres CDC with `update_timestamp` as the
cursor, so it accumulates **one row per customer per change**, not one row per
customer. Joining directly fans out: a customer with three recorded changes
multiplies their transactions by three.

For `high_value_transactions_alert` the comparison is
`amount > transaction_limit`, so the fan-out would also emit duplicate alerts
evaluated against *stale limits* — an alert firing against a limit that is no
longer in effect.

**Why it had not surfaced:** the table currently holds exactly one row per
customer. The fault appears the first time any customer attribute is updated at
source. This is the most dangerous class of bug — correct today, wrong later,
with no code change in between.

**Fix.** Reduce to the latest row per customer before joining:

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

Note `Window` imports from `pyspark.sql`, not `pyspark.sql.functions` — a
detail that costs a deployment cycle if you get it wrong.

**Optimization — and the architectural decision.** This deliberately does *not*
attempt point-in-time attribution. A stream cannot look up historical dimension
versions without unbounded state.

The boundary:

| Layer | Enrichment | Rationale |
|---|---|---|
| Gold streaming | Current state | An analyst responding to a live alert needs current contact details |
| dbt marts | Point-in-time SCD2 | Historical analysis must reflect the profile in effect at transaction time |

Two consumers, two definitions of correct, two tables.

**Interview angle.** *"Stream-static or stream-stream?"* and the senior
follow-up *"shouldn't alerts use the profile at transaction time?"* Scoring
whether you understand that correctness is defined by the consumer. Also a
strong answer to *"tell me about a bug that hadn't broken yet."*

---

## 9. Seventeen duplicate transactions from Kafka replay

**Problem.** The dbt uniqueness test on `fct_transactions.transaction_id`
failed with 17 duplicates.

**Cause.** Kafka is at-least-once. Verified directly: `TXN266669` exists at
partition 1, offset **146** and offset **392** — the same logical transaction
delivered twice, because a producer retry or consumer replay re-sent it.

Bronze correctly stored both: it is an append-only log of what arrived, and
discarding the second copy at ingest would lose the evidence that a replay
happened.

**Fix.** Deduplicate in the mart, keeping the latest offset:

```sql
select * from (
    select *, row_number() over (
        partition by transaction_id
        order by kafka_partition, kafka_offset desc) as _dedup_rank
    from source_transactions
) where _dedup_rank = 1
```

**Optimization.** Deduplication belongs in the mart, not bronze:

- Bronze stays a faithful record of what Kafka delivered, replay artefacts
  included, so the duplication rate is itself measurable.
- The mart is where a business key must be unique, and where a `MERGE` on
  `transaction_id` needs one row per key or it fails.

Ordering by `kafka_offset desc` is deliberate: within a partition, higher
offset means later arrival, so the most recent version wins.

**Prevention.** The uniqueness test *is* the prevention. This bug was invisible
in every layer that did not assert uniqueness — the pipeline was green.

**Interview angle.** *"After a restart, some messages processed twice — bug or
expected?"* Expected, and the answer is idempotency, not elimination. Having a
concrete offset pair to cite is far stronger than reciting the theory.

---

## 10. SCD2 cold start — every dimension attribute NULL

**Problem.** After building the dimensional layer, all 4,396 rows in
`fct_transactions` had NULL customer and merchant attributes. Joins returned
nothing.

**Cause.** The point-in-time join is correct:

```sql
ON f.customer_id = d.customer_id
AND f.transaction_timestamp >= d.valid_from
AND f.transaction_timestamp <  d.valid_to
```

But `dbt_valid_from` for the first snapshot version was **the date the snapshot
first ran** — 2026-08-10 — while the transactions were from 2026-07-13. Every
fact fell *before* the first validity window and matched nothing.

The dimension was not wrong. It honestly recorded "I first observed this
customer on 2026-08-10." The fact predates observation.

**Fix.** Backdate the first version of each entity to a sentinel:

```sql
case when row_number() over (
         partition by customer_id order by dbt_valid_from) = 1
    then timestamp '1900-01-01 00:00:00'
    else dbt_valid_from
end as valid_from,
```

**Optimization.** The semantics are defensible: we do not know when this
customer's first version became true, only that it was in effect before we
started observing. A sentinel makes that explicit rather than pretending the
snapshot date is meaningful.

The alternative — backfilling snapshot history from a source audit trail — is
better when the source *has* one. Postgres here does not retain pre-CDC
history.

**Prevention.** Any SCD2 built by snapshotting a live source has this problem
on day one. It is not a bug in dbt; it is the gap between when data was created
and when you started watching.

**Interview angle.** *"How do you handle late-arriving facts against an SCD2
dimension?"* The window join is the expected answer; the cold-start problem is
the follow-up most candidates have not hit. Naming it unprompted is a strong
signal.

---

## 11. NULL semantics silently swallowed quarantine rows

**Problem.** Rows failing silver quality rules were dropped by expectations
*and* missed by the quarantine table — disappearing with no record anywhere.

**Cause.** The quarantine filter was the natural negation of the drop rules:

```sql
WHERE NOT (transaction_id IS NOT NULL AND amount > 0)
```

In SQL three-valued logic, if `amount` is NULL then `amount > 0` is NULL, the
whole conjunction is NULL, and `NOT NULL` is **NULL**. `WHERE` treats NULL as
false, so the row is not selected into quarantine — while the expectation
independently drops it.

The row is gone. No error, no count, no evidence it ever arrived.

**Fix.** `IS NOT TRUE` rather than `NOT (...)`:

```python
return parsed.where(f"({passes_all}) IS NOT TRUE").select(...)
```

`IS NOT TRUE` evaluates to true for both false *and* NULL, which is exactly the
intended semantics: "did not definitively pass."

**Optimization.** Every rule in the current rule set is an `IS NOT NULL` check,
which can never itself be NULL — so plain negation works *today*. The defensive
form costs nothing and removes the trap for whoever adds `amount > 0` later.

The per-rule attribution uses the same construct, so the recorded
`failed_rules` string matches the filter exactly:

```python
F.when(F.expr(f"({rule}) IS NOT TRUE"), F.lit(name))
```

**Prevention.** Whenever a filter negates a predicate that can be NULL, use
`IS NOT TRUE` (or `IS NOT FALSE`). This is the SQL equivalent of a null-pointer
bug and just as easy to miss in review.

**Interview angle.** *"Is `NOT (predicate)` the inverse of the predicate?"* No,
not with NULLs — and the failure mode here is the best possible illustration:
the safety net and the filter both miss the same row, so the row vanishes
entirely.

---

## 12. `isNotTrue()` does not exist in PySpark

**Problem.** After fixing the NULL semantics, both quarantine tables failed at
runtime.

**Error.**
```
TypeError: 'Column' object is not callable
```

**Cause.** I wrote `F.expr(rule).isNotTrue()`. `isNotTrue` is a **Scala**
`Column` method. PySpark's `Column` has no such attribute, so Python resolved
`isNotTrue` via `__getattr__` to a *column reference* named `isNotTrue`, then
tried to call it.

That is why the error is `'Column' object is not callable` rather than
`AttributeError` — PySpark's `Column.__getattr__` returns a nested-field
accessor for any unknown name, so typos become columns rather than errors.

**Fix.** Express it as SQL text, which has the semantics natively:

```python
F.when(F.expr(f"({rule}) IS NOT TRUE"), F.lit(name))
```

**Optimization.** Where the Python API and SQL diverge, SQL text through
`F.expr` is often the more portable choice — the SQL standard is stable across
Spark versions, while Column methods differ between the Scala and Python APIs.

**Prevention.** Do not assume the PySpark `Column` API mirrors Scala's. And
know that `Column.__getattr__` silently accepts any name, so a typo becomes a
column reference rather than an immediate error.

**Interview angle.** *"Have you hit a PySpark/Scala API difference?"* The
diagnostic value is explaining *why* the error message was so misleading —
`__getattr__` turning a typo into a nested-field accessor.

---

## 13. Liquid clustering — a negative result

**Status:** measured, and the hypothesis was not supported.

**Hypothesis.** Applying liquid clustering on
`(customer_id, transaction_date)` to `fct_transactions` would improve
point-lookup and range-scan performance.

**Method.** Four representative queries, five runs each, median reported
(a single timing on a serverless warehouse is dominated by scheduling noise).
File statistics from `DESCRIBE DETAIL`, before and after `ALTER TABLE ...
CLUSTER BY` plus `OPTIMIZE`.

**Result.** 8–13% apparent improvement.

**Why that number is not real.** `DESCRIBE DETAIL` reported the table as **one
file, 129KB**. Clustering optimises which files a query can skip; with a single
file there is nothing to skip. The measured difference was warehouse warm-up
between the two measurement rounds, not data layout.

To test fairly I built a 5-million-row synthetic table. It also compacted to a
single file, so the same limitation applied.

**Conclusion.** Liquid clustering could not be demonstrated at this project's
data scale. The benchmark tables were dropped and **no number is claimed**.

**What this actually teaches.** The useful finding is about when the
optimization applies at all:

| Condition | Does clustering help? |
|---|---|
| Table is a single file | No — nothing to prune |
| Table below the file-size target (~1GB) | Minimal |
| High-cardinality filter columns, many files | Yes — the intended case |
| Query patterns unstable | Yes, and better than partitioning: keys can be redefined without rewriting data |
| Low-cardinality column, stable patterns | Partitioning may still be simpler |

Databricks recommends liquid clustering as the default for *new* tables, and
that recommendation is sound — it avoids the over-partitioning failure mode
where a high-cardinality partition column produces thousands of tiny files.
None of that is measurable here.

**Optimization that would apply at scale.** For a genuinely large
`fct_transactions`, cluster on the columns that actually appear in predicates —
`customer_id` for investigation lookups, `transaction_date` for reporting
ranges — and rely on incremental `OPTIMIZE`. The reason to prefer it over
partitioning by date is that the keys can change later without rewriting
history.

**Interview angle.** *"Tell me about a time you were wrong"* — and one of the
strongest available answers, because it shows measuring rather than assuming,
recognising that a favourable number was an artefact, and reporting a negative
result instead of a fabricated win.

An interviewer who hears "I measured an 8% improvement from liquid clustering"
on a 129KB table will know it is noise. Saying so first is the difference
between credibility and the opposite.

---

## 14. Git Bash path mangling broke dbt connections

**Problem.** dbt could not connect to the Databricks SQL warehouse. The error
was a bare HTTP 404 on `OpenSession` with no useful detail.

**Cause.** MSYS/Git Bash rewrites arguments that look like Unix paths into
Windows paths. `DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>` starts with `/`,
so it became:

```
C:/Users/17105/AppData/Local/Programs/Git/sql/1.0/warehouses/<id>
```

dbt sent that as the HTTP path. The server had no such endpoint, so it returned
404 — technically accurate and completely unhelpful, because the error says
nothing about the path having been rewritten.

**Fix.** Disable the conversion before setting the variable:

```bash
export MSYS_NO_PATHCONV=1
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>
```

**Optimization.** `set_env.sh` sets this and echoes the resolved values, so a
mangled path is visible immediately rather than surfacing as a 404 later.

**Prevention.** On Windows with Git Bash, any environment variable holding a
value that begins with `/` is a candidate for mangling. When a config value
arrives at a server transformed into something containing `Program Files` or
`Git`, this is why.

**Interview angle.** *"Describe a hard-to-diagnose environment issue."* The
signal is recognising that the 404 was a *symptom* — the request was
well-formed and genuinely pointed at nothing.

---

## 15. Cross-cluster Kafka migration invalidated the checkpoint

**Problem.** After migrating from a Confluent cluster in `asia-south1` to one
in `us-east1`, `bronze.transactions` failed even with valid credentials.

**Error.**
```
Some data may have been lost because they are not available in Kafka any more
Reason: Partitions HashSet(credit_card_transcations-5, ..., -0) have been deleted
```

**Cause.** Two distinct problems, in sequence.

First, `SaslAuthenticationException`: Confluent API keys are scoped to a single
cluster, so the old key could not authenticate against the new one. Not
obvious — the key is valid, just not *there*.

Second, after fixing credentials, the streaming checkpoint still held committed
offsets for the old topic on the old cluster. Those partitions do not exist on
the new cluster, so Spark refused to proceed. It cannot distinguish "the topic
moved" from "data was silently lost," and defaulting to failure is correct.

**Fix.** Full refresh, resetting the checkpoint so the stream starts clean
against the new topic.

**The option deliberately not taken:** `failOnDataLoss: false` silences the
guard and would have "worked." It is wrong here — it permanently disables a
real data-loss alarm to work around a one-time migration. The cost of the wrong
fix is that the *next* genuine data loss is silent.

**Optimization.** A cluster migration is a checkpoint-invalidating change and
should be planned as one:

- Drain or accept the loss of in-flight data before cutting over.
- Recreate topics on the target cluster with matching partition counts —
  partition count affects key-to-partition assignment and therefore ordering
  guarantees.
- Reset the checkpoint deliberately, not by disabling the safety check.
- Verify region alignment. A workspace in `asia-south1` reading from a broker
  in `us-east1` pays cross-region latency and egress on every message.

**Prevention.** Treat checkpoints as coupled to the source's identity, not just
its data. Changing cluster, topic name, or partition count invalidates the
checkpoint's meaning even when the schema is identical.

**Interview angle.** *"How do you migrate a streaming source without losing
data?"* Scoring whether you recognise `failOnDataLoss: false` as a trap. It
makes the error disappear, which is not the same as solving the problem.

---

## Cross-cutting themes

**Silent failure is the dominant risk.** Items 1, 7, 8, 9 and 11 all produce
plausible-looking success while doing the wrong thing. Items 1 and 11 are the
worst kind: the safety mechanism itself fails silently. Loud failure is a
feature, and any code path that degrades to a no-op on error deserves scrutiny.

**Three-valued logic breaks intuitive negation.** Item 11 is the SQL analogue
of a null-pointer bug: `NOT (x)` is not the complement of `x` when `x` can be
NULL. In a quality-control context this is especially dangerous, because the
filter and the safety net fail *together* on the same row.

**Managed runtimes are not plain Python.** Items 2, 3 and 12 come from assuming
standard semantics — `__file__` exists, uploading a `.py` creates a module,
the PySpark API mirrors Scala's.

**Deployment must reconcile, not accumulate.** Item 4 is the direct
consequence of one-way sync and the argument for declarative deployment. "What
did you change permanently" has a real answer here.

**Correctness is defined by the consumer.** Item 8 is not a bug with one right
answer. The operational path needs current state; the analytical path needs
point-in-time. Recognising that two consumers need two different tables is the
senior-level judgement.

**Measure before claiming.** Item 13 is the only entry where the hypothesis
failed, and it is the most useful one. The instinct to report the favourable
8–13% was strong; `DESCRIBE DETAIL` showed it was noise. An optimization you
cannot demonstrate at your data scale is an optimization you should not claim.

**Blast radius should follow dependency, not process.** Item 5 shows correct
behaviour — unrelated sources completed while a genuinely dependent chain
stopped. The improvement is shrinking the dependency graph, not catching the
error.

---

## Sources

- [Use liquid clustering for tables — Databricks](https://docs.databricks.com/aws/en/tables/clustering)
- [Debunking 8 data layout myths: why Liquid Clustering outperforms partitioning — Databricks](https://www.databricks.com/blog/debunking-8-data-layout-myths-why-liquid-clustering-outperforms-partitioning)
- [How to Choose Between Liquid Clustering and Partitioning with Z-Order — Canadian Data Guy](https://www.canadiandataguy.com/p/optimizing-delta-lake-tables-liquid)
- [Structured Streaming Programming Guide — Apache Spark](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Use foreachBatch to write to arbitrary sinks — Databricks](https://docs.databricks.com/aws/en/structured-streaming/foreach)
- [Delta Lake table streaming reads and writes — Databricks](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)
- [Kafka Consumer Scenario-Based Questions for Experienced Engineers](https://medium.com/@javalearners/kafka-consumer-scenarios-based-interview-questions-for-exprienced-engineers-48aac972e6d2)
- [10 Data Modeling Problems That Gate Senior DE Offers — DataExpert](https://dataexpert.medium.com/10-data-modeling-problems-that-gate-senior-de-offers-67be8712ce3f)
- [dbt snapshots and hard_deletes — dbt Developer Hub](https://docs.getdbt.com/reference/resource-configs/hard-deletes)
- [Ultimate Guide to Behavioral Data Engineer Interviews — DataExpert](https://www.dataexpert.io/blog/ultimate-guide-behavioral-data-engineer-interviews)
