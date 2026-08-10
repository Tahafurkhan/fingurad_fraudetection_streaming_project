# Engineering Challenges

Every entry below is a failure that actually occurred in this project, with the
real error text, the diagnosis, the fix, and what would make it not happen
again. Nothing here is hypothetical.

The pipeline update history for the refactor session reads:

```
11:25:54  FAILED   e4950eb9   NameError: __file__ is not defined
11:26:18  FAILED   8a30e497   (same, retry)
11:26:49  FAILED   f07b8f28   ImportError: cannot import name 'build_all'
11:27:40  FAILED   2da013a6   (same, retry)
11:29:08  FAILED   d0901d8f   DLTAnalysisException: Found duplicate table
11:32:00  FAILED   11cb2c4e   SaslAuthenticationException (Kafka creds)
```

Six consecutive failures, four distinct root causes. Each one got further than
the last. That progression is the useful part: a deployment that fails
differently each time is making progress; one that fails identically is not.

---

## 1. Silent secret failure meant fraud alerts never sent

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

The pipeline reported healthy. The alerting path was dead.

**Fix.** Construct `DBUtils` from the active session, inside the batch function:

```python
from pyspark.dbutils import DBUtils
app_password = DBUtils(df.sparkSession).secrets.get(
    scope="finguard-scope", key="gmail_api_key"
)
```

**Optimisation.** Three changes beyond the immediate fix:

- Secret resolution moved *inside* the batch function. At module scope the
  credential is captured in the closure that gets serialised to executors;
  inside, it stays on the driver.
- Failure is now per-batch and logged with the batch id, so it surfaces in the
  event log instead of once at import.
- The empty-batch check short-circuits before any secret work.

**Prevention.** Never wrap credential retrieval in a bare `except` that
degrades to a no-op. If a secret is required, failing loudly is correct — a
pipeline that "succeeds" while doing nothing is worse than one that stops.

---

## 2. `__file__` is not defined in a Lakeflow pipeline

**Problem.** The new config-driven bronze entry point failed instantly on
deployment, though it worked locally.

**Error.**
```
File ".../src/pipelines/streaming/bronze/bronze_ingestion.py", cell 1, line 19
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
import, which is exactly why local testing missed it.

**Fix.** Locate the root by walking up from the working directory, looking for
a marker directory, with an explicit fallback:

```python
def _find_project_root() -> Path:
    for candidate in [Path.cwd(), *Path.cwd().parents]:
        if (candidate / "src" / "pipelines" / "framework").is_dir():
            return candidate
    return Path("/Workspace/Users/<user>/finguard_project")
```

**Optimisation.** Long term this belongs in a Databricks Asset Bundle, where
the root is a deployment variable rather than something discovered at runtime.
Path discovery is a workaround for not having deployment configuration.

**Prevention.** Assume nothing about the execution model of a managed runtime.
`__file__`, `__name__`, and the working directory all differ between import,
`exec()`, and notebook execution.

---

## 3. Python cannot import a NOTEBOOK object

**Problem.** After fixing the path resolution, the import itself failed.

**Error.**
```
ImportError: cannot import name 'build_all' from 'pipelines.framework'
(unknown location)
```

"unknown location" is the tell: Python resolved the *namespace* but found no
module file behind it.

**Cause.** Files were uploaded with:

```python
{'format': 'SOURCE', 'language': 'PYTHON'}
```

which creates a **NOTEBOOK** object in the workspace. Notebooks are not
importable modules. Listing the directory confirmed it:

```
__init__.py        NOTEBOOK   <- not importable
bronze_factory.py  NOTEBOOK
readers.py         NOTEBOOK
```

**Fix.** Upload importable modules with `format: AUTO` so they remain plain
`FILE` objects. Note that overwriting does **not** change an existing object's
type — the notebooks had to be deleted first, then re-uploaded.

**Optimisation.** The distinction is now explicit in the sync tooling: anything
under `framework/` uploads as a file, pipeline entry points upload as
notebooks. Getting this wrong is invisible until import time.

**Prevention.** Databricks workspace objects have types, and the upload format
determines them. Verify with `workspace/list` after uploading, not by assuming
a 200 response means the right thing was created.

---

## 4. Duplicate table definitions after a refactor

**Problem.** With imports working, the pipeline failed during analysis.

**Error.**
```
DLTAnalysisException: Found duplicate table 'finguard.bronze.fraud_watchlist'
```

**Cause.** The refactor replaced two per-source files with one config-driven
entry point. The old files were deleted locally, but the sync tooling only
*uploads* — it never removes remote files that no longer exist in the source
tree. So the pipeline glob matched three files:

```
bronze_ingestion.py         <- defines fraud_watchlist from config
fraud_watchlist_bronze.py   <- stale, defines the same table
finguard_bronze.py          <- stale, defines transactions
```

Two definitions of the same table is a hard failure at analysis time.

**Fix.** Delete the stale remote files, and the superseded workspace tree.

**Optimisation.** One-way sync is the underlying defect. Asset Bundles
(`databricks bundle deploy`) reconcile state rather than accumulating it —
deleting a file locally removes it remotely. That is the real fix; manual
deletion is a patch.

**Prevention.** Treat the workspace as deployment output, never as a place
where files are edited or accumulate. If the deploy mechanism cannot delete, it
is not a deploy mechanism.

---

## 5. Kafka authentication failure blocks the whole DAG

**Problem.** With every code defect resolved, the pipeline reached `RUNNING`,
built two tables, and then failed.

**Error.**
```
kafkashaded.org.apache.kafka.common.errors.SaslAuthenticationException:
Authentication failed.
```

**Cause.** Not a code defect. Confluent Cloud rejected the API key held in the
Databricks secret scope. The credentials had been rotated at the provider
without the secret scope being updated.

The framework was working correctly — the error trace proves it got all the way
to a live Kafka subscription:

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
| `bronze.transactions` | **FAILED** |
| `silver.transactions` | SKIPPED |
| `gold.fraud_card_alert` | SKIPPED |
| `gold.high_value_transactions_alert` | SKIPPED |
| `gold.transaciton_count_by_minute` | SKIPPED |
| `gold.…_sliding_window` | SKIPPED |

**Fix.** Update the secret scope with current Confluent credentials.

**Optimisation.** The sources that did not depend on Kafka completed normally.
That is correct behaviour — a single upstream failure should not stop unrelated
ingestion — and it is why the framework isolates registration failures per
source rather than aborting the whole build.

**Prevention.** Credential rotation needs to update every consumer, not just
the provider. A secret scope holding a stale key is indistinguishable from a
correct one until something tries to use it.

---

## 6. Credentials committed to a public repository

**Problem.** `02_Setup_Secret_Scope.ipynb` contains live credentials in
plaintext, committed and pushed to a public GitHub repository.

**Cause.** The notebook was written as a working script with values inline:
Confluent API key and secret, a Gmail app password, and `print(api_token)`
which writes a Databricks PAT into saved notebook output.

**Fix.** Rotation, not redaction. Removing the values from the file does not
remove them from git history — every prior commit still contains them and
remains retrievable. The credentials must be invalidated at the provider.

**Optimisation.** The notebook should take values from `dbutils.widgets` or
environment variables so no secret is ever literal in the file.

**Prevention.** Secret scanning in CI (`gitleaks`, `trufflehog`) as a
pre-commit hook or pipeline gate. This is cheap and catches the class of
mistake entirely.

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
writes to a topic the pipeline never subscribes to, and bronze silently stays
empty. No error anywhere — the producer reports successful delivery, the
pipeline reports zero new rows.

**Fix.** Pin `TOPIC_NAME` explicitly in `.env` to the name that actually exists
in Confluent, with a comment explaining why it is misspelled.

**Optimisation.** Renaming the topic properly requires changing Confluent, the
secret scope, and `.env` together. Worth doing, but as a deliberate migration
rather than a silent edit.

**Prevention.** Defaults that differ from production values are a trap. A
required setting with no default fails fast; a wrong default fails silently.

---

## 8. Stream-static join against a streaming table

**Status:** known, not yet fixed. Documented here because it is a real defect.

**Problem.** In `gold/fraud_card_alert.py`:

```python
customers = spark.read.table("finguard.silver.customers")   # static read
```

**Cause.** `finguard.silver.customers` is itself a continuously updating
streaming table fed by Postgres CDC. Reading it with `spark.read` takes a
snapshot at query-plan time. Customer attributes that change after the stream
starts — `risk_score`, `customer_segment`, `transaction_limit` — are not
reflected in alerts.

**Impact.** A customer upgraded to high risk after the stream started is still
evaluated against their old profile. For a fraud system this is the wrong
answer, not merely a stale one.

**Planned fix.** Either a stream-static join that re-reads per micro-batch, or
a stream-stream join with an appropriate watermark. The correct choice depends
on whether alerts must reflect the profile *at transaction time* (which argues
for SCD2 and a point-in-time join) or the current profile.

This connects directly to the SCD2 work: the reason to build
`dim_customer` with history is precisely so a transaction can be evaluated
against the profile that was current when it occurred.

---

## Themes

**Silent failure is the dominant risk.** Items 1, 5, 7 and 8 all produce
plausible-looking success while doing the wrong thing. Loud failure is a
feature.

**Managed runtimes have their own execution model.** Items 2 and 3 come from
assuming standard Python semantics inside Lakeflow.

**One-way sync accumulates state.** Item 4 is the direct consequence, and the
argument for declarative deployment.

**Blast radius should be bounded by dependency, not by process.** Item 5 shows
correct behaviour: unrelated sources completed while a dependent chain
stopped.
