# Metadata-Driven Bronze Ingestion

## Why

The original bronze layer had one Python file per source. Both files did the
same four things -- open a stream, select columns, stamp an ingestion
timestamp, write a table -- and differed only in configuration. Onboarding a
source meant copying a file and editing it, which does not scale and drifts:
the two files had already diverged in how they named and ordered audit columns.

Bronze is now generated from declarative config. Adding a source is a YAML file.

## What is and isn't generated

Metadata-driven design pays off where logic is uniform and only configuration
varies. It becomes a liability where the logic is genuinely per-source, because
you end up inventing a transformation DSL that is strictly worse than SQL.

So the boundary is deliberate:

| Layer | Approach | Reason |
|---|---|---|
| Bronze | Generated from YAML | Raw ingestion is uniform: read, project, stamp |
| Silver | Explicit Python per source | Cleaning rules are source-specific business logic |
| Marts | dbt models | SQL-first, analyst-contributable |

`fraud_watchlist_silver` uppercases specific fields and parses a
`dd-MMM-yyyy HH:mm:ss` timestamp; `transactions_silver` parses a JSON envelope
against an explicit schema. Encoding either in YAML would obscure them.

## Layout

```
config/sources/           one YAML per source -- the registry
  transactions.yaml              kafka
  fraud_watchlist.yaml           autoloader (json)
  merchants.yaml                 autoloader (csv)
  customers.yaml                 managed CDC (documented, not generated)
  merchant_risk_api.yaml.example rest_api template (inert until renamed)

src/pipelines/framework/
  source_config.py        load + validate config, fail fast
  readers.py              one reader per ingestion type + dispatch table
  bronze_factory.py       generate @dp.table per config

src/pipelines/streaming/bronze/
  bronze_ingestion.py     entry point the pipeline loads
```

## Streaming vs batch sources

Not every source is a stream. `rest_api` returns a bounded snapshot, so the
factory registers it as a **materialised view** rather than a streaming table:
full recomputation each update is the correct semantic for reference data,
whereas streaming append would duplicate every row on every run. `BATCH_TYPES`
in `readers.py` controls this split.

The REST reader handles pagination, bearer auth resolved from a secret scope,
and retry with exponential backoff on 429/5xx. Other 4xx responses fail
immediately, because a malformed request will not fix itself on retry. An empty
result raises rather than silently materialising an empty table over existing
data.

**Transactions deliberately stay on Kafka.** Polling REST for a 5 TPS event
stream would discard ordering, replay-from-offset and exactly-once semantics --
the properties that make the streaming layer worth having. REST is the right
tool for reference data, not for events.

## Adding a source

Create `config/sources/<name>.yaml`:

```yaml
name: merchant_feed
target: finguard.bronze.merchant_feed
comment: "Merchant reference data"
ingestion:
  type: autoloader
  path: /Volumes/finguard/source/merchants/
  format: json
  options:
    cloudFiles.inferColumnTypes: "true"
columns:
  - {name: merchant_id}
  - {name: merchant_name}
  - {name: source_file, expr: "_metadata.file_path"}
```

No Python changes. The next pipeline update picks it up.

Adding a new *ingestion type* (Kinesis, JDBC snapshot, Delta Share) means one
function in `readers.py` and one entry in the `READERS` dispatch table.

## Config in git, state in Delta

A metadata-driven pipeline needs two kinds of metadata, and they belong in
different places:

| | Where | Why |
|---|---|---|
| **Structural** — what sources exist, their type, target, columns | YAML in git | Needs diffs, PR review, env promotion. A bad change should fail in CI, not at 3am |
| **Operational** — what ran, when, how it went | Delta tables | Written at runtime by the pipeline. Queryable, dashboardable, alertable |

Putting source definitions in a table loses version history -- someone runs an
`UPDATE` and production changes with no diff to point at. Putting run history in
YAML is impossible; the pipeline would have to commit to git on every update.

Two tables carry the operational half:

- `finguard.ops.source_registry` — one row per source, fully replaced from the
  YAML on each update, so a deleted config disappears rather than lingering as
  a false claim.
- `finguard.ops.ingestion_audit` — one row per source per update, including
  `SKIPPED` and `FAILED`, so the record reflects what the platform was *asked*
  to do, not only what worked.

Queries live in `sql/ops/health_checks.sql`. The one that justifies the whole
design is the drift check: **declared in config but no bronze table exists.** A
source can be committed, reviewed and merged and still never produce a table --
the pipeline was not repointed, a volume was missing, an update failed quietly.
Grepping YAML cannot detect that; joining the registry against
`information_schema` can.

Registration failures are isolated per source. One malformed config records a
`FAILED` row and the remaining sources still build, because losing every table
to one bad config is worse than losing one. The ops writes themselves are
wrapped in try/except: observability must never be the reason ingestion fails.

## Design notes

**Closure binding.** `@dp.table` registers a function object, so generating N
tables requires N distinct closures. `build_bronze_table` takes `cfg` as a
parameter rather than defining the function inside the loop body -- otherwise
Python's late binding makes every generated table read the last config in the
list. This is the classic bug in loop-generated pipelines and the reason the
factory exists as a separate function.

**Validation fails at import.** A malformed config raises immediately with the
filename and the missing key, rather than producing a table that silently
ingests nothing. Three-level Unity Catalog names are enforced, because a
two-part name would quietly resolve against the pipeline's default schema.

**Managed ingestion is registered, not generated.** `customers` comes from
Postgres via a Lakeflow managed pipeline declared as a spec
(`resources/fingurad_ingestion.json`), so there is no `@dp.table` to build. It
still gets a config file, marked `managed: true`, so the registry describes
every bronze table rather than only the ones this framework happens to build.

**Secrets stay out of config.** Kafka connection details are read at runtime
from the Databricks secret scope. The YAML names the scope and key only.

## Verification

The generated output was diffed against the live Unity Catalog schemas before
the per-source files were removed: `finguard.bronze.transactions` (8 columns)
and `finguard.bronze.fraud_watchlist` (16 columns) both match exactly, in name
and order. Closure binding was tested by registering both tables against
stubbed readers and asserting each reads its own source.
