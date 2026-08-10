# Setup

Getting this project running on a fresh machine.

## Prerequisites

- **Python 3.10+** (3.13 is what this was developed against)
- A **Databricks workspace** with Unity Catalog
- A **Confluent Cloud** cluster, if you want the transaction stream
- **git**

## 1. Clone and create a virtual environment

```bash
git clone https://github.com/Tahafurkhan/fingurad_fraudetection_streaming_project.git
cd fingurad_fraudetection_streaming_project

python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate
```

## 2. Install dependencies

```bash
pip install --upgrade pip
pip install -r src/producer/requirements.txt
pip install dbt-databricks pytest ruff pyyaml
```

For Asset Bundles you need the **modern** Databricks CLI, not the deprecated
`databricks-cli` PyPI package — the old one has no `bundle` command:

```bash
# macOS / Linux
brew tap databricks/tap && brew install databricks

# Windows: download the binary and put it on PATH
# https://github.com/databricks/cli/releases
```

Verify:

```bash
databricks --version   # expect v1.x, not 0.x
```

## 3. Authenticate to Databricks

Create a personal access token in the workspace under
**Settings → Developer → Access tokens**, then:

```bash
databricks configure --token --host https://<workspace-id>.cloud.databricks.com
```

This writes `~/.databrickscfg`. Verify with:

```bash
databricks catalogs list
```

## 4. Environment variables

Copy both templates and fill them in. Neither `.env` is committed.

```bash
cp .env.example .env
cp src/producer/.env.example src/producer/.env
```

**Project `.env`** — Databricks host, HTTP path, token, and Postgres details if
you use the CDC source.

**`src/producer/.env`** — Confluent bootstrap servers, API key/secret, and the
topic name.

> The topic name must match the value stored in the Databricks secret scope.
> If they differ, the producer writes to a topic the pipeline never reads and
> bronze silently stays empty, with no error anywhere.

## 5. Databricks secret scope

The pipeline reads Kafka credentials from a secret scope, not from `.env`.
Run `notebooks/exploration/02_Setup_Secret_Scope.ipynb` in the workspace after
setting these in the notebook session:

```
BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET, GMAIL_APP_PASSWORD
```

## 6. Unity Catalog objects

Create the catalog and schemas if they do not exist:

```sql
CREATE CATALOG IF NOT EXISTS finguard;
CREATE SCHEMA IF NOT EXISTS finguard.bronze;
CREATE SCHEMA IF NOT EXISTS finguard.silver;
CREATE SCHEMA IF NOT EXISTS finguard.gold;
CREATE SCHEMA IF NOT EXISTS finguard.marts;
CREATE SCHEMA IF NOT EXISTS finguard.snapshots;
CREATE SCHEMA IF NOT EXISTS finguard.ops;
CREATE SCHEMA IF NOT EXISTS finguard.source;

CREATE VOLUME IF NOT EXISTS finguard.source.merchants;
CREATE VOLUME IF NOT EXISTS finguard.source.fraud_watchlist;
CREATE VOLUME IF NOT EXISTS finguard.source.checkpoint;
```

## 7. Deploy the pipelines

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run finguard_pipeline -t dev
```

## 8. dbt

`transform/profiles.yml` reads everything from the environment, so no
credentials are stored in the repo.

```bash
cd transform
source set_env.sh        # exports host, http_path, token
dbt deps
dbt debug                # expect "All checks passed!"
dbt build
```

> **Git Bash on Windows:** `set_env.sh` sets `MSYS_NO_PATHCONV=1` before
> exporting `DATABRICKS_HTTP_PATH`. Without it, MSYS rewrites any value
> starting with `/` into a Windows path, and dbt fails with a bare HTTP 404 on
> `OpenSession` that gives no hint about the real cause.

On other shells, export the three variables directly:

```bash
export DATABRICKS_HOST="<workspace-id>.cloud.databricks.com"   # no https://
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<warehouse-id>"
export DATABRICKS_TOKEN="<token>"
```

Find the HTTP path under **SQL Warehouses → your warehouse → Connection
details**.

## 9. Generate data

```bash
cd src/producer

python upload_merchants.py     # merchant master -> UC volume
python producer_normal.py      # normal transaction traffic
python producer_fraud_card.py  # transactions on watchlisted cards
```

## 10. Run the checks

```bash
pytest tests/ -q          # 17 tests, no cluster needed
ruff check src/ tests/
cd transform && dbt test
```

## Troubleshooting

**`SaslAuthenticationException` on `bronze.transactions`** — Confluent rejected
the credentials in the secret scope. Rotating the key at the provider does not
update the scope; both have to change together.

**`Found duplicate table`** — two files define the same table. Usually a stale
file left in the workspace by a partial deploy. `bundle deploy` reconciles
state and removes what is no longer declared; the old sync script did not.

**`NameError: __file__ is not defined`** — Lakeflow runs pipeline files through
`exec()` rather than importing them, so `__file__` does not exist. Resolve
paths from the working directory instead.

**`ImportError: cannot import name X (unknown location)`** — the module was
uploaded as a NOTEBOOK object, which Python cannot import. Upload importable
modules with `format: AUTO` so they stay plain files. Overwriting does not
change an existing object's type; delete and re-upload.

**dbt `Database Error` with HTTP 404** — usually the mangled HTTP path on Git
Bash. See step 8.
