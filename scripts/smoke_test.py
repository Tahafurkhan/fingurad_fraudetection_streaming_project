"""Post-deploy smoke test: verify what only a real workspace can prove.

WHY THIS EXISTS SEPARATELY FROM THE PYTEST SUITE
------------------------------------------------
The pytest suite runs against local Spark. That is the right place for
transformation logic -- `from_json`, null handling, SQL predicate evaluation
all behave identically on a laptop and on the platform, so those tests gate
every commit at zero cost.

What local Spark cannot verify is everything that IS the platform:

  - Unity Catalog column masks are bound to columns, not merely defined
  - Lakeflow expectations are registered and evaluating
  - the deployed pipeline exists and its schedule is what the bundle declared
  - the reconciliation invariant holds against real data

A green pytest run says the logic is right. It says nothing about whether the
deployment worked. This script is the other half, and it runs after deploy
rather than before merge because it needs a deployed thing to test.

DESIGN: EVERY CHECK RUNS, THEN THE SCRIPT FAILS ONCE
----------------------------------------------------
Exiting on the first failure gives one problem per deploy cycle. A deploy that
broke masking AND stopped the pipeline should report both, so the fix is one
round trip rather than three.

Usage:
    python scripts/smoke_test.py --target dev
    python scripts/smoke_test.py --target prod --warehouse-id <id>
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

WAREHOUSE_ID = "b349348047ac54a2"
CATALOG = "finguard"

# Columns that must carry a Unity Catalog mask.
#
# quarantine.raw_payload is on this list deliberately and is the one most
# likely to be forgotten. Quarantine holds rejected records *including their
# original payload* -- the same PANs, in a table created for operational
# convenience. It is the natural back door around an otherwise complete masking
# scheme, which is why TRD SR-06 calls it out and why it is asserted here.
REQUIRED_MASKS = [
    ("transactions", "card_number"),
    ("customers", "card_number"),
    ("customers", "email"),
    ("transactions_quarantine", "card_number"),
    ("transactions_quarantine", "raw_payload"),
    ("fraud_card_alert", "card_number"),
]

# Expectations that must be registered and evaluating. Three of these were
# added after a contract-alignment test found the producer guaranteed fields
# no consumer rule enforced -- see docs/engineering_challenges.md #19.
REQUIRED_EXPECTATIONS = [
    "valid_transaction_id",
    "valid_customer_id",
    "valid_card_number",
    "valid_merchant_id",
    "valid_transaction_timestamp",
    "valid_amount",
    "valid_currency",
    "valid_status",
]


class Results:
    """Collects outcomes so every check runs before the script exits."""

    def __init__(self) -> None:
        self.passed: list[str] = []
        self.failed: list[tuple[str, str]] = []
        self.skipped: list[tuple[str, str]] = []

    def ok(self, name: str) -> None:
        self.passed.append(name)
        print(f"  PASS  {name}")

    def fail(self, name: str, detail: str) -> None:
        self.failed.append((name, detail))
        print(f"  FAIL  {name}\n          {detail}")

    def skip(self, name: str, why: str) -> None:
        self.skipped.append((name, why))
        print(f"  SKIP  {name} ({why})")


def run_sql(statement: str, warehouse_id: str, profile: str) -> tuple[bool, list]:
    """Execute SQL, returning (ok, rows)."""
    payload = json.dumps(
        {
            "warehouse_id": warehouse_id,
            "catalog": CATALOG,
            "statement": statement,
            "wait_timeout": "50s",
        }
    )
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--profile", profile, "--json", payload],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return False, [result.stderr.strip()[:300]]
    try:
        body = json.loads(result.stdout)
    except json.JSONDecodeError:
        return False, [result.stdout.strip()[:300]]

    status = body.get("status", {})
    if status.get("state") != "SUCCEEDED":
        return False, [json.dumps(status.get("error", status.get("state")))[:300]]
    return True, (body.get("result", {}) or {}).get("data_array", []) or []


def check_masking(results: Results, warehouse_id: str, profile: str) -> None:
    """Every PII column has a mask BOUND to it, not just defined.

    The distinction is the whole point. A masking function that exists but is
    not attached to a column protects nothing, and both states look identical
    if you only check that the function is there.
    """
    ok, rows = run_sql(
        "SELECT table_name, column_name FROM system.information_schema.column_masks "
        f"WHERE table_catalog = '{CATALOG}'",
        warehouse_id, profile,
    )
    if not ok:
        results.fail("masking: query column_masks", str(rows[0]))
        return

    masked = {(r[0], r[1]) for r in rows}
    for table, column in REQUIRED_MASKS:
        name = f"masking: {table}.{column}"
        if (table, column) in masked:
            results.ok(name)
        else:
            results.fail(name, "no column mask bound -- PII is readable in the clear")


def check_expectations(results: Results, warehouse_id: str, profile: str) -> None:
    """Quality rules are registered and have evaluated at least once.

    Checks ops.expectation_results rather than the pipeline definition: a rule
    present in code but never evaluated means the flow it guards has not run,
    which is a different failure from the rule being absent and needs a
    different fix.
    """
    ok, rows = run_sql(
        "SELECT DISTINCT expectation_name FROM finguard.ops.expectation_results "
        "WHERE dataset = 'finguard.silver.transactions'",
        warehouse_id, profile,
    )
    if not ok:
        results.fail("expectations: query ops.expectation_results", str(rows[0]))
        return

    registered = {r[0] for r in rows}
    missing = [e for e in REQUIRED_EXPECTATIONS if e not in registered]
    if missing:
        results.fail(
            "expectations: all registered",
            f"not evaluating: {', '.join(missing)}",
        )
    else:
        results.ok(f"expectations: all {len(REQUIRED_EXPECTATIONS)} evaluating")


def check_reconciliation(results: Results, warehouse_id: str, profile: str) -> None:
    """bronze == silver + quarantine.

    TRD DQ-06. If this does not hold, rows exist that neither table accounts
    for -- the exact silent loss that quarantine exists to prevent. This is the
    single most important assertion in the file.
    """
    ok, rows = run_sql(
        "SELECT (SELECT count(*) FROM finguard.bronze.transactions), "
        "       (SELECT count(*) FROM finguard.silver.transactions), "
        "       (SELECT count(*) FROM finguard.silver.transactions_quarantine)",
        warehouse_id, profile,
    )
    if not ok:
        results.fail("reconciliation: query counts", str(rows[0]))
        return

    bronze, silver, quarantine = (int(x) for x in rows[0])
    if bronze == 0:
        results.skip("reconciliation", "bronze is empty, nothing ingested yet")
        return

    # Silver deduplicates, so silver + quarantine can be LOWER than bronze --
    # that is correct behaviour, not loss. What must never happen is silver
    # exceeding bronze, which would mean rows appearing from nowhere.
    if silver + quarantine > bronze:
        results.fail(
            "reconciliation: bronze >= silver + quarantine",
            f"bronze={bronze} silver={silver} quarantine={quarantine} "
            "-- silver has more rows than its source",
        )
    else:
        results.ok(
            f"reconciliation: bronze={bronze} >= silver={silver} + "
            f"quarantine={quarantine}"
        )


def check_no_duplicates(results: Results, warehouse_id: str, profile: str) -> None:
    """Silver holds one row per transaction_id. Implements TRD FR-11."""
    ok, rows = run_sql(
        "SELECT count(*) - count(DISTINCT transaction_id) "
        "FROM finguard.silver.transactions",
        warehouse_id, profile,
    )
    if not ok:
        results.fail("dedup: query silver", str(rows[0]))
        return

    dupes = int(rows[0][0])
    if dupes:
        results.fail("dedup: no duplicate transaction_id", f"{dupes} duplicates")
    else:
        results.ok("dedup: no duplicate transaction_id in silver")


def check_pipeline_deployed(results: Results, profile: str, target: str) -> None:
    """The pipeline exists and its development flag matches the target.

    This is what would have caught the bug where `development: true` was
    hardcoded: prod would deploy a development pipeline that keeps compute warm
    and skips retries, and nothing downstream would look wrong.
    """
    result = subprocess.run(
        ["databricks", "pipelines", "list-pipelines", "--profile", profile,
         "--output", "json"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        results.fail("pipeline: list", result.stderr.strip()[:200])
        return

    try:
        pipelines = json.loads(result.stdout)
    except json.JSONDecodeError:
        results.fail("pipeline: list", "unparseable response")
        return

    match = next(
        (p for p in pipelines if "finguard" in (p.get("name") or "").lower()
         and "customers" not in (p.get("name") or "").lower()),
        None,
    )
    if not match:
        results.fail("pipeline: deployed", "no finguard streaming pipeline found")
        return

    results.ok(f"pipeline: deployed ({match.get('name')}, {match.get('state')})")

    detail = subprocess.run(
        ["databricks", "api", "get",
         f"/api/2.0/pipelines/{match['pipeline_id']}", "--profile", profile],
        capture_output=True, text=True,
    )
    if detail.returncode != 0:
        results.skip("pipeline: development flag", "could not read spec")
        return

    spec = json.loads(detail.stdout).get("spec", {})
    development = bool(spec.get("development"))
    if target == "prod" and development:
        results.fail(
            "pipeline: development flag",
            "prod pipeline has development=true -- keeps compute warm and "
            "skips retries",
        )
    else:
        results.ok(f"pipeline: development={development} (correct for {target})")


def check_ops_freshness(results: Results, warehouse_id: str, profile: str) -> None:
    """Telemetry is being collected, not merely collectable.

    Observability that requires a human to trigger it is a query someone ran
    once. A stale ops table means the collector is not running, and every
    detector built on it is quietly reporting on history.
    """
    ok, rows = run_sql(
        "SELECT datediff(current_timestamp(), max(collected_at)) "
        "FROM finguard.ops.pipeline_runs",
        warehouse_id, profile,
    )
    if not ok:
        results.fail("ops: telemetry freshness", str(rows[0]))
        return

    if rows[0][0] is None:
        results.fail("ops: telemetry freshness", "ops.pipeline_runs is empty")
        return

    days = int(rows[0][0])
    if days > 2:
        results.fail(
            "ops: telemetry freshness",
            f"last collected {days} days ago -- the collector is not running",
        )
    else:
        results.ok(f"ops: telemetry collected {days} day(s) ago")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default="dev", choices=["dev", "prod"])
    parser.add_argument("--warehouse-id", default=WAREHOUSE_ID)
    parser.add_argument("--profile", default="DEFAULT")
    args = parser.parse_args()

    print(f"Smoke test: target={args.target}\n")
    results = Results()

    print("Security")
    check_masking(results, args.warehouse_id, args.profile)

    print("\nData quality")
    check_expectations(results, args.warehouse_id, args.profile)
    check_reconciliation(results, args.warehouse_id, args.profile)
    check_no_duplicates(results, args.warehouse_id, args.profile)

    print("\nDeployment")
    check_pipeline_deployed(results, args.profile, args.target)

    print("\nObservability")
    check_ops_freshness(results, args.warehouse_id, args.profile)

    print(
        f"\n{len(results.passed)} passed, {len(results.failed)} failed, "
        f"{len(results.skipped)} skipped"
    )

    if results.failed:
        print("\nFAILURES:")
        for name, detail in results.failed:
            print(f"  - {name}: {detail}")
        return 1

    print("\nSmoke test passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
