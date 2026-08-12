"""Apply a .sql file to a Databricks SQL warehouse, statement by statement.

WHY THIS EXISTS
---------------
The governance and ops SQL in this repo is idempotent DDL -- masking functions,
grants, ops views -- meant to be applied and re-applied. Doing that by pasting
into a notebook is not repeatable and leaves no record of what ran.

WHY THE SPLITTING IS NOT A ONE-LINER
------------------------------------
`sql.split(";")` is wrong for these files and fails in a way that produces
valid-looking garbage rather than an error. The comments in them are prose,
and prose contains semicolons and apostrophes. A naive split cuts statements
mid-comment and hands the warehouse fragments beginning "their sources do."

Stripping comments first and then splitting fails differently: an apostrophe
inside a comment ("Lakeflow's internal") is read as an unterminated string
literal once the surrounding comment marker is gone.

So the split respects SQL structure: track whether the cursor is inside a
string literal or a line comment, and only treat a semicolon as a terminator
when it is in neither. That is the minimum needed to be correct here, and it
is why this is a file rather than a shell pipeline.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def split_statements(sql: str) -> list[str]:
    """Split on statement-terminating semicolons only.

    Semicolons inside '...' literals or after -- on a line are text, not
    terminators. Comments are preserved in the output: the warehouse parses
    them fine, and keeping them means a failure message quotes the statement as
    it appears in the file.
    """
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    in_line_comment = False
    i = 0

    while i < len(sql):
        char = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
        elif in_string:
            current.append(char)
            # '' is an escaped quote inside a literal, not a close-then-open.
            if char == "'":
                if nxt == "'":
                    current.append(nxt)
                    i += 1
                else:
                    in_string = False
        elif char == "-" and nxt == "-":
            in_line_comment = True
            current.append(char)
        elif char == "'":
            in_string = True
            current.append(char)
        elif char == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(char)

        i += 1

    if current:
        statements.append("".join(current))

    # Keep only statements with executable content -- trailing comment blocks
    # after the final semicolon are not statements.
    return [
        s.strip()
        for s in statements
        if any(
            line.strip() and not line.strip().startswith("--") for line in s.split("\n")
        )
    ]


def run_statement(statement: str, warehouse_id: str, catalog: str, profile: str) -> tuple[bool, str]:
    """Execute one statement. Returns (ok, message)."""
    payload = json.dumps(
        {
            "warehouse_id": warehouse_id,
            "catalog": catalog,
            "statement": statement,
            "wait_timeout": "50s",
        }
    )
    result = subprocess.run(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "--profile",
            profile,
            "--json",
            payload,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False, result.stderr.strip()[:500]

    try:
        status = json.loads(result.stdout).get("status", {})
    except json.JSONDecodeError:
        return False, result.stdout.strip()[:500]

    if status.get("state") == "SUCCEEDED":
        return True, ""
    return False, json.dumps(status.get("error", status.get("state")))[:500]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sql_file", type=Path)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", default="finguard")
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List the statements that would run, without executing them.",
    )
    args = parser.parse_args()

    statements = split_statements(args.sql_file.read_text(encoding="utf-8"))
    print(f"{args.sql_file.name}: {len(statements)} statement(s)")

    failures = 0
    for n, statement in enumerate(statements, start=1):
        first_line = next(
            (
                line.strip()
                for line in statement.split("\n")
                if line.strip() and not line.strip().startswith("--")
            ),
            "",
        )
        label = f"  [{n}/{len(statements)}] {first_line[:70]}"

        if args.dry_run:
            print(f"{label}  (dry run)")
            continue

        ok, message = run_statement(
            statement, args.warehouse_id, args.catalog, args.profile
        )
        if ok:
            print(f"{label}  OK")
        else:
            failures += 1
            print(f"{label}  FAILED\n      {message}")

    # Every statement is attempted even after a failure. These files are
    # idempotent DDL, so a later statement failing does not invalidate an
    # earlier success, and seeing all the failures at once beats fixing them
    # one redeploy at a time.
    if failures:
        print(f"\n{failures} statement(s) failed.")
        return 1
    print("\nAll statements applied.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
