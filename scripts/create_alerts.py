"""Create or update the FinGuard monitoring alerts.

Alerts are workspace state, not files, so without this script they are
click-configured and invisible to review -- the same problem the Asset Bundle
solved for the pipeline. Running this is idempotent: an alert with a matching
display_name is updated rather than duplicated.

WHICH API
---------
Databricks exposes two alert APIs and they are not equivalent:

  /api/2.0/sql/alerts   (v1) has no schedule field and no email subscriptions.
                        An alert created here exists but never evaluates and
                        notifies nobody.

  /api/2.0/alerts       (v2) carries `schedule` and
                        `evaluation.notification.subscriptions` inline.

This uses v2. The distinction was found by probing, not from the field names:
v1 accepts a create call, returns 200, and produces an alert that looks correct
in the API response while being inert.

Note the payload is NOT wrapped in {"alert": {...}} for v2, unlike v1.

CONDITION BINDING
-----------------
`evaluation.source.name` must name a column that the query actually returns,
and the comparison is numeric. Binding to a string column -- update_id, a UUID
-- creates an alert that never fires, silently. Every detector therefore
returns an explicit numeric column as its first column, and that is what is
bound here. See sql/ops/monitoring_checks.sql.

Usage:
    python scripts/create_alerts.py [--dry-run]
"""

from __future__ import annotations

import configparser
import json
import os
import sys
import urllib.error
import urllib.request

WAREHOUSE_ID = "b349348047ac54a2"
NOTIFY_EMAIL = "tahafurkhan@gmail.com"
TIMEZONE = "Asia/Kolkata"


def _client():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    host = cfg["DEFAULT"]["host"].rstrip("/")
    token = cfg["DEFAULT"]["token"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return host, headers


def _api(host, headers, method, path, body=None):
    req = urllib.request.Request(
        host + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
        method=method,
    )
    try:
        return json.loads(urllib.request.urlopen(req).read() or b"{}")
    except urllib.error.HTTPError as exc:
        return {"ERROR": exc.code, "body": exc.read().decode()[:400]}


# ---------------------------------------------------------------------------
# Detector SQL. Kept in sync with sql/ops/monitoring_checks.sql -- that file is
# the readable reference; these are the deployed copies.
#
# Every query returns zero rows when healthy, and a numeric first column for
# the alert condition to bind to.
# ---------------------------------------------------------------------------

WATERMARK_STALLED = """-- PAGE: event-time watermark stalled or never advanced.
WITH latest_per_flow AS (
  SELECT flow_name, operator_name, watermark, event_timestamp,
         ROW_NUMBER() OVER (PARTITION BY flow_name ORDER BY event_timestamp DESC) AS rn
  FROM finguard.ops.stream_health
  WHERE watermark IS NOT NULL
)
SELECT
  ROUND((unix_timestamp(event_timestamp) - unix_timestamp(watermark)) / 3600.0, 1)
                    AS watermark_lag_hours,
  flow_name, operator_name, watermark,
  event_timestamp   AS last_seen,
  CASE WHEN watermark <= TIMESTAMP '1971-01-01'
       THEN 'NEVER_ADVANCED' ELSE 'STALLED' END AS failure_kind
FROM latest_per_flow
WHERE rn = 1
  AND (watermark <= TIMESTAMP '1971-01-01'
       OR watermark < event_timestamp - INTERVAL 6 HOURS)
ORDER BY watermark_lag_hours DESC"""

UPDATE_FAILED = """-- PAGE: a pipeline update failed in the last 24 hours.
SELECT
  count(*) OVER () AS failure_count,
  pipeline_name, update_id, event_timestamp,
  message          AS detail
FROM finguard.ops.pipeline_runs
WHERE state = 'FAILED'
  AND event_timestamp > current_timestamp() - INTERVAL 24 HOURS
ORDER BY event_timestamp DESC"""

EXPECTATION_DEGRADED = """-- TICKET: expectation pass rate below its own trailing baseline.
WITH per_run AS (
  SELECT dataset, expectation_name, update_id,
         max(event_timestamp) AS run_time,
         sum(passed_records)  AS passed,
         sum(failed_records)  AS failed
  FROM finguard.ops.expectation_results
  GROUP BY dataset, expectation_name, update_id
),
rates AS (
  SELECT *, passed / (passed + failed) AS pass_rate,
         ROW_NUMBER() OVER (PARTITION BY dataset, expectation_name
                            ORDER BY run_time DESC) AS recency
  FROM per_run WHERE passed + failed > 0
),
baseline AS (
  SELECT dataset, expectation_name,
         avg(pass_rate) AS baseline_rate, count(*) AS baseline_runs
  FROM rates WHERE recency BETWEEN 2 AND 11
  GROUP BY dataset, expectation_name
)
SELECT
  ROUND((b.baseline_rate - r.pass_rate) * 100, 2) AS drop_pct_points,
  r.dataset, r.expectation_name, r.update_id,
  ROUND(r.pass_rate * 100, 2)     AS current_pct,
  ROUND(b.baseline_rate * 100, 2) AS baseline_pct,
  r.failed                        AS failed_records
FROM rates r
JOIN baseline b USING (dataset, expectation_name)
WHERE r.recency = 1
  AND b.baseline_runs >= 3
  AND r.pass_rate < b.baseline_rate - 0.01
ORDER BY drop_pct_points DESC"""

STATE_GROWTH = """-- TICKET: streaming state far above its trailing median.
WITH recent AS (
  SELECT flow_name, operator_name, num_rows_total, event_timestamp,
         ROW_NUMBER() OVER (PARTITION BY flow_name ORDER BY event_timestamp DESC) AS rn
  FROM finguard.ops.stream_health
  WHERE num_rows_total IS NOT NULL AND operator_name IS NOT NULL
),
stats AS (
  SELECT flow_name,
         percentile_approx(num_rows_total, 0.5) AS median_rows,
         count(*) AS observations
  FROM recent WHERE rn BETWEEN 2 AND 21
  GROUP BY flow_name
)
SELECT
  ROUND(r.num_rows_total / s.median_rows, 2) AS growth_multiple,
  r.flow_name, r.operator_name,
  r.num_rows_total AS current_state_rows,
  s.median_rows    AS baseline_median_rows,
  r.event_timestamp
FROM recent r
JOIN stats s USING (flow_name)
WHERE r.rn = 1 AND s.observations >= 5 AND s.median_rows > 0
  AND r.num_rows_total > s.median_rows * 3
ORDER BY growth_multiple DESC"""


# Severity drives cadence, not just labelling. A PAGE check that runs daily is
# not a page. A TICKET check that runs every 10 minutes is a mailing list.
ALERTS = [
    {
        "name": "FinGuard PAGE - pipeline update failed",
        "sql": UPDATE_FAILED,
        "column": "failure_count",
        "cron": "0 5 * * * ?",          # hourly at :05
    },
    {
        "name": "FinGuard PAGE - watermark stalled",
        "sql": WATERMARK_STALLED,
        "column": "watermark_lag_hours",
        "cron": "0 5 * * * ?",          # hourly at :05
    },
    {
        "name": "FinGuard TICKET - expectation rate degraded",
        "sql": EXPECTATION_DEGRADED,
        "column": "drop_pct_points",
        "cron": "0 0 6 * * ?",          # daily 06:00
    },
    {
        "name": "FinGuard TICKET - state growth unbounded",
        "sql": STATE_GROWTH,
        "column": "growth_multiple",
        "cron": "0 0 6 * * ?",          # daily 06:00
    },
]


def _payload(spec):
    return {
        "display_name": spec["name"],
        "query_text": spec["sql"],
        "warehouse_id": WAREHOUSE_ID,
        "evaluation": {
            "source": {
                "name": spec["column"],
                "display": spec["column"],
                "aggregation": "FIRST",
            },
            "comparison_operator": "GREATER_THAN",
            "threshold": {"value": {"double_value": 0}},
            # Zero rows is the healthy state for every detector, so an empty
            # result must resolve to OK rather than to UNKNOWN. Left at
            # UNKNOWN, a recovered pipeline never clears the alert.
            "empty_result_state": "OK",
            "notification": {
                "subscriptions": [{"user_email": NOTIFY_EMAIL}],
                # Do not notify on recovery. Recovery mail doubles volume and
                # trains people to skim; the dashboard shows current state.
                "notify_on_ok": False,
            },
        },
        "schedule": {
            "quartz_cron_schedule": spec["cron"],
            "timezone_id": TIMEZONE,
            "pause_status": "UNPAUSED",
        },
    }


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    host, headers = _client()

    # v2 returns {"alerts": [...]}. v1 returns {"results": [...]}. Reading the
    # wrong key yields an empty dict, every alert looks new, and a rerun
    # silently duplicates all of them -- caught by running --dry-run twice.
    listing = _api(host, headers, "GET", "/api/2.0/alerts?page_size=100")
    existing = {
        a.get("display_name"): a.get("id") for a in listing.get("alerts", [])
    }

    for spec in ALERTS:
        payload = _payload(spec)
        alert_id = existing.get(spec["name"])

        if dry_run:
            action = "UPDATE" if alert_id else "CREATE"
            print(f"[dry-run] {action}: {spec['name']} "
                  f"(bind {spec['column']}, cron {spec['cron']})")
            continue

        if alert_id:
            result = _api(
                host, headers, "PATCH",
                f"/api/2.0/alerts/{alert_id}"
                "?update_mask=query_text,evaluation,schedule,warehouse_id",
                payload,
            )
        else:
            result = _api(host, headers, "POST", "/api/2.0/alerts", payload)

        if "ERROR" in result:
            print(f"FAILED  {spec['name']}: {result}")
        else:
            print(f"ok      {spec['name']}  id={result.get('id')}")


if __name__ == "__main__":
    main()
