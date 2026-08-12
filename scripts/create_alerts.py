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

# Must match SLACK_DESTINATION_NAME in create_notification_destinations.py.
# The destination is resolved by name at runtime rather than by a hardcoded id,
# so rotating the Slack webhook (which recreates nothing but updates the same
# destination) needs no change here.
SLACK_DESTINATION_NAME = "FinGuard PAGE - Slack"


def _client():
    """Resolve host and auth headers.

    Prefers a personal access token from ~/.databrickscfg, falling back to the
    CLI's OAuth session. The fallback is not optional on a workspace configured
    for OAuth: there is no `token` key in the config file at all, and reading
    it raises KeyError before any API call is attempted.

    `databricks auth token` prints the current OAuth access token, refreshing
    it if needed, which is the supported way to borrow the CLI's session for
    raw HTTP.
    """
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    section = cfg["DEFAULT"]
    host = section["host"].rstrip("/")

    token = section.get("token")
    if not token:
        import subprocess

        result = subprocess.run(
            ["databricks", "auth", "token", "--profile", "DEFAULT"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise SystemExit(
                "No token in ~/.databrickscfg and `databricks auth token` "
                f"failed:\n{result.stderr.strip()[:300]}"
            )
        token = json.loads(result.stdout)["access_token"]

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


COST_SPIKE = """-- PAGE: daily spend far above its own trailing baseline.
WITH daily AS (
  SELECT u.usage_date AS usage_date,
         sum(u.usage_quantity * p.pricing.default) AS usd
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
         ON p.sku_name = u.sku_name AND p.currency_code = 'USD'
        AND p.price_end_time IS NULL
  WHERE u.usage_date > current_date() - 30
  GROUP BY u.usage_date
),
baseline AS (
  SELECT percentile_approx(usd, 0.5) AS median_usd, count(*) AS baseline_days
  FROM daily
  WHERE usage_date BETWEEN current_date() - 15 AND current_date() - 2
)
SELECT
  round(d.usd / nullif(b.median_usd, 0), 2) AS spend_multiple,
  d.usage_date,
  round(d.usd, 2)                AS usd_spent,
  round(b.median_usd, 2)         AS baseline_usd,
  round(d.usd - b.median_usd, 2) AS excess_usd
FROM daily d CROSS JOIN baseline b
WHERE d.usage_date = current_date() - 1
  AND b.baseline_days >= 7 AND b.median_usd > 0
  AND d.usd > b.median_usd * 3
  AND d.usd > 5.00"""

IDLE_COMPUTE = """-- TICKET: always-on compute attached to no job or pipeline.
SELECT
  round(sum(daily_dbu), 1) AS dbu_last_7_days,
  sku_name,
  round(min(daily_dbu), 1) AS daily_floor_dbu,
  round(max(daily_dbu), 1) AS daily_peak_dbu,
  count(*)                 AS days_with_usage
FROM (
  SELECT sku_name, usage_date, sum(usage_quantity) AS daily_dbu
  FROM system.billing.usage
  WHERE usage_date > current_date() - 8
    AND usage_metadata.dlt_pipeline_id IS NULL
    AND usage_metadata.job_id IS NULL
  GROUP BY sku_name, usage_date
)
GROUP BY sku_name
HAVING min(daily_dbu) > 5 AND count(*) >= 6
ORDER BY dbu_last_7_days DESC"""

BUDGET_PROJECTION = """-- TICKET: month-to-date spend projects past the monthly budget.
WITH mtd AS (
  SELECT sum(u.usage_quantity * p.pricing.default) AS usd_so_far,
         day(current_date())           AS days_elapsed,
         day(last_day(current_date())) AS days_in_month
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
         ON p.sku_name = u.sku_name AND p.currency_code = 'USD'
        AND p.price_end_time IS NULL
  WHERE u.usage_date >= date_trunc('MONTH', current_date())
)
SELECT
  round(usd_so_far / nullif(days_elapsed, 0) * days_in_month, 2) AS projected_month_usd,
  round(usd_so_far, 2) AS spent_so_far_usd,
  300.00               AS budget_usd,
  days_elapsed, days_in_month
FROM mtd
WHERE days_elapsed >= 5
  AND usd_so_far / nullif(days_elapsed, 0) * days_in_month > 300.00"""


# Severity drives cadence, not just labelling. A PAGE check that runs daily is
# not a page. A TICKET check that runs every 10 minutes is a mailing list.
#
# Cost checks run DAILY even the PAGE one, which looks inconsistent beside the
# hourly pipeline pages. It is deliberate: billing data lands with a lag of a
# few hours, so an hourly cost check re-reads the same incomplete day and pages
# repeatedly about one event. Cadence should match how fast the underlying
# signal can actually change, not how urgent the topic feels.
#
# ---------------------------------------------------------------------------
# Distribution and staleness detectors.
#
# These select from views in sql/ops/distribution_views.sql rather than
# inlining their logic. An alert stores its own copy of the query text, so
# inlining would mean editing the file leaves the alert running the old
# version with nothing reporting the divergence. One definition, in the view.
#
# Each view already returns rows only when something is wrong, so the alert
# query is a plain select and the bound column is the view's leading numeric.
# ---------------------------------------------------------------------------

MART_LAG = """-- PAGE: the marts have stopped tracking silver.
-- Written after finding fct_transactions a month stale while every other
-- check reported healthy -- the orchestration job was paused and nothing
-- watched whether it ran.
SELECT mart_lag_days, silver_newest, mart_newest, silver_rows, mart_rows
FROM finguard.ops.v_marts_lag"""

ALERT_RATE_SHIFT = """-- PAGE: fraud alert volume has collapsed or spiked.
-- Alerts stopping looks identical to fraud stopping, and the first is far
-- more likely. This is the detector that watches the detectors.
SELECT alert_rate_multiple, alerts_today, baseline_alerts_per_day,
       transactions_today, diagnosis
FROM finguard.ops.v_alert_rate_shift"""

AMOUNT_DISTRIBUTION = """-- TICKET: the transaction amount distribution has shifted.
-- Every row can be individually valid while the aggregate shape changes,
-- which is exactly what a fraud pattern shift looks like.
SELECT median_shift_multiple, p95_shift_multiple, baseline_median,
       current_median, baseline_p95, current_p95, current_rows
FROM finguard.ops.v_amount_distribution_shift"""

CATEGORICAL_MIX = """-- TICKET: a category's share of volume has moved materially.
-- A fraud ring in one geography, or a compromised channel, changes a
-- category's share while no individual transaction is invalid.
SELECT share_change_points, dimension, value, baseline_pct, current_pct,
       current_rows, change_type
FROM finguard.ops.v_categorical_mix_shift"""

#
# ROUTING: `page` decides the CHANNEL, cron decides the CADENCE.
#
# A PAGE alert notifies Slack *and* email; a TICKET alert notifies email only.
# The reason is not that Slack is fancier -- it is that a Slack channel
# receiving every alert becomes unreadable within a week, and an unread channel
# looks like coverage while providing none.
#
# TRD OB-07 requires page and notify to be distinguishable. Sending both to the
# same place erases exactly the distinction it asks for. The test is simple:
# is there an action a person can take right now? Ingestion has stopped -- yes.
# A monthly budget projection moved -- no, that is a Monday conversation.
ALERTS = [
    {
        "name": "FinGuard PAGE - pipeline update failed",
        "sql": UPDATE_FAILED,
        "column": "failure_count",
        "cron": "0 5 * * * ?",          # hourly at :05
        "page": True,
    },
    {
        "name": "FinGuard PAGE - watermark stalled",
        "sql": WATERMARK_STALLED,
        "column": "watermark_lag_hours",
        "cron": "0 5 * * * ?",          # hourly at :05
        "page": True,
    },
    {
        "name": "FinGuard TICKET - expectation rate degraded",
        "sql": EXPECTATION_DEGRADED,
        "column": "drop_pct_points",
        "cron": "0 0 6 * * ?",          # daily 06:00
        "page": False,
    },
    {
        "name": "FinGuard TICKET - state growth unbounded",
        "sql": STATE_GROWTH,
        "column": "growth_multiple",
        "cron": "0 0 6 * * ?",          # daily 06:00
        "page": False,
    },
    {
        # PAGE, but daily -- deliberately. Billing data lands hours late, so an
        # hourly check would re-read the same incomplete day and page
        # repeatedly about one event. Urgent topic, slow-moving signal.
        "name": "FinGuard PAGE - daily spend spike",
        "sql": COST_SPIKE,
        "column": "spend_multiple",
        "cron": "0 30 7 * * ?",         # daily 07:30, after billing settles
        "page": True,
    },
    {
        "name": "FinGuard TICKET - idle compute burning DBU",
        "sql": IDLE_COMPUTE,
        "column": "dbu_last_7_days",
        "cron": "0 0 6 * * ?",          # daily 06:00
        "page": False,
    },
    {
        "name": "FinGuard TICKET - monthly budget projection",
        "sql": BUDGET_PROJECTION,
        "column": "projected_month_usd",
        "cron": "0 0 6 * * ?",          # daily 06:00
        "page": False,
    },
    {
        # PAGE: a stale mart serves plausible wrong numbers with no error.
        # Daily rather than hourly -- the upstream job is daily, so an hourly
        # check would re-report the same known gap all day.
        "name": "FinGuard PAGE - marts stale vs silver",
        "sql": MART_LAG,
        "column": "mart_lag_days",
        "cron": "0 0 8 * * ?",          # daily 08:00, after the 02:00 job
        "page": True,
    },
    {
        "name": "FinGuard PAGE - fraud alert rate shift",
        "sql": ALERT_RATE_SHIFT,
        "column": "alert_rate_multiple",
        "cron": "0 15 * * * ?",         # hourly at :15
        "page": True,
    },
    {
        # TICKET: a distribution shift is worth investigating, not worth
        # waking someone. Interpreting it needs context an analyst has and a
        # sleeping engineer does not.
        "name": "FinGuard TICKET - amount distribution shift",
        "sql": AMOUNT_DISTRIBUTION,
        "column": "median_shift_multiple",
        "cron": "0 0 7 * * ?",          # daily 07:00
        "page": False,
    },
    {
        "name": "FinGuard TICKET - categorical mix shift",
        "sql": CATEGORICAL_MIX,
        "column": "share_change_points",
        "cron": "0 0 7 * * ?",          # daily 07:00
        "page": False,
    },
]


def _subscriptions(spec, slack_destination_id):
    """Who this alert notifies.

    Email always: it is the durable record, searchable months later, and it
    survives someone leaving the Slack workspace.

    Slack additionally, for PAGE only. The Slack app pushes to a phone, which
    is what makes a page a page rather than an email nobody reads until
    morning. Restricting it to PAGE is what keeps the channel worth looking at
    -- see the ALERTS comment above.

    A missing destination id is not an error: the alerts are still worth having
    on email, and failing the whole run because Slack is not configured yet
    would make notification setup a prerequisite for monitoring rather than an
    enhancement of it.
    """
    subscriptions = [{"user_email": NOTIFY_EMAIL}]
    if spec.get("page") and slack_destination_id:
        subscriptions.append({"destination_id": slack_destination_id})
    return subscriptions


def _payload(spec, slack_destination_id=None):
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
                "subscriptions": _subscriptions(spec, slack_destination_id),
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


def _slack_destination_id(host, headers):
    """Look up the Slack destination by name, or None if absent.

    Resolved by NAME rather than taking an id as an argument, so this script
    stays runnable with no parameters and cannot be run against a stale id
    someone pasted from an old terminal. The name is the contract between this
    script and create_notification_destinations.py.
    """
    listing = _api(host, headers, "GET", "/api/2.0/notification-destinations")
    if "ERROR" in str(listing):
        return None
    for row in listing.get("results", []) or []:
        if row.get("display_name") == SLACK_DESTINATION_NAME:
            return row.get("id")
    return None


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    host, headers = _client()

    slack_id = _slack_destination_id(host, headers)
    if slack_id:
        print(f"Slack destination: {SLACK_DESTINATION_NAME} ({slack_id})")
    else:
        # Stated loudly rather than passed over. Someone running this expects
        # pages to reach their phone; silently degrading to email-only is the
        # kind of thing discovered during an incident.
        print(
            f"WARNING: no notification destination named "
            f"'{SLACK_DESTINATION_NAME}'.\n"
            "         PAGE alerts will notify EMAIL ONLY.\n"
            "         Run scripts/create_notification_destinations.py first."
        )

    # v2 returns {"alerts": [...]}. v1 returns {"results": [...]}. Reading the
    # wrong key yields an empty dict, every alert looks new, and a rerun
    # silently duplicates all of them -- caught by running --dry-run twice.
    listing = _api(host, headers, "GET", "/api/2.0/alerts?page_size=100")
    existing = {
        a.get("display_name"): a.get("id") for a in listing.get("alerts", [])
    }

    for spec in ALERTS:
        payload = _payload(spec, slack_id)
        alert_id = existing.get(spec["name"])
        channels = "slack+email" if (spec.get("page") and slack_id) else "email"

        if dry_run:
            action = "UPDATE" if alert_id else "CREATE"
            print(f"[dry-run] {action}: {spec['name']} "
                  f"(bind {spec['column']}, cron {spec['cron']}, -> {channels})")
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
            print(f"ok      {spec['name']}  id={result.get('id')}  -> {channels}")


if __name__ == "__main__":
    main()
