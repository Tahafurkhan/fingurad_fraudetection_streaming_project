"""Create or update the FinGuard operations dashboard.

Idempotent, matching scripts/create_alerts.py: an existing dashboard with the
same display_name is updated in place rather than duplicated, so the URL people
bookmark stays stable.

WHY A DASHBOARD IN ADDITION TO ALERTS
-------------------------------------
Alerts answer "is something wrong right now" and must stay quiet to stay
useful. A dashboard answers the questions that are not worth an email:

  * Is the failure rate trending up or was that one bad afternoon?
  * Which expectations actually exercise data, and which have never seen a row?
  * What does a run cost, and is the optimization work holding?
  * Which stateful operators are running with a partition count that does not
    match configuration?

Everything here is DIAGNOSTIC severity by design. The moment a panel becomes
something someone must react to within the hour, it belongs in
scripts/create_alerts.py instead.

Usage:
    python scripts/create_dashboard.py [--dry-run]
"""

from __future__ import annotations

import configparser
import json
import os
import sys
import urllib.error
import urllib.request

WAREHOUSE_ID = "b349348047ac54a2"
DASHBOARD_NAME = "FinGuard Operations"


def _client():
    cfg = configparser.ConfigParser()
    cfg.read(os.path.expanduser("~/.databrickscfg"))
    host = cfg["DEFAULT"]["host"].rstrip("/")
    token = cfg["DEFAULT"]["token"]
    return host, {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


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
# Datasets. One query per panel.
# ---------------------------------------------------------------------------

DATASETS = [
    {
        "name": "run_outcomes",
        "displayName": "Run outcomes by day",
        "queryLines": [
            "SELECT date_trunc('DAY', event_timestamp) AS day,\n",
            "       state,\n",
            "       count(*) AS runs\n",
            "FROM finguard.ops.pipeline_runs\n",
            "GROUP BY 1, 2\n",
            "ORDER BY day DESC",
        ],
    },
    {
        "name": "expectation_health",
        "displayName": "Expectation pass rate",
        # Rows evaluated is shown alongside pass rate deliberately: an
        # expectation at 100% over zero rows is not passing, it is idle, and
        # those two states look identical if only the rate is plotted.
        "queryLines": [
            "SELECT dataset,\n",
            "       expectation_name,\n",
            "       sum(passed_records) AS passed,\n",
            "       sum(failed_records) AS failed,\n",
            "       sum(passed_records + failed_records) AS rows_evaluated,\n",
            "       CASE WHEN sum(passed_records + failed_records) = 0 THEN NULL\n",
            "            ELSE round(100.0 * sum(passed_records)\n",
            "                       / sum(passed_records + failed_records), 2)\n",
            "       END AS pass_pct\n",
            "FROM finguard.ops.expectation_results\n",
            "GROUP BY 1, 2\n",
            "ORDER BY rows_evaluated DESC, dataset",
        ],
    },
    {
        "name": "watermark_lag",
        "displayName": "Watermark lag by flow",
        "queryLines": [
            "WITH latest AS (\n",
            "  SELECT flow_name, operator_name, watermark, event_timestamp,\n",
            "         ROW_NUMBER() OVER (PARTITION BY flow_name\n",
            "                            ORDER BY event_timestamp DESC) AS rn\n",
            "  FROM finguard.ops.stream_health\n",
            "  WHERE watermark IS NOT NULL\n",
            ")\n",
            "SELECT flow_name, operator_name, watermark,\n",
            "       event_timestamp AS last_seen,\n",
            "       round((unix_timestamp(event_timestamp)\n",
            "              - unix_timestamp(watermark)) / 3600.0, 1) AS lag_hours\n",
            "FROM latest\n",
            "WHERE rn = 1\n",
            "ORDER BY lag_hours DESC",
        ],
    },
    {
        "name": "state_vs_config",
        "displayName": "State partitions vs configured",
        "queryLines": [
            "SELECT flow_name,\n",
            "       operator_name,\n",
            "       max(num_state_instances) AS state_instances,\n",
            "       16 AS configured_shuffle_partitions\n",
            "FROM finguard.ops.stream_health\n",
            "WHERE operator_name IS NOT NULL\n",
            "  AND num_state_instances IS NOT NULL\n",
            "GROUP BY 1, 2\n",
            "ORDER BY state_instances DESC",
        ],
    },
    {
        "name": "cost_per_day",
        "displayName": "Pipeline cost by day",
        "queryLines": [
            "SELECT date_trunc('DAY', u.usage_start_time) AS day,\n",
            "       u.usage_metadata.dlt_pipeline_id AS pipeline_id,\n",
            "       round(sum(u.usage_quantity), 3) AS dbus,\n",
            "       round(sum(u.usage_quantity * p.pricing.default), 4) AS est_usd\n",
            "FROM system.billing.usage u\n",
            "LEFT JOIN system.billing.list_prices p\n",
            "       ON p.sku_name = u.sku_name\n",
            "      AND p.currency_code = 'USD'\n",
            "      AND p.price_end_time IS NULL\n",
            "WHERE u.usage_metadata.dlt_pipeline_id IS NOT NULL\n",
            "  AND u.usage_start_time > current_timestamp() - INTERVAL 30 DAYS\n",
            "GROUP BY 1, 2\n",
            "ORDER BY day DESC, dbus DESC",
        ],
    },
    {
        # Cost by project/environment tag rather than by resource id.
        #
        # The "(untagged)" row is the one that matters: it is everything
        # running in this workspace that no declared resource claims. A
        # $239 idle serving endpoint lived in that bucket for weeks.
        "name": "cost_by_tag",
        "displayName": "Cost by project tag",
        "queryLines": [
            "SELECT coalesce(u.custom_tags['project'], '(untagged)') AS project,\n",
            "       coalesce(u.custom_tags['environment'], '(untagged)') AS environment,\n",
            "       round(sum(u.usage_quantity), 2) AS dbus,\n",
            "       round(sum(u.usage_quantity * p.pricing.default), 2) AS est_usd,\n",
            "       count(DISTINCT u.usage_date) AS active_days\n",
            "FROM system.billing.usage u\n",
            "LEFT JOIN system.billing.list_prices p\n",
            "       ON p.sku_name = u.sku_name\n",
            "      AND p.currency_code = 'USD'\n",
            "      AND p.price_end_time IS NULL\n",
            "WHERE u.usage_date > current_date() - 30\n",
            "GROUP BY 1, 2\n",
            "ORDER BY est_usd DESC NULLS LAST",
        ],
    },
    {
        # Workspace-wide, deliberately. Cost monitoring scoped only to the
        # pipeline cannot see the resource nobody is watching, and that is
        # where runaway spend actually lives.
        "name": "spend_by_sku",
        "displayName": "Workspace spend by SKU",
        "queryLines": [
            "SELECT u.sku_name,\n",
            "       round(sum(u.usage_quantity), 1) AS dbus,\n",
            "       round(sum(u.usage_quantity * p.pricing.default), 2) AS est_usd,\n",
            "       round(min(daily.d), 1) AS daily_floor_dbu\n",
            "FROM system.billing.usage u\n",
            "LEFT JOIN system.billing.list_prices p\n",
            "       ON p.sku_name = u.sku_name\n",
            "      AND p.currency_code = 'USD'\n",
            "      AND p.price_end_time IS NULL\n",
            "LEFT JOIN (SELECT sku_name, usage_date, sum(usage_quantity) AS d\n",
            "           FROM system.billing.usage\n",
            "           WHERE usage_date > current_date() - 30\n",
            "           GROUP BY 1, 2) daily\n",
            "       ON daily.sku_name = u.sku_name\n",
            "WHERE u.usage_date > current_date() - 30\n",
            "GROUP BY u.sku_name\n",
            "ORDER BY est_usd DESC NULLS LAST",
        ],
    },
    {
        "name": "batch_latency",
        "displayName": "Trigger duration by flow",
        "queryLines": [
            "SELECT flow_name,\n",
            "       count(*) AS batches,\n",
            "       round(avg(trigger_duration_ms) / 1000.0, 1) AS avg_sec,\n",
            "       round(max(trigger_duration_ms) / 1000.0, 1) AS max_sec\n",
            "FROM finguard.ops.stream_health\n",
            "WHERE trigger_duration_ms IS NOT NULL\n",
            "GROUP BY 1\n",
            "ORDER BY avg_sec DESC",
        ],
    },
]


def _table_widget(name, dataset_name, columns, title, pos):
    """A table widget bound to one dataset."""
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": f"main_query_{dataset_name}",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [{"name": c, "expression": f"`{c}`"} for c in columns],
                        "disaggregated": True,
                    },
                }
            ],
            "spec": {
                "version": 1,
                "widgetType": "table",
                "encodings": {
                    "columns": [
                        {"fieldName": c, "displayName": c, "booleanValues": ["false", "true"]}
                        for c in columns
                    ]
                },
                "frame": {"title": title, "showTitle": True},
            },
        },
        "position": pos,
    }


def _serialized_dashboard():
    widgets = [
        _table_widget(
            "w_watermark", "watermark_lag",
            ["flow_name", "operator_name", "watermark", "last_seen", "lag_hours"],
            "Watermark lag — PAGE if > 6h or at epoch",
            {"x": 0, "y": 0, "width": 6, "height": 6},
        ),
        _table_widget(
            "w_runs", "run_outcomes", ["day", "state", "runs"],
            "Run outcomes by day",
            {"x": 6, "y": 0, "width": 6, "height": 6},
        ),
        _table_widget(
            "w_expect", "expectation_health",
            ["dataset", "expectation_name", "rows_evaluated", "pass_pct", "failed"],
            "Expectation pass rate (rows_evaluated = 0 means idle, not healthy)",
            {"x": 0, "y": 6, "width": 12, "height": 6},
        ),
        _table_widget(
            "w_state", "state_vs_config",
            ["flow_name", "operator_name", "state_instances",
             "configured_shuffle_partitions"],
            "State partitions vs config — divergence needs a fresh checkpoint",
            {"x": 0, "y": 12, "width": 6, "height": 6},
        ),
        _table_widget(
            "w_latency", "batch_latency",
            ["flow_name", "batches", "avg_sec", "max_sec"],
            "Trigger duration by flow",
            {"x": 6, "y": 12, "width": 6, "height": 6},
        ),
        _table_widget(
            "w_cost", "cost_per_day", ["day", "pipeline_id", "dbus", "est_usd"],
            "Pipeline cost by day (attribution and trend, not anomaly detection)",
            {"x": 0, "y": 18, "width": 12, "height": 6},
        ),
        _table_widget(
            "w_cost_tag", "cost_by_tag",
            ["project", "environment", "dbus", "est_usd", "active_days"],
            "Cost by project tag — '(untagged)' is spend no declared resource claims",
            {"x": 0, "y": 24, "width": 6, "height": 6},
        ),
        _table_widget(
            "w_sku", "spend_by_sku",
            ["sku_name", "dbus", "est_usd", "daily_floor_dbu"],
            "Workspace spend by SKU — a nonzero daily floor means always-on",
            {"x": 6, "y": 24, "width": 6, "height": 6},
        ),
    ]

    return json.dumps(
        {
            "datasets": DATASETS,
            "pages": [
                {
                    "name": "ops",
                    "displayName": "Operations",
                    "layout": widgets,
                }
            ],
        }
    )


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    host, headers = _client()

    listing = _api(host, headers, "GET", "/api/2.0/lakeview/dashboards?page_size=100")
    existing = {
        d.get("display_name"): d.get("dashboard_id")
        for d in listing.get("dashboards", [])
    }
    dashboard_id = existing.get(DASHBOARD_NAME)

    if dry_run:
        action = "UPDATE" if dashboard_id else "CREATE"
        print(f"[dry-run] {action}: {DASHBOARD_NAME} "
              f"({len(DATASETS)} datasets)")
        return

    payload = {
        "display_name": DASHBOARD_NAME,
        "warehouse_id": WAREHOUSE_ID,
        "serialized_dashboard": _serialized_dashboard(),
    }

    if dashboard_id:
        result = _api(host, headers, "PATCH",
                      f"/api/2.0/lakeview/dashboards/{dashboard_id}", payload)
    else:
        result = _api(host, headers, "POST", "/api/2.0/lakeview/dashboards", payload)

    if "ERROR" in result:
        print(f"FAILED: {result}")
        sys.exit(1)

    dashboard_id = result.get("dashboard_id")
    print(f"ok  {DASHBOARD_NAME}  id={dashboard_id}")

    # Publishing makes it visible to anyone with permission, rather than
    # remaining a draft only the author sees.
    published = _api(host, headers, "POST",
                     f"/api/2.0/lakeview/dashboards/{dashboard_id}/published",
                     {"embed_credentials": False,
                      "warehouse_id": WAREHOUSE_ID})
    if "ERROR" in published:
        print(f"  warning: publish failed: {published}")
    else:
        print(f"  published: {host}/dashboardsv3/{dashboard_id}")


if __name__ == "__main__":
    main()
