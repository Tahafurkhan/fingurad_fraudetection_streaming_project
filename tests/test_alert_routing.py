"""Tests for alert notification routing.

Alerting has a failure mode that no test of the alert *queries* would catch: a
detector that fires correctly and notifies nobody. These tests cover the
routing decision -- which channel each alert reaches -- because that is the
part with no feedback loop. A misrouted alert looks identical to a working one
right up until an incident.

Pure dict manipulation, no workspace call, so these run in CI.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def alerts():
    """Load create_alerts.py by path.

    scripts/ is not a package and the module is a CLI, so a normal import does
    not reach it. Loading by spec keeps the script a script while still making
    its logic testable.
    """
    path = PROJECT_ROOT / "scripts" / "create_alerts.py"
    spec = importlib.util.spec_from_file_location("create_alerts", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["create_alerts"] = module
    spec.loader.exec_module(module)
    return module


SLACK_ID = "test-destination-id"


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def test_page_alerts_reach_slack_and_email(alerts):
    """PAGE means a phone buzzes. Email alone is not a page."""
    for spec in alerts.ALERTS:
        if not spec.get("page"):
            continue
        subs = alerts._subscriptions(spec, SLACK_ID)
        assert any(s.get("destination_id") == SLACK_ID for s in subs), spec["name"]
        assert any(s.get("user_email") for s in subs), spec["name"]


def test_ticket_alerts_never_reach_slack(alerts):
    """THE ONE THAT PROTECTS THE CHANNEL.

    A Slack channel receiving every alert becomes unreadable within a week, and
    an unread channel looks like coverage while providing none. TRD OB-07 asks
    that page and notify be distinguishable; routing both to one place erases
    the distinction.

    This test is what stops someone adding `"page": True` to a budget
    projection because it felt important at the time.
    """
    for spec in alerts.ALERTS:
        if spec.get("page"):
            continue
        subs = alerts._subscriptions(spec, SLACK_ID)
        assert not any(s.get("destination_id") for s in subs), spec["name"]


def test_every_alert_notifies_someone(alerts):
    """No alert may have an empty subscription list.

    A detector that fires and notifies nobody is worse than no detector: it
    consumes warehouse time on every evaluation and produces the appearance of
    monitoring.
    """
    for spec in alerts.ALERTS:
        assert alerts._subscriptions(spec, SLACK_ID), spec["name"]
        assert alerts._subscriptions(spec, None), spec["name"]


def test_missing_slack_destination_degrades_to_email(alerts):
    """Slack being unconfigured must not break email delivery.

    Deliberate: making Slack a prerequisite would mean a workspace without a
    webhook has no alerting at all, which trades a partial channel for none.
    The script warns loudly about the degradation instead.
    """
    for spec in alerts.ALERTS:
        subs = alerts._subscriptions(spec, None)
        assert subs == [{"user_email": alerts.NOTIFY_EMAIL}], spec["name"]


def test_page_and_name_prefix_agree(alerts):
    """The `page` flag and the display name must not contradict each other.

    The name is what an operator sees in the workspace and in the notification
    itself. An alert titled PAGE that only emails, or one titled TICKET that
    buzzes a phone at 03:00, teaches people the labels are meaningless.
    """
    for spec in alerts.ALERTS:
        titled_page = "PAGE" in spec["name"]
        assert titled_page == bool(spec.get("page")), spec["name"]


# ---------------------------------------------------------------------------
# Payload shape
# ---------------------------------------------------------------------------


def test_destination_name_matches_the_creating_script(alerts):
    """Both scripts must agree on the destination name.

    create_alerts.py resolves the destination BY NAME. If the two names drift,
    the lookup silently returns None and every PAGE alert quietly degrades to
    email-only -- with a warning nobody reads because the run still succeeds.
    """
    path = PROJECT_ROOT / "scripts" / "create_notification_destinations.py"
    spec = importlib.util.spec_from_file_location("create_destinations", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.SLACK_DESTINATION_NAME == alerts.SLACK_DESTINATION_NAME


def test_empty_result_state_is_ok_not_unknown(alerts):
    """Zero rows is the healthy state for every detector here.

    Left at UNKNOWN, a recovered pipeline never clears the alert -- so the next
    real failure arrives on a channel already showing red, which is how a
    genuine page gets ignored.
    """
    for spec in alerts.ALERTS:
        payload = alerts._payload(spec, SLACK_ID)
        assert payload["evaluation"]["empty_result_state"] == "OK", spec["name"]


def test_alerts_bind_to_a_column_the_query_returns(alerts):
    """The bound column must appear in the detector's SQL.

    Binding to a column the query does not return creates an alert that never
    fires and reports no error -- documented in this script's header as a trap
    found by probing rather than from the field names.
    """
    for spec in alerts.ALERTS:
        assert spec["column"] in spec["sql"], spec["name"]


def test_no_alert_notifies_on_recovery(alerts):
    """Recovery notifications double volume and train people to skim."""
    for spec in alerts.ALERTS:
        payload = alerts._payload(spec, SLACK_ID)
        assert payload["evaluation"]["notification"]["notify_on_ok"] is False


def test_no_webhook_url_anywhere_in_the_repo(alerts):
    """A Slack webhook is a bearer credential and must never be committed.

    THIS TEST WAS TOO NARROW AND A REAL LEAK PROVED IT. The first version
    scanned only the two files in scripts/, on the reasoning that they were
    the ones most likely to acquire a webhook during a hurried fix. It passed
    while a live webhook sat in `.env.example` -- a file whose entire purpose
    is to be committed.

    The lesson is about scope, not about Slack: a secret scanner that checks
    the places you expect secrets to appear will miss the places you do not.
    It now walks every tracked text file, which is the only version that could
    have caught the actual incident.

    gitleaks in CI and pre-commit covers this too. Duplicated here because a
    hook can be skipped with --no-verify and this cannot.
    """
    import subprocess

    tracked = subprocess.run(
        ["git", "ls-files"],
        cwd=PROJECT_ROOT, capture_output=True, text=True,
    ).stdout.split()

    # The literal path segment of a Slack webhook. Matching the host alone
    # would flag documentation that legitimately mentions hooks.slack.com,
    # which is how a scanner earns enough false positives to be deleted.
    marker = "hooks.slack.com/services/T"

    offenders = []
    for rel in tracked:
        path = PROJECT_ROOT / rel
        if not path.is_file():
            continue
        try:
            source = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        # This test file necessarily contains the marker to search for it.
        if rel.endswith("test_alert_routing.py"):
            continue
        if marker in source:
            offenders.append(rel)

    assert not offenders, (
        f"Live Slack webhook committed in: {offenders}. Rotate it in Slack "
        "immediately -- git history keeps what a later edit removes."
    )
