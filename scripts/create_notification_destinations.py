"""Create or update the Slack notification destination for alerts.

WHY A DESTINATION RATHER THAN A WEBHOOK CALL PER ALERT
-----------------------------------------------------
Databricks alerts cannot post to a webhook directly. They notify
*subscriptions*, and a subscription is either a user email or a
**notification destination** registered at workspace level. So the webhook is
registered once here and referenced by id from every alert that should page.

The indirection is worth having: rotating the Slack webhook means updating one
destination, not seven alerts. Given that a webhook URL is a bearer credential
that will eventually leak and need rotating, that matters.

WHY ONLY *PAGE* ALERTS WILL USE IT
----------------------------------
Routing all seven alerts to one Slack channel is the fastest way to make the
channel unreadable, and an unread channel is worse than no channel -- it looks
like coverage while providing none. TRD OB-07 requires page and notify to be
distinguishable; sending both to the same place erases the distinction it asks
for.

PAGE means act now: ingestion has stopped, or spend has spiked. TICKET means
review within a day: an expectation rate drifted, a budget projection moved.
There is no 03:00 action for a budget projection, so it stays on email.

THE WEBHOOK IS NEVER STORED IN THIS REPO
----------------------------------------
It is read from, in order:

  1. $SLACK_WEBHOOK_URL
  2. the Databricks secret scope (finguard-scope/slack_webhook_url)

A webhook URL is a bearer credential: anyone holding it can post to the
channel as this integration. Committing one is the same class of mistake as
committing a token, which is why gitleaks runs in CI and in the pre-commit
hooks.

Usage:
    export SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'
    python scripts/create_notification_destinations.py --dry-run
    python scripts/create_notification_destinations.py
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

# One destination, referenced by every PAGE alert. Named so its purpose is
# obvious in the workspace UI, where someone will eventually find it without
# this file open.
SLACK_DESTINATION_NAME = "FinGuard PAGE - Slack"

SECRET_SCOPE = "finguard-scope"
SECRET_KEY = "slack_webhook_url"


def _databricks(args: list[str], profile: str) -> tuple[int, str]:
    """Run a databricks CLI command, returning (returncode, stdout).

    The CLI rather than raw HTTP because this workspace authenticates with
    OAuth, not a personal access token. create_alerts.py reads a `token` from
    ~/.databrickscfg, which does not exist here -- the CLI holds the OAuth
    session and refreshes it, so shelling out is what actually works.
    """
    result = subprocess.run(
        ["databricks", *args, "--profile", profile],
        capture_output=True,
        text=True,
    )
    return result.returncode, (result.stdout or result.stderr).strip()


def _api(method: str, path: str, profile: str, body: dict | None = None) -> dict:
    args = ["api", method, path]
    if body is not None:
        args += ["--json", json.dumps(body)]
    code, out = _databricks(args, profile)
    if code != 0:
        return {"ERROR": out[:500]}
    try:
        return json.loads(out) if out else {}
    except json.JSONDecodeError:
        return {"ERROR": out[:500]}


def resolve_webhook(profile: str) -> str | None:
    """Find the webhook URL, preferring the environment.

    Environment first so a developer can test a new webhook without writing it
    to the shared secret scope, and so CI can inject one. Falling back to the
    secret scope means a scheduled run needs no local configuration.
    """
    import os

    from_env = os.environ.get("SLACK_WEBHOOK_URL")
    if from_env:
        return from_env.strip()

    code, out = _databricks(
        ["secrets", "get-secret", SECRET_SCOPE, SECRET_KEY, "--output", "json"],
        profile,
    )
    if code != 0:
        return None
    try:
        import base64

        return base64.b64decode(json.loads(out)["value"]).decode().strip()
    except Exception:
        return None


def _redact(url: str) -> str:
    """Show enough of the URL to identify it, not enough to use it.

    Printed output ends up in CI logs and terminal scrollback, both of which
    outlive the person watching them.
    """
    if not url:
        return "<none>"
    tail = url.rstrip("/").split("/")[-1]
    return f"https://hooks.slack.com/services/.../{tail[:4]}..." if tail else "<malformed>"


def find_destination(profile: str, name: str) -> str | None:
    """Return the id of a destination with this display name, if it exists.

    Matching on display name makes reruns idempotent. Without it, every run
    creates another destination and the alerts keep pointing at the first --
    so a rotated webhook would appear to have been applied while the alerts
    silently kept using the old one.
    """
    listing = _api("get", "/api/2.0/notification-destinations", profile)
    for row in listing.get("results", []) or []:
        if row.get("display_name") == name:
            return row.get("id")
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    webhook = resolve_webhook(args.profile)
    if not webhook:
        print(
            "No webhook found.\n"
            "  Set $SLACK_WEBHOOK_URL, or store it with:\n"
            f"    databricks secrets put-secret {SECRET_SCOPE} {SECRET_KEY} "
            "--string-value '<url>'",
            file=sys.stderr,
        )
        return 1

    if not webhook.startswith("https://hooks.slack.com/"):
        # Fail rather than register it. A destination pointing at the wrong
        # host silently swallows every page -- the alert fires, the delivery
        # fails, and nothing surfaces the difference.
        print(f"Not a Slack webhook URL: {_redact(webhook)}", file=sys.stderr)
        return 1

    existing_id = find_destination(args.profile, SLACK_DESTINATION_NAME)
    action = "UPDATE" if existing_id else "CREATE"
    print(f"{action}: {SLACK_DESTINATION_NAME}  ->  {_redact(webhook)}")

    if args.dry_run:
        print("(dry run, nothing changed)")
        return 0

    payload = {
        "display_name": SLACK_DESTINATION_NAME,
        "config": {"slack": {"url": webhook}},
    }

    if existing_id:
        result = _api(
            "patch",
            f"/api/2.0/notification-destinations/{existing_id}",
            args.profile,
            payload,
        )
    else:
        result = _api("post", "/api/2.0/notification-destinations", args.profile, payload)

    if "ERROR" in result:
        print(f"FAILED: {result['ERROR']}", file=sys.stderr)
        return 1

    destination_id = result.get("id", existing_id)
    print(f"ok  id={destination_id}")

    # The id is what create_alerts.py needs, and it is stable across updates.
    # Printed rather than written to a file so it can be piped, and so this
    # script has no side effect beyond the workspace call it just made.
    print(f"\nUse in alerts:  --slack-destination-id {destination_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
