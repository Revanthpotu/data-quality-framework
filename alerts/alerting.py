"""
alerts/alerting.py
==================
Alerting stub for the Data Quality Framework.

In production, extend each stub to integrate with:
  - PagerDuty (send_pagerduty_alert)
  - Slack (send_slack_alert)
  - Email/SMTP (send_email_alert)
  - Datadog Events API (send_datadog_event)
  - Airflow callbacks / SLA misses

This module is intentionally decoupled from the pipeline so
alerting destinations can be swapped without touching core logic.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("dq_framework.alerts")

# ─── Configuration (load from env in production) ──────────────────────────────

SLACK_WEBHOOK_URL: Optional[str] = os.getenv("DQ_SLACK_WEBHOOK_URL")
PAGERDUTY_ROUTING_KEY: Optional[str] = os.getenv("DQ_PAGERDUTY_KEY")
ALERT_EMAIL: Optional[str] = os.getenv("DQ_ALERT_EMAIL")
ALERT_LOG_PATH = Path("reports/alerts.log")


# ─── Core Alert Dispatcher ────────────────────────────────────────────────────


def send_alert(suite_name: str, parsed_result: dict) -> None:
    """
    Main alert entrypoint. Called by the pipeline when a suite fails.

    Args:
        suite_name:     Name of the failed expectation suite.
        parsed_result:  Structured result dict from parse_result().
    """
    timestamp = datetime.utcnow().isoformat() + "Z"
    failed_expectations = [
        e for e in parsed_result["expectation_results"] if not e["success"]
    ]

    alert_payload = {
        "timestamp": timestamp,
        "severity": _determine_severity(parsed_result),
        "suite_name": suite_name,
        "failed_count": parsed_result["failed_count"],
        "total_count": parsed_result["total_count"],
        "success_percent": parsed_result["success_percent"],
        "failed_expectations": failed_expectations,
    }

    # Always log the alert
    _log_alert(alert_payload)

    # Dispatch to each configured channel
    _dispatch_slack(alert_payload)
    _dispatch_pagerduty(alert_payload)
    _dispatch_email(alert_payload)


# ─── Severity Classifier ──────────────────────────────────────────────────────


def _determine_severity(parsed_result: dict) -> str:
    """
    Map pass rate to a severity level:
      - CRITICAL  < 50% pass
      - HIGH      50–74% pass
      - MEDIUM    75–89% pass
      - LOW       90–99% pass
    """
    pct = parsed_result["success_percent"]
    if pct < 50:
        return "CRITICAL"
    elif pct < 75:
        return "HIGH"
    elif pct < 90:
        return "MEDIUM"
    else:
        return "LOW"


# ─── Log Alert ────────────────────────────────────────────────────────────────


def _log_alert(payload: dict) -> None:
    """Write alert to the alerts log file and emit a WARNING log line."""
    ALERT_LOG_PATH.parent.mkdir(exist_ok=True)

    severity = payload["severity"]
    suite = payload["suite_name"]
    failed = payload["failed_count"]
    total = payload["total_count"]
    pct = payload["success_percent"]

    msg = (
        f"[{payload['timestamp']}] {severity} ALERT | Suite: {suite} | "
        f"Failed: {failed}/{total} expectations | Pass rate: {pct:.1f}%"
    )

    # Emit as a WARNING so it surfaces in any log aggregator
    logger.warning(msg)

    # Also write structured JSON to the alerts log
    with open(ALERT_LOG_PATH, "a") as f:
        f.write(json.dumps(payload) + "\n")

    # Print to stdout for CI visibility
    print(f"\n{'='*60}")
    print(f"⚠️  DATA QUALITY ALERT — {severity}")
    print(f"{'='*60}")
    print(f"Suite   : {suite}")
    print(f"Failed  : {failed} / {total} expectations ({100 - pct:.1f}% failure rate)")
    print(f"Time    : {payload['timestamp']}")
    print("\nFailed expectations:")
    for exp in payload["failed_expectations"]:
        print(f"  ✗ [{exp['column']}] {exp['expectation_type']} — {exp['result_detail']}")
    print(f"{'='*60}\n")


# ─── Slack Stub ───────────────────────────────────────────────────────────────


def _dispatch_slack(payload: dict) -> None:
    """
    Send a Slack notification via Incoming Webhook.

    PRODUCTION SETUP:
      1. Create a Slack app at https://api.slack.com/apps
      2. Enable Incoming Webhooks
      3. Set env var: DQ_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...

    Uncomment the requests block below once the webhook is configured.
    """
    if not SLACK_WEBHOOK_URL:
        logger.debug("Slack webhook not configured — skipping Slack alert.")
        return

    severity_emoji = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}
    emoji = severity_emoji.get(payload["severity"], "⚠️")

    slack_body = {
        "text": f"{emoji} *DQ Alert — {payload['severity']}* | Suite: `{payload['suite_name']}`",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{emoji} *Data Quality Alert*\n"
                        f"*Suite:* `{payload['suite_name']}`\n"
                        f"*Severity:* {payload['severity']}\n"
                        f"*Failed:* {payload['failed_count']} / {payload['total_count']} "
                        f"({payload['success_percent']:.1f}% pass rate)\n"
                        f"*Time:* {payload['timestamp']}"
                    ),
                },
            }
        ],
    }

    # ── Uncomment to activate ──────────────────────────────────────────────
    # import requests
    # response = requests.post(SLACK_WEBHOOK_URL, json=slack_body, timeout=5)
    # if response.status_code != 200:
    #     logger.error(f"Slack alert failed: {response.status_code} {response.text}")
    # else:
    #     logger.info("Slack alert sent successfully.")
    # ──────────────────────────────────────────────────────────────────────

    logger.info(f"[STUB] Slack alert would be sent to webhook (payload ready, requests disabled)")


# ─── PagerDuty Stub ───────────────────────────────────────────────────────────


def _dispatch_pagerduty(payload: dict) -> None:
    """
    Trigger a PagerDuty incident via Events API v2.

    PRODUCTION SETUP:
      1. Create an integration in PagerDuty (Events API v2)
      2. Set env var: DQ_PAGERDUTY_KEY=<integration_routing_key>
    """
    if not PAGERDUTY_ROUTING_KEY:
        logger.debug("PagerDuty routing key not configured — skipping PagerDuty alert.")
        return

    severity_map = {"CRITICAL": "critical", "HIGH": "error", "MEDIUM": "warning", "LOW": "info"}

    pd_payload = {
        "routing_key": PAGERDUTY_ROUTING_KEY,
        "event_action": "trigger",
        "payload": {
            "summary": f"DQ Suite Failed: {payload['suite_name']} ({payload['failed_count']} failures)",
            "source": "data-quality-framework",
            "severity": severity_map.get(payload["severity"], "warning"),
            "timestamp": payload["timestamp"],
            "custom_details": payload,
        },
    }

    # ── Uncomment to activate ──────────────────────────────────────────────
    # import requests
    # response = requests.post(
    #     "https://events.pagerduty.com/v2/enqueue",
    #     json=pd_payload,
    #     timeout=5,
    # )
    # logger.info(f"PagerDuty response: {response.status_code}")
    # ──────────────────────────────────────────────────────────────────────

    logger.info("[STUB] PagerDuty alert would be triggered (requests disabled)")


# ─── Email Stub ───────────────────────────────────────────────────────────────


def _dispatch_email(payload: dict) -> None:
    """
    Send an alert email via SMTP.

    PRODUCTION SETUP:
      Set env vars:
        DQ_ALERT_EMAIL=oncall@yourcompany.com
        DQ_SMTP_HOST=smtp.yourcompany.com
        DQ_SMTP_PORT=587
        DQ_SMTP_USER=...
        DQ_SMTP_PASSWORD=...
    """
    if not ALERT_EMAIL:
        logger.debug("Alert email not configured — skipping email alert.")
        return

    # ── Uncomment to activate ──────────────────────────────────────────────
    # import smtplib
    # from email.mime.text import MIMEText
    # msg = MIMEText(json.dumps(payload, indent=2))
    # msg["Subject"] = f"[DQ Alert] {payload['severity']} — {payload['suite_name']}"
    # msg["From"] = "dq-framework@yourcompany.com"
    # msg["To"] = ALERT_EMAIL
    # with smtplib.SMTP(os.getenv("DQ_SMTP_HOST", "localhost"), int(os.getenv("DQ_SMTP_PORT", 25))) as s:
    #     s.send_message(msg)
    # ──────────────────────────────────────────────────────────────────────

    logger.info(f"[STUB] Email alert would be sent to {ALERT_EMAIL} (SMTP disabled)")


# ─── Summary Builder ──────────────────────────────────────────────────────────


def build_summary(all_results: list) -> str:
    """
    Build a terminal-friendly summary table for all suite results.
    Printed at the end of every pipeline run.
    """
    total_suites = len(all_results)
    passed_suites = sum(1 for r in all_results if r["success"])
    failed_suites = total_suites - passed_suites

    lines = [
        "┌─────────────────────────────────────────────────────────────┐",
        "│           DATA QUALITY FRAMEWORK — RUN SUMMARY              │",
        "├────────────────────────────────┬────────┬────────┬──────────┤",
        "│ Suite                          │ Status │ Passed │ Failed   │",
        "├────────────────────────────────┼────────┼────────┼──────────┤",
    ]

    for r in all_results:
        status = "  PASS  " if r["success"] else "  FAIL  "
        name = r["suite_name"][:30].ljust(30)
        passed = str(r["passed_count"]).center(6)
        failed = str(r["failed_count"]).center(8)
        lines.append(f"│ {name} │{status}│{passed}  │{failed}  │")

    lines += [
        "├────────────────────────────────┴────────┴────────┴──────────┤",
        f"│  Suites: {total_suites} total  ·  {passed_suites} passed  ·  {failed_suites} failed"
        + " " * (46 - len(f"{total_suites} total  ·  {passed_suites} passed  ·  {failed_suites} failed"))
        + "│",
        "└─────────────────────────────────────────────────────────────┘",
    ]

    return "\n".join(lines)
