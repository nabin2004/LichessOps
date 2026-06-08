"""Slack Incoming Webhook helpers for cross-component alerting."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

_SLACK_WEBHOOK_BASE = "https://hooks.slack.com/services/"


def slack_webhook_url() -> str | None:
    """Return the configured webhook URL, or None if not set."""
    explicit = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if explicit:
        return explicit

    token = os.getenv("SLACK_WEBHOOK_TOKEN", "").strip()
    if not token:
        return None

    if token.startswith("http://") or token.startswith("https://"):
        return token

    return f"{_SLACK_WEBHOOK_BASE}{token.lstrip('/')}"


def is_slack_configured() -> bool:
    """Return True when a Slack webhook URL or token is available."""
    return slack_webhook_url() is not None


def send_slack_message(text: str, *, raise_on_error: bool = False) -> bool:
    """Post a plain-text message to the configured Slack webhook."""
    url = slack_webhook_url()
    if not url:
        if raise_on_error:
            raise RuntimeError("Slack webhook is not configured (set SLACK_WEBHOOK_TOKEN or SLACK_WEBHOOK_URL)")
        return False

    payload = json.dumps({"text": text}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300
    except urllib.error.URLError as exc:
        if raise_on_error:
            raise RuntimeError(f"Slack webhook request failed: {exc}") from exc
        return False


def send_slack_alert(component: str, message: str, *, level: str = "error") -> bool:
    """Send a formatted alert for a Lichess component."""
    text = f"[lichess/{component}] ({level}) {message}"
    return send_slack_message(text)
