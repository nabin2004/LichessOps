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


def send_slack_success(
    component: str,
    phase: str,
    month: str,
    *,
    detail: str = "",
) -> bool:
    """Notify Slack that a pipeline phase completed successfully."""
    text = f":white_check_mark: [{component}] phase `{phase}` succeeded for `{month}`"
    if detail:
        text = f"{text}\n{detail}"
    return send_slack_message(text)


def send_slack_failure(
    component: str,
    phase: str,
    month: str,
    error: str,
) -> bool:
    """Notify Slack that a pipeline phase failed."""
    snippet = error.strip()
    if len(snippet) > 500:
        snippet = f"{snippet[:497]}..."
    text = f":x: [{component}] phase `{phase}` failed for `{month}`\n{snippet}"
    return send_slack_message(text)


def send_slack_pipeline_start(month: str, phases: list[str]) -> bool:
    """Notify Slack that an end-to-end pipeline run is starting."""
    phase_list = ", ".join(phases)
    text = f":rocket: [pipeline] starting run for `{month}`\nPhases: {phase_list}"
    return send_slack_message(text)


def send_slack_pipeline_complete(
    month: str,
    duration_s: float,
    *,
    extras: str = "",
) -> bool:
    """Notify Slack that the full pipeline completed successfully."""
    text = f":tada: [pipeline] completed for `{month}` in {duration_s:.1f}s"
    if extras:
        text = f"{text}\n{extras}"
    return send_slack_message(text)
