"""Tests for pipeline-oriented Slack notification helpers."""

from __future__ import annotations

import json
import os
from unittest import mock

import pytest

from lichess_libs.shared.slack import (
    is_slack_configured,
    send_slack_failure,
    send_slack_message,
    send_slack_pipeline_complete,
    send_slack_pipeline_start,
    send_slack_success,
)


@pytest.fixture(autouse=True)
def _clear_slack_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_TOKEN", raising=False)


def test_is_slack_configured_false_when_unset() -> None:
    assert is_slack_configured() is False


def test_send_slack_message_noop_when_unset() -> None:
    assert send_slack_message("hello") is False


def test_send_slack_success_format(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_WEBHOOK_TOKEN", "T/B/secret")
    captured: dict[str, str] = {}

    def fake_urlopen(request, timeout=10):
        captured["payload"] = request.data.decode("utf-8")
        return mock.Mock(status=200, __enter__=lambda s: s, __exit__=mock.Mock())

    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        assert send_slack_success("pipeline", "download", "2013-01", detail="ok") is True

    payload = json.loads(captured["payload"])
    assert ":white_check_mark:" in payload["text"]
    assert "download" in payload["text"]
    assert "2013-01" in payload["text"]
    assert "ok" in payload["text"]


def test_send_slack_failure_truncates_long_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_WEBHOOK_TOKEN", "T/B/secret")
    captured: dict[str, str] = {}

    def fake_urlopen(request, timeout=10):
        captured["payload"] = request.data.decode("utf-8")
        return mock.Mock(status=200, __enter__=lambda s: s, __exit__=mock.Mock())

    long_error = "x" * 600
    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        send_slack_failure("pipeline", "train", "2013-01", long_error)

    payload = json.loads(captured["payload"])
    assert ":x:" in payload["text"]
    assert len(payload["text"]) < 700


def test_send_slack_pipeline_start(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/test")
    captured: dict[str, str] = {}

    def fake_urlopen(request, timeout=10):
        captured["payload"] = request.data.decode("utf-8")
        return mock.Mock(status=200, __enter__=lambda s: s, __exit__=mock.Mock())

    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        send_slack_pipeline_start("2013-01", ["download", "train"])

    payload = json.loads(captured["payload"])
    assert ":rocket:" in payload["text"]
    assert "download, train" in payload["text"]


def test_send_slack_pipeline_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/test")
    captured: dict[str, str] = {}

    def fake_urlopen(request, timeout=10):
        captured["payload"] = request.data.decode("utf-8")
        return mock.Mock(status=200, __enter__=lambda s: s, __exit__=mock.Mock())

    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        send_slack_pipeline_complete("2013-01", 42.5, extras="run_dir=artifacts/foo")

    payload = json.loads(captured["payload"])
    assert ":tada:" in payload["text"]
    assert "42.5s" in payload["text"]
    assert "artifacts/foo" in payload["text"]
