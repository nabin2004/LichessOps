"""Reusable Slack webhook failure callbacks for Airflow DAGs."""

from __future__ import annotations

from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification

SLACK_WEBHOOK_CONN_ID = "slackwebhook"

dag_failure_slack_webhook_notification = send_slack_webhook_notification(
    slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
    text=":x: DAG `{{ dag.dag_id }}` failed\nRun: {{ run_id }}\nLogical date: {{ ds }}",
)

task_failure_slack_webhook_notification = send_slack_webhook_notification(
    slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
    text=(
        ":x: Task `{{ ti.task_id }}` failed in DAG `{{ dag.dag_id }}`\n"
        "Run: {{ run_id }}\nMap index: {{ ti.map_index }}\n"
        "Exception: {{ exception }}"
    ),
)
