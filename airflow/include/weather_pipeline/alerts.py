from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def format_failure(context: dict) -> tuple[str, str]:
    ti = context.get("task_instance")
    dag_id = getattr(ti, "dag_id", "unknown_dag")
    task_id = getattr(ti, "task_id", "unknown_task")
    run_id = context.get("run_id", "unknown_run")
    try_number = getattr(ti, "try_number", "?")
    log_url = getattr(ti, "log_url", "")
    exception = context.get("exception")
    subject = f"[Airflow] {dag_id}.{task_id} failed"
    message = (f"DAG: {dag_id}\nTask: {task_id}\nRun: {run_id}\nTry: {try_number}\n"
               f"Error: {exception!r}\nLogs: {log_url}")
    return subject, message


def notify_failure(context: dict) -> None:
    subject, message = format_failure(context)
    log.error("%s\n%s", subject, message)
    topic = os.environ.get("ALERT_SNS_TOPIC_ARN")
    if not topic:
        return
    try:  # an alerting problem must never mask the original failure
        from airflow.providers.amazon.aws.hooks.sns import SnsHook

        SnsHook(aws_conn_id=os.environ.get("AWS_CONN_ID", "aws_default")).publish_to_target(
            target_arn=topic, message=message, subject=subject[:100])
    except Exception:
        log.exception("Could not publish the failure alert to SNS")
