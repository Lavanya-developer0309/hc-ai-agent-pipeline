"""
Email alerts through AWS SNS (topic in .env: SNS_TOPIC_ARN; free tier covers 1,000 emails/month).
An alert that cannot be sent is printed and never stops the pipeline.
"""
import os

import boto3

from pipeline import config

APP_URL = os.getenv("PROFILING_APP_URL", "http://127.0.0.1:5050")
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")

NEXT_STEPS = {
    "LOADED": "Nothing to do. The files are in archive/ and the target tables are loaded.",
    "QUARANTINED": ("The batch was NOT loaded. Its files are in s3://{bucket}/rejected/{batch}/ with _REASON.txt.\n"
                    "Ask the client for corrected files and upload them to incoming/<new batch>/ - or, if the batch is\n"
                    "fine after all, move the files back to incoming/{batch}/ and remove _PROCESSING."),
    "NEEDS_HUMAN": ("The agent stopped and handed the batch to you. Files stay in incoming/{batch}/ (locked).\n"
                    "Read the decisions on the Pipeline page, fix the cause, then release the lock to re-run:\n"
                    "  .venv/bin/python -m pipeline.steps release {batch}"),
}


def send(subject, body):
    topic = os.getenv("SNS_TOPIC_ARN")
    if not topic:
        print("alert not sent (SNS_TOPIC_ARN not set):", subject)
        return False
    try:
        boto3.client("sns", region_name=config.AWS_REGION).publish(TopicArn=topic, Subject=subject[:99], Message=body)
        return True
    except Exception as e:   # never let an alert break a run
        print("alert not sent:", e)
        return False


def run_finished(batch, status, summary, run_id, steps_taken):
    body = (f"Batch:   {batch}\nStatus:  {status}\nRun:     {run_id}  ({steps_taken} steps, agent qwen3:8b)\n\n"
            f"What happened:\n{summary}\n\nWhat to do:\n"
            f"{NEXT_STEPS.get(status, 'Check the run.').format(bucket=config.S3_BUCKET, batch=batch)}\n\n"
            f"Pipeline page: {APP_URL}/pipeline\nAirflow:       {AIRFLOW_URL}/dags/hc_agent_pipeline\n"
            f"Snowflake:     SELECT * FROM HC_MIGRATION_DB.AUDIT.AGENT_DECISION_LOG WHERE RUN_ID = '{run_id}' ORDER BY STEP_NO;\n")
    icon = {"LOADED": "✅", "QUARANTINED": "⛔", "NEEDS_HUMAN": "⚠️"}.get(status, "")
    return send(f"{icon} [HC Pipeline] {batch} {status}", body)


def approval_needed(batch, approval_id, tool, reason):
    body = (f"The pipeline agent wants to run '{tool}' for {batch} and is waiting for you.\n\n"
            f"Its reason: {reason}\n\nApprove or reject on the Pipeline page: {APP_URL}/pipeline\n"
            f"(or: .venv/bin/python -m agent.approvals {approval_id} yes|no)\n")
    return send(f"⏳ [HC Pipeline] {batch}: approve {tool}?", body)
