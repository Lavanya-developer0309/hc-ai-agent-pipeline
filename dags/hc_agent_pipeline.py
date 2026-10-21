"""
hc_agent_pipeline - the AI agent (qwen3:8b, local) runs the batch.

Every 10 minutes Airflow checks s3://<bucket>/incoming/ (S3 only - free). When a batch
is ready, Airflow claims it and hands it to the agent, which decides step by step:
check files -> Snowpipe -> load checks -> profiling (quarantine?) -> dbt (stop?) ->
target -> scorecard -> archive, and ends with LOADED / QUARANTINED / NEEDS_HUMAN.

APPROVAL_MODE in hc-pipeline/.env:  auto = agent acts on its own,
human = agent asks before every action (answer with: python -m agent.approvals <id> yes|no).

Every decision: HC_MIGRATION_DB.AUDIT.V_AGENT_RUNS and AUDIT.AGENT_DECISION_LOG.
Trigger a specific batch by hand: Trigger DAG w/ config  {"batch": "BATCH_005"}
The rule-based DAG hc_s3_batch_pipeline is kept (paused) as a fallback.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from agent import runner  # noqa: E402
from pipeline import steps  # noqa: E402


@dag(
    dag_id="hc_agent_pipeline",
    description="AI agent (qwen3:8b) processes each S3 batch: load, profile, dbt, target, scorecard",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 10, 21),
    catchup=False,
    max_active_runs=1,
    params={"batch": ""},
    tags=["hc-migration", "agent", "ai", "s3", "snowpipe", "dbt"],
)
def hc_agent_pipeline():

    @task.short_circuit
    def find_batch():
        context = get_current_context()
        return steps.pick_batch(context["run_id"], context["params"].get("batch")) or False

    @task(retries=0, execution_timeout=timedelta(hours=3))
    def run_agent(batch):
        """NEEDS_HUMAN fails the task (red in Airflow) so it gets attention; LOADED and QUARANTINED succeed."""
        result = runner.run(batch)
        if result["status"] == "NEEDS_HUMAN":
            raise AirflowFailException(f"{batch} needs a human: {result['summary']}")
        return result

    run_agent(find_batch())


hc_agent_pipeline()
