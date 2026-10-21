"""
hc_s3_batch_pipeline - one batch from S3 through the whole migration pipeline.

Every 10 minutes Airflow looks in s3://<bucket>/incoming/ (S3 only - no Snowflake
warehouse is woken up when nothing arrived). If a batch folder is ready, it runs:

  check_files -> wait_for_snowpipe -> register_batch -> check_load
     -> profile_source -> dbt_build -> load_target -> build_scorecard -> archive

Hard rules (missing/empty files, 0 rows loaded, >1% rows rejected) quarantine the
batch to rejected/<batch>/ with a _REASON.txt and stop. One batch per run, oldest first.
A run claims its batch with an atomic _PROCESSING marker, so two runs never take the same batch.
Step 4 adds the AI agent's decisions between these steps.

Run a specific batch by hand: Trigger DAG w/ config  {"batch": "BATCH_002"}
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline import steps  # noqa: E402

def _stop(result, quarantine=True):
    """Hard rule broken: optionally quarantine the batch, then fail the task with the reason."""
    reason = "; ".join(result.get("problems") or ["step failed"])
    if quarantine:
        steps.quarantine_batch(result["batch"], f"{result['step']}: {reason}")
    raise AirflowFailException(f"{result['step']} failed for {result['batch']}: {reason}")


@dag(
    dag_id="hc_s3_batch_pipeline",
    description="S3 -> Snowpipe -> checks -> profiling -> dbt -> target -> scorecard -> archive",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 10, 21),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    params={"batch": ""},
    tags=["hc-migration", "s3", "snowpipe", "dbt"],
)
def hc_s3_batch_pipeline():

    @task.short_circuit
    def find_batch():
        """Pick the batch to process (or skip the run if nothing is waiting). S3 only - free."""
        context = get_current_context()
        return steps.pick_batch(context["run_id"], context["params"].get("batch")) or False

    @task
    def check_files(batch):
        r = steps.check_batch_files(batch)
        print(r)
        if not r["ok"]:
            _stop(r)
        return batch

    @task(retries=0)
    def wait_for_snowpipe(batch):
        r = steps.wait_for_snowpipe(batch)
        print(r)
        if not r["ok"]:
            _stop(r)
        return batch

    @task
    def register_batch(batch):
        r = steps.register_batch(batch)
        print(r["message"])
        if not r["ok"]:
            _stop({**r, "problems": [r["message"]]}, quarantine=False)
        return batch

    @task
    def check_load(batch):
        r = steps.check_load(batch)
        print(r)
        if not r["ok"]:
            _stop(r)
        return batch

    @task
    def profile_source(batch):
        r = steps.profile_source(batch)
        print(r["message"])
        if not r["ok"]:
            _stop({**r, "problems": [r["message"]]}, quarantine=False)
        return batch

    @task(retries=0)
    def dbt_build(batch):
        """dbt test failures don't stop the run (like run_pipeline.sh): entities that failed
        validation get no keyword file, and the scorecard shows exactly what loaded."""
        r = steps.dbt_build(batch)
        print("dbt summary:", r["summary"])
        for e in r["errors"]:
            print("ERROR:", e)
        print(f"{len(r['warnings'])} warning(s):", [w["node"] for w in r["warnings"]])
        return batch

    @task
    def load_target(batch):
        r = steps.load_target(batch)
        print(r["message"])
        if not r["ok"]:
            _stop({**r, "problems": [r["message"]]}, quarantine=False)
        return batch

    @task
    def build_scorecard(batch):
        r = steps.build_scorecard(batch)
        print(r["message"])
        for row in r["scorecard"]:
            print(row)
        return batch

    @task
    def archive(batch):
        r = steps.archive_batch(batch)
        print(f"moved {len(r['files'])} files to {r['moved_to']}")
        return r

    archive(build_scorecard(load_target(dbt_build(profile_source(
        check_load(register_batch(wait_for_snowpipe(check_files(find_batch())))))))))


hc_s3_batch_pipeline()
