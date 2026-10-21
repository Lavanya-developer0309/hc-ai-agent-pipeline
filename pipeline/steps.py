"""
Pipeline steps for one batch that arrived in s3://<bucket>/incoming/<BATCH_ID>/.

Each function does one thing and returns a plain dict, so it can be used
  - by the Airflow DAG (dags/hc_s3_batch_pipeline.py), and
  - as a tool by the AI agent (step 4).
Every number comes from S3, Snowflake or dbt - nothing here is guessed.
"""
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone

import boto3
import snowflake.connector

from pipeline import config

INCOMING, ARCHIVE, REJECTED = "incoming/", "archive/", "rejected/"
LOCK = "_PROCESSING"   # marker a run writes into incoming/<batch>/ so no other run takes the same batch
STALE_LOCK_HOURS = 4   # longer than any run may take (agent task timeout is 3 h): the run that held it died


# ---------------------------------------------------------------- connections

def snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.getenv("SNOWFLAKE_USER", "MIG_DBT_SVC"),
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(config.path("SNOWFLAKE_PRIVATE_KEY_PATH")),
        role=os.getenv("SNOWFLAKE_ROLE", "MIG_DEVELOPER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "MIG_WH"),
        database=config.DATABASE,
    )


def query(sql, binds=None, conn=None):
    """Run one statement; pass conn to reuse an open connection (each new one costs ~2-3 s)."""
    own = conn is None
    conn = conn or snowflake_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, binds)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        if own:
            conn.close()


def s3():
    return boto3.client("s3", region_name=config.AWS_REGION)


def _call(proc, batch):
    """Call an AUDIT procedure the way the dbt macro does: an 'error' in the message means failure."""
    message = query(f"CALL {config.DATABASE}.AUDIT.{proc}(%s)", (batch,))[0][proc]
    failed = "error" in message.lower() and "with 0 error" not in message.lower()
    return {"step": proc, "batch": batch, "ok": not failed, "message": message}


def expected_files(conn=None):
    """Source files every batch must contain, from AUDIT.MIG_SOURCE_FILE_CONFIG."""
    return query(f"SELECT RAW_TABLE, FILE_NAME FROM {config.DATABASE}.AUDIT.MIG_SOURCE_FILE_CONFIG "
                 "WHERE IS_ACTIVE ORDER BY LOAD_SEQ", conn=conn)


# ---------------------------------------------------------------- S3 (no Snowflake, so polling costs nothing)

def find_new_batches():
    """Batch folders waiting in incoming/. Uses only S3, so it can run every few minutes for free."""
    found = {}
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=config.S3_BUCKET, Prefix=INCOMING):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) == 3 and parts[1] and parts[2]:
                found.setdefault(parts[1], []).append({"file": parts[2], "size": obj["Size"],
                                                       "uploaded": obj["LastModified"].isoformat()})
    batches = []
    for b, files in sorted(found.items()):
        lock = next((f for f in files if f["file"] == LOCK), None)
        stale = bool(lock) and (datetime.now(timezone.utc) - datetime.fromisoformat(lock["uploaded"])).total_seconds() > STALE_LOCK_HOURS * 3600
        batches.append({"batch": b, "claimed": bool(lock) and not stale, "stale_lock": stale,
                        "files": sorted((f for f in files if not f["file"].startswith("_")), key=lambda f: f["file"])})
    return batches


def release_batch(batch):
    """Remove a batch's lock so the next run picks it up again (after a crashed run, or once a human has fixed it)."""
    s3().delete_object(Bucket=config.S3_BUCKET, Key=f"{INCOMING}{batch}/{LOCK}")
    return {"step": "release", "batch": batch, "ok": True}


def claim_batch(batch, run_id):
    """Atomically mark the batch as taken (S3 conditional write). False if another run already has it."""
    try:
        s3().put_object(Bucket=config.S3_BUCKET, Key=f"{INCOMING}{batch}/{LOCK}", IfNoneMatch="*",
                        Body=f"{run_id}\n{datetime.now(timezone.utc).isoformat()}\n".encode())
        return True
    except s3().exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict"):
            return False
        raise


def pick_batch(run_id, forced=None, upload_grace_minutes=30):
    """Oldest batch that is ready and not taken; claims it atomically. None if nothing to do.
    A batch is ready when all expected files are there, or when its newest file is older than the
    grace period (then the missing files are reported by check_batch_files)."""
    now = datetime.now(timezone.utc)
    for b in find_new_batches():
        if forced and b["batch"] != forced:
            continue
        if b["claimed"]:
            print(f"{b['batch']}: already being processed by another run - skipped")
            continue
        newest = max((datetime.fromisoformat(f["uploaded"]) for f in b["files"]), default=now)
        ready = forced or check_batch_files(b["batch"])["ok"] or (now - newest).total_seconds() > upload_grace_minutes * 60
        if not ready:
            print(f"{b['batch']}: upload still in progress, checking again next run")
        elif b["stale_lock"]:
            print(f"{b['batch']}: lock is older than {STALE_LOCK_HOURS} h (the run that held it died) - taking it over")
            release_batch(b["batch"])
            if claim_batch(b["batch"], run_id):
                return b["batch"]
        elif claim_batch(b["batch"], run_id):
            print(f"{b['batch']}: claimed by {run_id}")
            return b["batch"]
    return None


def _move(batch, to_prefix, note=None):
    client, moved = s3(), []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=config.S3_BUCKET, Prefix=f"{INCOMING}{batch}/"):
        for obj in page.get("Contents", []):
            new_key = to_prefix + obj["Key"][len(INCOMING):]
            client.copy_object(Bucket=config.S3_BUCKET, Key=new_key,
                               CopySource={"Bucket": config.S3_BUCKET, "Key": obj["Key"]})
            client.delete_object(Bucket=config.S3_BUCKET, Key=obj["Key"])
            moved.append(new_key)
    if note:
        client.put_object(Bucket=config.S3_BUCKET, Key=f"{to_prefix}{batch}/_REASON.txt",
                          Body=f"{datetime.now(timezone.utc).isoformat()}\n{note}\n".encode())
    return moved


def archive_batch(batch):
    """Processed: move the files to archive/<batch>/."""
    return {"step": "archive", "batch": batch, "ok": True, "moved_to": f"s3://{config.S3_BUCKET}/{ARCHIVE}{batch}/",
            "files": _move(batch, ARCHIVE)}


def quarantine_batch(batch, reason):
    """Not safe to load: move the files to rejected/<batch>/ with the reason, and remove the rows
    Snowpipe already put in RAW. Reversible: move the files back to incoming/ and refresh the pipes."""
    conn = snowflake_conn()
    try:
        removed = {f["RAW_TABLE"]: query(f"DELETE FROM {config.DATABASE}.RAW.{f['RAW_TABLE']} WHERE BATCH_ID = %s",
                                         (batch,), conn)[0]["number of rows deleted"]
                   for f in expected_files(conn)}
    finally:
        conn.close()
    return {"step": "quarantine", "batch": batch, "ok": True, "reason": reason, "raw_rows_removed": removed,
            "moved_to": f"s3://{config.S3_BUCKET}/{REJECTED}{batch}/", "files": _move(batch, REJECTED, reason)}


# ---------------------------------------------------------------- loading (Snowpipe)

def check_batch_files(batch):
    """Hard rule: every expected source file is present and not empty."""
    present = {f["file"]: f["size"] for b in find_new_batches() if b["batch"] == batch for f in b["files"]}
    expected = [f["FILE_NAME"] for f in expected_files()]
    missing = [f for f in expected if f not in present]
    empty = [f for f in expected if present.get(f) == 0]
    unexpected = [f for f in present if f not in expected]
    problems = [f"missing file: {f}" for f in missing] + [f"empty file: {f}" for f in empty]
    return {"step": "check_files", "batch": batch, "ok": not problems, "problems": problems,
            "files": present, "unexpected_files": unexpected}


def snowpipe_status(batch):
    """Rows in RAW for this batch + what Snowpipe reported per file (from COPY_HISTORY)."""
    conn = snowflake_conn()
    try:
        return _snowpipe_status(batch, conn)
    finally:
        conn.close()


def _snowpipe_status(batch, conn):
    files, status = expected_files(conn), []
    for f in files:
        table = f"{config.DATABASE}.RAW.{f['RAW_TABLE']}"
        rows = query(f"SELECT COUNT(*) AS N FROM {table} WHERE BATCH_ID = %s", (batch,), conn)[0]["N"]
        hist = query(f"""SELECT STATUS, ROW_PARSED, ROW_COUNT, ERROR_COUNT, FIRST_ERROR_MESSAGE
                         FROM TABLE({config.DATABASE}.INFORMATION_SCHEMA.COPY_HISTORY(
                              TABLE_NAME => %s, START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())))
                         WHERE FILE_NAME = %s AND PIPE_NAME IS NOT NULL      -- path is relative to the pipe's incoming/ folder
                         ORDER BY LAST_LOAD_TIME DESC LIMIT 1""",
                     (table, f"{batch}/{f['FILE_NAME']}"), conn)
        h = hist[0] if hist else {}
        status.append({"table": f["RAW_TABLE"], "file": f["FILE_NAME"], "rows_in_raw": rows,
                       "snowpipe_status": h.get("STATUS", "NOT RECEIVED"), "rows_parsed": h.get("ROW_PARSED"),
                       "rows_rejected": h.get("ERROR_COUNT"), "first_error": h.get("FIRST_ERROR_MESSAGE")})
    return status


def refresh_snowpipe(batch):
    """Catch up on files whose S3 event was missed (ALTER PIPE ... REFRESH; Snowpipe never loads a file twice)."""
    sent = []
    for f in expected_files():
        for r in query(f"ALTER PIPE {config.DATABASE}.RAW.PIPE_{f['RAW_TABLE']} REFRESH PREFIX = %s", (f"{batch}/",)):
            sent.append(r.get("File"))
    return {"step": "refresh_snowpipe", "batch": batch, "ok": True, "files_sent": sent}


def wait_for_snowpipe(batch, timeout=None):
    """Wait until Snowpipe has finished every file of the batch. Refreshes once if a file never arrived."""
    timeout = timeout or config.SNOWPIPE_TIMEOUT
    start, refreshed = time.time(), False
    while True:
        status = snowpipe_status(batch)
        done = all(s["snowpipe_status"] != "NOT RECEIVED" for s in status)
        if done:
            return {"step": "wait_for_snowpipe", "batch": batch, "ok": True,
                    "seconds": round(time.time() - start), "refreshed": refreshed, "files": status}
        if not refreshed and time.time() - start > 120:
            refresh_snowpipe(batch)
            refreshed = True
        if time.time() - start > timeout:
            return {"step": "wait_for_snowpipe", "batch": batch, "ok": False, "seconds": round(time.time() - start),
                    "refreshed": refreshed, "files": status,
                    "problems": [f"Snowpipe did not load {s['file']}" for s in status if s["snowpipe_status"] == "NOT RECEIVED"]}
        time.sleep(15)


def register_batch(batch):
    """Bookkeeping after Snowpipe (batch control, keyword files, MIG_LOAD_LOG)."""
    return _call("SP_REGISTER_S3_BATCH", batch)


def check_load(batch):
    """Hard rules after loading - these stop the batch whatever the AI thinks:
    every file loaded rows, and Snowpipe rejected no more than 1% of a file's rows."""
    status, problems = snowpipe_status(batch), []
    for s in status:
        if s["rows_in_raw"] == 0:
            problems.append(f"{s['file']}: 0 rows loaded (Snowpipe status {s['snowpipe_status']})")
        elif s["rows_rejected"] and s["rows_parsed"] and s["rows_rejected"] / s["rows_parsed"] > 0.01:
            problems.append(f"{s['file']}: {s['rows_rejected']} of {s['rows_parsed']} rows rejected by Snowpipe "
                            f"({s['first_error']})")
    return {"step": "check_load", "batch": batch, "ok": not problems, "problems": problems, "files": status}


# ---------------------------------------------------------------- the migration project's steps

def profile_source(batch):
    return _call("SP_PROFILE_ALL", batch)


def profiling_summary(batch):
    """Profiling observations for the batch (what the AI gate reads in step 4)."""
    rows = query(f"SELECT * FROM {config.DATABASE}.AUDIT.V_DQ_PROFILE_REPORT WHERE BATCH_ID = %s", (batch,))
    return {"step": "profiling_summary", "batch": batch, "ok": True, "observations": rows}


def _dbt_env():
    env = dict(os.environ)
    env["PATH"] = f"{config.DBT_BIN.parent}:{env.get('PATH', '')}"
    env["SNOWFLAKE_PRIVATE_KEY_PATH"] = str(config.path("SNOWFLAKE_PRIVATE_KEY_PATH"))
    return env


def dbt(args, batch):
    cmd = [str(config.DBT_BIN), *args, "--vars", json.dumps({"batch_id": batch})]
    p = subprocess.run(cmd, cwd=config.DBT_DIR, env=_dbt_env(), capture_output=True, text=True)
    return p.returncode, re.sub(r"\x1b\[[0-9;]*m", "", p.stdout + p.stderr)


def dbt_build(batch):
    """Clear old keyword files, run dbt build, and summarise run_results.json (failed tests with row counts)."""
    code, out = dbt(["run-operation", "clear_keyword_files"], batch)
    if code:
        return {"step": "dbt_build", "batch": batch, "ok": False, "problems": ["clear_keyword_files failed"], "log_tail": out[-1500:]}
    code, out = dbt(["build"], batch)
    results = json.loads((config.DBT_DIR / "target" / "run_results.json").read_text())["results"]
    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    not_passed = [{"node": r["unique_id"].split(".")[-2] if r["unique_id"].startswith("test.") else r["unique_id"],
                   "status": r["status"], "failures": r.get("failures"), "message": (r.get("message") or "")[:300]}
                  for r in results if r["status"] not in ("pass", "success")]
    return {"step": "dbt_build", "batch": batch, "ok": code == 0, "summary": counts,
            "errors": [n for n in not_passed if n["status"] in ("error", "fail")],
            "warnings": [n for n in not_passed if n["status"] == "warn"]}


def load_target(batch):
    return _call("SP_LOAD_FACETS_TARGET", batch)


def build_scorecard(batch):
    result = _call("SP_BUILD_SCORECARD", batch)
    result["scorecard"] = query(f"""SELECT ENTITY_NAME, SOURCE_COUNT, REJECT_COUNT, TARGET_COUNT, LOAD_PCT,
                                           SOURCE_RECON_STATUS, TARGET_RECON_STATUS, OVERALL_STATUS
                                    FROM {config.DATABASE}.AUDIT.V_SCORECARD WHERE BATCH_ID = %s ORDER BY ENTITY_NAME""", (batch,))
    return result


if __name__ == "__main__":
    # .venv/bin/python -m pipeline.steps release BATCH_005   - unlock a batch after a crashed run
    import sys
    if len(sys.argv) == 3 and sys.argv[1] == "release":
        print(release_batch(sys.argv[2]))
    else:
        for b in find_new_batches():
            print(b["batch"], "claimed" if b["claimed"] else ("STALE LOCK" if b["stale_lock"] else "waiting"), [f["file"] for f in b["files"]])
