# HC Pipeline — AI-agent data migration pipeline (AWS · Snowflake · dbt · Airflow · local LLM)

A healthcare payer data migration (legacy extracts → Facets-like target) run end to end by an
**AI agent**: files land in **AWS S3**, **Snowpipe** loads them, **Airflow** hands each batch to a
**qwen3:8b agent** (running locally, free) that decides step by step — profile, quarantine or
continue, **dbt** build, load target, reconcile, archive — and explains every decision.
Everything runs on free tiers: AWS Free plan, Snowflake trial, Ollama, Airflow on a laptop.

```mermaid
flowchart LR
    client([Client drops batch]) --> s3in[(S3 incoming/BATCH_x/)]
    s3in -- S3 event --> pipe[Snowpipe x5]
    pipe --> raw[(Snowflake RAW)]
    af[Airflow<br/>every 10 min] -- claims batch --> agent{{AI agent<br/>qwen3:8b}}
    agent -- tools --> raw
    agent -- profiling --> dq[(AUDIT profile)]
    agent -- dbt build --> dbt[dbt: staging → crosswalks →<br/>tests → keyword files]
    dbt --> tgt[(FACETS_TGT)]
    agent -- scorecard --> sc[(AUDIT scorecard)]
    agent -- archive / quarantine --> s3out[(S3 archive/ · rejected/)]
    agent -- every decision --> log[(AGENT_RUN<br/>AGENT_DECISION_LOG)]
    agent -- alert --> sns[SNS email]
    human([Data engineer]) -. approve / reject .-> ui[Pipeline page]
    ui -.-> agent
```

> Placeholders: replace `<your-landing-bucket>` and `<aws-account-id>` in `snowflake/` and `aws/`, and fill `.env`
> from `.env.example`, before running the setup scripts.

## How a batch flows
1. Upload `SRC_*.txt` files to `s3://<your-landing-bucket>/incoming/BATCH_007/`.
2. S3 notifies **Snowpipe**; five pipes load the files into `RAW.SRC_*` (batch id from the folder name) in ~1 min.
3. **Airflow** (`hc_agent_pipeline`, every 10 min) finds the batch in S3 (S3-only check — no warehouse cost when idle)
   and claims it with an atomic `_PROCESSING` marker, so two runs never take the same batch.
4. The **agent** processes it with tools and ends with **LOADED**, **QUARANTINED** or **NEEDS_HUMAN**, then sends an email.

## The agent
| | |
|---|---|
| Model | `qwen3:8b` via Ollama, local, temperature 0 |
| Tools | `check_batch_files` · `wait_for_snowpipe` · `register_batch` · `check_load` · `profile_source` · `continue_despite_concerns` · `dbt_build` · `load_target` · `build_scorecard` · `archive_batch` · `quarantine_batch` · `finish_run` |
| Each call | carries a **reason**; a **guard** checks it is allowed; result goes back to the model; everything is logged |
| Modes | `APPROVAL_MODE=auto` acts on its own · `human` asks before **every** action (Pipeline page or CLI) |

**Rules vs AI** — testing showed an 8B model will happily load a batch with 40 % missing member ids, so
everything measurable is a rule the guard enforces; the AI orders the work, judges what rules can't, and explains:

| Situation | Decided by |
|---|---|
| Missing/empty file, 0 rows loaded, > 1 % rows rejected by Snowpipe | rule → quarantine |
| Key column (ids) > 1 % bad, or critical column (DOB, SSN, gender, member names, claim dates/amounts/codes) > 20 % bad | rule → quarantine |
| Any other column > 20 % bad | **agent** — quarantine, or continue with a logged justification |
| Empty optional columns (middle initial, modifiers, org vs person names…) | rule → ignored |
| dbt errors · an entity FAILED reconciliation · a human said no · a tool failed twice | rule → only `NEEDS_HUMAN` |
| Step order, when to stop, the summary for the engineer | **agent** (numbers in the summary are checked against tool results) |

## Tests
```bash
.venv/bin/python -m tests.test_guard   # 7 rule tests, < 1 s
.venv/bin/python -m tests.eval_agent   # 11 scenarios with the real model and scripted tool results (~10 min)
```
Scenarios: clean batch · missing file · 0 rows · bad key column · bad DOB · unused column concern · dbt errors ·
failed scorecard · tool crash · human rejects · human approves all — **11/11 pass**.
Real batches BATCH_002–006 produce identical scorecards (33,753 source rows, 449 rejected, 33,304 loaded).

## Run it
```bash
./run_airflow.sh                                          # Airflow  http://localhost:8080  (user admin, password in airflow_home/…generated)
cd ../ai-data-profiling-agent && ./run.sh                 # web app  http://127.0.0.1:5050  → Pipeline page
.venv/bin/python -m agent.runner BATCH_007 --mode human   # run the agent by hand
.venv/bin/python -m agent.approvals                       # list / answer approvals from the terminal
.venv/bin/python -m pipeline.steps                        # batches waiting in S3
.venv/bin/python -m pipeline.steps release BATCH_007      # unlock a batch after a crash or a fix
```
Agent history in Snowflake: `SELECT * FROM HC_MIGRATION_DB.AUDIT.V_AGENT_RUNS;`

## Layout
```
agent/       runner.py (loop, logging, summary check) · tools.py (tools + guard + profile classification) · llm.py · approvals.py
pipeline/    steps.py (S3, Snowpipe, procedures, dbt, archive/quarantine, batch locks) · alerts.py (SNS) · config.py
dags/        hc_agent_pipeline.py (agent, active) · hc_s3_batch_pipeline.py (rule-based fallback, paused)
snowflake/   bootstrap · SP_LOAD_RAW_S3 · Snowpipes · SP_REGISTER_S3_BATCH · agent log tables
aws/         IAM policy for the read-only Snowflake user · S3 → Snowpipe notification
tests/       test_guard.py · eval_agent.py · fixture from a real profiling run
```
The dbt project, stored procedures and source data live in the migration project
(`~/Downloads/Lavanya/snowflake-healthcare-data-migration`), unchanged except one line: source tests are
filtered to the batch being run.

## Lessons (what went wrong and how it was fixed)
- **AWS Free plan accounts sit in an AWS-managed organisation**: Sydney only, no IAM console passwords, and
  outside accounts cannot assume roles — so the Snowflake storage integration cannot work; the stage uses a
  read-only IAM user scoped to one bucket instead. Snowflake was moved to AWS Sydney to match.
- **A new S3 → Snowpipe notification takes minutes to activate**; files uploaded straight away are missed.
  `ALTER PIPE … REFRESH PREFIX = '<batch>/'` (path relative to the pipe) catches up; `wait_for_snowpipe` does it automatically.
- **A 0-row load "succeeded" end to end** in the original pipeline (wrong file names) — now a hard rule.
- **dbt source tests counted every batch in RAW** (8,000 false duplicate claims) — fixed with a `where: BATCH_ID = …` config.
- **Two Airflow runs overlapped** despite `max_active_runs=1` — fixed with an atomic S3 lock (`IfNoneMatch="*"`).
- **A 16 GB Mac running an 8B model swaps hard**; Airflow killed a healthy task. Fewer workers, a 20-minute
  heartbeat timeout, smaller model context (8k) and stale-lock recovery fixed it.

## Cost
AWS Free plan (no charges possible; S3/SNS usage within credits) · Snowflake trial credits (XS warehouse, 60 s auto-suspend,
10-credit monthly cap) · Ollama, Airflow and dbt Core are free and local.
