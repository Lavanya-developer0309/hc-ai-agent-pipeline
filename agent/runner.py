"""
The pipeline agent: qwen3:8b decides, step by step, what happens to a batch.

    .venv/bin/python -m agent.runner BATCH_005                 # mode from .env (APPROVAL_MODE)
    .venv/bin/python -m agent.runner BATCH_005 --mode human    # ask before every action

Each turn the model sees only the tools the guard allows right now, picks one and
says why; the tool runs (after approval in human mode) and the short result goes
back to the model. Every step is logged to AUDIT.AGENT_DECISION_LOG, every run to
AUDIT.AGENT_RUN. The run ends with LOADED, QUARANTINED or NEEDS_HUMAN.
"""
import argparse
import json
import re
import uuid
from datetime import datetime

from agent import approvals, llm
from pipeline import alerts
from agent.tools import TOOL_SPECS, Guard, run_tool
from pipeline import config

MAX_STEPS = 25
MAX_NUDGES = 3

SYSTEM = """You are the data pipeline agent for a healthcare payer data migration (legacy system -> Facets).
A batch of source files arrived in S3. Process it with the tools, one tool call per turn.
You only get the tools that are allowed at that moment. Give every call a clear reason.

Normal order: check_batch_files -> wait_for_snowpipe -> register_batch -> check_load -> profile_source
-> dbt_build -> load_target -> build_scorecard -> archive_batch -> finish_run(LOADED).

Your decisions:
1. Hard checks (check_batch_files, wait_for_snowpipe, check_load): if one fails, quarantine_batch, then finish_run(QUARANTINED).
2. After profile_source - decide: dbt_build (continue) or quarantine_batch. The result is already classified:
   - key_column_breaches: key columns or business-critical columns (date of birth, SSN, gender, member names,
     claim dates and amounts...) with too many problems. The guard forces quarantine.
   - concerns: required columns with problems in more than 20% of rows. You must then choose quarantine_batch
     (the safe default) or continue_despite_concerns (only if every concern column is clearly not needed for the
     migration, with a justification per column).
   - optional_columns and minor_issues are normal (dbt reject rules handle them): they are no reason to quarantine.
3. After dbt_build - warnings are normal. If dbt had errors, the guard only offers finish_run(NEEDS_HUMAN).
4. After build_scorecard - if every entity passed, archive_batch then finish_run(LOADED). If an entity FAILED, the guard
   only offers finish_run(NEEDS_HUMAN).
5. If a human rejects an action, or a tool keeps failing, you can only finish_run(NEEDS_HUMAN): explain what happened.

In finish_run, write a short summary for the data engineer with the key numbers from the tool results.
Never invent numbers - only use numbers that appear in tool results."""


# ---------------------------------------------------------------- logging

class SnowflakeLog:
    """AUDIT.AGENT_RUN + AUDIT.AGENT_DECISION_LOG, over one connection for the whole run."""

    def __init__(self, run_id, batch, mode):
        from pipeline import steps
        self.steps, self.conn = steps, steps.snowflake_conn()
        self.run_id, self.batch = run_id, batch
        steps.query(f"""INSERT INTO {config.DATABASE}.AUDIT.AGENT_RUN (RUN_ID, BATCH_ID, APPROVAL_MODE, MODEL, STARTED_AT)
                        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())""", (run_id, batch, mode, llm.MODEL), self.conn)

    def step(self, n, tool, reason, args, verdict, approval, result):
        self.steps.query(f"""INSERT INTO {config.DATABASE}.AUDIT.AGENT_DECISION_LOG
                             (RUN_ID, BATCH_ID, STEP_NO, TOOL, REASON, ARGS, VERDICT, APPROVAL, RESULT_OK, RESULT)
                             SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, PARSE_JSON(%s)""",
                         (self.run_id, self.batch, n, tool, reason, json.dumps(args, default=str), verdict, approval,
                          bool(result.get("ok")) if result else None, json.dumps(result, default=str)[:60000]), self.conn)

    def finish(self, status, summary, steps_taken):
        self.steps.query(f"""UPDATE {config.DATABASE}.AUDIT.AGENT_RUN SET ENDED_AT = CURRENT_TIMESTAMP(),
                             FINAL_STATUS = %s, SUMMARY = %s, STEPS = %s WHERE RUN_ID = %s""",
                         (status, summary, steps_taken, self.run_id), self.conn)
        self.conn.close()


class MemoryLog:
    """For tests: keeps the steps in a list."""

    def __init__(self, *_):
        self.rows, self.final = [], None

    def step(self, n, tool, reason, args, verdict, approval, result):
        self.rows.append({"step": n, "tool": tool, "reason": reason, "verdict": verdict, "approval": approval, "result": result})

    def finish(self, status, summary, steps_taken):
        self.final = {"status": status, "summary": summary, "steps": steps_taken}


# ---------------------------------------------------------------- the loop

def run(batch, mode=None, tool_impl=run_tool, log=None, approver=None, chat=llm.chat, echo=print, notify=None):
    """Process one batch. Returns {"status", "summary", "steps", "run_id"}.
    tool_impl / log / approver / chat can be replaced in tests."""
    mode = (mode or config.APPROVAL_MODE).lower()
    notify = (log is None) if notify is None else notify      # real runs send email alerts, tests don't
    run_id = f"agent_{datetime.now():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
    log = log or SnowflakeLog(run_id, batch, mode)
    guard = Guard()
    messages = [{"role": "system", "content": SYSTEM},
                {"role": "user", "content": f"New batch {batch} arrived in s3://{config.S3_BUCKET}/incoming/{batch}/. Process it."}]
    nudges, failures, step_no = 0, {}, 0
    evidence, retried_summary = [batch], False     # tool results: the only allowed source of numbers
    echo(f"[{run_id}] batch {batch}, mode {mode}, model {llm.MODEL}")

    def end(status, summary):
        log.finish(status, summary, step_no)
        echo(f"[{run_id}] FINISHED {status}: {summary}")
        if notify:
            alerts.run_finished(batch, status, summary, run_id, step_no)
        return {"status": status, "summary": summary, "steps": step_no, "run_id": run_id}

    while step_no < MAX_STEPS:
        allowed = guard.allowed()
        msg = chat(messages, [TOOL_SPECS[t] for t in allowed])
        calls = msg.get("tool_calls") or []
        if not calls:
            nudges += 1
            if nudges > MAX_NUDGES:
                return end("NEEDS_HUMAN", f"Agent stopped calling tools after step {step_no}; last message: {(msg.get('content') or '')[:300]}")
            messages += [msg, {"role": "user", "content": f"Call one of these tools: {', '.join(allowed)}."}]
            continue

        call = calls[0]["function"]
        name, args = call["name"], call.get("arguments") or {}
        if isinstance(args, str):
            args = json.loads(args or "{}")
        reason = args.get("reason", "")
        step_no += 1
        messages.append({"role": "assistant", "content": msg.get("content") or "",
                         "tool_calls": [calls[0]]})

        problem = guard.check(name, args)
        if problem:
            log.step(step_no, name, reason, args, "BLOCKED", None, {"ok": False, "error": problem})
            echo(f"  {step_no}. {name} BLOCKED: {problem}")
            messages.append({"role": "tool", "tool_name": name, "content": json.dumps({"ok": False, "error": problem})})
            continue

        if name == "finish_run":
            summary = args.get("summary", "")
            invented = unsupported_numbers(summary, evidence)
            if invented and not retried_summary:
                retried_summary = True
                problem = (f"Your summary uses numbers that are not in any tool result: {', '.join(invented)}. "
                           "Call finish_run again using only numbers from the tool results.")
                log.step(step_no, name, reason, args, "BLOCKED", None, {"ok": False, "error": problem})
                messages.append({"role": "tool", "tool_name": name, "content": json.dumps({"ok": False, "error": problem})})
                continue
            if invented:
                summary = factual_summary(args.get("status"), guard, evidence)
            log.step(step_no, name, reason, args, "ALLOWED", None, {"ok": True, "summary_replaced": bool(invented)})
            return end(args.get("status", "NEEDS_HUMAN"), summary)

        approval = None
        if mode == "human":
            echo(f"  {step_no}. waiting for approval: {name} - {reason}")
            approval, note = (approver or _ask_human)(run_id, batch, step_no, name, reason)
            if approval != "APPROVED":
                guard.hand_over(f"a human did not approve {name} ({note or approval})")
                result = {"ok": False, "rejected_by_human": True, "note": note or approval,
                          "allowed_next": guard.allowed()}
                log.step(step_no, name, reason, args, "ALLOWED", approval, result)
                echo(f"  {step_no}. {name} {approval} by human: {note}")
                messages.append({"role": "tool", "tool_name": name, "content": json.dumps(result)})
                continue

        try:
            result = tool_impl(name, batch, args)
        except Exception as e:  # a crashing tool is reported to the agent, not fatal
            result = {"ok": False, "error": f"{type(e).__name__}: {str(e).splitlines()[0][:300]}"}
        if not result.get("ok"):
            failures[name] = failures.get(name, 0) + 1
        guard.record(name, result)
        if failures.get(name, 0) >= 2:
            guard.hand_over(f"{name} failed twice")
        evidence.append(json.dumps(result, default=str))
        log.step(step_no, name, reason, args, "ALLOWED", approval, result)
        echo(f"  {step_no}. {name} -> ok={result.get('ok')}  | why: {reason}")
        messages.append({"role": "tool", "tool_name": name,
                         "content": json.dumps({**result, "allowed_next": guard.allowed()}, default=str)})

    return end("NEEDS_HUMAN", f"Agent did not finish within {MAX_STEPS} steps.")


def _numbers(text):
    return {n.replace(",", "").rstrip(".") for n in re.findall(r"\d[\d,]*(?:\.\d+)?", text)}


def unsupported_numbers(summary, evidence):
    """Numbers in the agent's summary that appear in no tool result (made up or miscalculated)."""
    allowed = _numbers(" ".join(evidence))
    allowed |= {n.split(".")[0] for n in allowed if re.fullmatch(r"\d+\.0+", n)}
    return sorted(_numbers(summary) - allowed, key=float)


def factual_summary(status, guard, evidence):
    steps_done = ", ".join(sorted(guard.done)) or "none"
    why = f" Reason: {guard.stuck_reason}." if guard.stuck_reason else ""
    return f"Run ended with {status}. Steps completed: {steps_done}.{why} (Summary written by code: the agent's text used numbers not found in the tool results.)"


def _ask_human(run_id, batch, step, tool, reason):
    req = approvals.request(run_id, batch, step, tool, reason)
    print(f"     approval #{req} pending - approve on the Pipeline page or: .venv/bin/python -m agent.approvals {req} yes")
    alerts.approval_needed(batch, req, tool, reason)
    return approvals.wait(req)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("batch")
    p.add_argument("--mode", choices=["auto", "human"])
    a = p.parse_args()
    print(json.dumps(run(a.batch, a.mode), indent=2))
