"""
The agent's tools: thin wrappers around pipeline/steps.py that return SHORT results
(small models lose track of long JSON), plus the GUARD that decides which tools are
allowed at each point of a run.

Every tool takes a "reason" - the agent must say why it is calling it; the reason is
logged with the result in AUDIT.AGENT_DECISION_LOG.
"""
import re

from pipeline import steps

# ---------------------------------------------------------------- what the agent sees

def _tool(name, description, **params):
    props = {"reason": {"type": "string", "description": "One sentence: why you are calling this tool now."}}
    props.update(params)
    return {"type": "function", "function": {"name": name, "description": description,
            "parameters": {"type": "object", "properties": props, "required": list(props)}}}


TOOL_SPECS = {
    "check_batch_files": _tool("check_batch_files", "Check every expected source file of the batch is in S3 and not empty."),
    "wait_for_snowpipe": _tool("wait_for_snowpipe", "Wait until Snowpipe has loaded every file of the batch into RAW (can take a few minutes)."),
    "register_batch": _tool("register_batch", "Register the loaded batch (batch control, load log). Call after Snowpipe has loaded it."),
    "check_load": _tool("check_load", "Check rows were loaded for every file and Snowpipe rejected no more than 1% of rows."),
    "profile_source": _tool("profile_source", "Profile the batch's source data and return the data-quality observations per column."),
    "dbt_build": _tool("dbt_build", "Run dbt build (transformations, crosswalks, tests, keyword files) for the batch."),
    "load_target": _tool("load_target", "Load the validated keyword files into the target (FACETS_TGT) tables."),
    "build_scorecard": _tool("build_scorecard", "Build the reconciliation scorecard (source vs rejected vs loaded per entity)."),
    "archive_batch": _tool("archive_batch", "Move the batch's files from incoming/ to archive/ in S3 (batch is done)."),
    "quarantine_batch": _tool("quarantine_batch", "Do NOT load this batch: move its files to rejected/ in S3 with the reason, and remove its RAW rows."),
    "continue_despite_concerns": _tool("continue_despite_concerns", "Continue to dbt_build DESPITE the profiling concerns. Only if every concern "
                             "column is clearly not needed for the migration; your justification is logged for review.",
                             justification={"type": "string", "description": "Per concern column: why it is acceptable."}),
    "finish_run": _tool("finish_run", "End the run with a final status and a short plain-English summary for the data engineer.",
                        status={"type": "string", "enum": ["LOADED", "QUARANTINED", "NEEDS_HUMAN"]},
                        summary={"type": "string", "description": "2-4 sentences: what happened and why, with the key numbers."}),
}

# ---------------------------------------------------------------- the guard

NEXT = {  # after a successful tool -> tools that may come next
    None: ["check_batch_files"],
    "check_batch_files": ["wait_for_snowpipe"],
    "wait_for_snowpipe": ["register_batch"],
    "register_batch": ["check_load"],
    "check_load": ["profile_source"],
    "profile_source": ["dbt_build", "quarantine_batch"],          # judgement call 1 (no concerns)
    "continue_despite_concerns": ["dbt_build"],
    "profile_concerns": ["quarantine_batch", "continue_despite_concerns"],  # judgement call 1 (with concerns)
    "dbt_build": ["load_target"],                                 # judgement call 2 (or finish NEEDS_HUMAN)
    "load_target": ["build_scorecard"],
    "build_scorecard": ["archive_batch"],                         # judgement call 3 (or finish NEEDS_HUMAN)
    "archive_batch": [],
    "quarantine_batch": [],
}
HARD_RULE_TOOLS = {"check_batch_files", "wait_for_snowpipe", "check_load"}   # a failure here forces quarantine


class Guard:
    """Tracks the run and says which tools are allowed now. Hard-rule failures leave only quarantine."""

    def __init__(self):
        self.last_ok = None          # last tool that succeeded
        self.forced_quarantine = None
        self.stuck = False           # set -> only finish_run(NEEDS_HUMAN) is left
        self.stuck_reason = None
        self.done = set()

    def allowed(self):
        if self.stuck:
            return ["finish_run"]
        if "archive_batch" in self.done:
            return ["finish_run"]
        if "quarantine_batch" in self.done:
            return ["finish_run"]
        if self.forced_quarantine:
            return ["quarantine_batch"]
        tools = list(NEXT[self.last_ok])
        if "load_target" not in self.done:
            tools.append("finish_run")      # escape hatch: stop and hand over to a human
        elif self.last_ok == "build_scorecard":
            tools.append("finish_run")
        return tools

    def check(self, name, args):
        """None if allowed, else an explanation the agent can act on."""
        if name not in TOOL_SPECS:
            return f"There is no tool called {name}. Allowed now: {', '.join(self.allowed())}."
        if name not in self.allowed():
            return f"{name} is not allowed at this point. Allowed now: {', '.join(self.allowed())}."
        if name == "finish_run":
            status = args.get("status")
            if status != "NEEDS_HUMAN" and self.stuck:
                return f"Only status NEEDS_HUMAN is possible now: {self.stuck_reason}."
            if status == "LOADED" and "archive_batch" not in self.done:
                return "Status LOADED is only possible after archive_batch."
            if status == "QUARANTINED" and "quarantine_batch" not in self.done:
                return "Status QUARANTINED is only possible after quarantine_batch."
        return None

    def hand_over(self, reason):
        self.stuck, self.stuck_reason = True, reason

    def record(self, name, result):
        """Update the run state from a tool result. Rules that are facts, not judgement, live here."""
        ok = bool(result.get("ok"))
        if name == "profile_source" and ok and result.get("key_column_breaches"):
            self.done.add(name)
            self.last_ok = name
            self.forced_quarantine = "key column breach"
            return
        if name == "profile_source" and ok and result.get("concerns"):
            self.done.add(name)
            self.last_ok = "profile_concerns"     # decide first: quarantine_batch or continue_despite_concerns
            return
        if name == "dbt_build" and not ok:
            return self.hand_over("dbt build had errors - a developer must look before anything is loaded")
        if name == "build_scorecard" and ok and any(e.get("status") == "FAILED" for e in result.get("entities", [])):
            self.done.add(name)
            return self.hand_over("an entity FAILED reconciliation - a human must review before archiving")
        if ok:
            self.done.add(name)
            self.last_ok = name
        elif name in HARD_RULE_TOOLS:
            self.forced_quarantine = name


# ---------------------------------------------------------------- the real tools (short results)

KEY_COLUMN = re.compile(r"(^|_)(ID|NUMBER|NBR|NPI|KEY)$")
OPTIONAL = {"MIDDLE_INITIAL", "MIDDLE_NAME", "SECONDARY_DIAG_CODE", "PROCEDURE_MODIFIER", "ADDRESS_LINE2",
            "COVERAGE_TERM_DATE", "GROUP_TERM_DATE"}
OPTIONAL_IN = {"SRC_PROVIDER": {"FIRST_NAME", "LAST_NAME", "ORGANIZATION_NAME"}}   # depends on individual vs organisation
CRITICAL = re.compile(r"^(DATE_OF_BIRTH|SSN|GENDER_CODE|RELATIONSHIP_CODE|PLAN_CODE|CLAIM_STATUS|CLAIM_TYPE|"
                      r"PRIMARY_DIAG_CODE|PROCEDURE_CODE|[A-Z_]*_AMT|SERVICE_[A-Z_]*DATE|RECEIVED_DATE|COVERAGE_EFF_DATE)$")
CRITICAL_IN = {"SRC_MEMBER": {"FIRST_NAME", "LAST_NAME"}}
KEY_LIMIT, OTHER_LIMIT = 1.0, 20.0


def summarise_profile(obs):
    """Issues per column, classified HERE (the model never does arithmetic or long-list scanning):
    key_column_breaches  key/ID column with problems in > 1% of rows             -> rule: quarantine
                         or business-critical column (DOB, SSN, gender, member names,
                         claim dates/amounts/codes...) with problems in > 20% of rows -> rule: quarantine
    concerns             any other column with problems in > 20% of rows        -> the agent decides
    optional_columns     empty values in columns that are allowed to be empty -> ignored
    minor_issues         everything else (handled by dbt reject rules)"""
    out = {"key_column_breaches": [], "concerns": [], "optional_columns": [], "minor_issues": 0}
    for o in obs:
        if not o["DQ_OBSERVATIONS"]:
            continue
        rows = o["TOTAL_ROWS"] or 0
        pct = lambda n: round(100 * (n or 0) / rows, 2) if rows else 0.0
        worst = max(float(o["NULL_PCT"] or 0), pct(o["INVALID_CHAR_COUNT"]), pct(o["LEADING_TRAILING_SPACE_COUNT"]))
        issue = {"column": f"{o['TABLE_NAME']}.{o['COLUMN_NAME']}", "rows": rows, "problem_pct": worst,
                 "observations": o["DQ_OBSERVATIONS"][:150]}
        column = o["COLUMN_NAME"]
        if KEY_COLUMN.search(column) and worst > KEY_LIMIT:
            out["key_column_breaches"].append(issue)
        elif (CRITICAL.match(column) or column in CRITICAL_IN.get(o["TABLE_NAME"], ())) and worst > OTHER_LIMIT:
            out["key_column_breaches"].append({**issue, "critical_column": True})
        elif column in OPTIONAL or column in OPTIONAL_IN.get(o["TABLE_NAME"], ()):
            out["optional_columns"].append(issue["column"])
        elif worst > OTHER_LIMIT:
            out["concerns"].append(issue)
        else:
            out["minor_issues"] += 1
    return out

def _files(status):
    return [{"file": s["file"], "rows": s["rows_in_raw"], "rejected": s["rows_rejected"], "snowpipe": s["snowpipe_status"]}
            for s in status]


def run_tool(name, batch, args):
    """Execute a tool for real. Returns a short dict with at least 'ok'."""
    if name == "check_batch_files":
        r = steps.check_batch_files(batch)
        return {"ok": r["ok"], "problems": r["problems"], "files_found": sorted(r["files"])}
    if name == "wait_for_snowpipe":
        r = steps.wait_for_snowpipe(batch)
        return {"ok": r["ok"], "seconds_waited": r["seconds"], "files": _files(r["files"]), "problems": r.get("problems", [])}
    if name == "register_batch":
        r = steps.register_batch(batch)
        return {"ok": r["ok"], "message": r["message"]}
    if name == "check_load":
        r = steps.check_load(batch)
        return {"ok": r["ok"], "problems": r["problems"], "files": _files(r["files"])}
    if name == "profile_source":
        r = steps.profile_source(batch)
        if not r["ok"]:
            return {"ok": False, "message": r["message"]}
        obs = steps.profiling_summary(batch)["observations"]
        return {"ok": True, "columns_profiled": len(obs), **summarise_profile(obs)}
    if name == "dbt_build":
        r = steps.dbt_build(batch)
        return {"ok": r["ok"], "summary": r["summary"],
                "errors": [{"test_or_model": e["node"], "failing_rows": e["failures"], "message": e["message"][:200]} for e in r["errors"]],
                "warnings": [{"test": w["node"], "failing_rows": w["failures"]} for w in r["warnings"]]}
    if name == "load_target":
        r = steps.load_target(batch)
        return {"ok": r["ok"], "message": r["message"]}
    if name == "build_scorecard":
        r = steps.build_scorecard(batch)
        return {"ok": r["ok"], "message": r["message"],
                "entities": [{"entity": s["ENTITY_NAME"], "source": s["SOURCE_COUNT"], "rejected": s["REJECT_COUNT"],
                              "loaded": s["TARGET_COUNT"], "load_pct": float(s["LOAD_PCT"] or 0), "status": s["OVERALL_STATUS"]}
                             for s in r["scorecard"]]}
    if name == "archive_batch":
        r = steps.archive_batch(batch)
        return {"ok": True, "moved_to": r["moved_to"], "files": len(r["files"])}
    if name == "continue_despite_concerns":
        return {"ok": True, "accepted": True, "justification": args.get("justification", "")}
    if name == "quarantine_batch":
        r = steps.quarantine_batch(batch, args.get("reason", "quarantined by agent"))
        return {"ok": True, "moved_to": r["moved_to"], "raw_rows_removed": r["raw_rows_removed"]}
    raise ValueError(name)
