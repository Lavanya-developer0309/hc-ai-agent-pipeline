"""
Agent evaluation: the REAL model (qwen3:8b) against scripted tool results.
Fast, free (no Snowflake, no S3) and repeatable.

    .venv/bin/python -m tests.eval_agent            # all scenarios
    .venv/bin/python -m tests.eval_agent dbt_errors # one scenario
"""
import copy
import json
import sys
import time
from pathlib import Path

from agent.runner import MemoryLog, run

PROFILE = json.loads((Path(__file__).parent / "fixture_profile_batch003.json").read_text())
FILES = [{"file": f, "rows": n, "rejected": 0, "snowpipe": "Loaded"} for f, n in
         [("SRC_GROUP.txt", 60), ("SRC_MEMBER.txt", 3938), ("SRC_PROVIDER.txt", 350),
          ("SRC_CLAIM_HEADER.txt", 8000), ("SRC_CLAIM_LINE.txt", 15962)]]
SCORECARD = [{"entity": e, "source": s, "rejected": r, "loaded": s - r, "load_pct": round(100 * (s - r) / s, 2),
              "status": "PASSED" if r == 0 else "PASSED_WITH_WARNINGS"} for e, s, r in
             [("CLAIM", 8000, 125), ("CLAIM_LINE", 15962, 240), ("ELIGIBILITY", 3938, 36), ("GROUP", 60, 0),
              ("MEMBER", 3938, 36), ("PROVIDER", 350, 3), ("SUBSCRIBER", 1505, 9)]]
CLEAN = {
    "check_batch_files": {"ok": True, "problems": [], "files_found": [f["file"] for f in FILES]},
    "wait_for_snowpipe": {"ok": True, "seconds_waited": 40, "files": FILES, "problems": []},
    "register_batch": {"ok": True, "message": "LOAD_RAW complete: SRC_GROUP=60 (SUCCESS) SRC_MEMBER=3938 (SUCCESS) ..."},
    "check_load": {"ok": True, "problems": [], "files": FILES},
    "profile_source": {"ok": True, **PROFILE},
    "dbt_build": {"ok": True, "summary": {"pass": 104, "warn": 13}, "errors": [],
                  "warnings": [{"test": "source_matches_regex_legacy_src_member_ssn", "failing_rows": 9},
                               {"test": "source_not_null_legacy_src_member_date_of_birth", "failing_rows": 5}]},
    "load_target": {"ok": True, "message": "LOAD_FACETS_TARGET complete: GROUP=60 (SUCCESS) MEMBER=3902 (SUCCESS) ..."},
    "build_scorecard": {"ok": True, "message": "Scorecard built: 1 passed, 6 passed with warnings, 0 failed.", "entities": SCORECARD},
    "archive_batch": {"ok": True, "moved_to": "s3://bucket/archive/BATCH_T/", "files": 5},
    "quarantine_batch": {"ok": True, "moved_to": "s3://bucket/rejected/BATCH_T/", "raw_rows_removed": {}},
    "continue_despite_concerns": {"ok": True, "accepted": True},
}


def variant(**changes):
    v = copy.deepcopy(CLEAN)
    v.update(changes)
    return v


bad_key = variant(profile_source={**copy.deepcopy(PROFILE), "ok": True, "key_column_breaches": [
    {"column": "SRC_MEMBER.MEMBER_ID", "rows": 3938, "problem_pct": 40.12, "observations": "NULL values: 1580"}]})
bad_dob = variant(profile_source={**copy.deepcopy(PROFILE), "ok": True, "key_column_breaches": [
    {"column": "SRC_MEMBER.DATE_OF_BIRTH", "rows": 3938, "problem_pct": 35.04, "observations": "NULL values: 1380",
     "critical_column": True}]})
missing = variant(check_batch_files={"ok": False, "problems": ["missing file: SRC_PROVIDER.txt"],
                                     "files_found": ["SRC_CLAIM_HEADER.txt", "SRC_CLAIM_LINE.txt", "SRC_GROUP.txt", "SRC_MEMBER.txt"]})
zero_rows = variant(check_load={"ok": False, "problems": ["SRC_MEMBER.txt: 0 rows loaded (Snowpipe status Load failed)"], "files": FILES})
dbt_err = variant(dbt_build={"ok": False, "summary": {"pass": 98, "warn": 13, "error": 2, "skipped": 4},
                             "errors": [{"test_or_model": "int_member__xwalk", "failing_rows": None,
                                         "message": "Database Error: invalid identifier 'GENDER_XWALK.TARGET_CD'"},
                                        {"test_or_model": "unique_fct_member_meme_ck", "failing_rows": 12, "message": "Got 12 results, configured to fail if != 0"}],
                             "warnings": []})
sc_failed = variant(build_scorecard={"ok": True, "message": "Scorecard built: 5 passed with warnings, 2 failed.",
                                     "entities": [*SCORECARD[:4], {"entity": "MEMBER", "source": 3938, "rejected": 2100, "loaded": 1838,
                                                  "load_pct": 46.67, "status": "FAILED"}, *SCORECARD[5:]]})


def crash_twice(name, batch, args):
    if name == "load_target":
        raise RuntimeError("Snowflake: Warehouse 'MIG_WH' cannot be resumed because resource monitor 'MIG_RM' has exceeded its quota")
    return CLEAN[name]


# name: (tool results or callable, mode, approver, expected status, tools that must run, tools that must NOT run)
SCENARIOS = {
    "clean_batch": (CLEAN, "auto", None, "LOADED", ["profile_source", "dbt_build", "load_target", "archive_batch"], ["quarantine_batch"]),
    "missing_file": (missing, "auto", None, "QUARANTINED", ["quarantine_batch"], ["wait_for_snowpipe", "dbt_build"]),
    "zero_rows_loaded": (zero_rows, "auto", None, "QUARANTINED", ["quarantine_batch"], ["profile_source", "dbt_build"]),
    "bad_key_column": (bad_key, "auto", None, "QUARANTINED", ["profile_source", "quarantine_batch"], ["dbt_build"]),
    "bad_dob_35pct": (bad_dob, "auto", None, "QUARANTINED", ["profile_source", "quarantine_batch"], ["dbt_build"]),
    "unused_column_concern": (variant(profile_source={**copy.deepcopy(PROFILE), "ok": True, "concerns": [
        {"column": "SRC_GROUP.LEGACY_FAX_NUMBER_TEXT", "rows": 60, "problem_pct": 85.0,
         "observations": "NULL values: 51 (column is not mapped to any Facets target field)"}]}),
        "auto", None, "LOADED", ["continue_despite_concerns", "dbt_build"], ["quarantine_batch"]),
    "dbt_errors": (dbt_err, "auto", None, "NEEDS_HUMAN", ["dbt_build"], ["load_target", "archive_batch"]),
    "scorecard_failed": (sc_failed, "auto", None, "NEEDS_HUMAN", ["build_scorecard"], ["archive_batch"]),
    "tool_crashes": (crash_twice, "auto", None, "NEEDS_HUMAN", [], ["build_scorecard", "archive_batch"]),
    "human_rejects_dbt": (CLEAN, "human",
                          lambda run_id, batch, step, tool, reason: ("REJECTED", "client is sending a corrected file") if tool == "dbt_build" else ("APPROVED", None),
                          "NEEDS_HUMAN", ["profile_source"], ["dbt_build", "load_target"]),
    "human_approves_all": (CLEAN, "human", lambda *a: ("APPROVED", None), "LOADED", ["dbt_build", "archive_batch"], []),
}


def evaluate(name):
    results, mode, approver, want, must, must_not = SCENARIOS[name]
    impl = results if callable(results) else (lambda tool, batch, args: copy.deepcopy(results[tool]))
    executed, asked = [], []

    def tracking_impl(tool, batch, args):
        executed.append(tool)
        return impl(tool, batch, args)

    def tracking_approver(*a):
        asked.append(a[3])
        return approver(*a)

    log = MemoryLog()
    t = time.time()
    out = run("BATCH_T", mode, tool_impl=tracking_impl, log=log, approver=tracking_approver if approver else None, echo=lambda *_: None)
    problems = []
    if out["status"] != want:
        problems.append(f"status {out['status']} (expected {want})")
    problems += [f"did not run {t}" for t in must if t not in executed]
    problems += [f"ran {t} (should not)" for t in must_not if t in executed]
    if mode == "human":
        unasked = [t for t in executed if t not in asked]
        problems += [f"ran {t} without asking" for t in unasked]
    blocked = [r["tool"] for r in log.rows if r["verdict"] == "BLOCKED"]
    return {"scenario": name, "pass": not problems, "status": out["status"], "seconds": round(time.time() - t),
            "path": " > ".join(executed), "blocked": blocked, "problems": problems, "summary": out["summary"]}


if __name__ == "__main__":
    names = sys.argv[1:] or list(SCENARIOS)
    passed = 0
    for n in names:
        r = evaluate(n)
        passed += r["pass"]
        print(f"{'PASS' if r['pass'] else 'FAIL'}  {n:<20} {r['status']:<12} {r['seconds']:>3}s  path: {r['path']}")
        if r["blocked"]:
            print(f"      guard blocked: {r['blocked']}")
        for p in r["problems"]:
            print(f"      !! {p}")
        print(f"      summary: {r['summary'][:260]}")
    print(f"\n{passed}/{len(names)} scenarios passed")
