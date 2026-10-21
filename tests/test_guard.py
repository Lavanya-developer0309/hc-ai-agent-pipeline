"""Fast checks of the agent's guard rules and number check (no LLM, no Snowflake).
    .venv/bin/python -m tests.test_guard"""
from agent.runner import unsupported_numbers
from agent.tools import Guard, summarise_profile

OK = {"ok": True}


def walk(guard, *tools):
    for t in tools:
        assert guard.check(t, {}) is None, (t, guard.allowed())
        guard.record(t, OK)


def test_normal_order_and_blocking():
    g = Guard()
    assert g.allowed() == ["check_batch_files", "finish_run"]
    assert "not allowed" in g.check("load_target", {})
    walk(g, "check_batch_files", "wait_for_snowpipe", "register_batch", "check_load", "profile_source")
    assert set(g.allowed()) == {"dbt_build", "quarantine_batch", "finish_run"}
    walk(g, "dbt_build", "load_target")
    assert g.allowed() == ["build_scorecard"]                          # no quarantine / finish after target load
    walk(g, "build_scorecard")
    assert "LOADED is only possible after archive" in g.check("finish_run", {"status": "LOADED"})
    walk(g, "archive_batch")
    assert g.allowed() == ["finish_run"] and g.check("finish_run", {"status": "LOADED"}) is None


def test_hard_rule_forces_quarantine():
    g = Guard()
    g.record("check_batch_files", {"ok": False})
    assert g.allowed() == ["quarantine_batch"]
    g.record("quarantine_batch", OK)
    assert g.check("finish_run", {"status": "QUARANTINED"}) is None


def test_key_breach_forces_quarantine():
    g = Guard()
    walk(g, "check_batch_files", "wait_for_snowpipe", "register_batch", "check_load")
    g.record("profile_source", {"ok": True, "key_column_breaches": [{"column": "SRC_MEMBER.MEMBER_ID"}]})
    assert g.allowed() == ["quarantine_batch"]


def test_concerns_need_an_explicit_decision():
    g = Guard()
    walk(g, "check_batch_files", "wait_for_snowpipe", "register_batch", "check_load")
    g.record("profile_source", {"ok": True, "concerns": [{"column": "SRC_MEMBER.DATE_OF_BIRTH"}]})
    assert set(g.allowed()) == {"quarantine_batch", "continue_despite_concerns", "finish_run"}
    assert "not allowed" in g.check("dbt_build", {})
    walk(g, "continue_despite_concerns", "dbt_build")


def test_dbt_errors_scorecard_failed_and_rejection_hand_over():
    for setup in ("dbt", "scorecard", "human"):
        g = Guard()
        walk(g, "check_batch_files", "wait_for_snowpipe", "register_batch", "check_load", "profile_source")
        if setup == "dbt":
            g.record("dbt_build", {"ok": False})
        elif setup == "scorecard":
            walk(g, "dbt_build", "load_target")
            g.record("build_scorecard", {"ok": True, "entities": [{"status": "PASSED"}, {"status": "FAILED"}]})
        else:
            g.hand_over("a human did not approve dbt_build")
        assert g.allowed() == ["finish_run"], setup
        assert "Only status NEEDS_HUMAN" in g.check("finish_run", {"status": "QUARANTINED"})
        assert g.check("finish_run", {"status": "NEEDS_HUMAN"}) is None


def test_profile_classification():
    row = lambda col, nulls, table="SRC_MEMBER": {"TABLE_NAME": table, "COLUMN_NAME": col, "TOTAL_ROWS": 1000,
        "NULL_PCT": nulls / 10, "INVALID_CHAR_COUNT": 0, "LEADING_TRAILING_SPACE_COUNT": 0, "DQ_OBSERVATIONS": f"NULL values: {nulls}"}
    s = summarise_profile([row("MEMBER_ID", 20), row("GROUP_NUMBER", 5), row("DATE_OF_BIRTH", 300), row("MIDDLE_INITIAL", 900),
                           row("ORGANIZATION_NAME", 700, "SRC_PROVIDER"), row("CITY", 30), row("LEGACY_FAX", 250)])
    assert [i["column"] for i in s["key_column_breaches"]] == ["SRC_MEMBER.MEMBER_ID", "SRC_MEMBER.DATE_OF_BIRTH"]
    assert [i["column"] for i in s["concerns"]] == ["SRC_MEMBER.LEGACY_FAX"]   # unknown column 25% -> agent decides
    assert set(s["optional_columns"]) == {"SRC_MEMBER.MIDDLE_INITIAL", "SRC_PROVIDER.ORGANIZATION_NAME"}
    assert s["minor_issues"] == 2


def test_summary_number_check():
    evidence = ['{"rows": 3938, "load_pct": 99.09, "entities": [{"loaded": 7875}]}']
    assert unsupported_numbers("Loaded 7,875 claims, 99.09% of 3938 members", evidence) == []
    assert unsupported_numbers("Total rows loaded: 45,854", evidence) == ["45854"]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); print("PASS", name)
