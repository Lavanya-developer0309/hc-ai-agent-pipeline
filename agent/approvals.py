"""
Human approvals (APPROVAL_MODE=human): the agent asks before every action.

Requests live in a small local SQLite file (state/approvals.db) - waiting for an
answer costs nothing (no Snowflake warehouse kept running).

    .venv/bin/python -m agent.approvals              # list pending requests
    .venv/bin/python -m agent.approvals 12 yes       # approve request 12
    .venv/bin/python -m agent.approvals 12 no "files look wrong, ask client"
"""
import sqlite3
import sys
import time
from datetime import datetime

from pipeline.config import ROOT

DB = ROOT / "state" / "approvals.db"


def _db():
    DB.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS approvals (
        id INTEGER PRIMARY KEY, run_id TEXT, batch TEXT, step INTEGER, tool TEXT, reason TEXT,
        status TEXT DEFAULT 'PENDING', note TEXT, requested_at TEXT, answered_at TEXT)""")
    return conn


def request(run_id, batch, step, tool, reason):
    with _db() as conn:
        cur = conn.execute("INSERT INTO approvals (run_id, batch, step, tool, reason, requested_at) VALUES (?,?,?,?,?,?)",
                           (run_id, batch, step, tool, reason, datetime.now().isoformat(timespec="seconds")))
        return cur.lastrowid


def wait(approval_id, timeout_seconds=3600, poll=5):
    """Block until the request is answered. Returns ('APPROVED'|'REJECTED'|'TIMED_OUT', note)."""
    start = time.time()
    while time.time() - start < timeout_seconds:
        with _db() as conn:
            row = conn.execute("SELECT status, note FROM approvals WHERE id = ?", (approval_id,)).fetchone()
        if row["status"] != "PENDING":
            return row["status"], row["note"]
        time.sleep(poll)
    answer(approval_id, False, "no answer in time")
    return "TIMED_OUT", None


def answer(approval_id, approve, note=None):
    with _db() as conn:
        conn.execute("UPDATE approvals SET status = ?, note = ?, answered_at = ? WHERE id = ? AND status = 'PENDING'",
                     ("APPROVED" if approve else "REJECTED", note, datetime.now().isoformat(timespec="seconds"), approval_id))


def pending():
    with _db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM approvals WHERE status = 'PENDING' ORDER BY id")]


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        answer(int(sys.argv[1]), sys.argv[2].lower() in ("y", "yes", "approve"), " ".join(sys.argv[3:]) or None)
        print("answered", sys.argv[1])
    for p in pending():
        print(f"[{p['id']}] {p['batch']} step {p['step']}: {p['tool']}  - {p['reason']}")
    if not pending():
        print("no pending approvals")
