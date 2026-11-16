"""Focused tests for the cancel-vs-complete arbitration in SQLite.

The cancel endpoint and the worker both decide the final status through
conditional UPDATEs; these tests prove neither side can overwrite the other's
terminal decision.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app import config, db  # noqa: E402

FAILURES = []


def check(name: str, ok: bool, detail: str = "") -> None:
    FAILURES.append(not ok)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))


def main() -> int:
    tmp = Path(tempfile.mkdtemp(dir=str(ROOT / "tests" / "output")))
    config.DATA_DIR = tmp
    config.DB_PATH = tmp / "t.db"
    config.TASKS_DIR = tmp / "tasks"
    db._initialized = False
    db.init_db()

    # Case 1: cancel wins while task is processing -> cancelling sticks,
    # and the worker's conditional finalize must match zero rows.
    conn = db.connect()
    db.create_task(conn, "t1", "a", "b", 1, 1)
    db.update_task(conn, "t1", status="processing", stage=config.STAGE_PARSE)
    prev = db.try_set_cancelling(conn, "t1")
    check("cancel on processing returns previous status", prev == "processing", str(prev))
    cur = conn.execute(
        "UPDATE tasks SET status = 'succeeded' WHERE id = ? "
        "AND status != 'cancelling'", ("t1",))
    check("worker finalize blocked after cancel won", cur.rowcount == 0)
    row = db.get_task(conn, "t1")
    check("status remains cancelling", row["status"] == "cancelling", row["status"])

    # Case 2: worker wins (succeeded) before cancel's conditional update.
    db.create_task(conn, "t2", "a", "b", 1, 1)
    db.update_task(conn, "t2", status="processing", stage=config.STAGE_PUBLISH)
    conn.execute(
        "UPDATE tasks SET status = 'succeeded' WHERE id = ? "
        "AND status != 'cancelling'", ("t2",))
    result = db.try_set_cancelling(conn, "t2")
    check("cancel after success reports terminal status",
          result == "succeeded", str(result))
    row = db.get_task(conn, "t2")
    check("succeeded is never overwritten", row["status"] == "succeeded",
          row["status"])

    # Case 3: already terminal -> no change, terminal status returned.
    db.create_task(conn, "t3", "a", "b", 1, 1)
    db.update_task(conn, "t3", status="failed", stage=config.STAGE_DONE)
    result = db.try_set_cancelling(conn, "t3")
    check("cancel on failed reports failed", result == "failed", str(result))
    check("failed stays failed",
          db.get_task(conn, "t3")["status"] == "failed")

    # Case 4: unknown task -> None.
    check("unknown task returns None", db.try_set_cancelling(conn, "nope") is None)

    # Case 5: cancelling is itself idempotent.
    db.create_task(conn, "t5", "a", "b", 1, 1)
    db.update_task(conn, "t5", status="processing")
    db.try_set_cancelling(conn, "t5")
    again = db.try_set_cancelling(conn, "t5")
    check("second cancel keeps cancelling", again == "cancelling", str(again))
    conn.close()

    return 1 if any(FAILURES) else 0


if __name__ == "__main__":
    raise SystemExit(main())
