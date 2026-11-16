"""SQLite persistence layer.

A single database file stores task metadata and every parsed FASTQ record.
Records are written and read in small batches so that the working set on disk
can be much larger than the available RAM; SQLite is configured with a small
page cache and WAL journaling for concurrent status reads while a worker is
writing.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Iterable, Optional

from . import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    stage           TEXT NOT NULL,
    progress        REAL NOT NULL DEFAULT 0,
    message         TEXT NOT NULL DEFAULT '',
    r1_name         TEXT NOT NULL DEFAULT '',
    r2_name         TEXT NOT NULL DEFAULT '',
    r1_bytes        INTEGER NOT NULL DEFAULT 0,
    r2_bytes        INTEGER NOT NULL DEFAULT 0,
    result_r1       TEXT NOT NULL DEFAULT '',
    result_r2       TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    task_id   TEXT NOT NULL,
    side      INTEGER NOT NULL,      -- 1 for R1, 2 for R2
    ord       INTEGER NOT NULL,      -- zero-based ordinal within the side
    read_id   TEXT NOT NULL,
    data      BLOB NOT NULL,         -- exact four FASTQ lines, bytes unchanged
    PRIMARY KEY (task_id, side, ord)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_records_lookup
    ON records (task_id, side, read_id);
"""

_TERMINAL_STATUSES = ("succeeded", "failed", "cancelled")

_init_lock = threading.Lock()
_initialized = False


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> sqlite3.Connection:
    """Open a connection tuned for small-memory, disk-backed operation."""
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=config.SQLITE_BUSY_TIMEOUT_MS / 1000,
        isolation_level=None,  # explicit transactions
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    # Negative value = KiB; keep the page cache tiny so big tables spill.
    conn.execute(f"PRAGMA cache_size=-{config.SQLITE_CACHE_PAGES}")
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute("PRAGMA mmap_size=0")
    conn.execute("PRAGMA busy_timeout=%d" % config.SQLITE_BUSY_TIMEOUT_MS)
    return conn


def init_db() -> None:
    global _initialized
    with _init_lock:
        if _initialized:
            return
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        config.TASKS_DIR.mkdir(parents=True, exist_ok=True)
        conn = connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()
        _initialized = True


def create_task(
    conn: sqlite3.Connection,
    task_id: str,
    r1_name: str,
    r2_name: str,
    r1_bytes: int,
    r2_bytes: int,
) -> None:
    now = utcnow()
    conn.execute(
        """
        INSERT INTO tasks (id, status, stage, progress, message,
                           r1_name, r2_name, r1_bytes, r2_bytes,
                           created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            "pending",
            config.STAGE_UPLOAD,
            0,
            "",
            r1_name,
            r2_name,
            r1_bytes,
            r2_bytes,
            now,
            now,
        ),
    )


def get_task(conn: sqlite3.Connection, task_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM tasks WHERE id = ?", (task_id,)
    ).fetchone()


def update_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    status: Optional[str] = None,
    stage: Optional[str] = None,
    progress: Optional[float] = None,
    message: Optional[str] = None,
    result_r1: Optional[str] = None,
    result_r2: Optional[str] = None,
) -> None:
    fields = ["updated_at = ?"]
    values: list = [utcnow()]
    if status is not None:
        fields.append("status = ?")
        values.append(status)
    if stage is not None:
        fields.append("stage = ?")
        values.append(stage)
    if progress is not None:
        fields.append("progress = ?")
        values.append(progress)
    if message is not None:
        fields.append("message = ?")
        values.append(message)
    if result_r1 is not None:
        fields.append("result_r1 = ?")
        values.append(result_r1)
    if result_r2 is not None:
        fields.append("result_r2 = ?")
        values.append(result_r2)
    values.append(task_id)
    conn.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE id = ?", values)


def try_set_cancelling(conn: sqlite3.Connection, task_id: str) -> Optional[str]:
    """Atomically move a non-terminal task to 'cancelling'.

    Returns the previous status, or ``None`` when the task does not exist or
    is already terminal (in which case nothing is changed). The UPDATE itself
    is conditional, so a task that reaches 'succeeded' concurrently cannot be
    overwritten.
    """
    row = get_task(conn, task_id)
    if row is None:
        return None
    if row["status"] in _TERMINAL_STATUSES:
        return row["status"]
    cur = conn.execute(
        "UPDATE tasks SET status = 'cancelling', updated_at = ? "
        "WHERE id = ? AND status NOT IN ('succeeded', 'failed', 'cancelled')",
        (utcnow(), task_id),
    )
    if cur.rowcount == 0:
        # Lost the race against a concurrent terminal transition.
        latest = get_task(conn, task_id)
        return latest["status"] if latest is not None else None
    return row["status"]


def insert_records(
    conn: sqlite3.Connection, task_id: str, rows: Iterable[tuple]
) -> None:
    """Insert a batch of (side, ord, read_id, data) records."""
    payload = [(task_id, *row) for row in rows]
    conn.executemany(
        "INSERT INTO records (task_id, side, ord, read_id, data) "
        "VALUES (?, ?, ?, ?, ?)",
        payload,
    )


def delete_records(conn: sqlite3.Connection, task_id: str) -> None:
    conn.execute("DELETE FROM records WHERE task_id = ?", (task_id,))


def non_terminal_task_ids(conn: sqlite3.Connection) -> list[str]:
    return [
        row["id"]
        for row in conn.execute(
            "SELECT id FROM tasks WHERE status NOT IN ('succeeded','failed','cancelled')"
        )
    ]
