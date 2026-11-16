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
import time
from datetime import datetime, timezone
from typing import Iterable, Optional

from . import config

TERMINAL_STATUSES = frozenset(("succeeded", "failed", "cancelled"))
# SQLite primary result codes for a transient lock conflict.
_BUSY_CODES = frozenset((5, 6))  # SQLITE_BUSY, SQLITE_LOCKED

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

_init_lock = threading.Lock()
_initialized = False
# Database path this process has already put in WAL mode. Checking/setting
# ``journal_mode`` can fail with SQLITE_BUSY while another connection writes,
# so once known, opening a connection runs no lock-taking pragma at all.
_wal_path: Optional[str] = None


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_lock_error(exc: BaseException) -> bool:
    """True when ``exc`` is a transient SQLite busy/locked conflict."""
    return (
        isinstance(exc, sqlite3.OperationalError)
        and getattr(exc, "sqlite_errorcode", None) is not None
        and (exc.sqlite_errorcode & 0xFF) in _BUSY_CODES
    )


def _base_connect(timeout_ms: int) -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=timeout_ms / 1000,
        isolation_level=None,  # explicit transactions
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    return conn


def connect(*, timeout_ms: Optional[int] = None) -> sqlite3.Connection:
    """Open a connection tuned for small-memory, disk-backed operation.

    ``timeout_ms`` overrides the default busy timeout. A small (few-ms)
    timeout is used for reads and cancel bookkeeping: it absorbs SQLite's
    transient WAL index contention without ever waiting seconds behind a
    writer. Once the WAL journal mode has been established (by
    :func:`init_db` or the first WAL connection in this process), no
    lock-taking pragma runs here, so opening a connection needs no write lock.
    """
    global _wal_path
    busy_ms = config.SQLITE_BUSY_TIMEOUT_MS if timeout_ms is None else timeout_ms
    conn = _base_connect(busy_ms)
    try:
        if _wal_path != str(config.DB_PATH):
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            if str(mode).lower() != "wal":
                conn.execute("PRAGMA journal_mode=WAL")
            _wal_path = str(config.DB_PATH)
        # The remaining pragmas only tune memory/IO. Under heavy concurrent
        # commits SQLite can briefly return SQLITE_BUSY even for these; they
        # are not required for correctness, so a conflict is ignored rather
        # than failing the query.
        for pragma in (
            "PRAGMA synchronous=NORMAL",
            # Negative value = KiB; keep the page cache tiny so big tables
            # spill.
            f"PRAGMA cache_size=-{config.SQLITE_CACHE_PAGES}",
            "PRAGMA temp_store=FILE",
            "PRAGMA mmap_size=0",
            "PRAGMA busy_timeout=%d" % busy_ms,
        ):
            try:
                conn.execute(pragma)
            except sqlite3.OperationalError as exc:
                if not is_lock_error(exc):
                    raise
    except BaseException:
        conn.close()
        raise
    return conn


def init_db() -> None:
    global _initialized, _wal_path
    with _init_lock:
        if _initialized:
            return
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        config.TASKS_DIR.mkdir(parents=True, exist_ok=True)
        # Schema creation is idempotent; retry briefly if another process
        # briefly holds the write lock, but never wait the full busy timeout.
        last: Optional[BaseException] = None
        for attempt in range(50):
            conn = None
            try:
                conn = connect()
                conn.executescript(SCHEMA)
                _wal_path = str(config.DB_PATH)
                last = None
                break
            except sqlite3.OperationalError as exc:
                last = exc
                if not is_lock_error(exc):
                    raise
                time.sleep(0.02 if attempt < 25 else 0.1)
            finally:
                if conn is not None:
                    conn.close()
        if last is not None:
            raise last
        _initialized = True


def retry_write(fn, *, attempts: int = 40, delay: float = 0.05):
    """Run a short transaction, retrying transient lock conflicts.

    Used for bookkeeping writes (a new task row, the ``cancelling`` flag) and
    for status reads. It never turns a deliberate "no row matched" result into
    an error and it never stretches SQLite's busy timeout: each attempt uses
    the small non-blocking timeout, and the bounded retry loop only rides out
    brief writer contention (at most ``attempts * delay`` seconds).
    """
    last: Optional[BaseException] = None
    for attempt in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            if not is_lock_error(exc):
                raise
            last = exc
            if attempt + 1 < attempts:
                time.sleep(delay)
    assert last is not None
    raise last


def read_task(task_id: str) -> Optional[sqlite3.Row]:
    """Read one task without ever blocking on another connection's lock.

    SQLite WAL readers normally proceed while a writer holds the lock; this
    adds a few short retries for transient WAL-index contention so a status
    poll or an already-succeeded download never fails during a write burst.
    """
    def attempt() -> Optional[sqlite3.Row]:
        conn = connect(timeout_ms=config.SQLITE_NONBLOCKING_TIMEOUT_MS)
        try:
            return get_task(conn, task_id)
        finally:
            conn.close()

    return retry_write(attempt, attempts=25, delay=0.01)


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
    forbid_statuses: Optional[Iterable[str]] = None,
) -> int:
    """Update task fields, optionally guarded by the current status.

    ``forbid_statuses`` adds a conditional ``status NOT IN (...)`` clause and
    the number of matched rows is returned, so a late progress or success
    write can never drag a task out of ``cancelling`` or a terminal state.
    """
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
    sql = f"UPDATE tasks SET {', '.join(fields)} WHERE id = ?"
    values.append(task_id)
    forbidden = tuple(forbid_statuses or ())
    if forbidden:
        sql += " AND status NOT IN (%s)" % ",".join("?" * len(forbidden))
        values.extend(forbidden)
    return conn.execute(sql, values).rowcount


def try_set_cancelling(conn: sqlite3.Connection, task_id: str) -> Optional[str]:
    """Atomically move a non-terminal task to 'cancelling'.

    Returns the previous status, or ``None`` when the task does not exist. A
    task that is already terminal is left untouched and its terminal status is
    returned. The UPDATE is conditional, so a task that reaches 'succeeded'
    concurrently can never be overwritten, and moving an already-requested
    cancellation to 'cancelling' again is idempotent.
    """
    row = get_task(conn, task_id)
    if row is None:
        return None
    if row["status"] in TERMINAL_STATUSES:
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


def request_cancel(task_id: str) -> Optional[str]:
    """Persist a cancellation request without waiting for any write lock.

    The request is a single short transaction on a small-timeout connection.
    If another SQLite connection holds the write lock the transaction is
    retried only for a short bounded window (no 30 s busy wait); if the lock
    is still held, the caller keeps the accepted request in memory and the
    reaper writes the ``cancelling`` row as soon as it can. Returns the status
    observed (``None`` for an unknown task) and raises
    ``sqlite3.OperationalError`` (busy) when the request could not be
    persisted yet.
    """
    def attempt() -> Optional[str]:
        conn = connect(timeout_ms=config.SQLITE_NONBLOCKING_TIMEOUT_MS)
        try:
            return try_set_cancelling(conn, task_id)
        finally:
            conn.close()

    return retry_write(attempt, attempts=6, delay=0.01)


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


def finish_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    status: str,
    message: str,
    progress: float,
    stage: str = config.STAGE_DONE,
    result_r1: str = "",
    result_r2: str = "",
) -> bool:
    """Commit a terminal transition and drop the task's records atomically.

    The UPDATE is conditional on the task still being non-terminal, so a
    ``cancelling`` row can never be overwritten by a late success and two
    concurrent finalizers cannot both win. Returns ``True`` when the
    transition was applied.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "UPDATE tasks SET status = ?, stage = ?, progress = ?, message = ?, "
            "result_r1 = ?, result_r2 = ?, updated_at = ? "
            "WHERE id = ? AND status NOT IN ('succeeded', 'failed', 'cancelled')",
            (status, stage, progress, message, result_r1, result_r2,
             utcnow(), task_id),
        )
        if cur.rowcount:
            conn.execute("DELETE FROM records WHERE task_id = ?", (task_id,))
        conn.execute("COMMIT")
        return bool(cur.rowcount)
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise


def non_terminal_task_ids(conn: sqlite3.Connection) -> list[str]:
    return [
        row["id"]
        for row in conn.execute(
            "SELECT id FROM tasks WHERE status NOT IN ('succeeded','failed','cancelled')"
        )
    ]


def mark_all_finished(conn: sqlite3.Connection, message: str) -> int:
    """Startup recovery in one short transaction.

    Every task still on the service's books is at best incomplete: any
    ``cancelling`` one becomes ``cancelled``, every other non-terminal one
    becomes ``failed``. Succeeded tasks and their results are untouched.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = utcnow()
        cur = conn.execute(
            "UPDATE tasks SET status = 'cancelled', stage = ?, progress = 1.0, "
            "message = 'task cancelled before completion', result_r1 = '', "
            "result_r2 = '', updated_at = ? WHERE status = 'cancelling'",
            (config.STAGE_DONE, now),
        )
        cancelled = cur.rowcount
        cur = conn.execute(
            "UPDATE tasks SET status = 'failed', stage = ?, progress = 0.0, "
            "message = ?, result_r1 = '', result_r2 = '', updated_at = ? "
            "WHERE status NOT IN ('succeeded','failed','cancelled')",
            (config.STAGE_DONE, message, now),
        )
        failed = cur.rowcount
        conn.execute(
            "DELETE FROM records WHERE task_id IN "
            "(SELECT id FROM tasks WHERE status IN ('failed','cancelled'))"
        )
        conn.execute("COMMIT")
        return cancelled + failed
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
