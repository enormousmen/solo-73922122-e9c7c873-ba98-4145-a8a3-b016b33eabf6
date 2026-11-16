"""Background task orchestration with lock-safe cancellation.

Each upload is processed by a daemon worker thread. Cancellation is
cooperative: the cancel endpoint records the request in memory (so it is
accepted even while another SQLite connection holds the write lock), tries a
short non-blocking write of ``cancelling`` and returns immediately. The worker
observes the request at every batch boundary and never publishes results after
the request has been accepted.

All SQLite writes performed here use a small busy timeout and explicit short
retries: a held write lock never turns into a 30 s stall, a progress update is
simply skipped, and a failure/cancel finalization that cannot write yet is
handed to a background reaper that retries until the lock is released. Only
after the worker has stopped, the staging directory is gone and the parsed
records are deleted does the task become ``cancelled``/``failed``.

The cancel-vs-finish decision for one task is serialized by a per-task lock so
that a cancellation accepted by the API is guaranteed to be honored: the
worker re-checks the in-memory flag while holding that lock, immediately
before it commits success.
"""

from __future__ import annotations

import shutil
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import config, db
from .fastq import (
    FastqFormatError,
    TaskCancelled,
    parse_file,
    promote_results,
    publish,
    validate_pairing,
)

# Statuses that no late write may overwrite or move a task out of.
_FROZEN = (
    "cancelling",
    "cancelled",
    "failed",
    "succeeded",
    "cleanup_pending",
)
# Statuses that mean "a decision is already made; a worker must stop".
_DECIDED = _FROZEN

# task_id -> event set when the task is asked to cancel. The event is the
# in-process source of truth and is readable/writable without any DB lock.
_cancel_events: dict[str, threading.Event] = {}
_registry_lock = threading.Lock()
# Keep worker threads reachable (mostly for tests / diagnostics).
_workers: dict[str, threading.Thread] = {}
# Tasks whose worker has recorded the active 'processing' status. Kept so a
# cancel accepted while the row is still 'pending' can be reported correctly.
_active_workers: set[str] = set()
# Tasks with a worker executing in this process (registered by run_task
# itself, so workers started directly rather than via start_worker are also
# protected from the orphan reaper).
_running_tasks: set[str] = set()
# Non-terminal tasks discovered by the reaper; their staging is removed even
# if the terminal row cannot be written yet.
_orphans: set[str] = set()

# Worker finalizations that could not be written because the DB write lock
# was held. The worker has already stopped and removed staging; the reaper
# only has to persist the status + drop records. kind: "cancel" or "fail".
_deferred: dict[str, dict] = {}

# Cancel requests accepted by the API whose "cancelling" row could not be
# written because the write lock was held. The in-memory event carries the
# request; this set keeps the status overlay honest and lets the reaper
# persist the request once the lock is released (so a restart still knows).
_pending_cancel_writes: set[str] = set()

# Serializes the cancel-vs-finish decision for one task. It is held only for
# in-memory decisions and short DB transactions, never across a lock wait.
_decision_locks: dict[str, threading.Lock] = {}

_reaper_lock = threading.Lock()
_reaper_started = False
_reaper_wakeup = threading.Event()


class UploadTooLarge(Exception):
    def __init__(self, side: str, limit: int) -> None:
        self.side = side
        self.limit = limit
        super().__init__(f"{side} exceeds the {limit} byte upload limit")


def _decision_lock(task_id: str) -> threading.Lock:
    with _registry_lock:
        lock = _decision_locks.get(task_id)
        if lock is None:
            lock = threading.Lock()
            _decision_locks[task_id] = lock
        return lock


def register_cancel_event(task_id: str) -> threading.Event:
    with _registry_lock:
        event = _cancel_events.get(task_id)
        if event is None:
            event = threading.Event()
            _cancel_events[task_id] = event
        return event


def cancel_requested(task_id: str) -> bool:
    with _registry_lock:
        event = _cancel_events.get(task_id)
        return event is not None and event.is_set()


def request_cancel(task_id: str) -> Optional[str]:
    """Accept a cancellation request without waiting for a write lock.

    Returns ``None`` for an unknown task, the terminal status when the task
    already finished (the request lost and nothing changes), or
    ``"cancelling"`` when the request was accepted. Acceptance is recorded in
    memory while holding the per-task decision lock, before any database
    access, so the worker cannot publish after it even if the ``cancelling``
    row can only be written once the lock is released.
    """
    row = db.read_task(task_id)
    if row is None:
        return None
    if row["status"] in db.TERMINAL_STATUSES:
        return row["status"]

    with _decision_lock(task_id):
        # Re-read under the lock: the worker commits success under this same
        # lock, so this closes the check-then-act race.
        row = db.read_task(task_id)
        if row is None:
            return None
        current = row["status"]
        if current in db.TERMINAL_STATUSES:
            return current

        # Accept first, before touching the database. No lock is needed for
        # the flag, so a request that arrives while another connection holds
        # the SQLite write lock is honored before that lock is released.
        event = register_cancel_event(task_id)
        event.set()
        warn_reaper()

        try:
            previous = db.request_cancel(task_id)
        except sqlite3.OperationalError as exc:
            if not db.is_lock_error(exc):
                raise
            # The write lock is held by another connection. The in-memory
            # event already guarantees the worker will abort; the reaper
            # persists the cancelling row when the lock is released.
            with _registry_lock:
                _pending_cancel_writes.add(task_id)
            warn_reaper()
            return "cancelling"

    if previous is None:
        # The row disappeared; treat as unknown. The event is harmless.
        with _registry_lock:
            _pending_cancel_writes.discard(task_id)
        return None
    if previous in db.TERMINAL_STATUSES:
        # Genuinely finished before the request; drop the cooperative flag.
        with _registry_lock:
            _cancel_events.pop(task_id, None)
            _pending_cancel_writes.discard(task_id)
        return previous
    with _registry_lock:
        _pending_cancel_writes.discard(task_id)
    return "cancelling"


def task_dir(task_id: str) -> Path:
    return config.TASKS_DIR / task_id


def cleanup_task_dir(task_id: str) -> None:
    """Remove only this task's own directory; UUID names isolate tasks."""
    path = task_dir(task_id)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


def effective_status(task_id: str, row) -> str:
    """Status shown by the API, including in-memory cancellation overlays.

    A task whose cancel was accepted but whose ``cancelling`` row could not
    be written yet is reported as ``cancelling`` (never as ``processing``).
    A failure whose cleanup is waiting on the lock is reported as
    ``cleanup_pending``. Database terminal statuses always win.
    """
    status = row["status"]
    if status in db.TERMINAL_STATUSES:
        return status
    with _registry_lock:
        entry = _deferred.get(task_id)
        pending_cancel = task_id in _pending_cancel_writes
        event = _cancel_events.get(task_id)
        requested = event is not None and event.is_set()
        active = task_id in _active_workers
    # An accepted cancellation is visible immediately, even before the row or
    # a deferred failure cleanup can be written.
    if pending_cancel or requested:
        return "cancelling"
    if entry is not None:
        return "cancelling" if entry["kind"] == "cancel" else "cleanup_pending"
    if active:
        return "processing"
    return status


def effective_message(task_id: str, row) -> str:
    status = effective_status(task_id, row)
    if status == "cancelling":
        return "cancellation requested; waiting to finalize"
    if status == "cleanup_pending":
        with _registry_lock:
            entry = _deferred.get(task_id) or {}
        return entry.get("message", "cleanup pending")
    return row["message"]


def _clear_task_state(task_id: str, *, keep_workers: bool = False) -> None:
    with _registry_lock:
        _deferred.pop(task_id, None)
        _pending_cancel_writes.discard(task_id)
        _cancel_events.pop(task_id, None)
        if not keep_workers:
            _workers.pop(task_id, None)
        _active_workers.discard(task_id)


def _finalize(
    task_id: str,
    *,
    cancelled: bool,
    message: str,
    progress: Optional[float] = None,
) -> Optional[bool]:
    """Commit a terminal status without waiting for a write lock.

    Callers must already have stopped the worker and removed the staging
    directory. The per-task decision lock makes the cancel-vs-fail choice
    atomic with :func:`request_cancel`: if a cancellation was accepted before
    this call, the outcome is ``cancelled`` even for an error path.

    Returns ``True`` when committed, ``False`` when another terminal decision
    had already won, and ``None`` when the write lock is busy (the caller
    defers to the reaper).
    """
    with _decision_lock(task_id):
        return _finalize_locked(
            task_id, cancelled=cancelled, message=message, progress=progress
        )


def _finalize_locked(
    task_id: str,
    *,
    cancelled: bool,
    message: str,
    progress: Optional[float],
) -> Optional[bool]:
    """:func:`_finalize` body; the caller already holds the decision lock."""
    if cancel_requested(task_id):
        cancelled = True
    try:
        conn = db.connect(timeout_ms=config.SQLITE_NONBLOCKING_TIMEOUT_MS)
    except sqlite3.OperationalError as exc:
        if db.is_lock_error(exc):
            return None
        raise
    try:
        row = db.get_task(conn, task_id)
        if row is None:
            return False
        if row["status"] in db.TERMINAL_STATUSES:
            return False
        if row["status"] == "cancelling":
            cancelled = True
        if cancelled:
            message = "task cancelled before completion"
        status = "cancelled" if cancelled else "failed"
        return db.finish_task(
            conn,
            task_id,
            status=status,
            stage=config.STAGE_DONE,
            progress=1.0 if cancelled else (progress or 0.0),
            message=message,
        )
    except sqlite3.OperationalError as exc:
        if conn.in_transaction:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
        if db.is_lock_error(exc):
            return None
        raise
    finally:
        conn.close()


def _fail(task_id: str, message: str, *, cancelled: bool) -> None:
    """Stop-side cleanup: remove staging, then record the terminal status.

    The status check, the file removal and the terminal write happen under the
    per-task decision lock, so this can never delete the published results of
    a task that succeeded concurrently.
    """
    with _decision_lock(task_id):
        if cancel_requested(task_id):
            cancelled = True
        if cancelled:
            message = "task cancelled before completion"

        # If the task already succeeded (or otherwise reached a terminal
        # state), its results must be preserved. When the row cannot be read
        # (write lock busy) we must not delete files either: defer the whole
        # cleanup until the lock is free.
        try:
            row = db.read_task(task_id)
        except sqlite3.Error:
            with _registry_lock:
                existing = _deferred.get(task_id)
                kind = "cancel" if cancelled else (existing or {}).get(
                    "kind", "fail"
                )
                _deferred[task_id] = {"kind": kind, "message": message}
            warn_reaper()
            return
        if row is not None and row["status"] in db.TERMINAL_STATUSES:
            if row["status"] == "succeeded":
                # Results are durable; nothing to clean.
                _clear_task_state(task_id)
            else:
                # Already terminal: only stray staging could remain.
                cleanup_task_dir(task_id)
                _clear_task_state(task_id)
            return

        # Files first, DB second: an observer of 'cancelled'/'failed' must see
        # a staging directory that is already gone. File removal needs no DB
        # lock, and the DB transaction also deletes the parsed records, so the
        # task is not reported terminal until both are clean.
        cleanup_task_dir(task_id)

        try:
            result = _finalize_locked(
                task_id,
                cancelled=cancelled,
                message=message,
                progress=0.0,
            )
        except sqlite3.Error:
            result = None

        if result is None:
            # The write lock belongs to another connection. The worker is
            # done; keep a non-active overlay state and let the reaper finish
            # once the lock is released.
            with _registry_lock:
                existing = _deferred.get(task_id)
                kind = "cancel" if cancelled else (existing or {}).get(
                    "kind", "fail"
                )
                _deferred[task_id] = {"kind": kind, "message": message}
            warn_reaper()
            return

        _clear_task_state(task_id)


def _safe_update(task_id: str, **fields) -> None:
    """Best-effort progress/status bookkeeping that never waits or raises."""
    guarded = dict(fields)
    guarded.setdefault("forbid_statuses", _FROZEN)
    try:
        conn = db.connect(timeout_ms=config.SQLITE_NONBLOCKING_TIMEOUT_MS)
        try:
            db.update_task(conn, task_id, **guarded)
        finally:
            conn.close()
    except sqlite3.Error:
        # Another connection holds the write lock, or bookkeeping raced with
        # the terminal update: progress ticks are allowed to be skipped and
        # must never take down the worker.
        pass


def _retry_while_busy(task_id: str, event: threading.Event, action,
                      deadline_seconds: float = 10.0):
    """Run a read/publish step, retrying transient lock errors.

    Cancellation is checked between attempts, so a cancel accepted while the
    worker waits for the lock still aborts before the lock is released. When
    the bounded wait expires the error is re-raised; the caller's failure path
    removes staging and defers the DB status to the reaper.
    """
    deadline = time.monotonic() + deadline_seconds
    delay = 0.01
    while True:
        if event.is_set():
            raise TaskCancelled()
        try:
            return action()
        except sqlite3.OperationalError as exc:
            if not db.is_lock_error(exc):
                raise
            if event.is_set():
                raise TaskCancelled()
            if time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 0.1)


def _commit_success(
    task_id: str,
    event: threading.Event,
    n1: int,
    tmp1: str,
    tmp2: str,
    out_dir: Path,
    r1_path: Path,
    r2_path: Path,
) -> str:
    """Atomically publish results unless cancellation was accepted first.

    Returns ``"published"`` on success, ``"cancelled"`` when the worker lost
    the arbitration to a cancellation and ``"busy"`` when the write lock is
    held (the caller retries, checking its cancel flag between attempts).
    """
    with _decision_lock(task_id):
        if event.is_set() or cancel_requested(task_id):
            return "cancelled"
        try:
            write_conn = db.connect(timeout_ms=config.SQLITE_NONBLOCKING_TIMEOUT_MS)
        except sqlite3.OperationalError as exc:
            if db.is_lock_error(exc):
                return "busy"
            raise
        try:
            try:
                write_conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if db.is_lock_error(exc):
                    return "busy"
                raise
            try:
                row = db.get_task(write_conn, task_id)
                if row is None:
                    write_conn.execute("ROLLBACK")
                    return "cancelled"
                if row["status"] in _DECIDED:
                    write_conn.execute("ROLLBACK")
                    return "cancelled"
                # Re-check under the decision lock while holding the write
                # lock: a cancel accepted in between must still win.
                if event.is_set() or cancel_requested(task_id):
                    write_conn.execute("ROLLBACK")
                    return "cancelled"
                cur = write_conn.execute(
                    "UPDATE tasks SET status = 'succeeded', stage = ?, "
                    "progress = 1.0, message = ?, result_r1 = ?, result_r2 = ?, "
                    "updated_at = ? WHERE id = ? AND status NOT IN "
                    "('succeeded','failed','cancelled','cancelling','cleanup_pending')",
                    (
                        config.STAGE_DONE,
                        f"paired {n1} reads",
                        "output/R1.fastq",
                        "output/R2.fastq",
                        db.utcnow(),
                        task_id,
                    ),
                )
                if cur.rowcount == 0:
                    write_conn.execute("ROLLBACK")
                    return "cancelled"
                # Results become visible only in the same transaction as the
                # status; a failure here is rolled back and cleaned up.
                promote_results(out_dir, tmp1, tmp2)
                db.delete_records(write_conn, task_id)
                write_conn.execute("COMMIT")
            except BaseException:
                if write_conn.in_transaction:
                    try:
                        write_conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                raise
        finally:
            write_conn.close()

    # Inputs are no longer needed once results are durable.
    for stale in (r1_path, r2_path):
        try:
            stale.unlink(missing_ok=True)
        except OSError:
            pass
    _clear_task_state(task_id)
    return "published"


def run_task(task_id: str) -> None:
    """Worker entry point: parse, validate, pair, publish or fail atomically."""
    event = register_cancel_event(task_id)
    ensure_reaper()
    with _registry_lock:
        _running_tasks.add(task_id)

    def finish() -> None:
        # Read the cancel flag under the registry lock without re-entering it:
        # cancel_requested() takes the same non-reentrant lock and would
        # deadlock here.
        with _registry_lock:
            _running_tasks.discard(task_id)
            _workers.pop(task_id, None)
            _active_workers.discard(task_id)
            event = _cancel_events.get(task_id)
            if not (event is not None and event.is_set()):
                _cancel_events.pop(task_id, None)

    try:
        base = task_dir(task_id)
        r1_path = base / "input_R1.fastq"
        r2_path = base / "input_R2.fastq"
        total_bytes = max(r1_path.stat().st_size + r2_path.stat().st_size, 1)
    except OSError:
        # The staging directory disappeared (e.g. startup recovery).
        _fail(task_id, "staging files are missing", cancelled=False)
        finish()
        return

    conn = db.connect(timeout_ms=config.SQLITE_NONBLOCKING_TIMEOUT_MS)

    def parse_progress(side: int, byte_offset: int) -> None:
        other = 0 if side == 1 else r1_path.stat().st_size
        current = other + byte_offset
        _safe_update(
            task_id,
            stage=config.STAGE_PARSE,
            progress=min(0.9 * current / total_bytes, 0.9),
        )

    def mark_processing() -> None:
        try:
            cur = db.update_task(
                conn,
                task_id,
                status="processing",
                stage=config.STAGE_PARSE,
                forbid_statuses=_FROZEN,
            )
        except sqlite3.OperationalError:
            if conn.in_transaction:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            raise
        if cur == 0:
            # Cancellation (or another terminal decision) was recorded before
            # we got started: never flip back to processing.
            raise TaskCancelled()
        with _registry_lock:
            _active_workers.add(task_id)

    try:
        if event.is_set():
            raise TaskCancelled()

        # The initial status flip may race with a writer holding the lock;
        # retry briefly, checking the cancel flag between attempts.
        _retry_while_busy(task_id, event, mark_processing)

        n1 = parse_file(
            conn, task_id, r1_path, 1, event.is_set,
            lambda off: parse_progress(1, off),
        )
        n2 = parse_file(
            conn, task_id, r2_path, 2, event.is_set,
            lambda off: parse_progress(2, off),
        )
        if event.is_set():
            raise TaskCancelled()

        _safe_update(task_id, stage=config.STAGE_VALIDATE, progress=0.92)
        _retry_while_busy(task_id, event, lambda: validate_pairing(conn, task_id))
        if n1 != n2:
            # Defensive: orphan validation should already have caught this.
            raise FastqFormatError(
                f"record count mismatch: R1 has {n1} records, R2 has {n2}"
            )
        if event.is_set():
            raise TaskCancelled()

        _safe_update(task_id, stage=config.STAGE_PUBLISH, progress=0.95)
        out_dir = base / "output"
        tmp1, tmp2 = _retry_while_busy(
            task_id,
            event,
            lambda: publish(
                conn,
                task_id,
                out_dir,
                event.is_set,
                lambda fraction: _safe_update(
                    task_id,
                    stage=config.STAGE_PUBLISH,
                    progress=0.95 + 0.05 * fraction,
                ),
            ),
        )

        # Arbitration: never publishes after a cancellation is accepted, and
        # never waits 30 s for the write lock. If the lock is busy we retry;
        # the cancel flag is checked between attempts, so a cancel accepted
        # while we wait aborts before the lock can be released.
        while True:
            if event.is_set():
                raise TaskCancelled()
            outcome = _commit_success(
                task_id, event, n1, tmp1, tmp2, out_dir, r1_path, r2_path
            )
            if outcome == "published":
                return
            if outcome != "busy" or event.is_set():
                raise TaskCancelled()
            row = db.read_task(task_id)
            if row is not None and row["status"] in _DECIDED:
                raise TaskCancelled()
            time.sleep(0.02)
    except TaskCancelled:
        _fail(task_id, "task cancelled before completion", cancelled=True)
    except FastqFormatError as exc:
        _fail(
            task_id,
            f"rejected: {exc}",
            cancelled=event.is_set(),
        )
    except Exception as exc:  # noqa: BLE001 - any failure fails the whole task
        _fail(task_id, f"unexpected error: {exc!r}", cancelled=event.is_set())
    finally:
        conn.close()
        finish()


def start_worker(task_id: str) -> None:
    ensure_reaper()
    thread = threading.Thread(
        target=run_task, args=(task_id,), name=f"fastq-task-{task_id[:8]}", daemon=True
    )
    with _registry_lock:
        _workers[task_id] = thread
    thread.start()


def warn_reaper() -> None:
    _reaper_wakeup.set()


def ensure_reaper() -> None:
    """Start the deferred-finalization reaper once per process."""
    global _reaper_started
    with _reaper_lock:
        if _reaper_started:
            return
        _reaper_started = True
    thread = threading.Thread(target=_reaper_loop, name="fastq-reaper", daemon=True)
    thread.start()


def _reaper_loop() -> None:
    while True:
        _reaper_wakeup.wait(config.REAPER_INTERVAL_SECONDS)
        _reaper_wakeup.clear()
        try:
            reap_once()
        except Exception:  # noqa: BLE001 - the reaper must never die
            time.sleep(config.REAPER_INTERVAL_SECONDS)


def _worker_alive(task_id: str) -> bool:
    with _registry_lock:
        thread = _workers.get(task_id)
        running = task_id in _running_tasks
    return running or (thread is not None and thread.is_alive())


def reap_once() -> int:
    """Finalize deferred/abandoned work once the write lock is free.

    This is what guarantees that a task whose cancel or failure cleanup could
    not be written (because another connection held the SQLite write lock)
    still becomes terminal after the lock is released, without leaving an
    active task that nobody owns. Returns the number of tasks finalized.
    """
    finalized = 0

    # 1. Persist accepted cancel requests that could not be written yet. This
    #    is also what makes the request survive a restart.
    with _registry_lock:
        pending_cancels = list(_pending_cancel_writes)
    for task_id in pending_cancels:
        try:
            previous = db.request_cancel(task_id)
        except sqlite3.OperationalError as exc:
            if db.is_lock_error(exc):
                continue
            raise
        with _registry_lock:
            _pending_cancel_writes.discard(task_id)
        if previous in db.TERMINAL_STATUSES and previous != "cancelling":
            # The task finished before the request; drop the stale overlay.
            with _registry_lock:
                _cancel_events.pop(task_id, None)

    # 2. Worker-decided finalizations that could not be written yet. The
    #    worker has stopped and staging is already removed.
    with _registry_lock:
        deferred = list(_deferred.items())
    for task_id, entry in deferred:
        cancelled = entry["kind"] == "cancel" or cancel_requested(task_id)
        message = (
            "task cancelled before completion" if cancelled else entry["message"]
        )
        # The whole decision (files + terminal row) runs under the per-task
        # decision lock, which also protects against deleting the results of
        # a task that succeeded concurrently.
        _fail(task_id, message, cancelled=cancelled)

    # 3. Rows left non-terminal by a worker that is no longer running (for
    #    example a process was killed, or a cancel row was written but the
    #    worker died before its cleanup). Never leaves an active task behind.
    conn = db.connect(timeout_ms=config.SQLITE_NONBLOCKING_TIMEOUT_MS)
    try:
        rows = conn.execute(
            "SELECT id, status, created_at FROM tasks "
            "WHERE status NOT IN ('succeeded','failed','cancelled')"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if not db.is_lock_error(exc):
            raise
        rows = []
    finally:
        conn.close()

    # Tasks found by an earlier pass and still not finalized (e.g. because the
    # lock was held) get their staging removed even if the row is unreadable.
    with _registry_lock:
        orphans = set(_orphans)
    for row in rows:
        orphans.add(row["id"])
    with _registry_lock:
        _orphans.update(orphans)

    now = time.time()
    for task_id in list(orphans):
        row = next((r for r in rows if r["id"] == task_id), None)
        if _worker_alive(task_id):
            # A live worker owns the finalization. It checks its cancel flag
            # between lock attempts and will finish on its own.
            continue
        if row is not None:
            cancelled = row["status"] == "cancelling" or cancel_requested(task_id)
            if not cancelled:
                age = _age_seconds(row["created_at"], now)
                if age is not None and age < config.ORPHAN_GRACE_SECONDS:
                    # Still within the window between INSERT and thread start.
                    continue
                message = "interrupted: worker stopped before completion"
            else:
                message = "task cancelled before completion"
        else:
            cancelled = True
            message = "task cancelled before completion"
        # Re-check right before touching files: the row must still be
        # non-terminal (or already cancelled/failed). A task that succeeded
        # since the scan keeps its results.
        try:
            fresh = db.read_task(task_id)
        except sqlite3.Error:
            continue
        if fresh is None:
            with _registry_lock:
                _orphans.discard(task_id)
            _clear_task_state(task_id)
            continue
        if fresh["status"] == "succeeded":
            with _registry_lock:
                _orphans.discard(task_id)
            continue
        if fresh["status"] in ("cancelled", "failed"):
            # Already finalized; only staging might linger.
            cleanup_task_dir(task_id)
            with _registry_lock:
                _orphans.discard(task_id)
            _clear_task_state(task_id)
            continue
        # A fresh ``cancelling`` row wins over the scan's snapshot.
        if fresh["status"] == "cancelling" or cancel_requested(task_id):
            cancelled = True
            message = "task cancelled before completion"
        # _fail re-checks for a concurrent success under the decision lock
        # before deleting anything, then removes files and commits the row.
        _fail(task_id, message, cancelled=cancelled)
        if db_status_terminal(task_id):
            with _registry_lock:
                _orphans.discard(task_id)
            _clear_task_state(task_id)
            finalized += 1
    return finalized


def db_status_terminal(task_id: str) -> bool:
    try:
        row = db.read_task(task_id)
    except sqlite3.Error:
        return False
    return row is not None and row["status"] in db.TERMINAL_STATUSES


def _age_seconds(created_at: str, now: float) -> Optional[float]:
    try:
        created = datetime.fromisoformat(created_at)
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return now - created.timestamp()
    except (TypeError, ValueError):
        return None


def recover_on_startup() -> dict[str, int]:
    """Finalize tasks left unfinished by a previous process.

    Tasks whose cancellation had been recorded become ``cancelled``; every
    other unfinished task becomes ``failed``. In both cases staging and parsed
    records are removed. Succeeded tasks keep their results and stay
    downloadable. A stale lock held by another process cannot leave a task
    active: the writes use a short timeout and are otherwise left to the
    reaper.
    """
    ensure_reaper()
    ids: list[str] = []
    conn = None
    try:
        conn = db.connect(timeout_ms=config.SQLITE_STARTUP_BUSY_TIMEOUT_MS)
        ids = db.non_terminal_task_ids(conn)
        db.mark_all_finished(conn, "interrupted by service restart")
    except sqlite3.OperationalError as exc:
        if not db.is_lock_error(exc):
            raise
        # The lock is held elsewhere: remove what staging we can now and let
        # the reaper persist the terminal rows once the lock is released.
        with _registry_lock:
            _orphans.update(ids)
            for task_id in ids:
                _deferred.setdefault(
                    task_id,
                    {"kind": "fail", "message": "interrupted by service restart"},
                )
        for task_id in ids:
            cleanup_task_dir(task_id)
        warn_reaper()
        return {"recovered": len(ids)}
    finally:
        if conn is not None:
            conn.close()

    for task_id in ids:
        cleanup_task_dir(task_id)
    with _registry_lock:
        for task_id in ids:
            _deferred.pop(task_id, None)
            _cancel_events.pop(task_id, None)
            _pending_cancel_writes.discard(task_id)
    return {"recovered": len(ids)}
