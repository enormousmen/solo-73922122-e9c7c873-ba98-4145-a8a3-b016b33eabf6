"""Tests for cancellation and cleanup while another connection holds a write lock.

These target the failure modes where an external SQLite writer holds the
database write lock for longer than the SQLite busy timeout:

  * the cancel endpoint must accept the request before the lock is released,
    return promptly and show ``cancelling``;
  * status queries and downloads of already-succeeded tasks must keep working;
  * after the lock is released the task must become ``cancelled`` on its own,
    with staging and parsed records removed and no result published;
  * a late progress/success write must not move the task back to
    ``processing``/``succeeded``;
  * a failure whose cleanup could not be written must still finish (as
    ``failed``) after the lock is released.

Run with the project virtualenv:
    .venv/bin/python -I tests/test_lock_contention.py
"""

from __future__ import annotations

import asyncio
import io
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "tests" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, str(ROOT))

import httpx  # noqa: E402

from app import config, db, worker  # noqa: E402
from app.main import app  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))


class WriteLock:
    """Hold the SQLite write lock from an independent connection/thread."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn: sqlite3.Connection | None = None
        self.ready = threading.Event()
        self.release = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        conn = sqlite3.connect(
            self.path, timeout=5, isolation_level=None, check_same_thread=False
        )
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE tasks SET message = message WHERE 0")
        self.conn = conn
        self.ready.set()
        self.release.wait(60)
        try:
            conn.execute("COMMIT")
        finally:
            conn.close()

    def __enter__(self) -> "WriteLock":
        self.thread.start()
        assert self.ready.wait(10), "writer could not acquire the lock"
        # Make sure it really holds a write lock before the test proceeds.
        probe = sqlite3.connect(self.path, timeout=0, isolation_level=None)
        try:
            try:
                probe.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError:
                pass
            else:
                probe.execute("ROLLBACK")
                raise AssertionError("external writer does not hold the lock")
        finally:
            probe.close()
        return self

    def __exit__(self, *exc) -> None:
        self.release.set()
        self.thread.join(10)


def fastq_bytes(ids: list[str], side: int, seq: bytes = b"A" * 40) -> bytes:
    out = bytearray()
    for rid in ids:
        out += b"@" + rid.encode() + b"/%d\n" % side + seq + b"\n+\n"
        out += b"~" * len(seq) + b"\n"
    return bytes(out)


async def upload(client: httpx.AsyncClient, r1: bytes, r2: bytes) -> dict:
    r = await client.post(
        "/tasks",
        files={
            "r1": ("r1.fastq", io.BytesIO(r1), "application/octet-stream"),
            "r2": ("r2.fastq", io.BytesIO(r2), "application/octet-stream"),
        },
    )
    assert r.status_code == 202, r.text
    return r.json()


async def wait_status(
    client: httpx.AsyncClient, task_id: str, expected: set[str], timeout: float = 15.0
) -> dict:
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = (await client.get(f"/tasks/{task_id}")).json()
        if last["status"] in expected:
            return last
        await asyncio.sleep(0.02)
    raise AssertionError(
        f"task {task_id} never reached {sorted(expected)}; last={last}"
    )


def record_rows(task_id: str) -> int:
    conn = db.connect()
    try:
        return conn.execute(
            "SELECT COUNT(*) FROM records WHERE task_id = ?", (task_id,)
        ).fetchone()[0]
    finally:
        conn.close()


def db_status(task_id: str) -> str:
    conn = db.connect()
    try:
        row = db.get_task(conn, task_id)
        return row["status"] if row is not None else "<missing>"
    finally:
        conn.close()


def seed_task(task_id: str, r1: bytes, r2: bytes) -> None:
    """Create a task row and staging inputs without starting its worker.

    Used to make the lock/worker ordering deterministic: the external lock is
    acquired before the worker starts, so the worker cannot write its status.
    """
    base = config.TASKS_DIR / task_id
    base.mkdir(parents=True, exist_ok=True)
    (base / "input_R1.fastq").write_bytes(r1)
    (base / "input_R2.fastq").write_bytes(r2)
    conn = db.connect()
    try:
        db.create_task(conn, task_id, "r1.fastq", "r2.fastq", len(r1), len(r2))
    finally:
        conn.close()
    worker.register_cancel_event(task_id)


async def main_async() -> int:
    tmp_root = Path(
        tempfile.mkdtemp(prefix="fastq-lock-", dir=str(OUTPUT_DIR))
    )
    data_dir = tmp_root / "data"
    os.environ["FASTQ_DATA_DIR"] = str(data_dir)
    config.DATA_DIR = data_dir
    config.DB_PATH = data_dir / "tasks.db"
    config.TASKS_DIR = data_dir / "tasks"
    config.PROGRESS_UPDATE_EVERY_BYTES = 64 * 1024
    # Keep the worker finishing quickly but still mid-flight during the
    # cancel call; a large input is used for the main case.
    db._initialized = False
    db.init_db()
    worker.recover_on_startup()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", timeout=120
    ) as client:
        # -- A succeeded task must stay queryable/downloadable during a lock.
        ok_ids = ["ok-a", "ok-b", "ok-c"]
        done = await upload(
            client,
            fastq_bytes(ok_ids, 1),
            fastq_bytes(list(reversed(ok_ids)), 2),
        )
        done = await wait_status(client, done["id"], {"succeeded", "failed"})
        record("control task succeeds", done["status"] == "succeeded", done["status"])
        done_id = done["id"]

        # ---- 1. cancel while an external connection holds the write lock
        n = 120_000
        ids = [f"read-{i:07d}" for i in range(n)]
        r1 = fastq_bytes(ids, 1)
        r2 = fastq_bytes(list(reversed(ids)), 2, seq=b"T" * 40)
        body = await upload(client, r1, r2)
        task_id = body["id"]
        # Let the worker start parsing (it writes in batches, holding the
        # write lock itself at times). Then grab the lock externally.
        await asyncio.sleep(0.4)

        lock = WriteLock(config.DB_PATH)
        lock.__enter__()
        try:
            t0 = time.monotonic()
            cr = await client.post(f"/tasks/{task_id}/cancel")
            elapsed = time.monotonic() - t0
            record(
                "cancel returns promptly while write lock held",
                cr.status_code == 200 and elapsed < 2.0,
                f"status={cr.status_code} elapsed={elapsed:.2f}s",
            )
            record(
                "cancel response shows cancelling, not processing",
                cr.status_code == 200 and cr.json()["status"] == "cancelling",
                cr.text[:200],
            )
            record(
                "cancelled task has no download link while cancelling",
                cr.json().get("download_r1") is None
                and cr.json().get("download_r2") is None,
            )

            # Status polling is usable while the lock is held.
            t0 = time.monotonic()
            poll = await client.get(f"/tasks/{task_id}")
            poll_elapsed = time.monotonic() - t0
            record(
                "status query works while write lock held",
                poll.status_code == 200 and poll_elapsed < 2.0,
                f"status={poll.status_code} elapsed={poll_elapsed:.2f}s",
            )
            record(
                "status overlay stays cancelling while lock held",
                poll.json()["status"] == "cancelling",
                poll.json()["status"],
            )

            # A previously succeeded result is still downloadable.
            t0 = time.monotonic()
            dl1 = await client.get(f"/tasks/{done_id}/download/r1")
            dl2 = await client.get(f"/tasks/{done_id}/download/r2")
            dl_elapsed = time.monotonic() - t0
            record(
                "succeeded downloads still work while write lock held",
                dl1.status_code == 200
                and dl2.status_code == 200
                and dl1.content == fastq_bytes(ok_ids, 1)
                and dl2.content == fastq_bytes(ok_ids, 2),
                f"r1={dl1.status_code} r2={dl2.status_code} "
                f"elapsed={dl_elapsed:.2f}s",
            )
            not_published = await client.get(f"/tasks/{task_id}/download/r1")
            record(
                "cancelling task cannot be downloaded",
                not_published.status_code == 409,
                str(not_published.status_code),
            )

            # A late write must not resurrect the task. Simulate the worker's
            # progress/success update racing the accepted cancellation.
            try:
                conn = db.connect(timeout_ms=50)
            except sqlite3.OperationalError:
                conn = None
            if conn is not None:
                try:
                    conn.execute(
                        "UPDATE tasks SET status='processing', progress=0.5 "
                        "WHERE id = ? AND status NOT IN "
                        "('succeeded','failed','cancelled','cancelling',"
                        "'cleanup_pending')",
                        (task_id,),
                    )
                except sqlite3.OperationalError:
                    pass
                finally:
                    conn.close()
            still = (await client.get(f"/tasks/{task_id}")).json()
            record(
                "late progress write cannot move task out of cancelling",
                still["status"] == "cancelling",
                still["status"],
            )

            # The cancel must be accepted even though the row could not be
            # written yet: it is in memory and will be persisted.
            record(
                "cancel wins over worker publish while lock held",
                db_status(task_id) not in ("succeeded",),
                db_status(task_id),
            )
        finally:
            lock.__exit__()

        # ---- lock released: the task must self-finalize without help
        body = await wait_status(
            client, task_id, {"cancelled", "failed", "succeeded"}, timeout=20
        )
        record(
            "task becomes cancelled after the lock is released",
            body["status"] == "cancelled",
            f"{body['status']}: {body['message']}",
        )
        record(
            "cancelled task is not published",
            body["download_r1"] is None and body["download_r2"] is None,
        )
        record(
            "cancelled staging directory removed",
            not (config.TASKS_DIR / task_id).exists(),
            str(config.TASKS_DIR / task_id),
        )
        record(
            "cancelled parsed records removed",
            record_rows(task_id) == 0,
            str(record_rows(task_id)),
        )
        record(
            "cancelled status persisted in SQLite",
            db_status(task_id) == "cancelled",
            db_status(task_id),
        )
        record(
            "succeeded control task unaffected",
            (await client.get(f"/tasks/{done_id}")).json()["status"] == "succeeded"
            and (await client.get(f"/tasks/{done_id}/download/r1")).status_code == 200,
        )

        # ---- 2. failure cleanup while the lock is held
        # Simulate a worker that fails (format error) while another connection
        # holds the write lock: staging is removed immediately, but the
        # terminal row cannot be written yet. The task must not stay
        # 'processing' forever; after release the reaper must finish it.
        bad_id = uuid.uuid4().hex
        seed_task(
            bad_id,
            fastq_bytes([f"bad-{i:06d}" for i in range(600)], 1),
            b"@bad-000000/2\nACGT\n+\n~~\n",
        )
        conn = db.connect()
        try:
            db.update_task(
                conn, bad_id, status="processing", stage=config.STAGE_PARSE
            )
            db.insert_records(conn, bad_id, [(1, 0, "bad-000000", b"@x\nA\n+\n~\n")])
        finally:
            conn.close()
        lock = WriteLock(config.DB_PATH)
        with lock:
            # Runs on a helper thread so the test thread can poll the API.
            def run_failure() -> None:
                worker._fail(
                    bad_id,
                    "rejected: R2: record 1 sequence length 4 != quality length 2",
                    cancelled=False,
                )

            helper = threading.Thread(target=run_failure, daemon=True)
            helper.start()
            helper.join(5)
            record("failure cleanup does not block on the lock", not helper.is_alive())
            b = (await client.get(f"/tasks/{bad_id}")).json()
            record(
                "failure waiting on lock is not left processing",
                b["status"] == "cleanup_pending",
                b["status"],
            )
            record(
                "deferred failure staging already removed",
                not (config.TASKS_DIR / bad_id).exists(),
            )
            pending = await client.get(f"/tasks/{bad_id}/download/r1")
            record(
                "cleanup_pending results are not downloadable",
                pending.status_code == 409,
                str(pending.status_code),
            )
        body = await wait_status(
            client, bad_id, {"failed", "cancelled", "succeeded"}, timeout=20
        )
        record(
            "failure cleanup completes after lock release",
            body["status"] == "failed",
            f"{body['status']}: {body['message']}",
        )
        record(
            "failed staging removed", not (config.TASKS_DIR / bad_id).exists()
        )
        record("failed records removed", record_rows(bad_id) == 0)
        record(
            "failed status persisted in SQLite", db_status(bad_id) == "failed",
            db_status(bad_id),
        )

        # ---- 3. cancellation whose worker already failed on format
        # A malformed input fails fast; if cancel is accepted before cleanup,
        # cancelled must win over failed.
        bob_id = uuid.uuid4().hex
        seed_task(bob_id, b"@x\nAAAA\n+\n~~~\n", fastq_bytes(["x"], 2))
        lock = WriteLock(config.DB_PATH)
        with lock:
            worker.start_worker(bob_id)
            deadline = time.time() + 10
            while time.time() < deadline:
                st = (await client.get(f"/tasks/{bob_id}")).json()["status"]
                if st == "cleanup_pending":
                    break
                await asyncio.sleep(0.02)
            cr = await client.post(f"/tasks/{bob_id}/cancel")
            record(
                "cancel accepted while failure cleanup is deferred",
                cr.status_code == 200,
                f"{cr.status_code}: {cr.text[:120]}",
            )
        body = await wait_status(
            client, bob_id, {"failed", "cancelled", "succeeded"}, timeout=20
        )
        record(
            "accepted cancel wins over deferred failure",
            body["status"] == "cancelled",
            f"{body['status']}: {body['message']}",
        )
        record(
            "deferred-cancelled staging removed",
            not (config.TASKS_DIR / bob_id).exists(),
        )

        # ---- 4. cancel accepted while a success commit is about to run
        # Hold the write lock so the worker's commit returns "busy"; accept the
        # cancel; release. The worker must lose the arbitration and never
        # rename the temporary results into place.
        race_id = uuid.uuid4().hex
        race_ids = ["r1", "r2", "r3"]
        seed_task(race_id, fastq_bytes(race_ids, 1), fastq_bytes(race_ids, 2))
        out_dir = config.TASKS_DIR / race_id / "output"
        conn = db.connect()
        try:
            from app.fastq import publish

            db.update_task(conn, race_id, status="processing", stage=config.STAGE_PUBLISH)
            db.insert_records(
                conn,
                race_id,
                [
                    (1, 0, "r1", b"@r1/1\nA\n+\n~\n"),
                    (1, 1, "r2", b"@r2/1\nA\n+\n~\n"),
                    (1, 2, "r3", b"@r3/1\nA\n+\n~\n"),
                    (2, 0, "r1", b"@r1/2\nT\n+\n~\n"),
                    (2, 1, "r2", b"@r2/2\nT\n+\n~\n"),
                    (2, 2, "r3", b"@r3/2\nT\n+\n~\n"),
                ],
            )
            tmp1, tmp2 = publish(conn, race_id, out_dir, lambda: False, lambda f: None)
        finally:
            conn.close()
        event = worker.register_cancel_event(race_id)
        lock = WriteLock(config.DB_PATH)
        with lock:
            outcome = {}
            helper = threading.Thread(
                target=lambda: outcome.setdefault(
                    "r",
                    worker._commit_success(
                        race_id, event, 3, tmp1, tmp2, out_dir,
                        config.TASKS_DIR / race_id / "input_R1.fastq",
                        config.TASKS_DIR / race_id / "input_R2.fastq",
                    ),
                ),
                daemon=True,
            )
            helper.start()
            helper.join(5)
            record(
                "success commit does not block on the lock",
                not helper.is_alive(),
                str(outcome.get("r")),
            )
            record(
                "commit under a held lock defers (busy)",
                outcome.get("r") == "busy",
                str(outcome.get("r")),
            )
            cr = await client.post(f"/tasks/{race_id}/cancel")
            record(
                "cancel accepted during commit deferral",
                cr.status_code == 200 and cr.json()["status"] == "cancelling",
                f"{cr.status_code}: {cr.json().get('status')}",
            )
        after = {}
        helper2 = threading.Thread(
            target=lambda: after.setdefault(
                "r",
                worker._commit_success(
                    race_id, event, 3, tmp1, tmp2, out_dir,
                    config.TASKS_DIR / race_id / "input_R1.fastq",
                    config.TASKS_DIR / race_id / "input_R2.fastq",
                ),
            ),
            daemon=True,
        )
        helper2.start()
        helper2.join(5)
        record(
            "late commit loses to accepted cancel",
            after.get("r") == "cancelled",
            str(after.get("r")),
        )
        record(
            "no result file was published by the late commit",
            not (out_dir / "R1.fastq").exists()
            and not (out_dir / "R2.fastq").exists(),
        )
        worker._fail(race_id, "cleanup", cancelled=True)
        record(
            "worker cleanup finalizes the race task as cancelled",
            db_status(race_id) == "cancelled" and record_rows(race_id) == 0,
            db_status(race_id),
        )

        # ---- 5. cancel accepted before the worker starts
        # Force the worker to be at the publish-commit point, then accept a
        # cancel and prove no late success write can overwrite it. This uses
        # the real _commit_success path with a tiny input so it completes fast.
        pre_id = uuid.uuid4().hex
        pre_ids = ["r1", "r2", "r3"]
        seed_task(pre_id, fastq_bytes(pre_ids, 1), fastq_bytes(pre_ids, 2))
        # Cancel accepted before the worker runs: it must never publish, even
        # though it would otherwise succeed.
        worker.request_cancel(pre_id)
        worker.start_worker(pre_id)
        body = await wait_status(
            client, pre_id, {"cancelled", "failed", "succeeded"}, timeout=20
        )
        record(
            "cancel accepted before start prevents publish",
            body["status"] == "cancelled"
            and not (config.TASKS_DIR / pre_id / "output" / "R1.fastq").exists(),
            body["status"],
        )
        record("pre-start cancel staging removed",
               not (config.TASKS_DIR / pre_id).exists())

        # ---- 6. a stale failure cleanup must never delete succeeded results
        good_id = uuid.uuid4().hex
        good_dir = config.TASKS_DIR / good_id
        (good_dir / "output").mkdir(parents=True)
        (good_dir / "output" / "R1.fastq").write_bytes(b"@g/1\nA\n+\n~\n")
        (good_dir / "output" / "R2.fastq").write_bytes(b"@g/2\nT\n+\n~\n")
        conn = db.connect()
        try:
            db.create_task(conn, good_id, "r1", "r2", 12, 12)
            db.finish_task(
                conn,
                good_id,
                status="succeeded",
                message="paired 1 reads",
                progress=1.0,
                result_r1="output/R1.fastq",
                result_r2="output/R2.fastq",
            )
        finally:
            conn.close()
        worker._fail(good_id, "stale failure", cancelled=False)
        dl = await client.get(f"/tasks/{good_id}/download/r1")
        record(
            "stale cleanup cannot unpublish a succeeded task",
            db_status(good_id) == "succeeded"
            and (good_dir / "output" / "R1.fastq").exists()
            and dl.status_code == 200,
            f"{db_status(good_id)} download={dl.status_code}",
        )

        # ---- 7. startup recovery while the write lock is held
        # A task left non-terminal by a "previous process" must be finalized
        # by startup recovery + the reaper even though the row cannot be
        # written during recovery.
        stale_id = uuid.uuid4().hex
        seed_task(
            stale_id,
            fastq_bytes(["stale"], 1),
            fastq_bytes(["stale"], 2),
        )
        conn = db.connect()
        try:
            db.update_task(
                conn, stale_id, status="processing", stage=config.STAGE_PARSE
            )
        finally:
            conn.close()
        lock = WriteLock(config.DB_PATH)
        with lock:
            recovered = worker.recover_on_startup()
            record(
                "startup recovery does not block on the lock",
                recovered["recovered"] >= 1,
                str(recovered),
            )
            record(
                "startup recovery removes staging even while locked",
                not (config.TASKS_DIR / stale_id).exists(),
            )
        body = await wait_status(
            client, stale_id, {"failed", "cancelled", "succeeded"}, timeout=20
        )
        record(
            "startup recovery finalizes after lock release",
            body["status"] == "failed",
            f"{body['status']}: {body['message']}",
        )
        record("recovered records removed", record_rows(stale_id) == 0)

        # ---- 8. the operator console is self-contained and reachable
        page = await client.get("/console")
        record(
            "console page served",
            page.status_code == 200
            and "上传并开始配对" in page.text
            and "cdn.jsdelivr" not in page.text,
            str(page.status_code),
        )

        # ---- 9. a fresh task works after all of the above
        after = await upload(
            client, fastq_bytes(["n1", "n2"], 1), fastq_bytes(["n2", "n1"], 2)
        )
        after = await wait_status(client, after["id"], {"succeeded", "failed"})
        record(
            "new tasks work after contention scenarios",
            after["status"] == "succeeded",
            after["message"],
        )

    shutil.rmtree(tmp_root, ignore_errors=True)
    failed = [name for name, ok, _ in RESULTS if not ok]
    print("\n%d/%d checks passed" % (len(RESULTS) - len(failed), len(RESULTS)))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
