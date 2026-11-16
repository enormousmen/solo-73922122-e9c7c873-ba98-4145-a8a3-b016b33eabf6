"""Functional self-tests for the paired FASTQ service.

Run with the project virtualenv:
    .venv/bin/python -I tests/test_api.py
Each test run gets an isolated FASTQ_DATA_DIR under tests/output.
"""

from __future__ import annotations

import asyncio
import io
import os
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path

# Make the project root importable when executed as a script.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import httpx  # noqa: E402

from app import config, db, worker  # noqa: E402
from app.main import app  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))


def make_record(rid: str, seq: bytes, quality: bytes | None = None,
                suffix: str = "", desc: bytes = b"") -> bytes:
    if quality is None:
        quality = b"~" * len(seq)
    header = b"@" + rid.encode() + suffix.encode()
    if desc:
        header += b" " + desc
    return b"\n".join((header, seq, b"+", quality)) + b"\n"


def fastq(records: list[bytes]) -> io.BytesIO:
    return io.BytesIO(b"".join(records))


async def wait_terminal(client: httpx.AsyncClient, task_id: str,
                        timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = (await client.get(f"/tasks/{task_id}")).json()
        if body["status"] in ("succeeded", "failed", "cancelled"):
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"task {task_id} did not finish in {timeout}s")


async def upload(client: httpx.AsyncClient, r1: io.BytesIO, r2: io.BytesIO,
                 name1: str = "r1.fastq", name2: str = "r2.fastq") -> httpx.Response:
    r1.seek(0)
    r2.seek(0)
    return await client.post(
        "/tasks",
        files={"r1": (name1, r1, "application/octet-stream"),
               "r2": (name2, r2, "application/octet-stream")},
    )


def parse_fastq(data: bytes) -> list[tuple[str, bytes]]:
    lines = data.splitlines(keepends=True)
    assert len(lines) % 4 == 0
    out = []
    for i in range(0, len(lines), 4):
        header = lines[i].rstrip(b"\r\n")
        rid = header[1:].split(None, 1)[0]
        if rid.endswith(b"/1") or rid.endswith(b"/2"):
            rid = rid[:-2]
        out.append((rid.decode("latin-1"), b"".join(lines[i:i + 4])))
    return out


async def main_async() -> int:
    tmp_root = Path(tempfile.mkdtemp(prefix="fastq-test-",
                                     dir=str(ROOT / "tests" / "output")))
    data_dir = tmp_root / "data"
    os.environ["FASTQ_DATA_DIR"] = str(data_dir)
    config.DATA_DIR = data_dir
    config.DB_PATH = data_dir / "tasks.db"
    config.TASKS_DIR = data_dir / "tasks"
    config.PROGRESS_UPDATE_EVERY_BYTES = 64 * 1024

    # httpx's ASGITransport does not emit lifespan events, so initialize
    # explicitly (uvicorn itself runs the lifespan hook).
    db._initialized = False
    db.init_db()
    worker.recover_on_startup()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test",
                                 timeout=60) as client:
        # ---- 1. happy path: shuffled order, /1 /2 suffixes, bytes preserved
        r1_records = [
            make_record("a", b"AAAA", suffix="/1"),
            make_record("b", b"CCCC", suffix="/1", desc=b"sample=x"),
            make_record("c", b"GGGG", suffix="/1"),
        ]
        r2_records = [
            make_record("c", b"gggg", suffix="/2"),
            make_record("a", b"aaaa", suffix="/2"),
            make_record("b", b"cccc", suffix="/2"),
        ]
        r = await upload(client, fastq(r1_records), fastq(r2_records))
        record("upload returns 202", r.status_code == 202, str(r.status_code))
        body = await wait_terminal(client, r.json()["id"])
        record("happy path succeeds", body["status"] == "succeeded",
               body.get("message", ""))
        record("progress reaches 1.0", abs(body["progress"] - 1.0) < 1e-9)

        tid = body["id"]
        d1 = (await client.get(f"/tasks/{tid}/download/r1")).content
        d2 = (await client.get(f"/tasks/{tid}/download/r2")).content
        p1 = parse_fastq(d1)
        p2 = parse_fastq(d2)
        record("R1 order preserved", [rid for rid, _ in p1] == ["a", "b", "c"])
        record("R2 follows R1 order", [rid for rid, _ in p2] == ["a", "b", "c"])
        record("R1 bytes unchanged", [blob for _, blob in p1] == r1_records)
        record("R2 bytes unchanged",
               [blob for _, blob in p2] ==
               [r2_records[1], r2_records[2], r2_records[0]])

        # ---- 2. duplicate id inside R2
        dup_r2 = fastq([
            make_record("a", b"aaaa", suffix="/2"),
            make_record("b", b"cccc", suffix="/2"),
            make_record("a", b"tttt", suffix="/2"),
        ])
        r = await upload(client, fastq(r1_records), dup_r2)
        body = await wait_terminal(client, r.json()["id"])
        record("duplicate id fails whole task",
               body["status"] == "failed" and "duplicate" in body["message"],
               body.get("message", ""))
        tid = body["id"]
        record("duplicate task has no downloads",
               body["download_r1"] is None and body["download_r2"] is None)
        gd = await client.get(f"/tasks/{tid}/download/r1")
        record("no partial result published", gd.status_code == 409)
        record("failed task staging removed",
               not (config.TASKS_DIR / tid).exists())

        # ---- 3. orphan end
        orphan_r1 = fastq([make_record("a", b"AAAA"), make_record("z", b"TTTT")])
        orphan_r2 = fastq([make_record("a", b"aaaa")])
        r = await upload(client, orphan_r1, orphan_r2)
        body = await wait_terminal(client, r.json()["id"])
        record("orphan end fails whole task",
               body["status"] == "failed" and "orphan" in body["message"],
               body.get("message", ""))

        # ---- 4. malformed: not four lines
        bad = io.BytesIO(b"@x\nAAAA\n+\n")  # truncated final record
        r = await upload(client, bad, fastq([make_record("x", b"aaaa")]))
        body = await wait_terminal(client, r.json()["id"])
        record("truncated record rejected",
               body["status"] == "failed" and "four lines" in body["message"],
               body.get("message", ""))

        # ---- 5. malformed: seq/quality length mismatch
        mismatch = fastq([make_record("x", b"AAAA", b"~~~")])
        r = await upload(client, fastq([make_record("x", b"AAAA")]), mismatch)
        body = await wait_terminal(client, r.json()["id"])
        record("seq/quality length mismatch rejected",
               body["status"] == "failed" and "length" in body["message"],
               body.get("message", ""))

        # ---- 6. empty file
        r = await upload(client, io.BytesIO(b""),
                         fastq([make_record("x", b"aaaa")]))
        body = await wait_terminal(client, r.json()["id"])
        record("empty file rejected",
               body["status"] == "failed" and "no records" in body["message"],
               body.get("message", ""))

        # ---- 7. oversize upload -> 413, staging cleaned
        old_limit = config.MAX_FILE_BYTES
        config.MAX_FILE_BYTES = 64
        before = {p.name for p in config.TASKS_DIR.glob("*")}
        try:
            big = io.BytesIO(b"@x\n" + b"A" * 128 + b"\n+\n" + b"~" * 128 + b"\n")
            r = await upload(client, big, fastq([make_record("x", b"aaaa")]))
            record("oversize upload -> 413", r.status_code == 413,
                   str(r.status_code))
            after = {p.name for p in config.TASKS_DIR.glob("*")}
            # The rejected task must not have created a retained directory;
            # directories of earlier *successful* tasks legitimately remain.
            record("oversize staging cleaned", before == after,
                   f"unexpected new dirs: {sorted(after - before)}")
        finally:
            config.MAX_FILE_BYTES = old_limit

        # ---- 8. cancellation: big task, cancel returns before terminal state
        n = 20000
        ids = [f"read-{i}" for i in range(n)]
        big_r1 = fastq([make_record(rid, b"A" * 40) for rid in ids])
        shuffled = list(reversed(ids))
        big_r2 = fastq([make_record(rid, b"a" * 40) for rid in shuffled])
        r = await upload(client, big_r1, big_r2)
        cancel_id = r.json()["id"]
        cr = await client.post(f"/tasks/{cancel_id}/cancel")
        record("cancel accepted (200)", cr.status_code == 200, str(cr.status_code))
        record("cancel response does not wait for pairing",
               cr.json()["status"] in ("cancelling", "cancelled"))
        body = await wait_terminal(client, cancel_id, timeout=60)
        record("cancelled task terminal state",
               body["status"] == "cancelled", body.get("status", ""))
        record("cancelled task not published",
               body["download_r1"] is None
               and not (config.TASKS_DIR / cancel_id).exists())

        # a brand new task works immediately afterwards
        r = await upload(client, fastq(r1_records), fastq(r2_records))
        body = await wait_terminal(client, r.json()["id"])
        record("new task works right after cancel",
               body["status"] == "succeeded", body.get("message", ""))

        # ---- 9. cancel a finished task -> 409, result stays downloadable
        ok_id = body["id"]
        cr = await client.post(f"/tasks/{ok_id}/cancel")
        record("cancelling finished task -> 409", cr.status_code == 409,
               str(cr.status_code))
        again = await client.get(f"/tasks/{ok_id}")
        record("finished task still succeeded",
               again.json()["status"] == "succeeded")
        dl = await client.get(f"/tasks/{ok_id}/download/r1")
        record("finished result still downloadable", dl.status_code == 200)

        # ---- 10. unknown task
        unknown = await client.get(f"/tasks/{uuid.uuid4().hex}")
        record("unknown task -> 404", unknown.status_code == 404)

        # ---- 11. restart recovery: simulate interrupted task
        stuck_id = uuid.uuid4().hex
        base = config.TASKS_DIR / stuck_id
        (base / "output").mkdir(parents=True)
        (base / "input_R1.fastq").write_bytes(b"@x\nAAAA\n+\n~~~~\n")
        (base / "input_R2.fastq").write_bytes(b"@x\nAAAA\n+\n~~~~\n")
        conn = db.connect()
        db.create_task(conn, stuck_id, "r1.fastq", "r2.fastq", 20, 20)
        db.update_task(conn, stuck_id, status="processing",
                       stage=config.STAGE_PARSE)
        conn.close()
        worker.recover_on_startup()
        conn = db.connect()
        row = db.get_task(conn, stuck_id)
        conn.close()
        record("restart marks unfinished task failed",
               row["status"] == "failed", row["status"])
        record("restart removes unfinished staging", not base.exists())

        # succeeded task survives restart
        r = await upload(client, fastq(r1_records), fastq(r2_records))
        good_id = (await wait_terminal(client, r.json()["id"]))["id"]
        good_path = config.TASKS_DIR / good_id / "output" / "R1.fastq"
        worker.recover_on_startup()
        dl = await client.get(f"/tasks/{good_id}/download/r1")
        record("succeeded result survives restart",
               good_path.is_file() and dl.status_code == 200)

        # ---- 12. id normalization: /1 /2 stripped, mixed suffix styles
        mixed_r1 = fastq([make_record("m1", b"AAAA", suffix="/1"),
                          make_record("m2", b"CCCC")])
        mixed_r2 = fastq([make_record("m2", b"cccc", suffix="/2"),
                          make_record("m1", b"aaaa")])
        r = await upload(client, mixed_r1, mixed_r2)
        body = await wait_terminal(client, r.json()["id"])
        record("mixed suffix styles pair correctly",
               body["status"] == "succeeded", body.get("message", ""))

    shutil.rmtree(tmp_root, ignore_errors=True)
    failed = [name for name, ok, _ in RESULTS if not ok]
    print("\n%d/%d checks passed" % (len(RESULTS) - len(failed), len(RESULTS)))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
