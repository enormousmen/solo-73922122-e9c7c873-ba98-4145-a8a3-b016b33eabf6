"""FastAPI application: bounded uploads, status, cancellation, downloads."""

from __future__ import annotations

import sqlite3
import tempfile
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from . import config, db, worker
from .worker import UploadTooLarge, start_worker, task_dir


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.init_db()
    worker.recover_on_startup()
    worker.ensure_reaper()
    yield


app = FastAPI(
    title="Paired FASTQ Service",
    version="1.1.0",
    description=(
        "Upload shuffled paired-end FASTQ files (R1/R2, uncompressed, "
        "max 256 MiB each). The service re-pairs them by read id, delivers "
        "both mates in R1 order and preserves the original bytes. "
        "A self-contained operator page is available at /console."
    ),
    lifespan=lifespan,
)


class TaskOut(BaseModel):
    id: str
    status: str
    stage: str
    progress: float
    message: str
    r1_name: str
    r2_name: str
    r1_bytes: int
    r2_bytes: int
    created_at: str
    updated_at: str
    download_r1: str | None = None
    download_r2: str | None = None


def _serialize(row: sqlite3.Row) -> TaskOut:
    status_text = worker.effective_status(row["id"], row)
    message = worker.effective_message(row["id"], row)
    download_r1 = download_r2 = None
    if status_text == "succeeded" and row["result_r1"]:
        download_r1 = f"/tasks/{row['id']}/download/r1"
        download_r2 = f"/tasks/{row['id']}/download/r2"
    return TaskOut(
        id=row["id"],
        status=status_text,
        stage=row["stage"],
        progress=round(row["progress"], 4),
        message=message,
        r1_name=row["r1_name"],
        r2_name=row["r2_name"],
        r1_bytes=row["r1_bytes"],
        r2_bytes=row["r2_bytes"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        download_r1=download_r1,
        download_r2=download_r2,
    )


def _insert_task_with_retry(task_id: str, **fields) -> sqlite3.Row:
    """Persist the new task with zero-timeout writes and bounded retries.

    A concurrent writer must not turn task creation into a 30 s wait, but the
    task must be visible to its worker before the thread starts.
    """
    def attempt() -> sqlite3.Row:
        conn = db.connect(timeout_ms=config.SQLITE_NONBLOCKING_TIMEOUT_MS)
        try:
            db.create_task(conn, task_id, **fields)
            row = db.get_task(conn, task_id)
            assert row is not None
            return row
        finally:
            conn.close()

    last: BaseException | None = None
    # Bounded retry (about 4 s with the small per-attempt timeout): an upload
    # must not turn into a 30 s stall behind an unrelated writer, but a brief
    # lock conflict should not fail the request either.
    for _ in range(200):
        try:
            return attempt()
        except sqlite3.OperationalError as exc:
            if not db.is_lock_error(exc):
                raise
            last = exc
            time.sleep(0.02)
    assert last is not None
    raise last


async def _spool_bounded(upload: UploadFile, side: str, dest: Path) -> int:
    """Stream an upload to ``dest`` without holding it in memory.

    SpooledTemporaryFile rolls over to a real file after
    ``UPLOAD_SPOOL_THRESHOLD`` and the copy proceeds in fixed chunks; the
    per-file 256 MiB limit is enforced while streaming.
    """
    spool = tempfile.SpooledTemporaryFile(max_size=config.UPLOAD_SPOOL_THRESHOLD)
    total = 0
    try:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > config.MAX_FILE_BYTES:
                raise UploadTooLarge(side, config.MAX_FILE_BYTES)
            spool.write(chunk)
        spool.rollover()
        spool.seek(0)
        with dest.open("wb") as out:
            while True:
                chunk = spool.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
    finally:
        spool.close()
    return total


@app.post("/tasks", response_model=TaskOut, status_code=status.HTTP_202_ACCEPTED)
async def create_task(
    r1: UploadFile = File(..., alias="r1", description="R1 FASTQ, uncompressed, <= 256 MiB"),
    r2: UploadFile = File(..., alias="r2", description="R2 FASTQ, uncompressed, <= 256 MiB"),
) -> TaskOut:
    task_id = uuid.uuid4().hex
    base = task_dir(task_id)
    base.mkdir(parents=True, exist_ok=False)
    r1_dest = base / "input_R1.fastq"
    r2_dest = base / "input_R2.fastq"
    try:
        size1 = await _spool_bounded(r1, "R1", r1_dest)
        size2 = await _spool_bounded(r2, "R2", r2_dest)
    except UploadTooLarge as exc:
        worker.cleanup_task_dir(task_id)
        raise HTTPException(
            status_code=getattr(
                status, "HTTP_413_CONTENT_TOO_LARGE",
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            ),
            detail=str(exc),
        )
    except Exception:
        worker.cleanup_task_dir(task_id)
        raise
    finally:
        await r1.close()
        await r2.close()

    try:
        row = _insert_task_with_retry(
            task_id,
            r1_name=Path(r1.filename or "r1.fastq").name,
            r2_name=Path(r2.filename or "r2.fastq").name,
            r1_bytes=size1,
            r2_bytes=size2,
        )
    except Exception:
        worker.cleanup_task_dir(task_id)
        raise

    worker.register_cancel_event(task_id)
    start_worker(task_id)
    return _serialize(row)


def _read_row(task_id: str) -> sqlite3.Row:
    """Read task state without waiting on another connection's write lock."""
    row = db.read_task(task_id)
    if row is None:
        raise HTTPException(status_code=404, detail="task not found")
    return row


@app.get("/tasks/{task_id}", response_model=TaskOut)
def get_task(task_id: str) -> TaskOut:
    return _serialize(_read_row(task_id))


@app.post("/tasks/{task_id}/cancel", response_model=TaskOut)
def cancel_task(task_id: str) -> TaskOut:
    # The request is accepted in memory first, so it is honored even while a
    # worker (or any other SQLite connection) holds the write lock, and it is
    # always honored before the worker can publish. Nothing here waits for a
    # lock: the ``cancelling`` row is written with a small-timeout transaction
    # and, if the lock is busy, by the reaper once it is released.
    previous = worker.request_cancel(task_id)
    if previous is None:
        raise HTTPException(status_code=404, detail="task not found")
    if previous in db.TERMINAL_STATUSES:
        # The task reached a terminal state first; tell the caller plainly
        # that it cannot be cancelled.
        raise HTTPException(
            status_code=409,
            detail=f"task is already {previous} and cannot be cancelled",
        )
    return _serialize(_read_row(task_id))


def _result_path(task_id: str, side: int) -> Path:
    row = _read_row(task_id)
    status_text = worker.effective_status(task_id, row)
    if status_text != "succeeded":
        raise HTTPException(
            status_code=409,
            detail=f"task is {status_text}; results are not published",
        )
    # Always answer from the committed row: an overlay status can only make a
    # download less available, never expose unpublished result paths.
    if row["status"] != "succeeded":
        raise HTTPException(
            status_code=409,
            detail=f"task is {row['status']}; results are not published",
        )
    rel = row["result_r1"] if side == 1 else row["result_r2"]
    if not rel:
        raise HTTPException(status_code=404, detail="result file missing")

    base = task_dir(task_id).resolve()
    path = (base / rel).resolve()
    # Guard against path traversal even though the rel value is server-generated.
    if base not in path.parents or not path.is_file():
        raise HTTPException(status_code=404, detail="result file missing")
    return path


@app.get("/tasks/{task_id}/download/r1")
def download_r1(task_id: str) -> FileResponse:
    path = _result_path(task_id, 1)
    return FileResponse(
        path, media_type="text/plain", filename="R1.fastq",
        headers={"Content-Length": str(path.stat().st_size)},
    )


@app.get("/tasks/{task_id}/download/r2")
def download_r2(task_id: str) -> FileResponse:
    path = _result_path(task_id, 2)
    return FileResponse(
        path, media_type="text/plain", filename="R2.fastq",
        headers={"Content-Length": str(path.stat().st_size)},
    )


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


_STATIC_DIR = Path(__file__).resolve().parent / "static"
_CONSOLE_PAGE = _STATIC_DIR / "console.html"


@app.get("/console", include_in_schema=False)
def console() -> FileResponse:
    """Self-contained operator page (no CDN needed).

    ``/docs`` is the generated Swagger UI, but it fetches its JavaScript from
    a CDN and therefore needs internet access. This page implements upload,
    query, cancel and both downloads with plain fetch calls, so the service
    stays fully operable in an offline container.
    """
    if not _CONSOLE_PAGE.is_file():
        raise HTTPException(status_code=404, detail="console page missing")
    return FileResponse(_CONSOLE_PAGE, media_type="text/html")

