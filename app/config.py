"""Runtime configuration for the paired FASTQ service.

All limits can be overridden through environment variables, but the defaults
match the delivery requirement: each uploaded file <= 256 MiB and the whole
process is expected to live inside a 256 MiB memory cap (see
docker-compose.yml).
"""

from __future__ import annotations

import os
from pathlib import Path

MAX_FILE_BYTES = int(os.environ.get("FASTQ_MAX_FILE_BYTES", 256 * 1024 * 1024))
"""Maximum size of a single uploaded FASTQ file (256 MiB)."""

DATA_DIR = Path(os.environ.get("FASTQ_DATA_DIR", "/data"))
"""Root directory for the SQLite database and all task files."""

DB_PATH = DATA_DIR / "tasks.db"
TASKS_DIR = DATA_DIR / "tasks"

# Uploads are spooled to a temporary file once they exceed this size, so a big
# multipart upload never accumulates in RAM.
UPLOAD_SPOOL_THRESHOLD = int(
    os.environ.get("FASTQ_UPLOAD_SPOOL_THRESHOLD", 1 * 1024 * 1024)
)

# Records are inserted in batches; only one batch is held in memory at a time.
INSERT_BATCH_SIZE = int(os.environ.get("FASTQ_INSERT_BATCH_SIZE", 512))

# SQLite page cache kept deliberately small. The heavy data stays on disk.
SQLITE_CACHE_PAGES = int(os.environ.get("FASTQ_SQLITE_CACHE_PAGES", 2048))
# Busy timeout for the connections that still use SQLite's own waiting: the
# initial schema creation and atomic success publication hold the write lock
# only briefly, so this bounds those short waits. It is deliberately NOT
# increased to "solve" lock contention; cancel and status paths never use it.
SQLITE_BUSY_TIMEOUT_MS = 30_000
# README-level guarantee: a cancel/status request never waits behind another
# connection for seconds. Reads and cancel bookkeeping use this small timeout
# (a few milliseconds, enough to ride out SQLite's transient WAL index
# contention) plus bounded retries, not the 30 s busy timeout above.
SQLITE_NONBLOCKING_TIMEOUT_MS = int(
    os.environ.get("FASTQ_SQLITE_NONBLOCKING_TIMEOUT_MS", 20)
)
# Startup recovery may briefly wait for a stale lock, then hands the rest to
# the background reaper instead of leaving tasks active.
SQLITE_STARTUP_BUSY_TIMEOUT_MS = int(
    os.environ.get("FASTQ_SQLITE_STARTUP_BUSY_TIMEOUT_MS", 250)
)

# How often the deferred-finalization reaper retries work that could not be
# written because another connection held the SQLite write lock.
REAPER_INTERVAL_SECONDS = 0.5
# A non-terminal task with no running worker is only failed as an orphan once
# it is older than this, so a task between INSERT and thread start is safe.
ORPHAN_GRACE_SECONDS = float(os.environ.get("FASTQ_ORPHAN_GRACE_SECONDS", 2.0))

# How often the worker refreshes the progress percentage in the database.
PROGRESS_UPDATE_EVERY_BYTES = 8 * 1024 * 1024

STAGE_UPLOAD = "upload"
STAGE_PARSE = "parse"
STAGE_VALIDATE = "validate"
STAGE_PUBLISH = "publish"
STAGE_DONE = "done"
