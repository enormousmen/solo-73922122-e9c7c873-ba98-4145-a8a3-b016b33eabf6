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
SQLITE_BUSY_TIMEOUT_MS = 30_000

# How often the worker refreshes the progress percentage in the database.
PROGRESS_UPDATE_EVERY_BYTES = 8 * 1024 * 1024

STAGE_UPLOAD = "upload"
STAGE_PARSE = "parse"
STAGE_VALIDATE = "validate"
STAGE_PUBLISH = "publish"
STAGE_DONE = "done"
