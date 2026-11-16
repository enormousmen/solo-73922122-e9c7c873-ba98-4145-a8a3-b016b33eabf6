"""Streaming FASTQ parsing, validation and paired publishing.

Everything here works in fixed-size batches and streams from disk through
SQLite, so the total input size is allowed to exceed the process memory cap.
The original four lines of every record are stored byte-for-byte and written
out unchanged.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable, Iterator
from pathlib import Path

from . import config, db


class FastqFormatError(ValueError):
    """Raised when a FASTQ file violates the four-line record contract."""


class TaskCancelled(Exception):
    """Raised internally when a worker observes its cancellation flag."""


def extract_read_id(header_line: bytes) -> str:
    """Return the canonical read id from a FASTQ header line.

    The id is the field before the first whitespace, without the leading
    ``@`` and without a trailing ``/1`` or ``/2``.
    """
    if not header_line.startswith(b"@"):
        raise FastqFormatError("header line does not start with '@'")
    first_field = header_line[1:].split(None, 1)[0] if len(header_line) > 1 else b""
    if not first_field:
        raise FastqFormatError("empty read id in header line")
    if len(first_field) >= 2 and first_field[-2:-1] == b"/" and first_field[-1:] in (
        b"1",
        b"2",
    ):
        first_field = first_field[:-2]
    if not first_field:
        raise FastqFormatError("empty read id after stripping '/1' or '/2'")
    # Latin-1 maps every byte 1:1 to a unicode code point, so no input byte
    # sequence can fail to decode and ids round-trip losslessly.
    return first_field.decode("latin-1")


def _iter_raw_records(path: Path, side_label: str) -> Iterator[tuple[bytes, bytes, bytes, bytes]]:
    with path.open("rb") as fh:
        record_no = 0
        while True:
            lines = [fh.readline(), fh.readline(), fh.readline(), fh.readline()]
            if not lines[0]:
                # EOF: the other three must be empty too.
                if any(lines[1:]):
                    raise FastqFormatError(
                        f"{side_label}: truncated final record (expected four lines)"
                    )
                if record_no == 0:
                    raise FastqFormatError(f"{side_label}: file contains no records")
                return
            record_no += 1
            if any(not line for line in lines):
                raise FastqFormatError(
                    f"{side_label}: record {record_no} is missing lines "
                    "(FASTQ records must span exactly four lines)"
                )
            header, sequence, plus, quality = lines
            if not header.startswith(b"@"):
                raise FastqFormatError(
                    f"{side_label}: record {record_no} header does not start with '@'"
                )
            if not plus.startswith(b"+"):
                raise FastqFormatError(
                    f"{side_label}: record {record_no} third line does not start with '+'"
                )
            seq = sequence.rstrip(b"\r\n")
            qual = quality.rstrip(b"\r\n")
            if not seq:
                raise FastqFormatError(
                    f"{side_label}: record {record_no} has an empty sequence"
                )
            if not qual:
                raise FastqFormatError(
                    f"{side_label}: record {record_no} has an empty quality string"
                )
            if len(seq) != len(qual):
                raise FastqFormatError(
                    f"{side_label}: record {record_no} sequence length {len(seq)} "
                    f"!= quality length {len(qual)}"
                )
            yield header, sequence, plus, quality


def parse_file(
    conn: sqlite3.Connection,
    task_id: str,
    path: Path,
    side: int,
    is_cancelled: Callable[[], bool],
    progress_cb: Callable[[int], None],
) -> int:
    """Parse one FASTQ file into the records table in fixed-size batches.

    Returns the number of records parsed. Raises FastqFormatError on any
    malformed record and TaskCancelled on cancellation.
    """
    side_label = f"R{side}"
    batch: list[tuple] = []
    seen_in_batch: set[str] = set()
    ordinal = 0

    def flush() -> None:
        if not batch:
            return
        ids = [row[1] for row in batch]
        existing = conn.execute(
            f"SELECT read_id FROM records WHERE task_id = ? AND side = ? "
            f"AND read_id IN ({','.join('?' * len(ids))}) LIMIT 1",
            (task_id, side, *ids),
        ).fetchone()
        if existing is not None:
            raise FastqFormatError(
                f"{side_label}: duplicate read id '{existing['read_id']}'"
            )
        conn.execute("BEGIN IMMEDIATE")
        try:
            db.insert_records(conn, task_id, batch)
            conn.execute("COMMIT")
        except sqlite3.IntegrityError as exc:
            # Belt-and-braces: the unique index must never see a duplicate.
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise FastqFormatError(f"{side_label}: duplicate read id ({exc})") from exc
        except sqlite3.Error:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        batch.clear()
        seen_in_batch.clear()

    bytes_read = 0
    last_progress = 0
    for header, sequence, plus, quality in _iter_raw_records(path, side_label):
        if is_cancelled():
            raise TaskCancelled()
        read_id = extract_read_id(header.rstrip(b"\r\n"))
        if read_id in seen_in_batch:
            raise FastqFormatError(f"{side_label}: duplicate read id '{read_id}'")
        seen_in_batch.add(read_id)
        data = b"".join((header, sequence, plus, quality))
        bytes_read += len(data)
        batch.append((side, ordinal, read_id, data))
        ordinal += 1
        if len(batch) >= config.INSERT_BATCH_SIZE:
            flush()
        if bytes_read - last_progress >= config.PROGRESS_UPDATE_EVERY_BYTES:
            progress_cb(bytes_read)
            last_progress = bytes_read
    flush()
    progress_cb(bytes_read)
    if ordinal == 0:
        raise FastqFormatError(f"{side_label}: file contains no records")
    return ordinal


def validate_pairing(conn: sqlite3.Connection, task_id: str) -> None:
    """Fail the whole task if any read id lacks its mate on the other side."""
    orphan_r1 = conn.execute(
        "SELECT a.read_id AS rid FROM records a "
        "WHERE a.task_id = ? AND a.side = 1 AND NOT EXISTS ("
        "  SELECT 1 FROM records b WHERE b.task_id = a.task_id "
        "  AND b.side = 2 AND b.read_id = a.read_id) LIMIT 1",
        (task_id,),
    ).fetchone()
    if orphan_r1 is not None:
        raise FastqFormatError(
            f"orphan R1 read without an R2 mate: '{orphan_r1['rid']}'"
        )
    orphan_r2 = conn.execute(
        "SELECT a.read_id AS rid FROM records a "
        "WHERE a.task_id = ? AND a.side = 2 AND NOT EXISTS ("
        "  SELECT 1 FROM records b WHERE b.task_id = a.task_id "
        "  AND b.side = 1 AND b.read_id = a.read_id) LIMIT 1",
        (task_id,),
    ).fetchone()
    if orphan_r2 is not None:
        raise FastqFormatError(
            f"orphan R2 read without an R1 mate: '{orphan_r2['rid']}'"
        )


def publish(
    conn: sqlite3.Connection,
    task_id: str,
    out_dir: Path,
    is_cancelled: Callable[[], bool],
    progress_cb: Callable[[float], None],
) -> tuple[str, str]:
    """Stream joined pairs to two temporary files in R1 ordinal order.

    Only complete temporary files are produced here; the caller performs the
    final status arbitration and the atomic rename, so a cancelled task can
    never publish result files. Returns the temporary file names.
    """
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM records WHERE task_id = ? AND side = 1",
        (task_id,),
    ).fetchone()["n"]
    if total == 0:
        raise FastqFormatError("no R1 records to publish")

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_r1 = out_dir / ".R1.fastq.tmp"
    tmp_r2 = out_dir / ".R2.fastq.tmp"

    cursor = conn.execute(
        "SELECT a.data AS d1, b.data AS d2 "
        "FROM records a JOIN records b "
        "  ON b.task_id = a.task_id AND b.side = 2 AND b.read_id = a.read_id "
        "WHERE a.task_id = ? AND a.side = 1 "
        "ORDER BY a.ord",
        (task_id,),
    )
    written = 0
    try:
        with tmp_r1.open("wb") as f1, tmp_r2.open("wb") as f2:
            while True:
                if is_cancelled():
                    raise TaskCancelled()
                rows = cursor.fetchmany(config.INSERT_BATCH_SIZE)
                if not rows:
                    break
                f1.write(b"".join(row["d1"] for row in rows))
                f2.write(b"".join(row["d2"] for row in rows))
                written += len(rows)
                progress_cb(written / total)
            f1.flush()
            f2.flush()
            os.fsync(f1.fileno())
            os.fsync(f2.fileno())
        if is_cancelled():
            raise TaskCancelled()
    except BaseException:
        for tmp in (tmp_r1, tmp_r2):
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
        raise
    if written != total:
        for tmp in (tmp_r1, tmp_r2):
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
        raise FastqFormatError(f"published {written} pairs but expected {total}")
    return tmp_r1.name, tmp_r2.name


def promote_results(out_dir: Path, tmp1_name: str, tmp2_name: str) -> tuple[str, str]:
    """Atomically rename complete temp files into the published result names."""
    final_r1 = out_dir / "R1.fastq"
    final_r2 = out_dir / "R2.fastq"
    os.replace(out_dir / tmp1_name, final_r1)
    os.replace(out_dir / tmp2_name, final_r2)
    return final_r1.name, final_r2.name
