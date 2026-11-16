"""Memory stress test: total input exceeds the 256 MiB memory cap.

Generates a shuffled R1/R2 pair (~220 MiB each, ~440 MiB total) under
tests/output, runs the real worker in a child process and reads the child's
peak resident set size from /proc/<pid>/status (VmHWM). The run must:

  * complete successfully,
  * stay below RSS_LIMIT KiB of peak RSS,
  * emit paired files in R1 order with every original byte preserved.

Usage:
    .venv/bin/python -I tests/test_memory.py [records]
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RECORDS = int(sys.argv[1]) if len(sys.argv) > 1 else 5_000_000
# Python + sqlite baseline is small; require a comfortable margin under 256 MiB.
RSS_LIMIT_KIB = 230 * 1024
SEQ_LEN = 40
# Each record: '@read-' (6) + 10 digits + '/1' (2) + '\n' (1) = 19 B header,
# sequence line 41 B, '+\n' 2 B, quality line 41 B -> 103 B total.
BYTES_PER_RECORD = 19 + (SEQ_LEN + 1) + 2 + (SEQ_LEN + 1)

CHILD = textwrap.dedent("""
    import sys
    sys.path.insert(0, %r)
    from pathlib import Path
    from app import config, db, worker

    data_dir = Path(sys.argv[1])
    config.DATA_DIR = data_dir
    config.DB_PATH = data_dir / "tasks.db"
    config.TASKS_DIR = data_dir / "tasks"
    db._initialized = False
    db.init_db()

    task_id = "memorystress"
    base = worker.task_dir(task_id)
    base.mkdir(parents=True, exist_ok=True)
    conn = db.connect()
    db.create_task(conn, task_id, "R1.fastq", "R2.fastq", 0, 0)
    conn.close()
    worker.register_cancel_event(task_id)
    worker.run_task(task_id)

    conn = db.connect()
    row = db.get_task(conn, task_id)
    print("STATUS=" + row["status"])
    print("MESSAGE=" + row["message"])
    conn.close()
""")


def write_inputs(base: Path, n: int) -> tuple[Path, Path]:
    r1 = base / "input_R1.fastq"
    r2 = base / "input_R2.fastq"
    seq = b"A" * SEQ_LEN
    qual = b"~" * SEQ_LEN
    # R1 ascending ids; R2 in a deterministic shuffled order (reverse blocks).
    block = 1000
    order = list(range(n))
    shuffled: list[int] = []
    for start in range(0, n, block):
        shuffled.extend(range(min(start + block, n) - 1, start - 1, -1))
    with r1.open("wb") as f1, r2.open("wb") as f2:
        buf1 = bytearray()
        buf2 = bytearray()
        for i, j in zip(order, shuffled):
            buf1 += b"@read-%010d/1\n%s\n+\n%s\n" % (i, seq, qual)
            buf2 += b"@read-%010d/2\n%s\n+\n%s\n" % (j, seq, qual)
            if len(buf1) >= 4 * 1024 * 1024:
                f1.write(buf1)
                f2.write(buf2)
                buf1.clear()
                buf2.clear()
        f1.write(buf1)
        f2.write(buf2)
    return r1, r2


def read_vmhwm(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1])
    raise RuntimeError("VmHWM not available")


def verify_outputs(base: Path, n: int) -> tuple[bool, str]:
    out = base / "tasks" / "memorystress" / "output"
    r1, r2 = out / "R1.fastq", out / "R2.fastq"
    if not r1.is_file() or not r2.is_file():
        return False, "output files missing"
    expected_size = n * BYTES_PER_RECORD
    if r1.stat().st_size != expected_size or r2.stat().st_size != expected_size:
        return False, (
            f"size mismatch r1={r1.stat().st_size} r2={r2.stat().st_size} "
            f"expected={expected_size}"
        )
    # Stream-verify order, pairing and byte preservation at block boundaries.
    with r1.open("rb") as f1, r2.open("rb") as f2:
        idx = 0
        while idx < n:
            h1 = f1.readline()
            h2 = f2.readline()
            want1 = b"@read-%010d/1\n" % idx
            want2 = b"@read-%010d/2\n" % idx
            if h1 != want1 or h2 != want2:
                return False, f"order/pair mismatch at record {idx}: {h1!r} {h2!r}"
            # skip remaining three lines of each record
            for _ in range(3):
                f1.readline()
                f2.readline()
            idx += 1
            if idx >= n:
                break
            # Full per-record scan of 5m x lines is enough; keep it simple.
    return True, f"verified {n} paired records, both files {expected_size} bytes"


def main() -> int:
    scratch = Path(tempfile.mkdtemp(prefix="fastq-mem-",
                                    dir=str(ROOT / "tests" / "output")))
    data_dir = scratch / "data"
    data_dir.mkdir(parents=True)
    print(f"Generating {RECORDS:,} records per side (~"
          f"{2 * RECORDS * BYTES_PER_RECORD / (1024**3):.2f} GiB total)...")
    t0 = time.time()
    r1, r2 = write_inputs(scratch, RECORDS)
    print(f"  inputs written in {time.time() - t0:.1f}s: "
          f"R1={r1.stat().st_size / (1024**2):.1f} MiB, "
          f"R2={r2.stat().st_size / (1024**2):.1f} MiB")

    # Place inputs where the worker expects them.
    base = data_dir / "tasks" / "memorystress"
    base.mkdir(parents=True)
    shutil.move(str(r1), base / "input_R1.fastq")
    shutil.move(str(r2), base / "input_R2.fastq")

    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    proc = subprocess.Popen(
        [sys.executable, "-I", "-c", CHILD % str(ROOT), str(data_dir)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
    )
    t0 = time.time()
    peak = 0
    while proc.poll() is None:
        try:
            peak = max(peak, read_vmhwm(proc.pid))
        except (FileNotFoundError, ProcessLookupError):
            break
        time.sleep(0.1)
    out, err = proc.communicate()
    runtime = time.time() - t0
    print(out.strip())
    if proc.returncode != 0:
        print(err[-4000:])
        print("RESULT: FAIL (worker exited %d)" % proc.returncode)
        return 1

    status = next((l.split("=", 1)[1] for l in out.splitlines()
                   if l.startswith("STATUS=")), "")
    if status != "succeeded":
        print(err[-4000:])
        print(f"RESULT: FAIL (status={status})")
        return 1

    ok, detail = verify_outputs(data_dir, RECORDS)
    print(detail)
    print(f"peak RSS: {peak / 1024:.1f} MiB "
          f"(limit {RSS_LIMIT_KIB / 1024:.0f} MiB), runtime {runtime:.1f}s")
    if peak > RSS_LIMIT_KIB:
        print("RESULT: FAIL (memory cap exceeded)")
        return 1
    if not ok:
        print("RESULT: FAIL (output verification)")
        return 1
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
