#!/usr/bin/env bash
# Live end-to-end check of cancellation while an external SQLite connection
# holds the write lock, against a running service (default localhost:8000).
# Proves: cancel is accepted before the lock is released; status stays
# cancelling; an already-succeeded result stays downloadable; after release the
# task finalizes as cancelled with no published result and staging removed.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
DATA_DIR="${DATA_DIR:-/tmp/fastq-live}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

need() { command -v "$1" >/dev/null || { echo "missing tool: $1" >&2; exit 1; }; }
need curl
need python3

json() { python3 -c 'import json,sys; print(json.load(sys.stdin)["'"$1"'"])'; }
jget() { python3 -c "import json,sys; print(json.load(sys.stdin).get('$1'))"; }

echo "== ensure a succeeded task exists for the download-availability check =="
printf '@keep/1 d\nACGT\n+\n~~~~\n' > "$WORK/keep_R1.fastq"
printf '@keep/2 d\nTGCA\n+\n~~~~\n' > "$WORK/keep_R2.fastq"
KEEP=$(curl -fsS -X POST "$BASE_URL/tasks" \
  -F "r1=@$WORK/keep_R1.fastq;filename=keep_R1.fastq" \
  -F "r2=@$WORK/keep_R2.fastq;filename=keep_R2.fastq")
KEEP_ID=$(json id <<<"$KEEP")
for _ in $(seq 1 100); do
  ST=$(curl -fsS "$BASE_URL/tasks/$KEEP_ID" | jget status)
  [ "$ST" = succeeded ] && break
  sleep 0.1
done
echo "kept task: $KEEP_ID ($ST)"

echo "== generate a big shuffled pair and upload =="
python3 - "$WORK" <<'PY'
import sys
n = 200_000
with open(sys.argv[1] + "/big_R1.fastq", "wb") as f1, \
     open(sys.argv[1] + "/big_R2.fastq", "wb") as f2:
    for i in range(n):
        f1.write(b"@r%09d/1\n" % i + b"A" * 40 + b"\n+\n" + b"~" * 40 + b"\n")
    for i in range(n - 1, -1, -1):
        f2.write(b"@r%09d/2\n" % i + b"T" * 40 + b"\n+\n" + b"!" * 40 + b"\n")
PY
TASK=$(curl -fsS -X POST "$BASE_URL/tasks" \
  -F "r1=@$WORK/big_R1.fastq;filename=big_R1.fastq" \
  -F "r2=@$WORK/big_R2.fastq;filename=big_R2.fastq")
TASK_ID=$(json id <<<"$TASK")
sleep 1
echo "task: $TASK_ID"

echo "== hold the SQLite write lock from an independent process =="
python3 - "$DATA_DIR/tasks.db" "$WORK/lock_ready" "$WORK/lock_release" <<'PY' &
import sqlite3, sys, time, os
db, ready, release = sys.argv[1], sys.argv[2], sys.argv[3]
conn = sqlite3.connect(db, timeout=10, isolation_level=None)
conn.execute("BEGIN IMMEDIATE")
conn.execute("UPDATE tasks SET message=message WHERE 0")
open(ready, "w").close()
while not os.path.exists(release):
    time.sleep(0.05)
conn.execute("COMMIT")
conn.close()
PY
LOCK_PID=$!
for _ in $(seq 1 100); do [ -f "$WORK/lock_ready" ] && break; sleep 0.05; done
[ -f "$WORK/lock_ready" ] || { echo "lock not acquired" >&2; exit 1; }
echo "write lock held"

echo "== cancel while the lock is held =="
START=$(python3 -c 'import time; print(time.time())')
CANCEL=$(curl -fsS -X POST "$BASE_URL/tasks/$TASK_ID/cancel")
ELAPSED=$(python3 -c "import time; print(round(time.time()-$START, 2))")
echo "$CANCEL"
echo "cancel elapsed: ${ELAPSED}s"
[ "$(jget status <<<"$CANCEL")" = cancelling ] || { echo "not cancelling" >&2; exit 1; }

echo "== query while locked =="
START=$(python3 -c 'import time; print(time.time())')
POLL=$(curl -fsS "$BASE_URL/tasks/$TASK_ID")
ELAPSED=$(python3 -c "import time; print(round(time.time()-$START, 2))")
echo "$POLL"
echo "status elapsed: ${ELAPSED}s"
[ "$(jget status <<<"$POLL")" = cancelling ] || { echo "status not cancelling" >&2; exit 1; }

echo "== download the already-succeeded result while locked =="
CODE=$(curl -s -o "$WORK/keep_out.fastq" -w '%{http_code}' "$BASE_URL/tasks/$KEEP_ID/download/r1")
[ "$CODE" = 200 ] || { echo "download failed: $CODE" >&2; exit 1; }
cmp -s "$WORK/keep_R1.fastq" "$WORK/keep_out.fastq" || { echo "download bytes differ" >&2; exit 1; }
echo "download OK (200, bytes identical)"

echo "== release the lock; wait for the task to self-finalize =="
touch "$WORK/lock_release"
wait "$LOCK_PID" || true
FINAL=""
for _ in $(seq 1 200); do
  BODY=$(curl -fsS "$BASE_URL/tasks/$TASK_ID")
  FINAL=$(jget status <<<"$BODY")
  case "$FINAL" in cancelled|failed|succeeded) break ;; esac
  sleep 0.1
done
echo "$BODY"
[ "$FINAL" = cancelled ] || { echo "final status $FINAL" >&2; exit 1; }
[ ! -e "$DATA_DIR/tasks/$TASK_ID" ] || { echo "staging not removed" >&2; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$BASE_URL/tasks/$TASK_ID/download/r1")
[ "$CODE" = 409 ] || { echo "cancelled download: $CODE" >&2; exit 1; }
echo "final: cancelled, staging removed, download refused (409)"
echo "LIVE LOCK TEST PASSED"
