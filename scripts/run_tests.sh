#!/usr/bin/env bash
# Run every self-test except the long memory stress test (use test_memory.py
# directly for that one). Run from the project root.
set -euo pipefail
cd "$(dirname "$0")/.."

PY=.venv/bin/python
[ -x "$PY" ] || PY=python3

echo "=== functional + lifecycle tests ==="
"$PY" -I tests/test_api.py

echo
echo "=== cancel/complete arbitration tests ==="
"$PY" -I tests/test_cancel_race.py

echo
echo "ALL SHORT SELF-TESTS PASSED"
