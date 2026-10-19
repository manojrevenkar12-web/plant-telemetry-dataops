#!/usr/bin/env bash
# Run a CI command; on failure, put the last lines of its output into one annotation so the
# cause is visible in the run summary without downloading logs.
set -o pipefail
out=$(mktemp)
"$@" 2>&1 | tee "$out"
status=$?
if [ $status -ne 0 ]; then
  msg=$(tail -n 60 "$out" | sed -e 's/%/%25/g' | awk '{printf "%s%%0A", $0}')
  echo "::error title=$1 failed (exit $status)::$msg"
fi
exit $status
