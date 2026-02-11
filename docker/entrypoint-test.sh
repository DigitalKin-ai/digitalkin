#!/bin/sh
set -e

if [ -n "$TEST_MARKER" ]; then
    pytest "${TEST_SELECTOR:-tests/}" -m "$TEST_MARKER" ${PYTEST_ARGS:-} || exit_code=$?
else
    pytest "${TEST_SELECTOR:-tests/}" ${PYTEST_ARGS:-} || exit_code=$?
fi

exit_code=${exit_code:-0}

# Exit code 5 = no tests collected (marker matched nothing), treat as success
if [ "$exit_code" -eq 5 ]; then
    echo "No tests matched marker '$TEST_MARKER' — skipping."
    exit 0
fi

exit "$exit_code"
