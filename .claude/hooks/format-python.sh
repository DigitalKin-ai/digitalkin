#!/usr/bin/env bash
# PostToolUse(Edit|Write): format the file just written and record it for the
# Stop gate. Never blocks; a broken env must not stall the session.
set -uo pipefail
. "$(dirname "$0")/_common.sh"

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')
session=$(printf '%s' "$input" | jq -r '.session_id // "nosession"')
[ -n "$file" ] && [ -f "$file" ] || exit 0
dk_is_owned_py "$file" || exit 0

ruff=$(dk_ruff) || exit 0
[ -n "$ruff" ] || exit 0

"$ruff" format -q "$file" >/dev/null 2>&1
"$ruff" check -q --fix --ignore "$DK_RUFF_DEBT" "$file" >/dev/null 2>&1

touched=$(dk_touched "$session")
mkdir -p "$(dirname "$touched")"
grep -qxF "$file" "$touched" 2>/dev/null || echo "$file" >> "$touched"
exit 0
