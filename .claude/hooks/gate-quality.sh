#!/usr/bin/env bash
# Stop: refuse to end the turn while code touched this session fails lint, or
# while mypy fails on src/digitalkin. Scoped to touched files because the repo
# carries pre-existing RUF105/RUF201 debt (see _common.sh).
set -uo pipefail
. "$(dirname "$0")/_common.sh"

input=$(cat)
[ "$(printf '%s' "$input" | jq -r '.stop_hook_active // false')" = "true" ] && exit 0
session=$(printf '%s' "$input" | jq -r '.session_id // "nosession"')

touched=$(dk_touched "$session")
[ -s "$touched" ] || exit 0

files=()
while IFS= read -r f; do [ -f "$f" ] && files+=("$f"); done < "$touched"
[ ${#files[@]} -gt 0 ] || exit 0

failed=""
ruff=$(dk_ruff)
if [ -n "$ruff" ]; then
  out=$("$ruff" check --no-fix --ignore "$DK_RUFF_DEBT" --output-format=concise "${files[@]}" 2>&1) || failed+="$out"$'\n'
fi

mypy=$(dk_mypy)
if [ -n "$mypy" ]; then
  out=$(cd "$(dk_root)" && "$mypy" src/digitalkin 2>&1) || failed+="$out"$'\n'
fi

[ -z "$failed" ] && exit 0

{
  echo "Quality gate failed on code changed this session. Fix these, then finish:"
  echo
  printf '%s' "$failed" | head -40
} >&2
exit 2
