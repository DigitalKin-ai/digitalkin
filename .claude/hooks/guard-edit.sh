#!/usr/bin/env bash
# PreToolUse(Edit|Write): refuse edits that introduce patterns CLAUDE.md
# prohibits outright. Advisory prose does not hold; this does.
set -uo pipefail
. "$(dirname "$0")/_common.sh"

input=$(cat)
file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')
case "$file" in
  */src/digitalkin/*.py) ;;
  *) exit 0 ;;
esac
case "$file" in
  */__init__.py|*_pb2.py|*_pb2_grpc.py) exit 0 ;;
esac

body=$(printf '%s' "$input" | jq -r '.tool_input.new_string // .tool_input.content // empty')
[ -n "$body" ] || exit 0
# Only added lines matter; an unchanged block must not be re-litigated.
[ "$(printf '%s' "$input" | jq -r '.tool_input.old_string // empty')" = "$body" ] && exit 0

if printf '%s' "$body" | grep -qE '\b(has|get|set)attr[[:space:]]*\('; then
  echo "BLOCKED: hasattr/getattr/setattr are prohibited in src/digitalkin (CLAUDE.md, Code philosophy)." >&2
  echo "Their presence means the type design is wrong. Use an explicit 'is None' check or a correct annotation." >&2
  exit 2
fi

offenders=$(printf '%s' "$body" | grep -nE '^def [a-zA-Z_]|^[A-Z][A-Z0-9_]{2,}[[:space:]]*[:=]' | grep -v 'dk-allow-global' || true)
if [ -n "$offenders" ]; then
  echo "BLOCKED: module-level function or constant in src/digitalkin (CLAUDE.md rules 3 and 4)." >&2
  printf '%s\n' "$offenders" | head -5 >&2
  echo "Make it a method on a class, or inline the default at its single use site alongside the env var." >&2
  echo "If this case is genuinely justified, append the marker 'dk-allow-global' as a trailing comment on the line." >&2
  exit 2
fi
exit 0
