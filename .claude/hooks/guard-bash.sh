#!/usr/bin/env bash
# PreToolUse(Bash): git is off-limits in the main session. Read-only subagents
# need a diff to review against, so they keep read access and lose mutations.
#
# Matching is on command position, not substring: heredoc bodies are dropped and
# the command is split on shell separators, so only a pseudo-command whose first
# word is `git` counts. Writing a file that documents a git command is not
# running one.
set -uo pipefail

input=$(cat)
cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty')
[ -n "$cmd" ] || exit 0

git_calls=$(printf '%s\n' "$cmd" | awk '
  # Drop heredoc bodies: everything from <<MARKER up to the closing MARKER.
  !inheredoc && match($0, /<<-?[[:space:]]*['"'"'"]?[A-Za-z_][A-Za-z0-9_]*/) {
    m = substr($0, RSTART, RLENGTH)
    gsub(/^<<-?[[:space:]]*['"'"'"]?/, "", m)
    marker = m; inheredoc = 1; print; next
  }
  inheredoc { if ($0 ~ "^[[:space:]]*" marker "[[:space:]]*$") inheredoc = 0; next }
  { print }
' | sed 's/&&/\n/g; s/||/\n/g; s/[;|]/\n/g; s/\$(/\n/g; s/`/\n/g' \
  | grep -E '^[[:space:]]*(sudo[[:space:]]+)?git[[:space:]]' || true)

[ -n "$git_calls" ] || exit 0

if printf '%s' "$git_calls" | grep -qE '(^|[[:space:]])git[[:space:]]+(commit|push|checkout|switch|stash|rebase|reset|merge|cherry-pick|revert|clean|tag|am|apply|restore|rm|mv|remote|submodule|filter-branch|branch[[:space:]]+-[dDmM])\b'; then
  echo "BLOCKED: this git command mutates repository state. The user runs every git write themselves." >&2
  echo "Print the command and explain what it would do instead of running it." >&2
  exit 2
fi

if [ -z "$(printf '%s' "$input" | jq -r '.agent_id // empty')" ]; then
  echo "BLOCKED: git is not run in the main session. Read files directly for baseline checks." >&2
  echo "If you need a diff, delegate to a review subagent (code-reviewer, grpc-breaking) — those may read git." >&2
  exit 2
fi
exit 0
