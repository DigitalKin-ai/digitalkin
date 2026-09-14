# Shared helpers for DigitalKin hooks. Sourced, never executed.
# Pre-existing repo-wide debt from the ruff 0.16 rename of noqa/selector codes to
# names: 166 RUF105 + 43 RUF201. Excluded so the gate stays actionable.
# Clear with `uv run ruff check --fix .`, then drop this.
DK_RUFF_DEBT="RUF105,RUF201"

dk_root() { echo "${CLAUDE_PROJECT_DIR:-$(pwd)}"; }
dk_ruff() { local r; r="$(dk_root)/.venv/bin/ruff"; [ -x "$r" ] && echo "$r"; }
dk_mypy() { local m; m="$(dk_root)/.venv/bin/mypy"; [ -x "$m" ] && echo "$m"; }
dk_touched() { echo "$(dk_root)/.claude/.cache/touched-${1:-nosession}"; }

# Python source we own, excluding generated protobuf stubs.
dk_is_owned_py() {
  case "$1" in
    *_pb2.py|*_pb2_grpc.py|*_pb2.pyi) return 1 ;;
    */src/digitalkin/*.py|*/tests/*.py) return 0 ;;
    *) return 1 ;;
  esac
}
