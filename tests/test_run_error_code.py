"""Contract tests for RunErrorCode, the SDK's closed set of RUN_ERROR codes."""

import ast
import json
import re
from pathlib import Path

import pytest

from digitalkin.models.events import RunErrorCode, RunErrorEvent, SubagentErrorEvent

pytestmark = pytest.mark.contract


def test_values_are_their_names_and_aip193_shaped() -> None:
    for code in RunErrorCode:
        assert code.value == code.name
        assert re.fullmatch(r"[A-Z][A-Z0-9_]+[A-Z0-9]", code.value)
        assert len(code.value) <= 63


def test_serializes_as_plain_string() -> None:
    assert json.dumps({"code": RunErrorCode.MODEL_TIMEOUT}) == '{"code": "MODEL_TIMEOUT"}'
    assert RunErrorCode("MODEL_TIMEOUT") is RunErrorCode.MODEL_TIMEOUT


def test_member_set_is_frozen() -> None:
    assert {c.value for c in RunErrorCode} == {
        "MODEL_CONTENT_FILTERED",
        "MODEL_CONTEXT_LENGTH_EXCEEDED",
        "MODEL_BUDGET_EXCEEDED",
        "MODEL_REQUEST_INVALID",
        "MODEL_AUTHENTICATION_FAILED",
        "MODEL_PERMISSION_DENIED",
        "MODEL_NOT_FOUND",
        "MODEL_RATE_LIMITED",
        "MODEL_OVERLOADED",
        "MODEL_TIMEOUT",
        "MODEL_STREAM_INTERRUPTED",
        "MODEL_UNAVAILABLE",
        "INPUT_INVALID",
        "INPUT_REJECTED",
        "OUTPUT_REJECTED",
        "TOOL_RESULTS_PARTIAL",
        "AUTO_CONTINUE_LIMIT",
        "RUN_CANCELLED",
        "PERMISSION_DENIED",
        "MODULE_FAILED",
        "MODULE_INIT_FAILED",
        "UNKNOWN",
        "RUN_INTERRUPTED",
    }


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (RunErrorCode.MODEL_RATE_LIMITED, True),
        (RunErrorCode.MODEL_OVERLOADED, True),
        (RunErrorCode.MODEL_TIMEOUT, True),
        (RunErrorCode.MODEL_STREAM_INTERRUPTED, True),
        (RunErrorCode.MODEL_UNAVAILABLE, True),
        (RunErrorCode.TOOL_RESULTS_PARTIAL, True),
        (RunErrorCode.AUTO_CONTINUE_LIMIT, True),
        (RunErrorCode.RUN_INTERRUPTED, True),
        (RunErrorCode.MODEL_CONTENT_FILTERED, False),
        (RunErrorCode.MODEL_CONTEXT_LENGTH_EXCEEDED, False),
        (RunErrorCode.MODEL_BUDGET_EXCEEDED, False),
        (RunErrorCode.MODEL_REQUEST_INVALID, False),
        (RunErrorCode.MODEL_AUTHENTICATION_FAILED, False),
        (RunErrorCode.MODEL_PERMISSION_DENIED, False),
        (RunErrorCode.MODEL_NOT_FOUND, False),
        (RunErrorCode.INPUT_INVALID, False),
        (RunErrorCode.INPUT_REJECTED, False),
        (RunErrorCode.OUTPUT_REJECTED, False),
        (RunErrorCode.RUN_CANCELLED, False),
        (RunErrorCode.PERMISSION_DENIED, False),
        (RunErrorCode.MODULE_FAILED, None),
        (RunErrorCode.MODULE_INIT_FAILED, None),
        (RunErrorCode.UNKNOWN, None),
    ],
)
def test_retryable(code: RunErrorCode, expected: bool | None) -> None:
    assert code.retryable is expected


def test_every_member_has_a_retryable_decision() -> None:
    # A new member must be placed in a list deliberately; falling through to the default fails here.
    assert {c for c in RunErrorCode if c.retryable is None} == {
        RunErrorCode.MODULE_FAILED,
        RunErrorCode.MODULE_INIT_FAILED,
        RunErrorCode.UNKNOWN,
    }


def test_event_defaults_hold_the_plain_value() -> None:
    """``use_enum_values`` skips unvalidated defaults; a member there logs as ``RunErrorCode.UNKNOWN``."""
    run_error = RunErrorEvent()
    subagent_error = SubagentErrorEvent(message="child failed")

    assert (str(run_error.error_type), str(subagent_error.code)) == ("UNKNOWN", "UNKNOWN")


def test_every_code_has_a_producer() -> None:
    """Each code is emitted somewhere in ``src``: a member nobody raises is dead weight on the wire contract."""
    package = Path(__file__).resolve().parents[1] / "src" / "digitalkin"
    used = {
        node.attr
        for path in package.rglob("*.py")
        if path.name != "run_error_code.py"
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "RunErrorCode"
    }

    assert {c.name for c in RunErrorCode} - used == set()


def test_every_code_is_documented_with_its_retryable_hint() -> None:
    """The protocol doc's code table lists every member once, with the hint ``retryable`` really sends."""
    doc = (Path(__file__).resolve().parents[1] / "docs" / "gateway_protocol.md").read_text(encoding="utf-8")
    section = doc.split("## RUN_ERROR codes", 1)[1].split("\n---", 1)[0]
    rows = re.findall(r"^\| `([A-Z_]+)` \|[^\n]*?\| (`true`|`false`|—) \|[^|\n]*\|$", section, re.MULTILINE)
    hint = {True: "`true`", False: "`false`", None: "—"}

    assert len(rows) == len(dict(rows))
    assert dict(rows) == {c.value: hint[c.retryable] for c in RunErrorCode}


def test_every_code_has_a_behaviour_test() -> None:
    """Each code is asserted by a test of the code that emits it, not only by this contract file."""
    tests = Path(__file__).resolve().parent
    text = "\n".join(
        path.read_text(encoding="utf-8") for path in tests.rglob("test_*.py") if path.name != Path(__file__).name
    )

    assert {c.name for c in RunErrorCode if not re.search(rf"\b{c.name}\b", text)} == set()
