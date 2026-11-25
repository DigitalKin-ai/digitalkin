"""SafeLiteLLM turns provider failures into RunErrorCode values that reach agno's RUN_ERROR."""

import asyncio
import logging
from types import SimpleNamespace
from typing import Any

import httpx
import openai
import pytest
from agno.agent import Agent, RunEvent
from agno.exceptions import OutputCheckError, RunCancelledException
from agno.models.message import Message
from litellm import exceptions as llm

from digitalkin.community.agno.safe_litellm import ModelCallError, SafeLiteLLM
from digitalkin.models.events import RunErrorCode


def _response(status: int) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("POST", "http://proxy"))


def _kw() -> dict[str, Any]:
    return {"model": "m", "llm_provider": "litellm_proxy"}


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (llm.ContentPolicyViolationError(message="policy", **_kw()), RunErrorCode.MODEL_CONTENT_FILTERED),
        (llm.ContextWindowExceededError(message="too long", **_kw()), RunErrorCode.MODEL_CONTEXT_LENGTH_EXCEEDED),
        (llm.PaymentRequiredError(message="pay", **_kw()), RunErrorCode.MODEL_BUDGET_EXCEEDED),
        (llm.BudgetExceededError(current_cost=2.0, max_budget=1.0), RunErrorCode.MODEL_BUDGET_EXCEEDED),
        (llm.BadRequestError(message="bad", **_kw()), RunErrorCode.MODEL_REQUEST_INVALID),
        (
            llm.BadRequestError(message="prompt is too long: 250000 tokens > 200000 maximum", **_kw()),
            RunErrorCode.MODEL_CONTEXT_LENGTH_EXCEEDED,
        ),
        (
            llm.BadRequestError(
                message="The input token count (1200000) exceeds the maximum number of tokens allowed (1048576).",
                **_kw(),
            ),
            RunErrorCode.MODEL_CONTEXT_LENGTH_EXCEEDED,
        ),
        (
            llm.BadRequestError(message="Request token count exceeds the limit of 1048576", **_kw()),
            RunErrorCode.MODEL_CONTEXT_LENGTH_EXCEEDED,
        ),
        (
            llm.BadRequestError(message="max_tokens: 1048576 > 65536, which is the maximum allowed", **_kw()),
            RunErrorCode.MODEL_REQUEST_INVALID,
        ),
        (
            llm.UnprocessableEntityError(message="422", response=_response(422), **_kw()),
            RunErrorCode.MODEL_REQUEST_INVALID,
        ),
        (llm.AuthenticationError(message="401", **_kw()), RunErrorCode.MODEL_AUTHENTICATION_FAILED),
        (
            llm.PermissionDeniedError(message="403", response=_response(403), **_kw()),
            RunErrorCode.MODEL_PERMISSION_DENIED,
        ),
        (llm.NotFoundError(message="404", **_kw()), RunErrorCode.MODEL_NOT_FOUND),
        (llm.RateLimitError(message="429", **_kw()), RunErrorCode.MODEL_RATE_LIMITED),
        (
            llm.InternalServerError(message="AnthropicError - overloaded_error: Overloaded", **_kw()),
            RunErrorCode.MODEL_OVERLOADED,
        ),
        (llm.APIError(status_code=529, message="Overloaded", **_kw()), RunErrorCode.MODEL_OVERLOADED),
        (llm.Timeout(message="timed out", **_kw()), RunErrorCode.MODEL_TIMEOUT),
        (openai.APITimeoutError(request=httpx.Request("POST", "http://proxy")), RunErrorCode.MODEL_TIMEOUT),
        (llm.MidStreamFallbackError(message="dropped", **_kw()), RunErrorCode.MODEL_STREAM_INTERRUPTED),
        (llm.ServiceUnavailableError(message="503", **_kw()), RunErrorCode.MODEL_UNAVAILABLE),
        (llm.BadGatewayError(message="502", **_kw()), RunErrorCode.MODEL_UNAVAILABLE),
        (llm.InternalServerError(message="500", **_kw()), RunErrorCode.MODEL_UNAVAILABLE),
        (llm.APIConnectionError(message="refused", **_kw()), RunErrorCode.MODEL_UNAVAILABLE),
        (llm.APIError(status_code=500, message="boom", **_kw()), RunErrorCode.MODEL_UNAVAILABLE),
        (ValueError("x"), None),
    ],
)
def test_error_code_table(exc: Exception, expected: RunErrorCode | None) -> None:
    assert SafeLiteLLM.error_code(exc) is expected


def test_subclass_precedence() -> None:
    # Each of these subclasses a broader litellm class that maps to another code.
    assert (
        SafeLiteLLM.error_code(llm.MidStreamFallbackError(message="d", **_kw()))
        is RunErrorCode.MODEL_STREAM_INTERRUPTED
    )
    assert (
        SafeLiteLLM.error_code(llm.ContentPolicyViolationError(message="p", **_kw()))
        is RunErrorCode.MODEL_CONTENT_FILTERED
    )
    assert (
        SafeLiteLLM.error_code(llm.ContextWindowExceededError(message="c", **_kw()))
        is RunErrorCode.MODEL_CONTEXT_LENGTH_EXCEEDED
    )
    assert SafeLiteLLM.error_code(llm.PaymentRequiredError(message="p", **_kw())) is RunErrorCode.MODEL_BUDGET_EXCEEDED


class _RaisingClient:
    """Stands in for the litellm module: every completion raises ``error``."""

    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.calls = 0

    async def acompletion(self, **_: Any) -> Any:
        self.calls += 1
        raise self.error


async def _stream_run_error(error: BaseException) -> Any:
    agent = Agent(
        model=SafeLiteLLM(id="claude-sonnet-5", client=_RaisingClient(error)),
        retries=0,
        telemetry=False,
    )
    events = [e async for e in agent.arun("Bonjour", stream=True, stream_events=True)]
    errors = [e for e in events if e.event == RunEvent.run_error]
    assert len(errors) == 1
    return errors[0]


class _Delta(SimpleNamespace):
    """A litellm delta stand-in: litellm's objects expose a dict-like ``get``."""

    def get(self, key: str, default: object = None) -> object:
        return self.__dict__.get(key, default)


class _StreamingClient:
    """Stands in for the litellm module: streams one text chunk per item of ``texts``."""

    def __init__(self, *texts: str) -> None:
        self.texts = texts

    async def acompletion(self, **_: Any) -> Any:
        async def stream() -> Any:  # ruff: ignore[unused-async] - an async generator
            for text in self.texts:
                yield SimpleNamespace(
                    choices=[SimpleNamespace(delta=_Delta(content=text, tool_calls=None), finish_reason=None)],
                    usage=None,
                )

        return stream()


def _model(error: BaseException) -> SafeLiteLLM:
    return SafeLiteLLM(id="claude-sonnet-5", client=_RaisingClient(error))


class TestReachesAgno:
    async def test_stream_failure_reaches_agno_as_code(self) -> None:
        error = await _stream_run_error(llm.RateLimitError(message="quota hit", **_kw()))

        assert error.error_type == "MODEL_RATE_LIMITED"
        assert "quota hit" in error.content
        assert "claude-sonnet-5" in error.content

    async def test_non_stream_failure_raises_classified_error(self) -> None:
        cause = llm.RateLimitError(message="quota hit", **_kw())

        with pytest.raises(ModelCallError) as raised:
            await _model(cause).ainvoke([Message(role="user", content="hi")], Message(role="assistant"))

        assert raised.value.code is RunErrorCode.MODEL_RATE_LIMITED
        assert raised.value.type == "MODEL_RATE_LIMITED"
        assert raised.value.__cause__ is cause

    @pytest.mark.parametrize(
        "error",
        [RunCancelledException("stop"), ValueError("bad"), OutputCheckError("guard"), asyncio.CancelledError()],
    )
    async def test_non_model_exceptions_pass_through(self, error: BaseException) -> None:
        with pytest.raises(type(error)) as raised:
            async for _ in _model(error).ainvoke_stream(
                [Message(role="user", content="hi")], Message(role="assistant")
            ):
                pass

        assert raised.value is error

    async def test_classification_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            await _stream_run_error(llm.RateLimitError(message="quota hit", **_kw()))

        assert "[VALIDATE RUN-ERROR-CODE-MODEL] RateLimitError -> MODEL_RATE_LIMITED" in caplog.text

    @pytest.mark.edge_case
    async def test_early_close_is_not_classified(self, caplog: pytest.LogCaptureFixture) -> None:
        """A consumer that stops reading closes the stream with GeneratorExit, not a provider failure."""
        stream = SafeLiteLLM(id="claude-sonnet-5", client=_StreamingClient("Hel", "lo")).ainvoke_stream(
            [Message(role="user", content="hi")], Message(role="assistant")
        )

        with caplog.at_level(logging.WARNING):
            first = await anext(stream)
            await stream.aclose()

        assert first.content == "Hel"
        assert "[VALIDATE RUN-ERROR-CODE-MODEL]" not in caplog.text
