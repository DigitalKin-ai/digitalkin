"""Agno LiteLLM model carrying the request-shaping fixes neither agno nor the LiteLLM proxy applies.

- Empty content: agno serializes a tool-call-only assistant message with ``content: ""``, which
  litellm's Anthropic adapter unconditionally rewrites to a ``[System: Empty message content
  sanitised ...]`` placeholder that leaks into the conversation. Empty content never reaches litellm.
- Parameters: the Claude 5 family rejects ``temperature``; Claude Opus/Fable 5 thinking is requested
  summarized (else it streams empty); Anthropic ``cache_control`` breakpoints mark the last tool
  definition, the system prompt and the newest turn.
- Bounds: a tool result is cut to ``max_tool_result_chars``, then litellm's ``trim_messages`` fits
  the request to 95% of the model's input window. The trim runs before the cache breakpoints: on
  cache-marked system blocks it fails and returns the request untrimmed.
- History: with a ``compactor`` the replayed history is folded at its token threshold and a request
  rejected as too long is folded and resent once. A history window opening mid-exchange is prefixed
  with a note, giving Claude the user-first transcript it expects.
- Recovery: an empty reply is retried once with a continuation prompt, then replaced by a notice; a
  400 blaming the request's images is retried once without them and later requests drop images.
- Cancellation: no request starts once ``cancel_event`` is set.
- Output: a provider refusal (``finish_reason: content_filter``) raises ``OutputCheckError`` instead
  of completing empty; ``url_citation`` annotations (http/https only) become agno citations, which
  the stream adapter forwards as ``source_citation`` events.
"""

from __future__ import annotations

import re
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import urlsplit

import litellm
from agno.exceptions import CheckTrigger, OutputCheckError, RunCancelledException
from agno.models.litellm import LiteLLM
from agno.models.message import Citations, Message, UrlCitation
from agno.models.response import ModelResponse
from litellm.exceptions import ContextWindowExceededError
from litellm.utils import trim_messages
from openai import APIConnectionError, BadRequestError, InternalServerError
from pydantic import BaseModel

from digitalkin.community.litellm.cost import CostTrackingLogger
from digitalkin.community.litellm.registry import LiteLLMRegistry
from digitalkin.logger import logger

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator

    from agno.metrics import MessageMetrics
    from agno.run.agent import RunOutput

    from digitalkin.community.agno.compaction import HistoryCompactor
    from digitalkin.community.litellm.model_specs import ModelSpec


@dataclass
class SafeLiteLLM(LiteLLM):
    """Agno LiteLLM model carrying every request-shaping fix of the module docstring.

    Attributes:
        send_temperature: Send ``temperature``; False for models that reject it.
        enable_prompt_caching: Place Anthropic ``cache_control`` breakpoints.
        cancel_event: Once set, no further request starts.
        max_tool_result_chars: Cap on one tool result (~37k tokens: a long page fits, six parallel
            results still leave most of a 1M window).
        empty_response_notice: Shown when the continuation retry is empty too; None keeps the empty reply.
        drop_images: Latched when the provider rejects the images: every later request of this
            instance (models are built per task) is sent without images.
        drop_image_urls: Latched when the provider cannot fetch an image URL: later requests of this
            instance keep inline images and drop only the ones sent by URL.
        compactor: Folds the replayed history at its threshold. Head models only.
        input_tokens: Input budget: the context window minus the output reserved by ``max_tokens``.
            The last-resort trim fits the request into it; None falls back to litellm's own
            window for the model id, unknown for most proxy model groups.
    """

    send_temperature: bool = True
    enable_prompt_caching: bool = False
    cancel_event: asyncio.Event | None = None
    max_tool_result_chars: int = 150_000
    empty_response_notice: str | None = (
        "I could not generate a response to your last message. Please send it again or rephrase it."
    )
    drop_images: bool = False
    drop_image_urls: bool = False
    compactor: HistoryCompactor | None = None
    input_tokens: int | None = None

    # Image parts of the request the current asyncio task is sending. One model can serve two calls
    # at once (parallel delegations to the same member); each task keeps its own count between
    # formatting the request and receiving its rejection.
    _request_image_parts: ClassVar[ContextVar[tuple[int, int]]] = ContextVar(
        "safe_litellm_request_image_parts", default=(0, 0)
    )

    @classmethod
    def from_spec(
        cls,
        model_spec: ModelSpec,
        *,
        extra_headers: dict[str, str] | None = None,
        session_id: str | None = None,
        temperature: float | None = None,
        mission_id: str | None = None,
        cancel_event: asyncio.Event | None = None,
        compactor: HistoryCompactor | None = None,
    ) -> SafeLiteLLM:
        """Build a model routed through the LiteLLM proxy of ``LiteLLMRegistry.shared()``.

        Also routes every litellm call through the proxy (``litellm.use_litellm_proxy``) and
        registers :class:`CostTrackingLogger` once per process.

        Args:
            model_spec: Spec of the proxy model group.
            extra_headers: Proxy headers (e.g. Langfuse trace linking).
            session_id: LiteLLM session id grouping the requests (typically the mission id).
            temperature: Sampling temperature; 0.7 when None.
            mission_id: Stamped into the request ``metadata`` so the cost callback books the call to
                the mission's cost service.
            cancel_event: Once set, the model refuses further requests.
            compactor: Folds the replayed history at its threshold. Head models only.

        Returns:
            The configured model.
        """
        litellm.use_litellm_proxy = True
        CostTrackingLogger.register()

        # Forwarded as top-level ``litellm.acompletion`` kwargs: ``metadata`` reaches the success
        # callback. ``num_retries`` only retries the HTTP request to the proxy, never the run.
        request_params: dict[str, Any] = {"drop_params": True, "num_retries": 2}
        if mission_id:
            request_params["metadata"] = {CostTrackingLogger.MISSION_METADATA_KEY: mission_id}
        # Anthropic only accepts the default temperature alongside thinking.
        summarized_thinking = cls.wants_summarized_thinking(model_spec.name)
        if summarized_thinking:
            request_params["thinking"] = {"type": "adaptive", "display": "summarized"}
        send_temperature = cls.supports_temperature(model_spec.name) and not summarized_thinking
        enable_prompt_caching = cls.uses_anthropic_cache(model_spec.name)
        logger.info(
            "model=%s send_temperature=%s enable_prompt_caching=%s summarized_thinking=%s",
            model_spec.name,
            send_temperature,
            enable_prompt_caching,
            summarized_thinking,
        )
        config = LiteLLMRegistry.shared().config
        capacity = model_spec.capacity
        return cls(
            id=model_spec.name,
            api_base=config.base_url,
            api_key=config.bearer_token,
            max_tokens=capacity.max_output,
            temperature=temperature if temperature is not None else 0.7,
            top_p=None,  # type: ignore[arg-type]  # Bedrock rejects temperature + top_p; agno drops None
            send_temperature=send_temperature,
            enable_prompt_caching=enable_prompt_caching,
            cancel_event=cancel_event,
            compactor=compactor,
            input_tokens=capacity.input_budget or None,
            extra_headers=extra_headers or {},
            extra_body={"litellm_session_id": session_id} if session_id else None,
            request_params=request_params,
        )

    @staticmethod
    def supports_temperature(model_id: str) -> bool:
        """Report whether a model group accepts ``temperature``.

        Args:
            model_id: LiteLLM model group name.

        Returns:
            False for the Claude 5 family (rejected on Vertex), True otherwise.
        """
        return re.match(r"^claude-(opus|sonnet|haiku)-5(?![0-9])", model_id) is None

    @staticmethod
    def uses_anthropic_cache(model_id: str) -> bool:
        """Report whether a model group honours Anthropic ``cache_control`` markers.

        Args:
            model_id: LiteLLM model group name.

        Returns:
            True for Claude models on any route.
        """
        return model_id.startswith("claude-")

    @staticmethod
    def wants_summarized_thinking(model_id: str) -> bool:
        """Report whether a model group must request its native thinking summarized.

        Args:
            model_id: LiteLLM model group name.

        Returns:
            True for Claude Opus 5 and Claude Fable 5, whose thinking otherwise arrives empty.
        """
        return re.match(r"^claude-(opus|fable)-5(?![0-9])", model_id) is not None

    def _raise_if_cancelled(self) -> None:
        """Refuse to start a request once the task is cancelled.

        Raises:
            RunCancelledException: If ``cancel_event`` is set; agno's run loop handles it cleanly,
                unlike ``asyncio.CancelledError``.
        """
        if self.cancel_event is not None and self.cancel_event.is_set():
            logger.warning("Model request refused after task cancellation (model=%s)", self.id)
            msg = "task cancelled before model request"
            raise RunCancelledException(msg)

    def _raise_on_refusal(self, response: Any) -> None:
        """Raise when a response or chunk ends on a provider refusal.

        Args:
            response: A litellm response or streaming chunk.

        Raises:
            OutputCheckError: If the first choice finished with ``content_filter``. agno does not
                retry it and ends the run on a ``RunErrorEvent``.
        """
        choices = response.choices
        if choices and choices[0].finish_reason == "content_filter":
            msg = (
                f"The model ({self.id}) declined to answer this request (provider content filter). "
                "Try rephrasing the request or switching to another model."
            )
            raise OutputCheckError(msg, check_trigger=CheckTrigger.OUTPUT_NOT_ALLOWED)

    def _parse_provider_response(self, response: Any, **kwargs: Any) -> ModelResponse:
        """Parse a completion, raising on a refusal and keeping the URL citations.

        Args:
            response: The litellm completion.
            **kwargs: Forwarded to agno's parser.

        Returns:
            The parsed response.
        """
        self._raise_on_refusal(response)
        model_response = super()._parse_provider_response(response, **kwargs)
        citations = self._url_citations(response.choices[0].message.get("annotations"))
        if citations is not None:
            model_response.citations = citations
        return model_response

    def _parse_provider_response_delta(self, response_delta: Any) -> ModelResponse:
        """Parse a streamed chunk, raising on a refusal and keeping the URL citations.

        Args:
            response_delta: One litellm streaming chunk.

        Returns:
            The parsed delta.
        """
        self._raise_on_refusal(response_delta)
        model_response = super()._parse_provider_response_delta(response_delta)
        if response_delta.choices:
            citations = self._url_citations(response_delta.choices[0].delta.get("annotations"))
            if citations is not None:
                model_response.citations = citations
        return model_response

    @staticmethod
    def _url_citations(annotations: Any) -> Citations | None:
        """Convert litellm's ``url_citation`` annotations into agno citations.

        Only absolute ``http``/``https`` URLs are kept: the front renders them as links and the
        annotations echo web content.

        Args:
            annotations: Message or delta ``annotations``, as dicts or pydantic objects.

        Returns:
            Citations with at least one URL, or None.
        """
        if not annotations:
            return None
        urls: list[UrlCitation] = []
        for annotation in annotations:
            raw = annotation.model_dump() if isinstance(annotation, BaseModel) else annotation
            url_citation = raw.get("url_citation") if isinstance(raw, dict) else None
            url = url_citation.get("url") if isinstance(url_citation, dict) else None
            if not isinstance(url, str):
                continue
            parts = urlsplit(url.strip())
            if parts.scheme.lower() in {"http", "https"} and parts.netloc:
                urls.append(UrlCitation(url=url, title=url_citation.get("title")))  # type: ignore[union-attr]
        return Citations(raw=list(annotations), urls=urls) if urls else None

    async def ainvoke(
        self,
        messages: list[Message],
        assistant_message: Message,
        response_format: dict | type[BaseModel] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        run_response: RunOutput | None = None,
        compress_tool_results: bool = False,  # ruff: ignore[boolean-type-hint-positional-argument, boolean-default-value-positional-argument]
    ) -> ModelResponse:
        """Send one completion request, recovering an empty reply.

        Args:
            messages: Agno messages to send.
            assistant_message: The message agno fills with the reply and metrics.
            response_format: Structured-output schema.
            tools: OpenAI-format tool definitions.
            tool_choice: Forwarded to agno.
            run_response: Forwarded to agno.
            compress_tool_results: Forwarded to agno.

        Returns:
            The parsed response; for an empty reply, the continuation retry's response, or the
            empty-response notice when that is empty too.
        """
        kwargs: dict[str, Any] = {
            "response_format": response_format,
            "tools": tools,
            "tool_choice": tool_choice,
            "run_response": run_response,
            "compress_tool_results": compress_tool_results,
        }
        first = await self._ainvoke_fitting(messages, assistant_message, kwargs)
        if not self._is_empty_reply(first.content, first.tool_calls):
            return first

        logger.warning("Empty model reply, retrying with a continuation prompt (model=%s)", self.id)
        sent = await self._compacted(messages, kwargs)
        retry = await self._ainvoke_once(self._with_continuation(sent), assistant_message, kwargs)
        if first.response_usage is not None:
            retry.response_usage = (
                first.response_usage if retry.response_usage is None else first.response_usage + retry.response_usage
            )
        if not self._is_empty_reply(retry.content, retry.tool_calls):
            return retry

        logger.error("Model reply still empty after continuation retry (model=%s)", self.id)
        if self._should_notify_empty(response_format):
            retry.content = self.empty_response_notice
        return retry

    async def ainvoke_stream(
        self,
        messages: list[Message],
        assistant_message: Message,
        response_format: dict | type[BaseModel] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        run_response: RunOutput | None = None,
        compress_tool_results: bool = False,  # ruff: ignore[boolean-type-hint-positional-argument, boolean-default-value-positional-argument]
    ) -> AsyncIterator[ModelResponse]:
        """Stream one completion request, recovering an empty stream.

        Args:
            messages: Agno messages to send.
            assistant_message: The message agno fills with the reply and metrics.
            response_format: Structured-output schema.
            tools: OpenAI-format tool definitions.
            tool_choice: Forwarded to agno.
            run_response: Forwarded to agno.
            compress_tool_results: Forwarded to agno.

        Yields:
            Streamed responses. An empty stream (nothing visible reached the client) is followed by
            the continuation retry's stream, then by the empty-response notice when that is empty too.
        """
        kwargs: dict[str, Any] = {
            "response_format": response_format,
            "tools": tools,
            "tool_choice": tool_choice,
            "run_response": run_response,
            "compress_tool_results": compress_tool_results,
        }
        for attempt in range(2):
            if attempt:
                logger.warning("Empty model reply, retrying with a continuation prompt (model=%s)", self.id)
                stream = self._ainvoke_stream_once(
                    self._with_continuation(await self._compacted(messages, kwargs)), assistant_message, kwargs
                )
            else:
                stream = self._ainvoke_stream_fitting(messages, assistant_message, kwargs)
            text: list[str] = []
            has_tool_calls = False
            async for chunk in stream:
                if isinstance(chunk.content, str):
                    text.append(chunk.content)
                has_tool_calls = has_tool_calls or bool(chunk.tool_calls)
                yield chunk
            if not self._is_empty_reply("".join(text), has_tool_calls):
                return

        logger.error("Model reply still empty after continuation retry (model=%s)", self.id)
        if self._should_notify_empty(response_format):
            yield ModelResponse(role="assistant", content=self.empty_response_notice)

    async def _compacted(self, messages: list[Message], kwargs: dict[str, Any]) -> list[Message]:
        """Return the request's messages with the replayed history compacted, when a compactor is set.

        Args:
            messages: Agno's messages for this call; never modified.
            kwargs: The remaining ``ainvoke`` arguments.

        Returns:
            The messages to send.
        """
        if self.compactor is None:
            return messages
        return await self.compactor.view(messages, kwargs.get("tools"), kwargs.get("run_response"))

    async def _refit_after_overflow(
        self, exc: BadRequestError, messages: list[Message], kwargs: dict[str, Any]
    ) -> list[Message] | None:
        """Fold the history after the provider rejected the request as too long.

        Through the proxy an overflow can arrive as a plain 400, so known provider wordings count too.

        Args:
            exc: The provider's 400.
            messages: Agno's messages for this call.
            kwargs: The remaining ``ainvoke`` arguments.

        Returns:
            The messages to resend, or None when the rejection is not an overflow or nothing folds.
        """
        if self.compactor is None:
            return None
        if not isinstance(exc, ContextWindowExceededError):
            text = str(exc).lower()
            markers = (
                "prompt is too long",
                "context window",
                "context_length_exceeded",
                "maximum context length",
                "input is too long",
                "too many tokens",
                "exceed context limit",
            )
            if not any(marker in text for marker in markers):
                return None
        logger.warning("Request exceeded the context window, compacting the history and retrying (model=%s)", self.id)
        return await self.compactor.force_fold(messages, kwargs.get("tools"), kwargs.get("run_response"))

    async def _ainvoke_fitting(
        self, messages: list[Message], assistant_message: Message, kwargs: dict[str, Any]
    ) -> ModelResponse:
        """Send the compacted request, folding and resending it once if it overflows.

        Args:
            messages: Agno's messages for this call.
            assistant_message: The message agno fills with the reply and metrics.
            kwargs: The remaining ``ainvoke`` arguments.

        Returns:
            The parsed response.

        Raises:
            BadRequestError: When the rejection is not an overflow, or folding cannot help.
        """
        sent = await self._compacted(messages, kwargs)
        try:
            response = await self._ainvoke_once(sent, assistant_message, kwargs)
        except BadRequestError as exc:
            refit = await self._refit_after_overflow(exc, messages, kwargs)
            if refit is None:
                raise
            sent = refit
            response = await self._ainvoke_once(sent, assistant_message, kwargs)
        await self._observe(sent, kwargs, response.response_usage)
        return response

    async def _ainvoke_stream_fitting(
        self, messages: list[Message], assistant_message: Message, kwargs: dict[str, Any]
    ) -> AsyncIterator[ModelResponse]:
        """Stream the compacted request, folding and resending it once if it overflows before output.

        Args:
            messages: Agno's messages for this call.
            assistant_message: The message agno fills with the reply and metrics.
            kwargs: The remaining ``ainvoke_stream`` arguments.

        Yields:
            Streamed responses.

        Raises:
            BadRequestError: When the rejection is not an overflow, output was already streamed, or
                folding cannot help.
        """
        sent = await self._compacted(messages, kwargs)
        usage: MessageMetrics | None = None
        streamed = False
        try:
            async for chunk in self._ainvoke_stream_once(sent, assistant_message, kwargs):
                streamed = True
                usage = chunk.response_usage or usage
                yield chunk
        except BadRequestError as exc:
            refit = None if streamed else await self._refit_after_overflow(exc, messages, kwargs)
            if refit is None:
                raise
            sent = refit
            async for chunk in self._ainvoke_stream_once(sent, assistant_message, kwargs):
                usage = chunk.response_usage or usage
                yield chunk
        await self._observe(sent, kwargs, usage)

    async def _observe(self, sent: list[Message], kwargs: dict[str, Any], usage: MessageMetrics | None) -> None:
        """Hand the provider's prompt size of a request to the compactor, which calibrates on it.

        Args:
            sent: The messages as sent.
            kwargs: The remaining ``ainvoke`` arguments.
            usage: The response usage; LiteLLM's ``prompt_tokens`` already counts cached tokens.
        """
        if self.compactor is not None and usage is not None and usage.input_tokens:
            await self.compactor.observe(sent, kwargs.get("tools"), usage.input_tokens)

    async def _ainvoke_once(
        self, messages: list[Message], assistant_message: Message, kwargs: dict[str, Any]
    ) -> ModelResponse:
        """Send one request, retrying it once without images if the provider rejects them.

        Args:
            messages: Agno messages to send.
            assistant_message: The message agno fills with the reply and metrics.
            kwargs: The remaining ``ainvoke`` arguments.

        Returns:
            The parsed response.

        Raises:
            BadRequestError: When the rejection is not the images' fault, or recurs without them.
            APIConnectionError: Likewise, for a proxy 500 surfaced as a connection error.
            InternalServerError: Likewise, for a proxy 500.
        """
        self._raise_if_cancelled()
        try:
            return await super().ainvoke(messages, assistant_message, **kwargs)
        except (BadRequestError, APIConnectionError, InternalServerError) as exc:
            if not self._should_retry_without_images(exc):
                raise
        self._raise_if_cancelled()
        return await super().ainvoke(messages, assistant_message, **kwargs)

    async def _ainvoke_stream_once(
        self, messages: list[Message], assistant_message: Message, kwargs: dict[str, Any]
    ) -> AsyncIterator[ModelResponse]:
        """Stream one request, retrying it once without images if the provider rejects them before output.

        Args:
            messages: Agno messages to send.
            assistant_message: The message agno fills with the reply and metrics.
            kwargs: The remaining ``ainvoke_stream`` arguments.

        Yields:
            Streamed responses.

        Raises:
            BadRequestError: When the rejection is not the images' fault, output was already
                streamed, or the request is rejected again without images.
            APIConnectionError: Likewise, for a proxy 500 surfaced as a connection error.
            InternalServerError: Likewise, for a proxy 500.
        """
        self._raise_if_cancelled()
        streamed = False
        try:
            async for chunk in super().ainvoke_stream(messages, assistant_message, **kwargs):
                streamed = True
                yield chunk
        except (BadRequestError, APIConnectionError, InternalServerError) as exc:
            if streamed or not self._should_retry_without_images(exc):
                raise
        else:
            return
        self._raise_if_cancelled()
        async for chunk in super().ainvoke_stream(messages, assistant_message, **kwargs):
            yield chunk

    def _should_retry_without_images(self, exc: Exception) -> bool:
        """Decide whether a rejected request is worth resending without images, latching if so.

        A provider that cannot fetch an image URL (through the proxy this can be a 500) costs only
        the URL images; any other image rejection costs them all.

        Args:
            exc: The provider's rejection.

        Returns:
            True when the request carried image parts and the error text blames an image;
            ``drop_image_urls`` or ``drop_images`` is then set.
        """
        image_parts, url_parts = self._request_image_parts.get()
        text = str(exc).lower()
        if not image_parts or ("image" not in text and "blocked address" not in text):
            return False
        unfetchable = url_parts and any(marker in text for marker in ("fetch", "blocked address", "download"))
        logger.warning(
            "Provider rejected the request's images, retrying without %s (images=%d, by url=%d, model=%s): %s",
            "the URL images" if unfetchable else "them",
            image_parts,
            url_parts,
            self.id,
            exc,
        )
        if unfetchable:
            self.drop_image_urls = True
        else:
            self.drop_images = True
        return True

    @staticmethod
    def _is_empty_reply(content: Any, tool_calls: Any) -> bool:
        """Report whether a reply carries neither text nor a tool call.

        Args:
            content: The reply content (reasoning excluded).
            tool_calls: The reply's tool calls, or a truthy flag for a stream.

        Returns:
            True when there is nothing to show and nothing to execute.
        """
        if tool_calls:
            return False
        if isinstance(content, str):
            return not content.strip()
        return content is None

    @staticmethod
    def _with_continuation(messages: list[Message]) -> list[Message]:
        """Return the request's messages followed by Anthropic's remedy for an empty ``end_turn``.

        Agno's own list, which the run persists, is left untouched.

        Args:
            messages: The messages of the empty request.

        Returns:
            A new list ending with a continuation user message.
        """
        return [*messages, Message(role="user", content="Please continue.")]

    def _should_notify_empty(self, response_format: dict | type[BaseModel] | None) -> bool:
        """Report whether an empty reply is replaced by the notice.

        Structured-output calls (session summaries) are excluded: the notice would fail parsing.

        Args:
            response_format: The request's structured-output schema.

        Returns:
            True when a notice is configured and the call expects free text.
        """
        return self.empty_response_notice is not None and response_format is None

    def get_request_params(
        self,
        tools: list[dict[str, Any]] | None = None,
        response_format: dict | type[BaseModel] | None = None,
    ) -> dict[str, Any]:
        """Build the litellm kwargs, minus the parameters this model rejects.

        Args:
            tools: OpenAI-format tool definitions.
            response_format: Structured-output schema, forwarded to agno.

        Returns:
            agno's kwargs without ``temperature`` when disabled, and with a cache marker on the last
            tool definition when caching is enabled.
        """
        params = super().get_request_params(tools=tools, response_format=response_format)
        if not self.send_temperature:
            params.pop("temperature", None)
        if self.enable_prompt_caching and tools:
            params["tools"] = [*tools[:-1], {**tools[-1], "cache_control": {"type": "ephemeral"}}]
        return params

    def _format_messages(
        self,
        messages: list[Message],
        compress_tool_results: bool = False,  # ruff: ignore[boolean-type-hint-positional-argument, boolean-default-value-positional-argument]
    ) -> list[dict[str, Any]]:
        """Format messages for LiteLLM, then apply the history, image, empty-content and size fixes.

        Args:
            messages: Agno messages; never modified.
            compress_tool_results: Forwarded to agno.

        Returns:
            Formatted messages: history opened by a note when it starts mid-exchange (agno's media
            follow-up answers a tool call, it is not a user turn), image parts replaced by a note once
            ``drop_images`` (or, for URL images, ``drop_image_urls``) is set, no empty content, tool
            results capped, the request trimmed to the input window, and cache breakpoints placed
            when caching is enabled.
        """
        start = next((index for index, msg in enumerate(messages) if msg.from_history), None)
        if start is not None and (
            messages[start].role != "user"
            or messages[start].content == "The tool call above generated the attached media."
        ):
            note = "[Earlier messages of this conversation were omitted; the history below starts mid-exchange.]"
            messages = [*messages[:start], Message(role="user", content=note), *messages[start:]]
        formatted = super()._format_messages(messages, compress_tool_results=compress_tool_results)
        sent = [part for msg in formatted for part in self._image_parts(msg) if not self._dropped(part)]
        self._request_image_parts.set((len(sent), sum(1 for part in sent if self._image_url(part) is not None)))

        bounded: list[dict[str, Any]] = []
        for msg in formatted:
            removed = sum(1 for part in self._image_parts(msg) if self._dropped(part))
            kept = (
                {
                    **msg,
                    "content": [
                        *(part for part in msg["content"] if not self._dropped(part)),
                        {
                            "type": "text",
                            "text": f"[{removed} image(s) removed from this message: "
                            "the model provider could not process them.]",
                        },
                    ],
                }
                if removed
                else msg
            )
            cleaned = self._strip_empty_content(kept)
            if cleaned is None:
                logger.info(
                    "Empty %s message dropped from the request (model=%s): %.100r", kept.get("role"), self.id, kept
                )
                continue
            content = cleaned.get("content")
            cap = self.max_tool_result_chars
            if cleaned.get("role") == "tool" and isinstance(content, str) and len(content) > cap:
                # Applies to history too, so a mission whose stored run holds an oversized result recovers.
                logger.warning(
                    "Tool result truncated to %d of %d characters (tool_call_id=%s, model=%s)",
                    cap,
                    len(content),
                    cleaned.get("tool_call_id"),
                    self.id,
                )
                cleaned = {
                    **cleaned,
                    "content": f"{content[:cap]}\n\n[Tool result truncated: the first {cap} of {len(content)} "
                    "characters are shown. Ask the tool for a narrower result instead of repeating the same call.]",
                }
            bounded.append(cleaned)

        # Kept above the compaction threshold so it only catches what a fold could not. Without an
        # input budget it relies on litellm's window for the model id: a no-op for an unknown one.
        # The window is read here: trim_messages' own lookup raises, and swallows it, on a map
        # entry without `max_tokens`, which leaves the prompt untrimmed.
        spec = litellm.model_cost.get(self.id, {})
        budget = self.input_tokens or spec.get("max_input_tokens") or spec.get("max_tokens")
        trimmed = cast(
            "list[dict[str, Any]]",
            trim_messages(bounded, self.id, max_tokens=int(budget * 0.95)) if budget else bounded,
        )
        if len(trimmed) < len(bounded):
            logger.warning(
                "Context window trim applied: %d -> %d messages (model=%s)", len(bounded), len(trimmed), self.id
            )
            # litellm returns copies, so the dropped (or shortened) messages are found by value.
            for message in bounded:
                if message not in trimmed:
                    logger.warning(
                        "Message trimmed or shortened in the request (model=%s): %s %.100r",
                        self.id,
                        message.get("role"),
                        message.get("content"),
                    )
        return self._mark_cache_breakpoints(trimmed) if self.enable_prompt_caching else trimmed

    @staticmethod
    def _image_parts(msg: dict[str, Any]) -> list[dict[str, Any]]:
        """The ``image_url`` parts of a formatted message.

        Args:
            msg: A formatted message.

        Returns:
            Its image parts; none for string content.
        """
        content = msg.get("content")
        if not isinstance(content, list):
            return []
        return [part for part in content if isinstance(part, dict) and part.get("type") == "image_url"]

    @staticmethod
    def _image_url(part: Any) -> str | None:
        """The remote URL an image part points to.

        Args:
            part: A content part.

        Returns:
            The URL, or None for an inline (``data:``) image or any other part.
        """
        if not isinstance(part, dict) or part.get("type") != "image_url":
            return None
        image = part.get("image_url")
        url = image.get("url") if isinstance(image, dict) else image
        return url if isinstance(url, str) and not url.startswith("data:") else None

    def _dropped(self, part: Any) -> bool:
        """Whether a content part is an image this instance no longer sends.

        Args:
            part: A content part.

        Returns:
            True for any image once ``drop_images`` is set, for a URL image once ``drop_image_urls`` is.
        """
        if not isinstance(part, dict) or part.get("type") != "image_url":
            return False
        return self.drop_images or (self.drop_image_urls and self._image_url(part) is not None)

    @staticmethod
    def _strip_empty_content(msg: dict[str, Any]) -> dict[str, Any] | None:
        """Strip empty content from a formatted message, or drop the message.

        Tool messages are kept as is (a tool result may be empty). Empty text blocks are removed from
        list content; an emptied list counts as empty string content, which is removed when the
        message carries tool calls and drops the message otherwise.

        Args:
            msg: A formatted message.

        Returns:
            The (possibly copied) message, or None if it must be dropped.
        """
        if msg.get("role") == "tool":
            return msg
        content = msg.get("content")
        if isinstance(content, list):
            blocks = [
                block
                for block in content
                if not (
                    isinstance(block, dict)
                    and block.get("type") == "text"
                    and (not isinstance(block.get("text"), str) or not block["text"].strip())
                )
            ]
            if blocks:
                return {**msg, "content": blocks} if len(blocks) != len(content) else msg
            content = ""
        if isinstance(content, str) and not content.strip():
            if msg.get("tool_calls"):
                return {key: value for key, value in msg.items() if key != "content"}
            return None
        return msg

    def _mark_cache_breakpoints(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Place a cache marker on the system prompt and on the newest user or tool message.

        With the last tool definition's marker, three of Anthropic's four breakpoints: the whole
        prefix up to the latest turn is served from cache.

        Args:
            messages: Formatted messages.

        Returns:
            A new list; marked messages are copies.
        """
        marked = list(messages)
        for index, msg in enumerate(marked):
            if msg.get("role") == "system":
                marked[index] = {**msg, "content": self._as_cached_blocks(msg.get("content"))}
                break
        if marked:
            last = marked[-1]
            if last.get("role") == "tool":
                marked[-1] = {**last, "cache_control": {"type": "ephemeral"}}
            elif last.get("role") == "user":
                marked[-1] = {**last, "content": self._as_cached_blocks(last.get("content"))}
        return marked

    @staticmethod
    def _as_cached_blocks(content: Any) -> list[dict[str, Any]]:
        """Turn message content into blocks whose last text block carries the cache marker.

        Args:
            content: A string or a list of OpenAI-style content parts.

        Returns:
            Content blocks.
        """
        blocks: list[dict[str, Any]] = (
            [{"type": "text", "text": content}] if isinstance(content, str) else [dict(b) for b in content or []]
        )
        for block in reversed(blocks):
            if block.get("type") == "text":
                block["cache_control"] = {"type": "ephemeral"}
                break
        return blocks
