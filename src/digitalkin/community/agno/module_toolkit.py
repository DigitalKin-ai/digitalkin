"""Agno Toolkit wrapper for SDK module tools.

Wraps a :class:`ToolModuleInfo` into Agno-compatible tool functions that call
the remote tool module via gRPC and parse the SDK's in-band sentinel protocol
(``{"root": {"protocol": ...}}`` frames, ``stream.error`` carrying
``code``/``message``).

Requires the optional ``agno`` dependency (``pip install digitalkin[agno]``).
"""

import asyncio
import json
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

from ag_ui.core.events import CustomEvent as AgUiCustomEvent
from agno.media import Image
from agno.tools.function import Function, ToolResult

from digitalkin.community.agno.models import ToolCallMetadata, ToolOutputMetadata
from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.logger import logger
from digitalkin.models.module import ModuleContext
from digitalkin.models.module.ag_ui import AgUiCustomEventOutput, AgUiOutput
from digitalkin.models.module.module_context import STREAM_SENTINEL_PROTOCOLS
from digitalkin.models.module.tool_cache import ToolDefinition, ToolModuleInfo

# Default timeout for tool calls in seconds
DEFAULT_TOOL_TIMEOUT_SECONDS = 300

# Protocol used by tool modules that return OpenAI-style multimodal content
# (a list of {"type": "text"} / {"type": "image_url"} parts).
TOOL_CONTENT_PROTOCOL = "tool_content"

# Protocol of an AG-UI custom event. A called tool streams its events on its own
# job, which the frontend never reads; we relay these onto the agent's stream.
AGUI_CUSTOM_PROTOCOL = "agui_custom"


class ModuleToolkit(DkToolkit):
    """Agno Toolkit wrapper for SDK module tools.

    Wraps a ToolModuleInfo containing multiple ToolDefinitions into
    Agno-compatible tool functions with:
    - Parameter-based docstring generation for LLM understanding
    - Cost metadata exposed in responses for LLM context
    - Structured JSON responses with metadata

    Each ToolDefinition in the ToolModuleInfo becomes a separate tool
    in this toolkit. The toolkit name is derived from the module name.

    Note:
        Cost metadata is exposed in tool responses and logged via events,
        but NOT actively tracked via CostStrategy.add(). The LLM can use
        the cost_budget field in tool inputs to specify cost constraints,
        and the tool itself will enforce limits before executing.

    Attributes:
        context: ModuleContext providing SDK access.
        tool_module_info: The SDK ToolModuleInfo being wrapped.

    Example:
        tool_module_info = context.tool_cache.entries.get("my_tool")
        toolkit = ModuleToolkit(
            context=context,
            tool_module_info=tool_module_info,
        )
        agent = Agent(tools=[toolkit])
    """

    def __init__(
        self,
        context: ModuleContext,
        tool_module_info: ToolModuleInfo,
        timeout_seconds: float = DEFAULT_TOOL_TIMEOUT_SECONDS,
        allowed_tools: set[str] | None = None,
    ) -> None:
        """Initialize the ModuleToolkit for a ToolModuleInfo.

        Args:
            context: ModuleContext providing create_tool_functions.
            tool_module_info: The SDK ToolModuleInfo with tools list.
            timeout_seconds: Timeout for tool calls in seconds. Default 300s.
            allowed_tools: If provided, only include tools whose name is in this set.
                When None, all tools from the module are included (backwards-compatible).
        """
        self._context = context
        self._tool_module_info = tool_module_info
        self._timeout = timeout_seconds

        tool_functions = context.create_tool_functions(tool_module_info.setup_id)

        if allowed_tools is not None:
            tool_functions = [(td, fn) for td, fn in tool_functions if td.name in allowed_tools]

        # Function objects with explicit JSON schema + skip_entrypoint_processing=True
        # bypass Agno's inspect.signature() introspection, which sees **kwargs: Any and
        # generates {kwargs: object} — causing the LLM to miss required parameters.
        agno_functions: list[Function] = []
        for tool_def, tool_fn in tool_functions:
            wrapper = self._create_tool_wrapper(tool_def, tool_fn)

            agno_functions.append(
                Function(
                    name=wrapper.__name__,
                    description=tool_def.description or f"Execute the {tool_def.name} tool.",
                    parameters=tool_def.parameters_schema,
                    entrypoint=wrapper,
                    skip_entrypoint_processing=True,
                )
            )

        if not agno_functions:
            sdk_tool_names = sorted(t.name for t in tool_module_info.tools)
            if not tool_module_info.tools:
                reason = "sdk_returned_zero_tools"
            elif not tool_functions:
                reason = "create_tool_functions_returned_empty"
            else:
                reason = "wrapper_pipeline_dropped_all"
            logger.warning(
                "ModuleToolkit empty: setup_id='%s' slug='%s' reason=%s "
                "sdk_tools_count=%d sdk_tool_names=%s fn_count=%d",
                tool_module_info.setup_id,
                tool_module_info.slug,
                reason,
                len(tool_module_info.tools),
                sdk_tool_names,
                len(tool_functions),
            )

        logger.debug(
            "ModuleToolkit built: slug=%s setup_id=%s tools=%d/%d",
            tool_module_info.slug,
            tool_module_info.setup_id,
            len(agno_functions),
            len(tool_module_info.tools),
        )

        toolkit_name = (
            tool_module_info.tool_name
            or tool_module_info.module_name
            or tool_module_info.slug.replace(":", "_").replace(".", "_")
        )
        super().__init__(name=f"{toolkit_name}_toolkit", tools=agno_functions, context=self._context)

    @property
    def module_id(self) -> str:
        """The SDK module ID being wrapped."""
        return self._tool_module_info.module_id

    @property
    def tool_module_info(self) -> ToolModuleInfo:
        """The ToolModuleInfo being wrapped."""
        return self._tool_module_info

    @staticmethod
    def _has_cost_metadata(tool_metadata: ToolOutputMetadata | None) -> bool:
        """Check if tool metadata contains cost information.

        Returns:
            True when the tool reported a cost estimate or API-call count.
        """
        if not tool_metadata:
            return False
        return tool_metadata.cost_estimate_usd is not None or tool_metadata.api_calls_made > 0

    @staticmethod
    def _extract_images(output: dict[str, Any] | str) -> tuple[dict[str, Any] | str, list[str]]:
        """Split a multimodal `tool_content` output into text payload and image URLs.

        A tool that returns images (e.g. screenshots) emits OpenAI-style content
        parts. Serialized into the tool message they would reach the model as a
        JSON string containing a URL — the model would see text, never an image.
        Lifting them out lets the caller hand them to Agno as `ToolResult.images`,
        which Agno re-attaches as a follow-up user message the model can see.

        Args:
            output: The tool's `output` payload (`{"root": {...}}`) or a raw string.

        Returns:
            A tuple of (payload with image parts removed, image URLs in order).
            The payload is returned unchanged when there is nothing to extract.
        """
        if not isinstance(output, dict):
            return output, []

        root = output.get("root")
        if not isinstance(root, dict) or root.get("protocol") != TOOL_CONTENT_PROTOCOL:
            return output, []

        content = root.get("content")
        if not isinstance(content, list):
            return output, []

        image_urls: list[str] = []
        remaining: list[Any] = []
        for part in content:
            image_url = part.get("image_url") if isinstance(part, dict) and part.get("type") == "image_url" else None
            url = image_url.get("url") if isinstance(image_url, dict) else None
            if url:
                image_urls.append(url)
            else:
                remaining.append(part)

        if not image_urls:
            return output, []

        return {**output, "root": {**root, "content": remaining}}, image_urls

    @staticmethod
    async def _relay_custom_event(context: ModuleContext, response: dict[str, Any]) -> None:
        """Relay a tool's AG-UI custom event onto the agent's own output stream.

        A called tool streams its AG-UI events on its own gRPC job. The frontend only
        consumes the agent's job stream, and nothing splices the two, so those events
        would be dropped. Custom events carry application payloads the UI needs (e.g.
        the virtual desktop's live-view URL), so we forward them here.

        Only `agui_custom` is relayed: forwarding the tool's run/text lifecycle events
        would nest a second run inside the agent's own, which the frontend already tracks.

        Relaying is best-effort — a failure here must never fail the tool call.

        Args:
            context: The agent's module context, whose callbacks feed the frontend stream.
            response: One streamed message from the tool, as yielded by `call_module`.
        """
        root = response.get("root", {})
        if not isinstance(root, dict) or root.get("protocol") != AGUI_CUSTOM_PROTOCOL:
            return

        event = root.get("event")
        if not isinstance(event, dict) or not event.get("name"):
            return

        # callbacks is a dict-driven SimpleNamespace (module_context.py:168);
        # send_message may legitimately be absent outside a running job.
        send_message = vars(context.callbacks).get("send_message")
        if send_message is None:
            return

        try:
            # Rebuilt from name/value rather than model_validate: the dict comes from
            # json_format.MessageToDict, so its keys are camelCase and its Struct numbers
            # are floats (a `value` of {"width": 1024} arrives as 1024.0).
            await send_message(
                AgUiOutput(
                    root=AgUiCustomEventOutput(
                        event=AgUiCustomEvent(name=event["name"], value=event.get("value")),
                    )
                )
            )
        except Exception:
            logger.exception("Failed to relay custom event '%s' to the agent stream", event["name"])

    def _handle_success(
        self,
        tool_name: str,
        output: dict[str, Any] | str,
        duration_ms: float,
        input_kwargs: dict[str, Any],
    ) -> str | ToolResult:
        """Handle successful tool execution.

        Returns:
            A JSON string with output and ToolCallMetadata (incl. cost), or a
            ToolResult carrying that JSON plus any images the tool returned.
        """
        tool_metadata = ToolCallMetadata.extract_tool_metadata(output) if isinstance(output, dict) else None

        metadata = ToolCallMetadata(
            module_id=self.module_id,
            success=True,
            duration_ms=duration_ms,
            cost_tracked=ModuleToolkit._has_cost_metadata(tool_metadata),
            input_kwargs=input_kwargs,
            tool_metadata=tool_metadata,
        )

        payload, image_urls = ModuleToolkit._extract_images(output)

        cost_info = ""
        if tool_metadata and tool_metadata.cost_estimate_usd is not None:
            cost_info = f", cost=${tool_metadata.cost_estimate_usd:.4f}"
        logger.info(
            "Tool '%s' completed in %.2fms (success=True%s, images=%d) args=%s setup_id=%s task_id=%s",
            tool_name,
            duration_ms,
            cost_info,
            len(image_urls),
            sorted(input_kwargs),
            self._tool_module_info.setup_id,
            self._context.session.job_id,
        )

        body = json.dumps({"output": payload, "metadata": metadata.to_success_dict()}, indent=2)
        if not image_urls:
            return body
        return ToolResult(content=body, images=[Image(url=url) for url in image_urls])

    def _handle_failure(
        self,
        tool_name: str,
        error_msg: str,
        duration_ms: float,
        input_kwargs: dict[str, Any],
    ) -> str:
        """Handle failed tool execution.

        Returns:
            JSON string with error and ToolCallMetadata.
        """
        metadata = ToolCallMetadata(
            module_id=self.module_id,
            success=False,
            duration_ms=duration_ms,
            error=error_msg,
            input_kwargs=input_kwargs,
        )
        logger.warning(
            "Tool '%s' failed in %.2fms: %s args=%s setup_id=%s task_id=%s",
            tool_name,
            duration_ms,
            error_msg,
            sorted(input_kwargs),
            self._tool_module_info.setup_id,
            self._context.session.job_id,
        )
        return json.dumps({"error": error_msg, "metadata": metadata.to_error_dict()}, indent=2)

    @staticmethod
    def _find_successful_response(results: list[dict[str, Any]]) -> dict[str, Any] | None:
        """Find the last domain output from the streamed SDK responses.

        Each response is the ``MessageToDict`` of a payload Struct, shape
        ``{"root": {"protocol": "...", ...}, "annotations": {...}}``. A
        "successful" response is the most recent one whose ``root.protocol``
        is *not* a lifecycle/error sentinel. A fatal ``stream.error`` anywhere in
        the stream (e.g. a tool that streamed progress lines and then died, or a
        gateway idle timeout ending the stream mid-call) means the tool did not
        finish, regardless of what streamed before or after it. A non-fatal
        ``stream.error`` (the target module kept the stream open) does not fail
        the call on its own.

        Returns:
            The matching dict, or None if any response was a fatal stream.error
            or every response was a sentinel.
        """
        for resp in results:
            root = resp.get("root")
            if isinstance(root, dict) and root.get("protocol") == "stream.error" and root.get("fatal"):
                return None

        for resp in reversed(results):
            root = resp.get("root")
            if not isinstance(root, dict):
                continue
            protocol = root.get("protocol", "")
            if protocol in STREAM_SENTINEL_PROTOCOLS:
                continue
            return resp
        return None

    @staticmethod
    def _extract_error_message(results: list[dict[str, Any]]) -> str:
        """Extract an error message from streamed SDK responses.

        Errors surface in-band as ``root.protocol == "stream.error"`` with
        ``code`` and ``message`` fields (per the SDK's sentinel protocol).
        Domain modules may also embed their own ``error`` field on a domain
        output.

        Returns:
            The most informative error string, or a default if none found.
        """
        default_error = "No successful response received from module"
        if not results:
            return default_error

        for resp in reversed(results):
            root = resp.get("root")
            if not isinstance(root, dict):
                continue
            if root.get("protocol") != "stream.error":
                continue
            code = root.get("code", "")
            message = root.get("message", "") or default_error
            return f"[{code}] {message}" if code else str(message)

        for resp in reversed(results):
            root = resp.get("root")
            if isinstance(root, dict) and root.get("error"):
                return str(root["error"])
            if isinstance(resp.get("error"), str):
                return str(resp["error"])

        return default_error

    @staticmethod
    def _unwrap_kwargs(
        kwargs: dict[str, Any],
        tool_name: str,
        expected_params: set[str],
    ) -> dict[str, Any]:
        """Unwrap kwargs that Agno or LLMs may have incorrectly nested.

        Handles two patterns:
        - Agno wrapping all params under a 'kwargs' key
        - LLMs wrapping params under the tool name key

        Args:
            kwargs: The raw keyword arguments from the tool call.
            tool_name: Name of the tool being called.
            expected_params: Set of expected parameter names for this tool.

        Returns:
            The unwrapped kwargs dict ready for the SDK call.
        """
        if "kwargs" in kwargs and isinstance(kwargs["kwargs"], dict) and len(kwargs) == 1:
            logger.warning("Unwrapping Agno 'kwargs' wrapper: %s", list(kwargs["kwargs"].keys()))
            kwargs = kwargs["kwargs"]

        if tool_name in kwargs and isinstance(kwargs[tool_name], dict):
            nested = kwargs[tool_name]
            if any(key in expected_params for key in nested):
                logger.warning(
                    "Unwrapping nested parameters from '%s' key: %s",
                    tool_name,
                    list[Any](nested.keys()),
                )
                kwargs = {k: v for k, v in kwargs.items() if k != tool_name}
                kwargs.update(nested)

        return kwargs

    def _create_tool_wrapper(
        self,
        tool_def: ToolDefinition,
        fn: Callable[..., AsyncGenerator[dict[str, Any], None]],
    ) -> Callable[..., Awaitable[str | ToolResult]]:
        """Create an async wrapper function for an SDK tool.

        Wraps the SDK module's async generator function into an async function
        that Agno can consume, with proper error handling and response formatting.

        Returns:
            Async function that calls the SDK tool and returns a JSON result, or a
            ToolResult when the tool returned images alongside its text output.
        """
        tool_name = tool_def.name
        expected_params = tool_def.parameter_names
        timeout = self._timeout
        handle_success = self._handle_success
        handle_failure = self._handle_failure
        # The agent's context: relayed events land on the stream the frontend reads.
        context = self._context
        # Capture correlation IDs + slug once from self (fixed for this toolkit's
        # lifetime) so the closure never reaches into private state at call time.
        task_id = self._context.session.job_id
        setup_id = self._tool_module_info.setup_id
        tag = f"tool.call[{self._tool_module_info.slug}/{tool_name}]"

        async def wrapper(**kwargs: Any) -> str | ToolResult:
            if context.session.cancelled:
                logger.warning("Tool call refused after task cancellation: tool=%s task_id=%s", tool_name, task_id)
                msg = f"task cancelled before tool '{tool_name}'"
                raise asyncio.CancelledError(msg)

            start_time = time.perf_counter()
            call_timer = StepTimer()
            outcome = "ok"

            kwargs = ModuleToolkit._unwrap_kwargs(kwargs, tool_name, expected_params)

            logger.debug(
                "Calling tool '%s' with kwargs: %s setup_id=%s task_id=%s",
                tool_name,
                list(kwargs.keys()),
                setup_id,
                task_id,
            )

            # Each yielded dict is the MessageToDict of one output Struct,
            # shape {"root": {"protocol": ...}}. The iterator terminates when
            # the remote module is done — drain it without an early break.
            async def consume_generator() -> list[dict[str, Any]]:
                results: list[dict[str, Any]] = []
                async for response in fn(**kwargs):
                    await ModuleToolkit._relay_custom_event(context, response)
                    results.append(response)
                return results

            try:
                results = await asyncio.wait_for(
                    consume_generator(),
                    timeout=timeout,
                )
            except TimeoutError:
                outcome = "timeout"
                duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
                error_msg = f"Tool '{tool_name}' timed out after {timeout}s"
                logger.warning("%s task_id=%s", error_msg, task_id)
                return handle_failure(tool_name, error_msg, duration_ms, kwargs)

            except Exception as e:
                outcome = "error"
                duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
                error_msg = f"Failed to call tool '{tool_name}': {e!s}"
                logger.warning("%s task_id=%s", error_msg, task_id, exc_info=True)
                return handle_failure(tool_name, error_msg, duration_ms, kwargs)

            else:
                call_timer.mark("gen_consume")
                duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

                successful_resp = ModuleToolkit._find_successful_response(results)
                if successful_resp:
                    return handle_success(tool_name, successful_resp, duration_ms, kwargs)

                outcome = "no_success"
                error_msg = ModuleToolkit._extract_error_message(results)
                return handle_failure(tool_name, error_msg, duration_ms, kwargs)

            finally:
                call_timer.mark("respond")
                call_timer.log(f"{tag} outcome={outcome}", task_id=task_id)

        wrapper.__name__ = self._tool_module_info.slug + "__" + tool_name
        return wrapper
