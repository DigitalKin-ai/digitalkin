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

        sdk_tool_names = sorted(t.name for t in tool_module_info.tools)
        logger.info(
            "Creating ModuleToolkit: setup_id='%s' slug='%s' module_name='%s' sdk_tools_count=%d sdk_tool_names=%s",
            tool_module_info.setup_id,
            tool_module_info.slug,
            tool_module_info.module_name,
            len(tool_module_info.tools),
            sdk_tool_names,
        )

        tool_functions = context.create_tool_functions(tool_module_info.setup_id)

        if allowed_tools is not None:
            tool_functions = [(td, fn) for td, fn in tool_functions if td.name in allowed_tools]

        fn_names = sorted(td.name for td, _ in tool_functions)
        logger.info(
            "Built tool_functions: setup_id='%s' slug='%s' fn_count=%d fn_names=%s",
            tool_module_info.setup_id,
            tool_module_info.slug,
            len(tool_functions),
            fn_names,
        )

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
            logger.info(
                "[lat-audit] tool_wrapped: setup_id='%s' fn_name='%s' param_count=%d param_names=%s desc_chars=%d",
                tool_module_info.setup_id,
                wrapper.__name__,
                tool_def.parameter_count,
                sorted(tool_def.parameter_names),
                len(tool_def.description or ""),
            )

        if not agno_functions:
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

        logger.info(
            "[lat-audit] toolkit_built: setup_id='%s' slug='%s' sdk_tools=%d wrapped=%d empty=%s",
            tool_module_info.setup_id,
            tool_module_info.slug,
            len(tool_module_info.tools),
            len(agno_functions),
            not agno_functions,
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
            tool_metadata=tool_metadata,
        )

        payload, image_urls = ModuleToolkit._extract_images(output)

        cost_info = ""
        if tool_metadata and tool_metadata.cost_estimate_usd is not None:
            cost_info = f", cost=${tool_metadata.cost_estimate_usd:.4f}"
        logger.info(
            "Tool '%s' completed in %.2fms (success=True%s, images=%d) setup_id=%s task_id=%s",
            tool_name,
            duration_ms,
            cost_info,
            len(image_urls),
            self._tool_module_info.setup_id,
            self._context.session.job_id,
        )

        body = json.dumps({"output": payload, "metadata": metadata.to_success_dict()})
        if not image_urls:
            return body
        return ToolResult(content=body, images=[Image(url=url) for url in image_urls])

    def _handle_failure(
        self,
        tool_name: str,
        error_msg: str,
        duration_ms: float,
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
        )
        logger.warning(
            "Tool '%s' failed in %.2fms: %s setup_id=%s task_id=%s",
            tool_name,
            duration_ms,
            error_msg,
            self._tool_module_info.setup_id,
            self._context.session.job_id,
        )
        return json.dumps({"error": error_msg, "metadata": metadata.to_error_dict()})

    @staticmethod
    def _extract_error_message(frame: dict[str, Any] | None) -> str:
        """Extract an error message from the last error frame of a tool call.

        Errors surface in-band as ``stream.error`` (``code``/``message``) or
        ``stream.cancelled`` (``reason``); a frame may also carry its own ``error`` field.

        Args:
            frame: The last error frame kept while draining the tool stream.

        Returns:
            The error string, or a default if the frame carries none.
        """
        default_error = "No successful response received from module"
        if frame is None:
            return default_error
        root = frame.get("root")
        if isinstance(root, dict):
            protocol = root.get("protocol")
            if protocol == "stream.cancelled":
                return f"[CANCELLED] {root.get('reason') or 'cancelled'}"
            if protocol == "stream.error":
                code = root.get("code", "")
                message = root.get("message", "") or default_error
                return f"[{code}] {message}" if code else str(message)
            if root.get("error"):
                return str(root["error"])
        if isinstance(frame.get("error"), str):
            return str(frame["error"])
        return default_error

    @staticmethod
    async def _drain(
        context: ModuleContext,
        stream: AsyncGenerator[dict[str, Any], None],
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, int]:
        """Drain a tool stream, keeping only the last domain frame and the last error frame.

        Each frame is the ``MessageToDict`` of one output Struct (``{"root": {"protocol": ...}}``).
        Custom events are relayed as they arrive. A ``stream.error``/``stream.cancelled``
        outranks a bare ``error`` field.

        Args:
            context: The agent's module context, for relaying custom events.
            stream: The tool's output stream.

        Returns:
            ``(last_success, last_error, frame_count)``.
        """
        last_success: dict[str, Any] | None = None
        last_error: dict[str, Any] | None = None
        sentinel_error_seen = False
        frames = 0
        async for response in stream:
            frames += 1
            await ModuleToolkit._relay_custom_event(context, response)
            root = response.get("root")
            protocol = root.get("protocol", "") if isinstance(root, dict) else ""
            if protocol in {"stream.error", "stream.cancelled"}:
                last_error = response
                sentinel_error_seen = True
            elif isinstance(root, dict) and protocol not in {"stream.start", "stream.end", "stream.init"}:
                last_success = response
            elif not sentinel_error_seen and (
                isinstance(response.get("error"), str) or (isinstance(root, dict) and root.get("error"))
            ):
                last_error = response
        return last_success, last_error, frames

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
            start_time = time.perf_counter()
            call_timer = StepTimer()
            outcome = "ok"

            kwargs = ModuleToolkit._unwrap_kwargs(kwargs, tool_name, expected_params)

            logger.info(
                "Calling tool '%s' with kwargs: %s setup_id=%s task_id=%s",
                tool_name,
                list(kwargs.keys()),
                setup_id,
                task_id,
            )

            try:
                successful_resp, error_frame, frames = await asyncio.wait_for(
                    ModuleToolkit._drain(context, fn(**kwargs)),
                    timeout=timeout,
                )
            except TimeoutError:
                outcome = "timeout"
                duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
                error_msg = f"Tool '{tool_name}' timed out after {timeout}s"
                logger.warning("%s task_id=%s", error_msg, task_id)
                return handle_failure(tool_name, error_msg, duration_ms)

            except Exception as e:
                outcome = "error"
                duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
                error_msg = f"Failed to call tool '{tool_name}': {e!s}"
                logger.warning("%s task_id=%s", error_msg, task_id, exc_info=True)
                return handle_failure(tool_name, error_msg, duration_ms)

            else:
                call_timer.mark("gen_consume")
                duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
                # TODO(validate): TOOL-LAST-FRAME tool calls buffer only the last success/error frame
                logger.info(
                    "[VALIDATE TOOL-LAST-FRAME] tool '%s' drained %d frames, kept success=%s error=%s "
                    "(input_kwargs not echoed)",
                    tool_name,
                    frames,
                    successful_resp is not None,
                    error_frame is not None,
                    extra={"task_id": task_id, "setup_id": setup_id},
                )
                if successful_resp:
                    return handle_success(tool_name, successful_resp, duration_ms)

                outcome = "no_success"
                error_msg = ModuleToolkit._extract_error_message(error_frame)
                return handle_failure(tool_name, error_msg, duration_ms)

            finally:
                call_timer.mark("respond")
                call_timer.log(f"{tag} outcome={outcome}", task_id=task_id)

        wrapper.__name__ = self._tool_module_info.slug + "__" + tool_name
        return wrapper
