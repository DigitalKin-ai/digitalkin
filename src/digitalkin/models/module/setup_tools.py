"""Functions a tool setup adds on top of its module's triggers.

A tool module's triggers are the same for every setup. Some modules reach a
different backend per setup (an MCP server, an OpenAPI spec) and learn its
operations when the setup is saved. Such a module stores them in its setup
content under :data:`SETUP_TOOLS_KEY`; when an agent resolves the setup, the
SDK reads them from the registry's copy of that content and offers each one as
a function of that setup only.

Calling one sends ``protocol=<name>`` and the arguments to the module, like a
trigger call; the module routes the protocol (e.g. to a generic ``call_tool``).
"""

import re
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from digitalkin.logger import logger
from digitalkin.utils.llm_ready_schema import LlmReadySchema

#: Setup content key holding the list of :class:`SetupTool`.
SETUP_TOOLS_KEY = "setup_tools"

#: Longest function name LLM providers accept (OpenAI's limit, the strictest).
MAX_FUNCTION_NAME_LENGTH = 64

_FUNCTION_NAME = re.compile(r"^[A-Za-z0-9_-]+$")


class SetupTool(BaseModel):
    """One function a setup offers, stored by the module in its setup content.

    Attributes:
        name: Protocol sent to the module, and suffix of the function name the LLM sees.
        title: Human label of the function.
        description: What the function does, for the LLM.
        input_schema: JSON Schema of the arguments (an object schema).
        trigger: Module trigger that serves the call (e.g. ``call_tool``). Disabling it in the
            tool selection also drops this function.
    """

    name: str = Field(min_length=1)
    title: str | None = None
    description: str = ""
    input_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object", "properties": {}})
    trigger: str = Field(min_length=1)


def parameters_schema(input_schema: dict[str, Any]) -> dict[str, Any]:
    """Turn a setup tool's input schema into the parameters schema of an LLM function.

    Args:
        input_schema: JSON Schema of the arguments, possibly with ``$defs`` / ``$ref``.

    Returns:
        An object schema with only ``type``, ``properties`` and ``required``, refs inlined.
    """
    inlined = LlmReadySchema.inline_refs(input_schema) if "$defs" in input_schema else input_schema
    properties = inlined.get("properties") or {}
    required = [name for name in inlined.get("required") or [] if name in properties]
    return {"type": "object", "properties": dict(properties), "required": required}


def read_setup_tools(config: dict[str, Any] | None, *, prefix: str, reserved: set[str]) -> list[SetupTool]:
    """Read the usable setup tools from a setup's content.

    An entry is skipped, with a warning, when it is malformed, when its name clashes
    with a module trigger or an earlier entry, when its trigger is not one of the
    module's, or when ``<prefix>__<name>`` is not a valid LLM function name: one bad
    entry must not break every call of the agent.

    Args:
        config: The setup content (``SetupInfo.config``).
        prefix: Function name prefix of the setup (``ToolModuleInfo.slug``).
        reserved: The module's trigger names: taken as names, and the only valid serving triggers.

    Returns:
        The setup tools to offer, in their stored order.
    """
    raw = (config or {}).get(SETUP_TOOLS_KEY)
    if not raw:
        return []
    if not isinstance(raw, list):
        logger.warning("Setup tools of '%s' ignored: '%s' is not a list", prefix, SETUP_TOOLS_KEY)
        return []

    tools: list[SetupTool] = []
    taken = set(reserved)
    for entry in raw:
        try:
            tool = SetupTool.model_validate(entry)
        except ValidationError as error:
            logger.warning("Setup tool of '%s' ignored: invalid entry (%s)", prefix, error.errors()[0].get("msg"))
            continue
        function_name = f"{prefix}__{tool.name}"
        if not _FUNCTION_NAME.match(tool.name) or len(function_name) > MAX_FUNCTION_NAME_LENGTH:
            logger.warning("Setup tool '%s' ignored: '%s' is not a valid function name", tool.name, function_name)
            continue
        if tool.name in taken:
            logger.warning(
                "Setup tool '%s' of '%s' ignored: the name is already used by the module or the setup",
                tool.name,
                prefix,
            )
            continue
        if tool.trigger not in reserved:
            logger.warning(
                "Setup tool '%s' of '%s' ignored: the module has no trigger '%s'", tool.name, prefix, tool.trigger
            )
            continue
        taken.add(tool.name)
        tools.append(tool)
    return tools
