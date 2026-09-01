"""EchoModule — a simple tool module that echoes transformed text.

Mirrors the template-tool pattern: ToolModule with TriggerHandler,
DataTrigger models, and ModuleServer with embedded gateway.
"""

from typing import Any, ClassVar

from models.input import EchoInput
from models.output import EchoOutput
from models.secret import EchoSecret
from models.setup import EchoSetup

from digitalkin.models.module import ModuleContext
from digitalkin.modules.tool_module import ToolModule
from digitalkin.utils.package_discover import ModuleDiscoverer


class EchoToolModule(ToolModule[EchoInput, EchoOutput, EchoSetup, EchoSecret]):
    """A tool module that echoes transformed text with streaming output."""

    name = "EchoToolModule"
    description = "Echoes input text with optional transforms (uppercase, prefix, reverse, repeat)."

    input_format = EchoInput
    output_format = EchoOutput
    setup_format = EchoSetup
    secret_format = EchoSecret

    metadata: ClassVar[dict[str, str | list[str]]] = {
        "name": "EchoToolModule",
        "description": "Echoes input text with transforms.",
        "version": "1.0.0",
        "tags": ["echo", "tool", "demo"],
    }

    services_config_strategies: ClassVar[dict[str, Any]] = {}
    services_config_params: ClassVar[dict[str, Any]] = {
        "storage": {"config": {}},
        "cost": {"config": {}},
    }

    triggers_discoverer = ModuleDiscoverer(packages=["triggers"])

    async def initialize(self, context: ModuleContext, setup_data: EchoSetup) -> None:
        """Initialize module.

        Args:
            context: The module context.
            setup_data: The setup configuration.
        """

    async def cleanup(self) -> None:
        """Clean up resources."""
