"""Definition of the Trigger type."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from typing import Any, ClassVar, Generic

from digitalkin.models.module.module_types import InputModelT, OutputModelT, SetupModelT
from digitalkin.modules._base_module import ModuleContext


class TriggerHandler(ABC, Generic[InputModelT, SetupModelT, OutputModelT]):
    """Base class for all input-trigger handlers.

    Each handler declares:
      - protocol_key: the Literal value this handler processes
      - handle(): logic to process the validated payload
    """

    protocol: ClassVar[str]
    input_format: type[InputModelT]
    output_format: type[OutputModelT]

    def __init__(self, context: ModuleContext) -> None:
        """Initialize the TriggerHandler with the given context."""

    @abstractmethod
    async def handle(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
        callback: Callable[[Any], Coroutine[Any, Any, None]],
        context: ModuleContext,
    ) -> None:
        """Asynchronously processes the input data specific to Handler and streams results via the provided callback.

        Args:
            input_data (InputModelT): The input data to be processed by the handler.
            setup_data (SetupModelT): The setup or configuration data required for processing.
            callback (Callable[[Any], Coroutine[Any, Any, None]]): callback that stream results.
            context (ModuleContext): The context object containing module-specific information and resources.

        Returns:
            Any: The result of the processing, if applicable.

        Note:
            The callback must be awaited to ensure results are streamed correctly during processing.
        """
