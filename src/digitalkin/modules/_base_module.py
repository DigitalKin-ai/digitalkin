"""BaseModule is the abstract base for all modules in the DigitalKin SDK."""

import asyncio
import contextlib
import json
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, ClassVar, Generic, TypeVar

from pydantic import BaseModel

from digitalkin.logger import logger
from digitalkin.models.module import ModuleStatus
from digitalkin.services.service_provider import ServiceProvider
from digitalkin.services.storage.storage_strategy import StorageStrategy

InputModelT = TypeVar("InputModelT", bound=BaseModel)
OutputModelT = TypeVar("OutputModelT", bound=BaseModel)
SetupModelT = TypeVar("SetupModelT", bound=BaseModel)


class BaseModule(ABC, Generic[InputModelT, OutputModelT, SetupModelT]):
    """BaseModule is the abstract base for all modules in the DigitalKin SDK."""

    input_format: type[InputModelT]
    output_format: type[OutputModelT]
    setup_format: type[SetupModelT]
    metadata: ClassVar[dict[str, Any]]

    local_services: type[ServiceProvider]
    dev_services: type[ServiceProvider]

    storage: StorageStrategy

    def __init__(
        self,
        job_id: str,
        name: str | None = None,
    ) -> None:
        """Initialize the module."""
        self.job_id: str = job_id
        self.name = name or self.__class__.__name__
        self._status = ModuleStatus.CREATED
        self._task: asyncio.Task | None = None

    @property
    def status(self) -> ModuleStatus:
        """Get the module status.

        Returns:
            The module status
        """
        return self._status

    @classmethod
    def get_input_format(cls, llm_format: bool) -> str:  # noqa: FBT001
        """Get the JSON schema of the input format model.

        Raises:
            NotImplementedError: If the `input_format` is not defined.

        Returns:
            The JSON schema of the input format as a string.
        """
        if cls.output_format is not None:
            if llm_format:
                return json.dumps(cls.input_format, indent=2)
            return json.dumps(cls.input_format.model_json_schema(), indent=2)
        msg = f"{cls.__name__}' class does not define an 'input_format'."
        raise NotImplementedError(msg)

    @classmethod
    def get_output_format(cls, llm_format: bool) -> str:  # noqa: FBT001
        """Get the JSON schema of the output format model.

        Raises:
            NotImplementedError: If the `output_format` is not defined.

        Returns:
            The JSON schema of the output format as a string.
        """
        if cls.output_format is not None:
            if llm_format:
                return json.dumps(cls.output_format, indent=2)
            return json.dumps(cls.output_format.model_json_schema(), indent=2)
        msg = "'%s' class does not define an 'output_format'."
        raise NotImplementedError(msg)

    @classmethod
    def get_setup_format(cls, llm_format: bool) -> str:  # noqa: FBT001
        """Gets the JSON schema of the setup format model.

        Raises:
            NotImplementedError: If the `setup_format` is not defined.

        Returns:
            The JSON schema of the setup format as a string.
        """
        if cls.setup_format is not None:
            if llm_format:
                return json.dumps(cls.setup_format, indent=2)
            return json.dumps(cls.setup_format.model_json_schema(), indent=2)
        msg = "'%s' class does not define an 'setup_format'."
        raise NotImplementedError(msg)

    @abstractmethod
    async def initialize(self, setup_data: dict[str, Any]) -> None:
        """Initialize the module."""
        raise NotImplementedError

    @abstractmethod
    async def run(
        self,
        input_data: dict[str, Any],
        setup_data: dict[str, Any],
        callback: Callable,
    ) -> None:
        """Run the module."""
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self) -> None:
        """Run the module."""
        raise NotImplementedError

    async def _run_lifecycle(
        self,
        input_data: dict[str, Any],
        setup_data: dict[str, Any],
        callback: Callable,
    ) -> None:
        """Run the module lifecycle.

        Raises:
            asyncio.CancelledError: If the module is cancelled
        """
        try:
            await self.run(input_data, setup_data, callback)
            await self.stop()
        except asyncio.CancelledError:
            logger.info(f"Module {self.name} cancelled")
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error inside module %s", self.name)
        else:
            self._status = ModuleStatus.STOPPED

    async def start(
        self,
        input_data: dict[str, Any],
        setup_data: dict[str, Any],
        callback: Callable,
    ) -> None:
        """Start the module."""
        try:
            await self.initialize(setup_data=setup_data)
            self._status = ModuleStatus.RUNNING
            self._task = asyncio.create_task(self._run_lifecycle(input_data, setup_data, callback))
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error starting module")

    async def stop(self) -> None:
        """Stop the module."""
        if self._status != ModuleStatus.RUNNING:
            return

        try:
            self._status = ModuleStatus.STOPPING
            if self._task and not self._task.done():
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._task
            await self.cleanup()
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error stopping module")
