"""Background module manager."""

import abc
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any, Generic

from digitalkin.models import ModuleStatus
from digitalkin.models.module import InputModelT, OutputModelT, SetupModelT
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode


class BaseJobManager(abc.ABC, Generic[InputModelT, SetupModelT]):
    """Abstract base class for managing background module jobs."""

    async def _start(self) -> None:
        """Start the job manager.

        This method initializes any necessary resources or configurations
        required for the job manager to function.
        """

    @staticmethod
    async def job_specific_callback(
        callback: Callable[[str, OutputModelT], Coroutine[Any, Any, None]], job_id: str
    ) -> Callable[[OutputModelT], Coroutine[Any, Any, None]]:
        """Generate a job-specific callback function.

        Args:
            callback: The callback function to be executed when the job completes.
            job_id: The unique identifier of the job.

        Returns:
            Callable: A wrapped callback function that includes the job ID.
        """

        def callback_wrapper(output_data: OutputModelT) -> Coroutine[Any, Any, None]:
            """Wrapper for the callback function.

            Args:
                output_data: The output data produced by the job.

            Returns:
                Coroutine: The wrapped callback function.
            """
            return callback(job_id, output_data)

        return callback_wrapper

    def __init__(
        self,
        module_class: type[BaseModule],
        services_mode: ServicesMode,
    ) -> None:
        """Initialize the job manager.

        Args:
            module_class: The class of the module to be managed.
            services_mode: The mode of operation for the services (e.g., ASYNC or SYNC).
        """
        self.module_class = module_class

        services_config = ServicesConfig(
            services_config_strategies=self.module_class.services_config_strategies,
            services_config_params=self.module_class.services_config_params,
            mode=services_mode,
        )
        setattr(self.module_class, "services_config", services_config)

    @abc.abstractmethod  # type: ignore
    @asynccontextmanager  # type: ignore
    async def generate_stream_consumer(self, job_id: str) -> AsyncIterator[AsyncGenerator[dict[str, Any], None]]:
        """Generate a stream consumer for the job's message stream.

        Args:
            job_id: The unique identifier of the job to filter messages for.

        Yields:
            dict[str, Any]: The messages from the associated module's stream.
        """

    @abc.abstractmethod
    async def create_module_instance_job(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
    ) -> str:
        """Create and start a new job for the module's instance.

        Args:
            input_data: The input data required to start the job.
            setup_data: The setup configuration for the module.
            mission_id: The mission ID associated with the job.
            setup_id: The setup ID.
            setup_version_id: The setup version ID associated with the module.

        Returns:
            str: The unique identifier (job ID) of the created job.
        """

    @abc.abstractmethod
    async def generate_config_setup_module_response(self, job_id: str) -> SetupModelT:
        """Generate a stream consumer for a module's output data.

        This method creates an asynchronous generator that streams output data
        from a specific module job. If the module does not exist, it generates
        an error message.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            SetupModelT: the SetupModelT object fully processed.
        """

    @abc.abstractmethod
    async def create_config_setup_instance_job(
        self,
        config_setup_data: SetupModelT,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
    ) -> str:
        """Create and start a new module job.

        This method initializes a new module job, assigns it a unique job ID,
        and starts it in the background.

        Args:
            config_setup_data: The input data required to start the job.
            mission_id: The mission ID associated with the job.
            setup_id: The setup ID.
            setup_version_id: The setup version ID.

        Returns:
            str: The unique identifier (job ID) of the created job.

        Raises:
            Exception: If the module fails to start.
        """

    @abc.abstractmethod
    async def stop_module(self, job_id: str) -> bool:
        """Stop a running module job.

        Args:
            job_id: The unique identifier of the job to stop.

        Returns:
            bool: True if the job was successfully stopped, False if it does not exist.
        """

    @abc.abstractmethod
    async def get_module_status(self, job_id: str) -> ModuleStatus | None:
        """Retrieve the status of a module job.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            ModuleStatus | None: The status of the job, or None if the job does not exist.
        """

    @abc.abstractmethod
    async def stop_all_modules(self) -> None:
        """Stop all currently running module jobs.

        This method ensures that all active jobs are gracefully terminated.
        """

    @abc.abstractmethod
    async def list_modules(self) -> dict[str, dict[str, Any]]:
        """List all modules along with their statuses.

        Returns:
            dict[str, dict[str, Any]]: A dictionary containing information about all modules and their statuses.
        """
