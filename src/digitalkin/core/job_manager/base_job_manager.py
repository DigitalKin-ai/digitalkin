"""Background module manager."""

import abc
from collections.abc import Callable, Coroutine
from typing import Any, Generic

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.module.module import ModuleCodeModel
from digitalkin.models.module.module_types import DataModel, InputModelT, OutputModelT, SetupModelT
from digitalkin.models.services.services import ServicesMode
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig


class BaseJobManager(abc.ABC, Generic[InputModelT, OutputModelT, SetupModelT]):
    """Abstract base class for managing background module jobs.

    Uses composition to delegate task lifecycle management to a TaskManager.
    """

    module_class: type[BaseModule]
    services_mode: ServicesMode
    _task_manager: BaseTaskManager

    def __init__(
        self,
        module_class: type[BaseModule],
        services_mode: ServicesMode,
        task_manager: BaseTaskManager,
    ) -> None:
        """Initialize the job manager.

        Args:
            module_class: The class of the module to be managed.
            services_mode: The mode of operation for the services (e.g., ASYNC or SYNC).
            task_manager: The task manager instance to use for task lifecycle management.
        """
        self.module_class = module_class
        self.services_mode = services_mode
        self._task_manager = task_manager

        services_config = ServicesConfig(
            services_config_strategies=self.module_class.services_config_strategies,
            services_config_params=self.module_class.services_config_params,
            mode=services_mode,
        )
        self.module_class.services_config = services_config

    # Properties to expose task manager attributes
    @property
    def tasks_sessions(self) -> dict[str, TaskSession]:
        """Get task sessions from the task manager."""
        return self._task_manager.tasks_sessions

    @property
    def tasks(self) -> dict[str, Any]:
        """Get tasks from the task manager."""
        return self._task_manager.tasks

    async def create_task(
        self,
        task_id: str,
        mission_id: str,
        module: BaseModule,
        coro: Coroutine[Any, Any, None],
        **kwargs: Any,
    ) -> None:
        """Create a task using the task manager.

        Delegate task lifecycle methods to task manager

        Args:
            task_id: Unique identifier for the task
            mission_id: Mission identifier
            module: Module instance
            coro: Coroutine to execute
            **kwargs: Additional arguments for task creation
        """
        await self._task_manager.create_task(task_id, mission_id, module, coro, **kwargs)

    async def cancel_task(self, task_id: str, mission_id: str, timeout: float | None = None) -> bool:
        """Cancel a task.

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier.
            timeout: Optional timeout in seconds to wait for the cancellation to complete.

        Returns:
            bool: True if the task was successfully cancelled, False otherwise.
        """
        return await self._task_manager.cancel_task(task_id, mission_id, timeout)

    async def send_signal(self, task_id: str, mission_id: str, signal_type: str, payload: dict) -> bool:
        """Send signal to a task.

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier.
            signal_type: Type of signal to send.
            payload: Payload data for the signal.

        Returns:
            bool: True if the signal was successfully sent, False otherwise.
        """
        return await self._task_manager.send_signal(task_id, mission_id, signal_type, payload)

    async def shutdown(self, mission_id: str, timeout: float = 30.0) -> None:
        """Shutdown all tasks."""
        await self._task_manager.shutdown(mission_id, timeout)

    @abc.abstractmethod
    async def start(self) -> None:
        """Start the job manager.

        This method initializes any necessary resources or configurations
        required for the job manager to function.
        """

    async def stop(self) -> None:
        """Stop the job manager and clean up resources.

        Default no-op. Subclasses with external connections override.
        """

    @staticmethod
    async def job_specific_callback(
        callback: Callable[[str, DataModel | ModuleCodeModel], Coroutine[Any, Any, None]],
        job_id: str,
    ) -> Callable[[DataModel | ModuleCodeModel], Coroutine[Any, Any, None]]:
        """Generate a job-specific callback function.

        Args:
            callback: The callback function to be executed when the job completes.
            job_id: The unique identifier of the job.

        Returns:
            Callable: A wrapped callback function that includes the job ID.
        """

        def callback_wrapper(output_data: DataModel | ModuleCodeModel) -> Coroutine[Any, Any, None]:
            """Wrapper for the callback function.

            Args:
                output_data: The output data produced by the job.

            Returns:
                Coroutine: The wrapped callback function.
            """
            return callback(job_id, output_data)

        return callback_wrapper

    @abc.abstractmethod
    async def generate_config_setup_module_response(self, job_id: str) -> SetupModelT | ModuleCodeModel:
        """Generate a stream consumer for a module's output data.

        This method creates an asynchronous generator that streams output data
        from a specific module job. If the module does not exist, it generates
        an error message.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            SetupModelT | ModuleCodeModel: the SetupModelT object fully processed, or an error code.
        """

    @abc.abstractmethod
    async def create_config_setup_instance_job(
        self,
        config_setup_data: SetupModelT,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        request_metadata: dict[str, str] | None = None,
    ) -> str:
        """Create and start a new module job.

        This method initializes a new module job, assigns it a unique job ID,
        and starts it in the background.

        Args:
            config_setup_data: The input data required to start the job.
            mission_id: The mission ID associated with the job.
            setup_id: The setup ID.
            setup_version_id: The setup version ID.
            request_metadata: gRPC request metadata (headers) to forward to the module.

        Returns:
            str: The unique identifier (job ID) of the created job.

        Raises:
            Exception: If the module fails to start.
        """

    @abc.abstractmethod
    async def list_modules(self) -> dict[str, dict[str, Any]]:
        """List all modules along with their statuses.

        Returns:
            dict[str, dict[str, Any]]: A dictionary containing information about all modules and their statuses.
        """
