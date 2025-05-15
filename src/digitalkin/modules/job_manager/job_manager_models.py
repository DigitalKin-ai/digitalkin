"""Job manager models."""

from enum import Enum

from pydantic import BaseModel

from digitalkin.modules.job_manager.base_job_manager import BaseJobManager


class StreamCodeModel(BaseModel):
    """Typed error/code model."""

    code: str


class JobManagerMode(Enum):
    """Job manager mode."""

    SINGLE = "single"
    TASKIQ = "taskiq"

    def __str__(self) -> str:
        """Get the string representation of the job manager mode.

        Returns:
            str: job manager mode name.
        """
        return self.value

    def get_manager_class(self) -> type[BaseJobManager]:
        """Get the job manager class based on the mode.

        Returns:
            type: The job manager class.
        """
        match self:
            case JobManagerMode.SINGLE:
                from digitalkin.modules.job_manager.single_job_manager import SingleJobManager  # noqa: PLC0415

                return SingleJobManager
            case JobManagerMode.TASKIQ:
                from digitalkin.modules.job_manager.taskiq_job_manager import TaskiqJobManager  # noqa: PLC0415

                return TaskiqJobManager
