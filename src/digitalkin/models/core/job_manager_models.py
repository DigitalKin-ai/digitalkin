"""Job manager models."""

from enum import Enum

from digitalkin.core.job_manager.base_job_manager import BaseJobManager


class BackpressureStrategy(str, Enum):
    """Backpressure strategy for module output queue writes."""

    BLOCK = "block"
    DROP_OLDEST = "drop_oldest"
    REJECT = "reject"


class JobManagerMode(Enum):
    """Job manager mode."""

    SINGLE = "single"

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
        from digitalkin.core.job_manager.single_job_manager import (
            SingleJobManager,
        )

        return SingleJobManager
