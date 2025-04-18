"""This module contains the abstract base class for filesystem strategies."""

from abc import ABC, abstractmethod
from enum import Enum, auto

from pydantic import BaseModel, Field

from digitalkin.services.base_strategy import BaseStrategy


class FilesystemServiceError(Exception):
    """Base exception for Setup service errors."""


class FileType(Enum):
    """Enum defining the types of data that can be stored."""

    DOCUMENT = auto()
    IMAGE = auto()
    VIDEO = auto()
    AUDIO = auto()
    ARCHIVE = auto()
    OTHER = auto()


class FilesystemData(BaseModel):
    """Data model for filesystem operations."""

    kin_context: str = Field(description="The context of the file in the filesystem")
    name: str = Field(description="The name of the file")
    file_type: FileType = Field(default=FileType.DOCUMENT, description="The type of data stored")
    url: str = Field(description="The URL of the file in the filesystem")


class FilesystemStrategy(BaseStrategy, ABC):
    """Abstract base class for filesystem strategies."""

    def __init__(self, mission_id: str, setup_version_id: str, config: dict[str, str]) -> None:
        """Initialize the strategy.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_version_id: The ID of the setup version this strategy is associated with
            config: configuration dictionary for the filesystem strategy
        """
        super().__init__(mission_id, setup_version_id)
        self.config: dict[str, str] = config

    @abstractmethod
    def upload(self, content: bytes, name: str, file_type: FileType) -> FilesystemData:
        """Create a new file in the filesystem."""

    @abstractmethod
    def get(self, name: str) -> FilesystemData:
        """Get file from the filesystem."""

    @abstractmethod
    def get_batch(self, names: list[str]) -> dict[str, FilesystemData | None]:
        """Get files from the filesystem."""

    @abstractmethod
    def get_all(self) -> list[FilesystemData]:
        """Get all files from the filesystem."""

    @abstractmethod
    def update(self, name: str, content: bytes, file_type: FileType) -> FilesystemData:
        """Update files in the filesystem."""

    @abstractmethod
    def delete(self, name: str) -> bool:
        """Delete file from the filesystem."""
