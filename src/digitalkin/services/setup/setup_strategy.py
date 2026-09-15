"""This module contains the abstract base class for setup strategies."""

import datetime
from abc import ABC, abstractmethod
from typing import Any

from pydantic import AliasChoices, BaseModel, Field

from digitalkin.models.services.registry import RegistrySetupStatus
from digitalkin.models.services.storage import Visibility


class SetupVersionData(BaseModel):
    """Pydantic model for SetupVersion data validation.

    ``structure`` maps a leaf key path in ``content`` to a description of what is there,
    written by the agent that created or updated the setup and used to fetch one key of a
    large configuration (see :class:`~digitalkin.utils.json_structure.JsonStructure`). It
    belongs to the services surface; other setup kinds leave it empty. ``None`` means the
    version was cut without one, as opposed to ``{}``, a carried empty map.

    ``documentation`` is free text indexed by the registry search, cut with the version
    that carries it.

    ``creation_date`` also validates from the wire name ``created_at``.
    """

    id: str
    setup_id: str
    version: str
    documentation: str = ""
    content: dict[str, Any]
    structure: dict[str, str] | None = None
    creation_date: datetime.datetime = Field(validation_alias=AliasChoices("creation_date", "created_at"))


class SetupVersionPage(BaseModel):
    """A page of a setup's versions, most recent first."""

    setup_versions: list[SetupVersionData]
    total_count: int
    current_setup_version_id: str = ""


class SetupData(BaseModel):
    """Pydantic model for Setup data validation.

    ``status``/``visibility`` are coerced to their SDK enums by name: a proto enum name
    (``READY``, ``PRIVATE``) or any-case string maps to the matching member, and an
    empty or unmirrored value becomes ``UNSPECIFIED``.

    The setup's documentation lives on the version that carries it — read it at
    ``current_setup_version.documentation``.

    ``organisation_id`` also validates from the wire name ``organization_id``.
    """

    id: str
    name: str
    organisation_id: str = Field(validation_alias=AliasChoices("organisation_id", "organization_id"))
    owner_id: str
    module_id: str
    current_setup_version: SetupVersionData
    status: RegistrySetupStatus = RegistrySetupStatus.UNSPECIFIED
    visibility: Visibility = Visibility.UNSPECIFIED


class SetupPage(BaseModel):
    """A page of setups matching a listing's filters."""

    setups: list[SetupData]
    total_count: int


class SetupStrategy(ABC):
    """Abstract base class for setup strategies.

    Mirrors the SetupService protocol (setup-level CRUD, listing, visibility change) and
    the SetupVersionService protocol (version CRUD, listing, activation).
    """

    def __init__(self) -> None:
        """Initialize the setup strategy."""

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        """Lifecycle hook for post-initialization. Subclasses override with specific params."""

    @abstractmethod
    async def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'setup_id', optional 'version', and optional
                'structure_key'. One key path projects the version content down to that
                path, and a path the content does not have is refused as not found;
                omitting it or passing an empty string returns the whole document.

        Returns:
            The setup with its current version populated.
        """

    async def create_service_setup(
        self,
        name: str,
        content: dict[str, Any],
        documentation: str = "",
        structure: dict[str, str] | None = None,
    ) -> SetupData:
        """Create a service setup — a shareable configuration document.

        Only a name and the content JSON are needed; everything else (owner,
        organisation, backing module, kind) is derived server-side.

        Args:
            name: Human-readable service name.
            content: The service configuration JSON.
            documentation: Free text describing the service, indexed by the registry search.
            structure: The ``{key path: description}`` map the agent wrote for ``content``.

        Returns:
            The created setup with its initial version.
        """
        return await self.create_setup({
            "name": name,
            "content": content,
            "documentation": documentation,
            "structure": structure,
        })

    @abstractmethod
    async def list_setups(self, setup_dict: dict[str, Any]) -> SetupPage:
        """List setups, optionally filtered.

        Args:
            setup_dict: Dictionary with optional 'organization_id', 'owner_id' and
                'module_id' filters, optional 'statuses' (``RegistrySetupStatus`` members or
                their names; UNSPECIFIED is refused) and optional 'limit' (clamped to
                1..100, default 20) / 'offset'.

        Returns:
            The requested page and the total count of matching setups.
        """

    @abstractmethod
    async def create_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Create a new setup; owner/organisation/module derive from the request context.

        Args:
            setup_dict: Dictionary with 'name', 'content', optional 'documentation' and
                optional 'structure' — the ``{key path: description}`` map the agent
                wrote, stored as written with only each description's length bounded.

        Returns:
            The created setup with its initial version.
        """

    @abstractmethod
    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Update a setup's name and current version content.

        Args:
            setup_dict: Dictionary with 'setup_id', 'name', 'content' and optional
                'documentation' (cut onto the new version, like 'content') and optional
                'structure'. The map belongs to the content it describes, so a revision
                carries only the map its own call supplied; omitting it leaves the new
                revision without one.

        Returns:
            The updated setup with its current version.
        """

    @abstractmethod
    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_id'.

        Returns:
            bool: True once deleted, False when the setup could not be deleted (e.g. not found).
        """

    @abstractmethod
    async def change_visibility(self, setup_dict: dict[str, Any]) -> SetupData:
        """Change a setup's visibility scope.

        Args:
            setup_dict: Dictionary with 'setup_id' and 'visibility'
                (``public`` | ``private`` | ``internal``).

        Returns:
            The setup with its updated visibility.
        """

    @abstractmethod
    async def create_setup_version(self, setup_dict: dict[str, Any]) -> SetupVersionData:
        """Cut a new version of a setup.

        Args:
            setup_dict: Dictionary with 'setup_id', 'version' (the label), 'content',
                optional 'structure', optional 'documentation' and optional
                'set_as_current' (defaults to False: the version is staged).

        Returns:
            The created version.
        """

    @abstractmethod
    async def get_setup_version(self, setup_dict: dict[str, Any]) -> SetupVersionData:
        """Retrieve a setup version by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_version_id'.

        Returns:
            The requested version.
        """

    @abstractmethod
    async def update_setup_version(self, setup_dict: dict[str, Any]) -> SetupVersionData:
        """Edit a setup version in place; only the supplied fields change.

        Args:
            setup_dict: Dictionary with 'setup_version_id' and at least one of 'version',
                'content', 'documentation' (``""`` clears it) or 'structure'. A missing or
                ``None`` field is left unchanged.

        Returns:
            The updated version.
        """

    @abstractmethod
    async def delete_setup_version(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup version by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_version_id'.

        Returns:
            bool: True once deleted, False when the version could not be deleted.
        """

    @abstractmethod
    async def list_setup_versions(self, setup_dict: dict[str, Any]) -> SetupVersionPage:
        """List a setup's versions, most recent first.

        Args:
            setup_dict: Dictionary with 'setup_id' and optional 'limit' / 'offset'.

        Returns:
            The requested page, its total count and the currently active version id.
        """

    @abstractmethod
    async def set_current_setup_version(self, setup_dict: dict[str, Any]) -> SetupData:
        """Activate an existing version of a setup, making it the current one.

        Args:
            setup_dict: Dictionary with 'setup_id' and 'setup_version_id'.

        Returns:
            The setup with its newly activated version.
        """
