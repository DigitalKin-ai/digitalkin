"""This module contains the abstract base class for setup strategies."""

import datetime
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from digitalkin.models.services.registry import RegistrySetupStatus
from digitalkin.models.services.storage import Visibility


class SetupVersionData(BaseModel):
    """Pydantic model for SetupVersion data validation.

    ``structure`` maps a key path in ``content`` to a short summary of what is there (see
    :class:`~digitalkin.utils.json_structure.JsonStructure`), written by whoever created or
    updated the setup. Empty means none was stored.

    ``documentation`` is free text indexed by the registry search. It is cut with the version
    that carries it, and ``SetupVersion`` returns it as of protocol 1.0.2.dev2 — before that
    the field existed only on the write requests and always read back empty.
    """

    id: str
    setup_id: str
    version: str
    documentation: str = ""
    content: dict[str, Any]
    structure: dict[str, str] = {}
    creation_date: datetime.datetime


class SetupVersionPage(BaseModel):
    """A page of a setup's versions, most recent first."""

    setup_versions: list[SetupVersionData]
    total_count: int
    current_setup_version_id: str = ""


class SetupData(BaseModel):
    """Pydantic model for Setup data validation.

    ``status``/``visibility`` are coerced to their SDK enums: a proto enum name
    (``READY``, ``VISIBILITY_PRIVATE``) or any-case string maps to the matching
    member, and an empty value (backends that predate the fields) becomes
    ``UNSPECIFIED``.

    The setup's documentation lives on the version that carries it — read it at
    ``current_setup_version.documentation``.
    """

    id: str
    name: str
    organisation_id: str
    owner_id: str
    module_id: str
    current_setup_version: SetupVersionData
    status: RegistrySetupStatus = RegistrySetupStatus.UNSPECIFIED
    visibility: Visibility = Visibility.UNSPECIFIED


class SetupStrategy(ABC):
    """Abstract base class for setup strategies.

    Mirrors the SetupService protocol: setup-level CRUD, visibility change, and the
    two read/activate version RPCs. Versions are still created only as a side effect
    of ``update_setup`` — there is no standalone create/update/delete for them.
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
                path; omitting it (or passing an empty string, which the wire cannot
                tell apart) returns the whole document.

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
            structure: The authored ``{key path: summary}`` map for ``content``. Omit to
                have one derived from the values instead.

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
    async def create_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Create a new setup; owner/organisation/module derive from the request context.

        Args:
            setup_dict: Dictionary with 'name', 'content', optional 'documentation' and
                optional 'structure' — the authored ``{key path: summary}`` map. Entries
                whose paths do not resolve in ``content`` are dropped; an absent map is
                derived from the values.

        Returns:
            The created setup with its initial version.
        """

    @abstractmethod
    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Update a setup's name and current version content.

        Args:
            setup_dict: Dictionary with 'setup_id', 'name', 'content' and optional
                'documentation' (cut onto the new version, like 'content') and optional
                'structure'. The map is always rewritten from what this call carries, so
                omitting it replaces an authored map with a derived one.

        Returns:
            The updated setup with its current version.
        """

    @abstractmethod
    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_id'.

        Returns:
            bool: Success status of deletion.
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
