"""In-memory setup strategy mirroring the SetupService protocol."""

import datetime
import secrets
import string
from typing import Any

from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.models.services.registry import RegistrySetupStatus
from digitalkin.models.services.storage import Visibility
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.setup.setup_strategy import (
    SetupData,
    SetupStrategy,
    SetupVersionData,
    SetupVersionPage,
)


class DefaultSetup(SetupStrategy):
    """In-memory implementation of the setup strategy (same contract as GrpcSetup)."""

    setups: dict[str, SetupData]
    # Every version ever cut, oldest first, keyed by setup id — the local stand-in for
    # the service's version table that ListSetupVersions pages over.
    versions: dict[str, list[SetupVersionData]]

    def __init__(self) -> None:
        """Initialize the default setup strategy."""
        super().__init__()
        self.setups = {}
        self.versions = {}

    @staticmethod
    def _new_id() -> str:
        """Generate a random identifier.

        Returns:
            A 16-char alphanumeric id.
        """
        return "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))

    def _get_or_raise(self, setup_id: str) -> SetupData:
        """Fetch a stored setup or raise.

        Args:
            setup_id: The setup identifier.

        Returns:
            The stored setup.

        Raises:
            SetupServiceError: setup_id does not exist.
        """
        setup = self.setups.get(setup_id)
        if setup is None:
            msg = f"setup_id = {setup_id}: DOESN'T EXIST"
            logger.error(msg)
            raise SetupServiceError(msg)
        return setup

    async def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Retrieve a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with 'setup_id' and optional 'version'.

        Returns:
            The setup with its current version populated.

        Raises:
            SetupServiceError: setup_id does not exist.
        """
        return self._get_or_raise(setup_dict.get("setup_id", ""))

    async def create_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Create a new setup; identifiers are generated locally.

        Args:
            setup_dict: Dictionary with 'name' and 'content'.

        Returns:
            The created setup with its initial version.

        Raises:
            ValueError: If name or content is invalid.
        """
        setup_id = self._new_id()
        try:
            setup = SetupData(
                id=setup_id,
                name=setup_dict.get("name", ""),
                organisation_id="local",
                owner_id="local",
                module_id="local",
                status=RegistrySetupStatus.READY,
                visibility=Visibility.PRIVATE,
                current_setup_version=SetupVersionData(
                    id=self._new_id(),
                    setup_id=setup_id,
                    version="1.0.0",
                    content=setup_dict.get("content") or {},
                    creation_date=datetime.datetime.now(datetime.timezone.utc),
                ),
            )
        except ValidationError as e:
            msg = f"Validation failed for SetupData: {e}"
            logger.exception("Validation failed for model SetupData")
            raise ValueError(msg) from e
        if not setup.name:
            msg = "name is required"
            raise ValueError(msg)
        self.setups[setup_id] = setup
        self.versions[setup_id] = [setup.current_setup_version]
        logger.debug("CREATE SETUP DATA %s:%s successful", setup_id, setup)
        return setup

    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Update a setup's name and current version content.

        Args:
            setup_dict: Dictionary with 'setup_id', 'name', 'content' and optional
                'set_as_current' (defaults to True).

        Returns:
            The updated setup with its current version.

        Raises:
            SetupServiceError: setup_id does not exist.
            ValueError: If the update payload is invalid.
        """
        setup = self._get_or_raise(setup_dict.get("setup_id", ""))
        name = setup_dict.get("name", "")
        content = setup_dict.get("content")
        if not name or not isinstance(content, dict):
            msg = "setup_id, name and content (object) are required"
            raise ValueError(msg)
        setup.name = name
        # A new revision rather than an in-place edit, matching UpdateSetup on the wire.
        history = self.versions.setdefault(setup.id, [setup.current_setup_version])
        version = SetupVersionData(
            id=self._new_id(),
            setup_id=setup.id,
            version=f"1.0.{len(history)}",
            content=content,
            creation_date=datetime.datetime.now(datetime.timezone.utc),
        )
        history.append(version)
        if setup_dict.get("set_as_current", True):
            setup.current_setup_version = version
        return setup

    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        """Delete a setup by its unique identifier.

        Args:
            setup_dict: Dictionary with the 'setup_id'.

        Returns:
            bool: Success status of deletion.
        """
        setup_id = setup_dict.get("setup_id", "")
        if setup_id not in self.setups:
            logger.debug("DELETE setup_id = %s: DOESN'T EXIST", setup_id)
            return False
        del self.setups[setup_id]
        self.versions.pop(setup_id, None)
        return True

    async def change_visibility(self, setup_dict: dict[str, Any]) -> SetupData:
        """Change a setup's visibility scope.

        Args:
            setup_dict: Dictionary with 'setup_id' and 'visibility'
                (``public`` | ``private`` | ``internal``).

        Returns:
            The setup with its updated visibility.

        Raises:
            SetupServiceError: setup_id does not exist.
            ValueError: If visibility is not a valid scope.
        """
        setup = self._get_or_raise(setup_dict.get("setup_id", ""))
        scope = str(setup_dict.get("visibility", "")).lower()
        if scope not in {"public", "private", "internal"}:
            msg = f"invalid visibility '{setup_dict.get('visibility')}'; use 'public', 'private' or 'internal'"
            raise ValueError(msg)
        setup.visibility = Visibility(scope)
        return setup

    async def list_setup_versions(self, setup_dict: dict[str, Any]) -> SetupVersionPage:
        """List a setup's versions, most recent first.

        Args:
            setup_dict: Dictionary with 'setup_id' and optional 'limit' / 'offset'.

        Returns:
            The requested page, its total count and the currently active version id.

        Raises:
            SetupServiceError: setup_id does not exist.
        """
        setup = self._get_or_raise(setup_dict.get("setup_id", ""))
        history = list(reversed(self.versions.get(setup.id, [setup.current_setup_version])))
        offset = int(setup_dict.get("offset") or 0)
        limit = int(setup_dict.get("limit") or 20)
        return SetupVersionPage(
            setup_versions=history[offset : offset + limit],
            total_count=len(history),
            current_setup_version_id=setup.current_setup_version.id,
        )

    async def set_current_setup_version(self, setup_dict: dict[str, Any]) -> SetupData:
        """Activate an existing version of a setup, making it the current one.

        Args:
            setup_dict: Dictionary with 'setup_id' and 'setup_version_id'.

        Returns:
            The setup with its newly activated version.

        Raises:
            SetupServiceError: setup_id does not exist, or the version does not belong to it.
        """
        setup = self._get_or_raise(setup_dict.get("setup_id", ""))
        setup_version_id = setup_dict.get("setup_version_id", "")
        history = self.versions.get(setup.id, [setup.current_setup_version])
        version = next((v for v in history if v.id == setup_version_id), None)
        if version is None:
            msg = f"setup version '{setup_version_id}' not found on setup '{setup.id}'"
            raise SetupServiceError(msg)
        setup.current_setup_version = version
        return setup
