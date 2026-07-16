"""In-memory setup strategy mirroring the SetupService protocol."""

import datetime
import secrets
import string
from typing import Any

from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.setup.setup_strategy import SetupData, SetupStrategy, SetupVersionData


class DefaultSetup(SetupStrategy):
    """In-memory implementation of the setup strategy (same contract as GrpcSetup)."""

    setups: dict[str, SetupData]

    def __init__(self) -> None:
        """Initialize the default setup strategy."""
        super().__init__()
        self.setups = {}

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
                status="READY",
                visibility="VISIBILITY_PRIVATE",
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
        logger.debug("CREATE SETUP DATA %s:%s successful", setup_id, setup)
        return setup

    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        """Update a setup's name and current version content.

        Args:
            setup_dict: Dictionary with 'setup_id', 'name' and 'content'.

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
        setup.current_setup_version.content = content
        setup.current_setup_version.creation_date = datetime.datetime.now(datetime.timezone.utc)
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
        setup.visibility = f"VISIBILITY_{scope.upper()}"
        return setup
