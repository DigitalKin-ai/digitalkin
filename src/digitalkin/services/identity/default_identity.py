"""Default identity."""

from digitalkin.services.identity.identity_strategy import IdentityStrategy


class DefaultIdentity(IdentityStrategy):
    """DefaultIdentity is the default identity strategy."""

    async def get_identity(  # ruff: ignore[no-self-use]
        self,
    ) -> str:  # Default stub implementation; self available for subclass overrides
        """Get the identity.

        Returns:
            str: The identity
        """
        return "default_identity"
