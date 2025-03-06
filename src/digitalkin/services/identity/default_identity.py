"""Default identity."""

from .identity_strategy import IdentityStrategy


class DefaultIdentity(IdentityStrategy):
    """DefaultIdentity is the default identity strategy."""

    async def get_identity(self) -> str:  # noqa: PLR6301
        """Get the identity.

        Returns:
            str: The identity
        """
        return "default_identity"
