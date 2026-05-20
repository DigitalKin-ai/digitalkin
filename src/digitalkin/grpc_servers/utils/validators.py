"""Gateway-input validators bundled as classmethods on a single class."""

import re
from typing import ClassVar


class GatewayValidator:
    """Validation + sanitization helpers used by the gateway surface.

    All methods are stateless classmethods; the class is the namespace.
    Compiled regexes and the wildcard-host frozen set live as ``ClassVar``
    so they're shared across all calls without a module-level binding.
    """

    _ID_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9_:.-]{1,256}$")
    _ADDRESS_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9_.-]{1,253}:\d{1,5}$")
    # Wildcard bind addresses — invalid as dial-back targets even though
    # servers commonly bind to them. (S104 flags the literal as a bind hint.)
    _WILDCARD_HOSTS: ClassVar[frozenset[str]] = frozenset({"[::]", "0.0.0.0", "::"})  # noqa: S104
    _MASK_PATTERN: ClassVar[re.Pattern[str]] = re.compile(r"://([^:]+):([^@]+)@")
    _MAX_TCP_PORT: ClassVar[int] = 65535

    @classmethod
    def validate_id(cls, value: str, field_name: str) -> str | None:
        """Validate a user-supplied ID against the safe character pattern.

        Allows alphanumeric, underscore, colon, dot, hyphen. Max 256 chars.
        Colons are needed for IDs like ``setups:my_setup`` and
        ``modules:01kjcsma75vee1m0rdny90tvqg``.

        Args:
            value: The ID to validate.
            field_name: Field name, used in the returned error message.

        Returns:
            None if valid; an error string if the value is missing or
            contains invalid characters.
        """
        if not isinstance(value, str) or not value:
            return f"{field_name} is required"
        if not cls._ID_PATTERN.match(value):
            return f"{field_name} contains invalid characters"
        return None

    @classmethod
    def validate_address(cls, value: str, field_name: str) -> str | None:
        """Validate a ``host:port`` address used for dial-back.

        Rejects empty, malformed, out-of-range, and wildcard bind
        addresses. Wildcards (``[::]``, ``0.0.0.0``, ``::``) are bind
        addresses, not routable destinations — accepting them as
        ``x-client-address`` is a debugging trap because the gateway
        cannot dial back to them.

        Args:
            value: The address to validate.
            field_name: Field name, used in the returned error message.

        Returns:
            None if valid; an error string describing the failure.
        """
        if not isinstance(value, str) or not value:
            return f"{field_name} is required"
        if not cls._ADDRESS_PATTERN.match(value):
            return f"{field_name} must be host:port"
        host, _, port_str = value.partition(":")
        port = int(port_str)
        if not (1 <= port <= cls._MAX_TCP_PORT):
            return f"{field_name} port out of range"
        if host in cls._WILDCARD_HOSTS:
            return f"{field_name} cannot be a wildcard bind address"
        return None

    @classmethod
    def mask_redis_url(cls, url: str) -> str:
        """Mask the password in a Redis URL for safe logging.

        Args:
            url: Redis connection URL of the form ``redis://user:pwd@host:port/db``.

        Returns:
            URL with the password segment replaced by ``****``.
        """
        return cls._MASK_PATTERN.sub(r"://\1:****@", url)
