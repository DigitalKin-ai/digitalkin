"""Immutable container for gRPC request metadata (headers)."""

from __future__ import annotations


class RequestMetadata:
    """Immutable container for gRPC request metadata (headers).

    Provides typed access to common auth headers and raw access to all metadata.
    Filters out gRPC-reserved keys (prefixed with ``grpc-``).

    Example::

        metadata = RequestMetadata({"authorization": "Bearer eyJ...", "x-tenant-id": "t-123"})
        token = metadata.bearer_token  # "eyJ..."
        tenant = metadata.get("x-tenant-id")  # "t-123"
    """

    __slots__ = ("_raw",)

    def __init__(self, raw: dict[str, str] | None = None) -> None:
        """Initialize RequestMetadata from a raw metadata dict.

        Args:
            raw: Dictionary of metadata key-value pairs. Keys prefixed with ``grpc-`` are filtered out.
        """
        if raw is None:
            self._raw: dict[str, str] = {}
        else:
            # Filter out gRPC-reserved keys (prefixed with "grpc-")
            self._raw = {k: v for k, v in raw.items() if not k.startswith("grpc-")}

    @property
    def authorization(self) -> str | None:
        """Get the Authorization header value (e.g., ``Bearer <token>``)."""
        return self._raw.get("authorization")

    @property
    def bearer_token(self) -> str | None:
        """Extract the bearer token from the Authorization header.

        Returns:
            The token string if the Authorization header starts with ``Bearer ``, otherwise None.
        """
        auth = self.authorization
        if auth and auth.lower().startswith("bearer "):
            return auth[7:]
        return None

    @property
    def api_key(self) -> str | None:
        """Get the ``x-api-key`` header value."""
        return self._raw.get("x-api-key")

    def get(self, key: str, default: str | None = None) -> str | None:
        """Get any metadata value by key.

        Args:
            key: Metadata key.
            default: Default value if key is not found.

        Returns:
            The metadata value or default.
        """
        return self._raw.get(key, default)

    def __contains__(self, key: object) -> bool:
        """Check if a metadata key exists.

        Returns:
            True if key is present.
        """
        return key in self._raw

    def __getitem__(self, key: str) -> str:
        """Get a metadata value by key.

        Args:
            key: Metadata key.

        Returns:
            The metadata value.

        Raises:
            KeyError: If the key is not found.
        """
        return self._raw[key]

    def __repr__(self) -> str:
        """Return a string representation (sensitive values masked)."""
        safe = {}
        for k, v in self._raw.items():
            if k in {"authorization", "x-api-key"}:
                mask_threshold = 10
                safe[k] = v[:mask_threshold] + "***" if len(v) > mask_threshold else "***"
            else:
                safe[k] = v
        return f"RequestMetadata({safe})"

    def __bool__(self) -> bool:
        """Return True if metadata is non-empty."""
        return bool(self._raw)

    def to_dict(self) -> dict[str, str]:
        """Return a copy of the raw metadata dictionary."""
        return self._raw.copy()

    def to_grpc_metadata(self) -> list[tuple[str, str]]:
        """Convert to gRPC metadata format for forwarding.

        Returns:
            List of (key, value) tuples suitable for gRPC ``metadata=`` parameter.
        """
        return list(self._raw.items())
