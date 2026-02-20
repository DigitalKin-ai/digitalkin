"""Shared gRPC test fixtures and utilities."""

from collections.abc import Callable
from typing import Any

import grpc


class FakeContext(grpc.ServicerContext):
    """Enhanced fake gRPC context for comprehensive testing.

    This mock implementation of grpc.ServicerContext provides all commonly used
    methods with sensible defaults for testing gRPC servicers without requiring
    actual gRPC infrastructure.

    Attributes:
        _code: The gRPC status code
        _details: Error details message
        _metadata: Initial metadata
        _trailing_metadata: Trailing metadata
        _invocation_metadata: Metadata from the client
        _peer: Peer identity string
        _is_active: Whether the RPC is active
        _callbacks: List of callbacks to invoke on RPC completion
    """

    def __init__(
        self,
        invocation_metadata: tuple[tuple[str, str], ...] | None = None,
        peer: str = "test-peer",
    ) -> None:
        """Initialize with default OK status and optional metadata.

        Args:
            invocation_metadata: Optional metadata from the client
            peer: Optional peer identity string
        """
        self._code = grpc.StatusCode.OK
        self._details = ""
        self._metadata: list[tuple[str, str]] = []
        self._trailing_metadata: list[tuple[str, str]] = []
        self._invocation_metadata = invocation_metadata or ()
        self._peer = peer
        self._is_active = True
        self._callbacks: list[Callable[[], None]] = []

    def set_code(self, code: grpc.StatusCode) -> None:
        """Set the gRPC status code.

        Args:
            code: The status code to set
        """
        self._code = code

    def set_details(self, details: str) -> None:
        """Set the error details.

        Args:
            details: The error message
        """
        self._details = details

    def invocation_metadata(self) -> tuple[tuple[str, str], ...]:
        """Get the metadata sent by the client.

        Returns:
            Tuple of metadata key-value pairs
        """
        return self._invocation_metadata

    def peer(self) -> str:
        """Get the peer identity.

        Returns:
            String identifying the peer (e.g., "ipv4:127.0.0.1:12345")
        """
        return self._peer

    def time_remaining(self) -> float | None:
        """Get the remaining time for the RPC.

        Returns:
            None (unlimited time for testing)
        """
        return None

    def add_callback(self, callback: Callable[[], None]) -> bool:
        """Add a callback to be invoked when the RPC completes.

        Args:
            callback: Function to call on RPC completion

        Returns:
            True if callback was added successfully
        """
        if self._is_active:
            self._callbacks.append(callback)
            return True
        return False

    def cancel(self) -> bool:
        """Cancel the RPC.

        Returns:
            True if RPC was cancelled successfully
        """
        if self._is_active:
            self._is_active = False
            self._code = grpc.StatusCode.CANCELLED
            self._invoke_callbacks()
            return True
        return False

    def is_active(self) -> bool:
        """Check if the RPC is still active.

        Returns:
            True if RPC is active, False otherwise
        """
        return self._is_active

    def cancelled(self) -> bool:
        """Check if the RPC has been cancelled.

        Returns:
            True if RPC was cancelled, False otherwise
        """
        return self._code == grpc.StatusCode.CANCELLED

    def auth_context(self) -> dict[str, Any]:
        """Get the authentication context.

        Returns:
            Empty dict for testing (no authentication)
        """
        return {}

    def peer_identities(self) -> list[bytes] | None:
        """Get peer identities from the authentication context.

        Returns:
            None for testing (no peer identities)
        """
        return None

    def peer_identity_key(self) -> str | None:
        """Get the peer identity key from the authentication context.

        Returns:
            None for testing (no peer identity key)
        """
        return None

    def send_initial_metadata(self, initial_metadata: list[tuple[str, str]]) -> None:
        """Send initial metadata to the client.

        Args:
            initial_metadata: List of metadata key-value pairs
        """
        self._metadata = initial_metadata

    def set_trailing_metadata(self, trailing_metadata: list[tuple[str, str]]) -> None:
        """Set trailing metadata to send when RPC completes.

        Args:
            trailing_metadata: List of metadata key-value pairs
        """
        self._trailing_metadata = trailing_metadata

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        """Abort the RPC with a status code and message.

        Args:
            code: The status code
            details: Error message

        Raises:
            Exception: Always raises to simulate RPC abortion
        """
        self._code = code
        self._details = details
        self._is_active = False
        self._invoke_callbacks()
        msg = f"RPC aborted: {code} - {details}"
        raise Exception(msg)

    def abort_with_status(self, status: grpc.Status) -> None:
        """Abort the RPC with a status object.

        Args:
            status: The status object containing code and details

        Raises:
            Exception: Always raises to simulate RPC abortion
        """
        self.abort(status.code, status.details)

    def _invoke_callbacks(self) -> None:
        """Invoke all registered callbacks."""
        for callback in self._callbacks:
            try:
                callback()
            except Exception:
                pass  # Ignore callback errors in tests

    def get_code(self) -> grpc.StatusCode:
        """Get the current status code (testing helper).

        Returns:
            The current status code
        """
        return self._code

    def get_details(self) -> str:
        """Get the current error details (testing helper).

        Returns:
            The current error details
        """
        return self._details


class AsyncStubWrapper:
    """Wraps a sync gRPC stub so its methods return awaitables.

    grpc_testing stubs return sync (non-awaitable) protobuf responses,
    but async gRPC client code uses ``await stub.Method(request)``.
    This wrapper makes each stub method return an awaitable.
    """

    def __init__(self, sync_stub: Any) -> None:
        self._stub = sync_stub

    def __getattr__(self, name: str) -> Callable[..., Any]:
        method = object.__getattribute__(self, "_stub")
        method = getattr(method, name)  # noqa: B009

        async def async_method(*args: Any, **kwargs: Any) -> Any:
            return method(*args, **kwargs)

        return async_method
