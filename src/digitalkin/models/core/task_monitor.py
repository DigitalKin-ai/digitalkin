"""Task monitoring models for signaling and heartbeat messages."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class CancellationReason(str, Enum):
    """Reason for task termination."""

    COMPLETED = "completed"
    SUCCESS_CLEANUP = "success_cleanup"  # Post-completion, terminating helper tasks
    FAILURE_CLEANUP = "failure_cleanup"  # Post-failure, releasing resources
    SIGNAL_SERVICE_CANCEL = (
        "signal_service_cancel"  # Cancel via SurrealDB live query (StopModule, orchestrator, mission)
    )
    HEARTBEAT_FAILURE = "heartbeat_failure"  # SurrealDB CREATE/MERGE failed (check error code)
    HEARTBEAT_WEBSOCKET_CLOSED = "heartbeat_ws_closed"  # WebSocket closed, keepalive ping timeout
    HEARTBEAT_TIMEOUT = "heartbeat_timeout"  # CREATE/MERGE operation timed out
    HEARTBEAT_CONNECTION_REFUSED = "heartbeat_conn_refused"  # SurrealDB not running
    SURREALDB_HANDSHAKE_TIMEOUT = "surrealdb_handshake_timeout"  # WebSocket handshake timed out
    SURREALDB_CONNECTION_LOST = "surrealdb_conn_lost"  # Connection established then lost
    GRPC_SETUP_UNAVAILABLE = "grpc_setup_unavailable"  # Setup service unreachable at startup
    GRPC_SERVICE_ERROR = "grpc_service_error"  # Service dependency failed during execution
    TIMEOUT = "timeout"  # Task exceeded time limit
    SHUTDOWN = "shutdown"  # TaskManager shutdown (SIGTERM/SIGINT)
    UNKNOWN = "unknown"  # Reason not set - investigate code path


class SignalType(str, Enum):
    """Signal type enumeration."""

    START = "start"
    STOP = "stop"
    CANCEL = "cancel"

    ACK_START = "ack_start"
    ACK_STOP = "ack_stop"
    ACK_CANCEL = "ack_cancel"


class SignalMessage(BaseModel):
    """Signal message model for task monitoring."""

    task_id: str = Field(..., description="Unique identifier for the task")
    mission_id: str = Field(..., description="Identifier for the mission")
    setup_id: str = Field(default="", description="Identifier for the setup")
    setup_version_id: str = Field(default="", description="Identifier for the setup version")
    action: SignalType = Field(..., description="Type of signal action")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] = Field(default_factory=dict, description="Optional payload for the signal")

    cancellation_reason: CancellationReason | None = Field(
        default=None,
        description="Reason for task termination (only set on ACK_STOP/ACK_CANCEL).",
    )
    error_message: str | None = Field(
        default=None,
        description="Human-readable error message if task failed",
    )
    exception_traceback: str | None = Field(
        default=None,
        description="Full traceback if task failed with exception",
    )


class HeartbeatMessage(BaseModel):
    """Heartbeat message model for task monitoring."""

    task_id: str = Field(..., description="Unique identifier for the task")
    mission_id: str = Field(..., description="Identifier for the mission")
    setup_id: str = Field(default="", description="Identifier for the setup")
    setup_version_id: str = Field(default="", description="Identifier for the setup version")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
