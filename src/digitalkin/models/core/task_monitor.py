"""Task monitoring models for signaling and heartbeat messages."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(Enum):
    """Task status enumeration."""

    PENDING = "pending"
    RUNNING = "running"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


class CancellationReason(Enum):
    """Reason for task cancellation - helps distinguish cleanup vs real cancellation."""

    # Cleanup cancellations (not errors)
    SUCCESS_CLEANUP = "success_cleanup"  # Main task completed, cleaning up helper tasks
    FAILURE_CLEANUP = "failure_cleanup"  # Main task failed, cleaning up helper tasks

    # Real cancellations
    SIGNAL = "signal"  # External signal requested cancellation
    HEARTBEAT_FAILURE = "heartbeat_failure"  # Heartbeat stopped working
    TIMEOUT = "timeout"  # Task timed out
    SHUTDOWN = "shutdown"  # Manager is shutting down

    # Unknown/unset
    UNKNOWN = "unknown"  # Reason not determined


class SignalType(Enum):
    """Signal type enumeration."""

    START = "start"
    STOP = "stop"
    CANCEL = "cancel"
    PAUSE = "pause"
    RESUME = "resume"
    STATUS = "status"

    ACK_CANCEL = "ack_cancel"
    ACK_PAUSE = "ack_pause"
    ACK_RESUME = "ack_resume"
    ACK_STATUS = "ack_status"


class SignalMessage(BaseModel):
    """Signal message model for task monitoring."""

    task_id: str = Field(..., description="Unique identifier for the task")
    mission_id: str = Field(..., description="Identifier for the mission")
    status: TaskStatus = Field(..., description="Current status of the task")
    action: SignalType = Field(..., description="Type of signal action")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] = Field(default={}, description="Optional payload for the signal")
    model_config = {"use_enum_values": True}


class HeartbeatMessage(BaseModel):
    """Heartbeat message model for task monitoring."""

    task_id: str = Field(..., description="Unique identifier for the task")
    mission_id: str = Field(..., description="Identifier for the mission")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
