"""."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class TaskStatus(Enum):
    """."""

    PENDING = "pending"
    RUNNING = "running"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


class SignalType(Enum):
    """."""

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
    """."""

    task_id: str = Field(..., description="Unique identifier for the task")
    status: TaskStatus = Field(..., description="Current status of the task")
    action: SignalType = Field(..., description="Type of signal action")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] = Field(default={}, description="Optional payload for the signal")
    model_config = {"use_enum_values": True}


class HeartbeatMessage(BaseModel):
    """."""

    task_id: str = Field(..., description="Unique identifier for the task")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
