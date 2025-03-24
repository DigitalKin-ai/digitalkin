"""Pydantic models for cost service."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class CostTypeEnum(Enum):
    """Enumeration of supported cost types."""

    TOKEN_INPUT = "token_input"
    TOKEN_OUTPUT = "token_output"
    API_CALL = "api_call"
    STORAGE = "storage"
    TIME = "time"
    CUSTOM = "custom"


class CostConfig(BaseModel):
    """Pydantic model that defines a cost configuration.

    :param cost_name: Name of the cost (unique identifier in the service).
    :param cost_type: The type/category of the cost.
    :param description: A short description of the cost.
    :param unit: The unit of measurement (e.g. token, call, MB).
    :param rate: The cost per unit (e.g. dollars per token).
    """

    name: str
    type: CostTypeEnum
    description: str | None = None
    unit: str
    rate: float


class CostEvent(BaseModel):
    """Pydantic model that represents a cost event registered during service execution.

    :param cost_name: Identifier for the cost configuration.
    :param cost_type: The type of cost.
    :param usage: The amount or units consumed.
    :param cost_amount: The computed cost amount; if not provided it is computed as usage*rate.
    :param timestamp: The time when the cost event was recorded.
    :param metadata: Additional contextual information about the cost event.
    """

    name: str
    usage: float
    amount: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] | None = None
