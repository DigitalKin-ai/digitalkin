"""Pydantic models for cost service."""

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal

from agentic_mesh_protocol.cost.v1.cost_enums_pb2 import CostType as CostTypeProto
from pydantic import BaseModel, Field

from digitalkin.models.base_enum import BaseEnum


class CostType(BaseEnum[CostTypeProto], Enum):
    """Enum defining the types of costs that can be registered."""

    OTHER = "OTHER"
    TOKEN_INPUT = "TOKEN_INPUT"
    TOKEN_OUTPUT = "TOKEN_OUTPUT"
    API_CALL = "API_CALL"
    STORAGE = "STORAGE"
    TIME = "TIME"
    CUSTOM = "CUSTOM"


class CostConfig(BaseModel):
    """Pydantic model that defines a cost configuration.

    :param name: Name of the cost (unique identifier in the service).
    :param type: The type/category of the cost.
    :param description: A short description of the cost.
    :param unit: The unit of measurement (e.g. token, call, MB).
    :param rate: The cost per unit (e.g. dollars per token).
    """

    name: str = Field(description="Unique name for the cost configuration")
    type: CostType = Field(description="The type/category of the cost")
    description: str | None = Field(default=None, description="A short description of the cost")
    unit: str = Field(description="The unit of measurement (e.g. token, call, MB)")
    rate: float = Field(description="The cost per unit (e.g. dollars per token)")


class CostData(BaseModel):
    """Data model for cost operations."""

    cost: float = Field(description="The computed cost amount in dollars")
    mission_id: str = Field(description="Identifier for the mission associated with the cost event")
    name: str = Field(description="Identifier for the cost configuration")
    type: CostType = Field(description="The type/category of the cost")
    unit: str = Field(description="The unit of measurement (e.g. token, call, MB)")
    rate: float = Field(description="The cost per unit (e.g. dollars per token)")
    setup_version_id: str = Field(description="Identifier for the setup version associated with the cost event")
    quantity: float = Field(description="The amount or units consumed (e.g. number of tokens, API calls)")


class QuantityLimit(BaseModel):
    """Cost limit based on quantity (e.g., max 10000 tokens)."""

    limit_type: Literal["quantity"] = Field(default="quantity", description="Discriminator for cost limit type")
    name: str = Field(description="Identifier for the cost configuration")
    type: CostType = Field(default=CostType.OTHER, description="The type/category of the cost")
    max_value: float = Field(description="The maximum allowed quantity (e.g. number of tokens, API calls)")


class AmountLimit(BaseModel):
    """Cost limit based on cost amount in dollars (e.g., max $1.00)."""

    limit_type: Literal["amount"] = Field(default="amount", description="Discriminator for cost limit type")
    name: str = Field(description="Identifier for the cost configuration")
    type: CostType = Field(description="The type/category of the cost")
    max_value: float = Field(description="The maximum allowed cost amount in dollars")


class CostEvent(BaseModel):
    """Pydantic model that represents a cost event registered during service execution.

    # DEPRECATED
    :param name: Identifier for the cost configuration.
    :param usage: The amount or units consumed.
    :param amount: The computed cost amount; if not provided it is computed as usage*rate.
    :param timestamp: The time when the cost event was recorded.
    :param metadata: Additional contextual information about the cost event.
    """

    name: str
    usage: float
    amount: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] | None = None


CostLimit = Annotated[QuantityLimit | AmountLimit, Field(discriminator="limit_type")]
