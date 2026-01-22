"""This module contains objects for cost strategies."""

from enum import Enum

from agentic_mesh_protocol.cost.v1.cost_enums_pb2 import CostType as CostTypeProto
from pydantic import BaseModel

from digitalkin.services.base_enum import BaseEnum


class CostType(BaseEnum[CostTypeProto], Enum):
    """Enum defining the types of costs that can be registered."""

    OTHER = "OTHER"
    TOKEN_INPUT = "TOKEN_INPUT"
    TOKEN_OUTPUT = "TOKEN_OUTPUT"
    API_CALL = "API_CALL"
    STORAGE = "STORAGE"
    TIME = "TIME"


class CostConfig(BaseModel):
    """Pydantic model that defines a cost configuration.

    :param name: Name of the cost (unique identifier in the service).
    :param type: The type/category of the cost.
    :param description: A short description of the cost.
    :param unit: The unit of measurement (e.g. token, call, MB).
    :param rate: The cost per unit (e.g. dollars per token).
    """

    name: str
    type: CostType
    description: str | None = None
    unit: str
    rate: float


class CostData(BaseModel):
    """Data model for cost operations."""

    cost: float
    mission_id: str
    name: str
    type: CostType
    unit: str
    rate: float
    setup_version_id: str
    quantity: float
