"""This module contains objects for storage strategies."""

import datetime
from enum import Enum

from agentic_mesh_protocol.storage.v1.storage_enums_pb2 import DataType as DataTypeProto
from pydantic import BaseModel, Field

from digitalkin.services.base_enum import BaseEnum


class DataType(BaseEnum[DataTypeProto], Enum):
    """Enum defining the types of data that can be stored."""

    UNSPECIFIED = "UNSPECIFIED"
    OUTPUT = "OUTPUT"
    VIEW = "VIEW"
    LOGS = "LOGS"
    OTHER = "OTHER"


class StorageRecord(BaseModel):
    """A single record stored in a collection, with metadata."""

    mission_id: str = Field(..., description="ID of the mission (bucket) this doc belongs to")
    collection: str = Field(..., description="Logical collection name")
    record_id: str = Field(..., description="Unique ID of this record in its collection")
    data_type: DataType = Field(default=DataType.OUTPUT, description="Category of the data of this record")
    data: BaseModel = Field(..., description="The typed payload of this record")
    created_at: datetime.datetime | None = Field(default=None, description="When this record was first created")
    updated_at: datetime.datetime | None = Field(default=None, description="When this record was last modified")
