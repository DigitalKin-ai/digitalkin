"""Models for module-to-module (M2M) call state."""

import asyncio
from typing import Any

from google.protobuf import struct_pb2
from pydantic import BaseModel, ConfigDict, Field


class _M2MCallEntry(BaseModel):
    """Per-call rendezvous between the call_module writer and the dial-back reader."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    task_id: str
    query: struct_pb2.Struct
    output_queue: asyncio.Queue[struct_pb2.Struct | None]
    expires_at: float
    target_key: str
    setup_id: str = ""
    mission_id: str = ""
    extra: dict[str, Any] = Field(default_factory=dict)
