"""Storage model."""

from pydantic import BaseModel


class StorageModel(BaseModel):
    """Storage model."""

    data: object
    type: str
