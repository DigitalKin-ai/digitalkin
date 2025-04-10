"""Types for module models."""

from typing import TypeVar

from pydantic import BaseModel

InputModelT = TypeVar("InputModelT", bound=BaseModel)
OutputModelT = TypeVar("OutputModelT", bound=BaseModel)
SetupModelT = TypeVar("SetupModelT", bound=BaseModel)
SecretModelT = TypeVar("SecretModelT", bound=BaseModel)
