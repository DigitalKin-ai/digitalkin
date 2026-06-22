"""Secret model for the EchoModule."""

from pydantic import BaseModel


class EchoSecret(BaseModel):
    """Secret model for the EchoModule.

    This module has no secrets.
    """
