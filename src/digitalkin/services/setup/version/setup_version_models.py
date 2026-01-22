"""This module contains obejct for setup strategies."""

import datetime
from typing import Any

from pydantic import BaseModel


class SetupVersionData(BaseModel):
    """Pydantic model for SetupVersion data validation."""

    id: str
    setup_id: str
    version: str
    content: dict[str, Any]
    created_at: datetime.datetime
