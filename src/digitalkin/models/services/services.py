"""Service-strategy execution-mode model."""

from enum import Enum


class ServicesMode(str, Enum):
    """Mode for strategy execution."""

    LOCAL = "local"
    REMOTE = "remote"
