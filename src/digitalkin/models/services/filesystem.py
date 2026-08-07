"""Filesystem service models."""

from enum import Enum


class ContextFile(Enum):
    """Owner/scope of a file in the filesystem service.

    Mirrors the filesystem proto context kinds. MISSIONS/SETUP are the read/write
    owner contexts this strategy operates on; USERS/ORGANIZATIONS are read-only
    cross-owner scopes whose concrete id is resolved server-side from the request
    metadata (the client sends only the kind).
    """

    UNSPECIFIED = "unspecified"
    MISSIONS = "mission"
    SETUP = "setup"
    USERS = "user"
    ORGANIZATIONS = "organization"
