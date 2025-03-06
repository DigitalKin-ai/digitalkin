"""This module is responsible for handling the identity service."""

from .default_identity import DefaultIdentity
from .identity_strategy import IdentityStrategy

__all__ = ["DefaultIdentity", "IdentityStrategy"]
