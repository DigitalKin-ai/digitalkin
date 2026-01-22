"""This module is responsible for handling the identity service."""

from digitalkin.services.identity.identity_default import DefaultIdentity
from digitalkin.services.identity.identity_strategy import IdentityStrategy

__all__ = ["DefaultIdentity", "IdentityStrategy"]
