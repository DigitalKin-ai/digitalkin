"""Exceptions for the DigitalKin utils package."""


class UnsafePackageError(Exception):
    """Raised when security constraints are violated during package discovery."""


class DiscoveryError(Exception):
    """Raised when discovery fails due to invalid inputs."""
