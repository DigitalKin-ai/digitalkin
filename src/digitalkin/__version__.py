"""Version information."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("digitalkin")
except PackageNotFoundError:
    __version__ = "0.2.20"
