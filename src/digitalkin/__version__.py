"""Version information."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("digitalkin")
except PackageNotFoundError:
    __version__ = "0.3.0-rc0"
