"""Version information."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("digitalkin")
except PackageNotFoundError:
    __version__ = "1.1.0.dev0"
