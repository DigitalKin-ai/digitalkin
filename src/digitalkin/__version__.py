"""Version information."""

import os
import sys
from importlib.metadata import PackageNotFoundError, version

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
try:
    __version__ = version("digitalkin")
except PackageNotFoundError:
    __version__ = "0.1.1"
