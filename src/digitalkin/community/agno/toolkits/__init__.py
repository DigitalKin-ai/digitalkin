"""Default Agno toolkits for DigitalKin modules.

Requires the optional ``agno`` dependency — importing this subpackage without
``agno`` installed raises ModuleNotFoundError. The parent
``digitalkin.community.agno`` package stays importable without agno.
"""

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.community.agno.toolkits.chat_history import ChatHistoryTools
from digitalkin.community.agno.toolkits.defaults import DefaultToolkits
from digitalkin.community.agno.toolkits.registry.kins.kit import KinsManager
from digitalkin.community.agno.toolkits.registry.loader.kit import LoadManager
from digitalkin.community.agno.toolkits.registry.services.kit import ServicesManager
from digitalkin.community.agno.toolkits.registry.tools.kit import ToolsManager
from digitalkin.community.agno.toolkits.user_profile import UserProfileTools

__all__ = [
    "ChatHistoryTools",
    "DefaultToolkits",
    "DkToolkit",
    "KinsManager",
    "LoadManager",
    "ServicesManager",
    "ToolsManager",
    "UserProfileTools",
]
