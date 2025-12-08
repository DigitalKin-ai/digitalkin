"""Simple toolkit class with basic and simple API access in the Triggers."""

from digitalkin.mixins.chat_history_mixin import ChatHistoryMixin
from digitalkin.mixins.cost_mixin import CostMixin
from digitalkin.mixins.file_history_mixin import FileHistoryMixin
from digitalkin.mixins.logger_mixin import LoggerMixin


class BaseMixin(CostMixin, ChatHistoryMixin, FileHistoryMixin, LoggerMixin):
    """Base Mixin to access to minimum Module Context functionnalities in the Triggers."""
