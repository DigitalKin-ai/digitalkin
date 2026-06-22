"""Agno framework integration for DigitalKin.

Adapters, converters, and HITL helpers for building DigitalKin modules
on top of the Agno agent framework. Exports:

- :class:`AgnoStreamAdapter` — Agno streaming events → DigitalKin events.
- :class:`AguiTools` — register AG-UI client-side (frontend) tools as Agno
  external Functions.
- :class:`AgnoHitlRunner`, :class:`PausedRunStore`, :class:`PauseInfo`,
  :class:`PausedRunRecord`, :data:`HITL_STORAGE_CONFIG`,
  :class:`HitlEvents` — human-in-the-loop (HITL) runner that persists a
  paused Agno run via the module's
  :class:`~digitalkin.services.storage.StorageStrategy` and resumes it
  when the front replies with a ``ToolMessage``.
"""

from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter
from digitalkin.community.agno.agui_tools import AguiTools
from digitalkin.community.agno.hitl import (
    HITL_STORAGE_CONFIG,
    AgnoHitlRunner,
    HitlEvents,
    PausedRunRecord,
    PausedRunStore,
)
from digitalkin.community.agno.models import PauseInfo

__all__ = [
    "HITL_STORAGE_CONFIG",
    "AgnoHitlRunner",
    "AgnoStreamAdapter",
    "AguiTools",
    "HitlEvents",
    "PauseInfo",
    "PausedRunRecord",
    "PausedRunStore",
]
