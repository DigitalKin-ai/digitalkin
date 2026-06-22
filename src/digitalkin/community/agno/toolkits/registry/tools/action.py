"""Discriminated action union for the ``tools_manager`` dispatcher.

Tools expose the shared CRUD + search actions. ``create`` is intentionally absent
(tools are not created through this surface) and ``load`` stays a dedicated
external-execution tool for now.
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from digitalkin.community.agno.toolkits.registry.action import (
    ChangeVisibilityAction,
    DeleteAction,
    GetAction,
    SearchAction,
    UpdateAction,
)

ToolActions = Annotated[
    GetAction | SearchAction | UpdateAction | DeleteAction | ChangeVisibilityAction,
    Field(discriminator="action"),
]
