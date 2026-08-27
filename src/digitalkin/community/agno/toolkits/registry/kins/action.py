"""Discriminated action union for the ``kins_manager`` dispatcher.

Kins expose the shared CRUD + search actions (no create/load on this surface).
"""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from digitalkin.community.agno.toolkits.registry.action import (
    ChangeVisibilityAction,
    DeleteAction,
    GetAction,
    ListVersionsAction,
    SearchAction,
    SetVersionAction,
    UpdateAction,
)

KinActions = Annotated[
    GetAction
    | SearchAction
    | UpdateAction
    | DeleteAction
    | ChangeVisibilityAction
    | ListVersionsAction
    | SetVersionAction,
    Field(discriminator="action"),
]
