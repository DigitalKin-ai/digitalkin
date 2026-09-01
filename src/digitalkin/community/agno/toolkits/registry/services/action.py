"""Actions for the ``services_manager`` dispatcher.

Adds the two service-specific actions to the shared CRUD + search set:

- ``create`` — create a shareable service from a name + configuration JSON;
- ``load`` — return the service's stored JSON configuration content (distinct from
  Tool.load, which loads a live tool into the agent).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal

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
from digitalkin.community.agno.toolkits.registry.base import RegistryAction

if TYPE_CHECKING:
    from digitalkin.community.agno.toolkits.registry.base import RegistryActionCtx


class CreateServiceAction(RegistryAction):
    """Create a shareable service other kins can discover.

    Only a name and the configuration JSON are needed — owner, organisation and kind
    are derived server-side. Once created it is discoverable via ``search`` and
    readable via ``load``.
    The service is always created *private* (owner only): visibility is not a creation
    parameter. Widening it to ``internal`` (whole organisation) or ``public`` (everyone)
    requires a separate ``change_visibility`` call.
    """

    action: Literal["create"] = "create"
    writes: ClassVar[bool] = True
    name: str = Field(..., description="Human-readable service name.")
    content: dict[str, Any] = Field(
        ...,
        min_length=1,
        description="The service configuration (a non-empty JSON object). "
        "Note: JSON numbers round-trip as floats over the wire.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Create the service setup from its name and content.

        Returns:
            The created service setup.
        """
        return await ctx.setup.create_service_setup(self.name, self.content)


class LoadServiceAction(RegistryAction):
    """Load a service: return its stored JSON configuration content."""

    action: Literal["load"] = "load"
    setup_id: str = Field(..., description="The service setup id to load (from a search result).")

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Return the service's configuration content (latest version).

        Guards the object type first: without it ``load`` would happily return a tool's
        internal configuration — the most dangerous type-confusion, since the response
        carries no field the caller could use to notice it read the wrong kind.

        Returns:
            The service configuration JSON object, or ``None`` when not found.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        return setup.current_setup_version.content


ServiceActions = Annotated[
    GetAction
    | CreateServiceAction
    | SearchAction
    | LoadServiceAction
    | UpdateAction
    | DeleteAction
    | ChangeVisibilityAction
    | ListVersionsAction
    | SetVersionAction,
    Field(discriminator="action"),
]
