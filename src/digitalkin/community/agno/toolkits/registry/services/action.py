"""Actions for the ``services_manager`` dispatcher.

Adds the two service-specific actions to the shared CRUD + search set:

- ``create`` — create a shareable service from a name + configuration JSON;
- ``structure`` — show a service's key paths and what lives at each, without its content;
- ``load`` — return the service's stored JSON configuration content, whole or one key of
  it (distinct from Tool.load, which loads a live tool into the agent).
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
from digitalkin.utils.json_structure import JsonStructure

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
    documentation: str = Field(
        default="",
        description="Free text describing what this service is for and when to use it. It is "
        "what ``search`` matches on and previews back, so a service created without it is "
        "findable only by name.",
    )
    structure: dict[str, str] = Field(
        ...,
        description="Map of key path -> one-line summary of what lives at it. This is what "
        "another agent reads to decide which part of the configuration to fetch, so summarise "
        "what the key is FOR, not what its value is, and keep every line far shorter than the "
        "content it describes. Cover each key worth finding on its own; point at a whole "
        "section instead when it is only ever read as a unit. Path syntax: join nested keys "
        'with "." (llm.model); bracket-quote a key containing . [ ] " or \\ '
        '(limits["max.tokens"]); index a list element (tools[0]). Paths that do not exist in '
        "content are dropped.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Create the service setup from its name, content, documentation and authored key map.

        Returns:
            The created service setup.
        """
        return await ctx.setup.create_service_setup(
            self.name, self.content, documentation=self.documentation, structure=self.structure
        )


class StructureServiceAction(RegistryAction):
    """Show a service's shape — key paths and what lives at each — without its content.

    Use it on a setup id you already hold; ``search`` already returns the same map on
    every row, so there is no need to call this straight after a search. Feed a key from
    it to ``load`` to read just that part of the configuration.
    """

    action: Literal["structure"] = "structure"
    setup_id: str = Field(..., description="The service setup id whose shape to show.")

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Return the stored ``{key path: summary}`` map, with no configuration content.

        The map is written by whoever created or updated the setup, and the only read that
        carries it is the setup search — so this restates a search for one id rather than
        deriving a map locally, which would discard the written summaries. A setup stored
        before the map existed returns an empty one.

        Returns:
            The key map, or an empty dict when the setup carries none.
        """
        await ctx.ensure_kind(self.setup_id)
        found = await ctx.registry.search_setups(setup_ids=[self.setup_id], module_types=[ctx.module_type], limit=1)
        return found[0].structure if found else {}


class LoadServiceAction(RegistryAction):
    """Load a service's configuration — one key from its structure, or the whole document."""

    action: Literal["load"] = "load"
    setup_id: str = Field(..., description="The service setup id to load (from a search result).")
    key: str | None = Field(
        default=None,
        description="One key path copied verbatim from the service's structure map, to read just "
        "that part of the configuration. Omit to load the whole document — do that only when the "
        "configuration is small or genuinely needed in full.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Return the service's configuration content, or the part ``key`` names.

        Guards the object type first: without it ``load`` would happily return a tool's
        internal configuration — the most dangerous type-confusion, since the response
        carries no field the caller could use to notice it read the wrong kind. That guard
        already reads the whole setup, so the key is applied to the content in hand rather
        than costing a second scoped fetch.

        Returns:
            The value at ``key``, or the whole configuration JSON object when no key is given.

        Raises:
            ValueError: ``key`` names a path the configuration does not have.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        content = setup.current_setup_version.content
        if self.key is None:
            return content
        found = JsonStructure.resolve(content, [self.key])
        if self.key not in found:
            msg = f"{self.key!r} is not a key of this configuration; copy one from its structure"
            raise ValueError(msg)
        return found[self.key]


ServiceActions = Annotated[
    GetAction
    | CreateServiceAction
    | SearchAction
    | StructureServiceAction
    | LoadServiceAction
    | UpdateAction
    | DeleteAction
    | ChangeVisibilityAction
    | ListVersionsAction
    | SetVersionAction,
    Field(discriminator="action"),
]
