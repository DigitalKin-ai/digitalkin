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
        max_length=300,
        description="Free text describing what this service is for and when to use it, at most "
        "300 characters. It is what ``search`` matches on and returns, so a service created "
        "without it is findable only by name.",
    )
    structure: dict[str, str] = Field(
        ...,
        description="Map of key path -> one-line summary of what lives at it. This is what "
        "another agent reads to decide which part of the configuration to fetch, so summarise "
        "what the key is FOR, not what its value is, and keep every line far shorter than the "
        "content it describes. Cover each key worth finding on its own; point at a whole "
        "section instead when it is only ever read as a unit. Path syntax: join nested keys "
        'with "." (llm.model); bracket-quote a key containing . [ ] " or \\ '
        '(limits["max.tokens"]); index a list element (tools[0]). Keep each description under '
        "512 characters — longer ones are trimmed.",
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
        return await ctx.structure_of(self.setup_id)


class LoadServiceAction(RegistryAction):
    """Load a service's configuration — one key from its structure, or the whole document."""

    action: Literal["load"] = "load"
    setup_id: str = Field(..., description="The service setup id to load (from a search result).")
    key: str | None = Field(
        default=None,
        description="One key path copied verbatim from the service's structure map, to read just "
        "that part of the configuration. Copy it, do not compose it: a key the configuration "
        "does not have fails with not found. Omit the key to load everything deliberately, "
        "when the configuration is small or genuinely needed in full.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Return the service's configuration content, or the part ``key`` names.

        Guards the object type first: without it ``load`` would happily return a tool's
        internal configuration — the most dangerous type-confusion, since the response
        carries no field the caller could use to notice it read the wrong kind. The read
        itself passes ``key`` to the setup service, so the narrowing happens where the
        document lives; a key the configuration does not have is refused as not found,
        which is what the wire does.

        Returns:
            The part of the configuration ``key`` names, or the whole configuration without one.
        """
        await ctx.ensure_kind(self.setup_id)
        setup = await ctx.setup.get_setup({"setup_id": self.setup_id, "structure_key": self.key or ""})
        return setup.current_setup_version.content


class UpdateServiceAction(UpdateAction):
    """Update a service, refreshing the key map alongside the configuration.

    The map belongs to the content it describes, so a new revision needs a new one; the
    stored map is replaced by whatever this call carries.
    """

    structure: dict[str, str] | None = Field(
        default=None,
        description="Refreshed map of key path -> one-line description of what lives at that "
        "leaf, describing the content this call carries. Same form and same 512-character limit "
        "as on create. Omitting it leaves the new revision without a map, so other agents can no "
        "longer see the service's shape — resupply it whenever the service had one.",
    )

    def _type_payload(self) -> dict[str, Any]:
        """Carry the refreshed map onto the new revision.

        Returns:
            The ``structure`` entry for the update payload.
        """
        return {"structure": self.structure}


ServiceActions = Annotated[
    GetAction
    | CreateServiceAction
    | SearchAction
    | StructureServiceAction
    | LoadServiceAction
    | UpdateServiceAction
    | DeleteAction
    | ChangeVisibilityAction
    | ListVersionsAction
    | SetVersionAction,
    Field(discriminator="action"),
]
