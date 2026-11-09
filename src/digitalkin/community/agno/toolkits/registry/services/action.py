"""Actions for the ``services_manager`` dispatcher.

Adds the two service-specific actions to the shared CRUD + search set:

- ``create`` — create a shareable service from a name + configuration JSON;
- ``structure`` — show a service's key paths and what lives at each, without its content;
- ``load`` — return the service's stored JSON configuration content, whole or one key of
  it (distinct from Tool.load, which loads a live tool into the agent);
- ``patch`` / ``append`` / ``remove`` — change one existing element, add one, or take one
  out, without resending the whole configuration; each cuts a new version like ``update``.
"""

from __future__ import annotations

import copy
from abc import abstractmethod
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
from digitalkin.utils.setup_content_validator import SetupContentValidator

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

    display_name = "Create"
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

    display_name = "Structure"
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

    display_name = "Load"
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

    The map belongs to the content it describes, and an update rewrites all of it, so the call
    carries a new map, as create does; the stored one is replaced by it.
    """

    structure: dict[str, str] = Field(
        ...,
        description="Map of key path -> one-line description of what lives at that leaf, describing "
        "the content this call carries. Same form and same 512-character limit as on create. It "
        "replaces the stored map, so resupply every entry still worth finding.",
    )

    def _type_payload(self) -> dict[str, Any]:
        """Carry the refreshed map onto the new revision.

        Returns:
            The ``structure`` entry for the update payload.
        """
        return {"structure": self.structure}


class ServiceEditAction(RegistryAction):
    """Base of ``patch``, ``append`` and ``remove``: edit one element and cut a new version.

    The new version inherits everything the call does not change — the rest of the content,
    the name, the documentation and the key map entries outside the element — so the caller
    sends only the element. Abstract: each subclass edits the content and the map its own way.
    """

    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The service setup id to edit.")
    path: str = Field(
        ...,
        min_length=1,
        description="Key path of the element, in the structure map's syntax: nested keys joined "
        'with "." (llm.model), a key containing . [ ] " or \\ bracket-quoted (limits["max.tokens"]), '
        "a list element indexed from 0 (tools[0]).",
    )
    setup_version_id: str | None = Field(
        default=None,
        description="The version to edit, from a ``list_versions`` ``setup_version_id``. Omit to edit "
        "the version the service currently serves.",
    )
    set_as_current: bool = Field(
        default=True,
        description="Activate the new version immediately (the default). Pass false to stage it, then "
        "activate it later with ``set_version``.",
    )

    @abstractmethod
    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> dict[str, str]:
        """Edit ``content`` in place and return the map describing the result.

        Args:
            content: A copy of the base version's configuration.
            structure: The base version's stored map; left untouched.

        Returns:
            The map for the new version.
        """

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Edit the base version's content at ``path`` and cut a new version from it.

        Returns:
            The updated setup.

        Raises:
            ValueError: ``setup_version_id`` belongs to another setup, or the edit is refused.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        base = setup.current_setup_version
        if self.setup_version_id:
            base = await ctx.setup.get_setup_version({"setup_version_id": self.setup_version_id})
            if base.setup_id != self.setup_id:
                msg = f"{self.setup_version_id} is not a version of {self.setup_id}; refused"
                raise ValueError(msg)
        content = copy.deepcopy(base.content)
        structure = self._edit(content, base.structure or {})
        return await ctx.setup.update_setup({
            "setup_id": self.setup_id,
            "name": setup.name,
            "content": SetupContentValidator.reject_unsafe_keys(content),
            "set_as_current": self.set_as_current,
            "documentation": base.documentation,
            "structure": structure,
        })


class ServiceWriteAction(ServiceEditAction):
    """Base of ``patch`` and ``append``: write ``value`` at ``path``, with the map entries describing it.

    Abstract: each subclass decides how ``value`` lands in the content.
    """

    value: Any = Field(..., description="The JSON value to write there; null is written as null.")
    structure: dict[str, str] = Field(
        ...,
        description="Map of key path -> one-line summary for the element this call writes, as on "
        "create: the element itself and the keys inside it worth finding (full paths, e.g. "
        "llm.temperature). An item appended to a list lands at the end: key it by that index "
        "(tools[2] when the list held two items). Every key must name the written element or a key "
        "inside it, or the call is refused with the element's real path. It replaces every stored "
        "entry at or under the edited path; entries elsewhere are kept. Pass {} only when nothing "
        "written is worth describing. Keep each description under 512 characters — longer ones are "
        "trimmed.",
    )

    @abstractmethod
    def _apply(self, content: dict[str, Any]) -> str:
        """Write ``value`` into ``content`` in place.

        Args:
            content: A copy of the base version's configuration.

        Returns:
            The path of the written element, whose stored map entries the call's own replace.
        """

    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> dict[str, str]:
        """Write ``value``, then swap the element's stored entries for the call's own.

        Args:
            content: A copy of the base version's configuration.
            structure: The base version's stored map.

        Returns:
            The stored entries outside the written element, plus the call's entries.

        Raises:
            ValueError: A ``structure`` key does not name the written element or a key inside it.
        """
        written = self._apply(content)
        misplaced = JsonStructure.misplaced(self.structure, content, written)
        if misplaced:
            msg = (
                f"structure keys {misplaced} do not name the written element {written!r} or a key inside "
                "it; nothing was written"
            )
            raise ValueError(msg)
        return {**JsonStructure.prune(structure, written), **self.structure}


class PatchServiceAction(ServiceWriteAction):
    """Change one existing element of a service's configuration, without resending the rest.

    ``path`` must already exist — a key, a whole section or a list item; it is replaced by
    ``value``. To add something new use ``append``; to rewrite the whole configuration use
    ``update``.
    """

    display_name = "Patch"
    action: Literal["patch"] = "patch"

    def _apply(self, content: dict[str, Any]) -> str:
        """Replace the existing element at ``path``.

        Returns:
            ``path``.
        """
        JsonStructure.patch(content, self.path, self.value)
        return self.path


class AppendServiceAction(ServiceWriteAction):
    """Add a new element to a service's configuration, without resending the rest.

    A ``path`` naming a list adds ``value`` at its end. Otherwise ``path`` must not exist yet:
    it is created, with any missing parent objects. To change an element that already exists
    use ``patch``.
    """

    display_name = "Append"
    action: Literal["append"] = "append"

    def _apply(self, content: dict[str, Any]) -> str:
        """Add ``value`` at ``path``.

        Returns:
            The path of the added element (``tools[2]`` for an item pushed onto ``tools``).
        """
        return JsonStructure.append(content, self.path, self.value)


class RemoveServiceAction(ServiceEditAction):
    """Remove one element of a service's configuration — a key or a list item — without resending the rest.

    ``path`` must exist. Its structure map entries go with it; removing a list item moves the
    later items down one index, and their entries follow. To remove the whole service use
    ``delete``.
    """

    display_name = "Remove"
    action: Literal["remove"] = "remove"

    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> dict[str, str]:
        """Remove the element at ``path`` and update the map to match.

        Args:
            content: A copy of the base version's configuration.
            structure: The base version's stored map.

        Returns:
            The map without the element's entries, later list items renumbered.
        """
        JsonStructure.remove(content, self.path)
        return JsonStructure.forget(structure, self.path)


ServiceActions = Annotated[
    GetAction
    | CreateServiceAction
    | SearchAction
    | StructureServiceAction
    | LoadServiceAction
    | UpdateServiceAction
    | PatchServiceAction
    | AppendServiceAction
    | RemoveServiceAction
    | DeleteAction
    | ChangeVisibilityAction
    | ListVersionsAction
    | SetVersionAction,
    Field(discriminator="action"),
]
