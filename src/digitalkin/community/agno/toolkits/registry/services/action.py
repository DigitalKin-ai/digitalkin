"""Actions for the ``services_manager`` dispatcher.

Adds the service-specific actions to the shared CRUD + search set:

- ``create`` — create a shareable service from a name + configuration JSON;
- ``structure`` — show a service's key paths and what lives at each, without its content;
- ``load`` — return the service's stored JSON configuration content, whole or one key of
  it (distinct from Tool.load, which loads a live tool into the agent);
- ``patch`` / ``append`` / ``remove`` — change one existing element, add one (or several list
  items, or text at the end of a text), or take one out, without resending the whole
  configuration; each cuts a new version like ``update``;
- ``generate`` — write a large configuration part by part, each part by its own model call,
  and save it as one version.

``get`` answers a service without its configuration, and ``load`` pages a large list or text.
Every read-modify-write runs under a per-service lock, so parallel writes never overwrite each other.

Every write (``create``, ``update``, ``patch``, ``append``, ``remove``, ``generate``) answers a
:class:`ServiceReceipt`, never the configuration: echoing a large service after each write of
an edit loop would fill the model's context.
"""

from __future__ import annotations

import asyncio
import copy
import json
import re
import time
import weakref
from abc import abstractmethod
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal

from agno.exceptions import RunCancelledException
from agno.models.message import Message
from pydantic import BaseModel, Field, model_validator
from semantic_text_splitter import TextSplitter

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
from digitalkin.logger import logger
from digitalkin.models.settings.services_manager import get_services_manager_settings
from digitalkin.utils.json_structure import JsonStructure
from digitalkin.utils.setup_content_validator import SetupContentValidator

if TYPE_CHECKING:
    from agno.models.base import Model

    from digitalkin.community.agno.toolkits.registry.base import RegistryActionCtx
    from digitalkin.services.setup.setup_strategy import SetupData, SetupVersionData


class ServiceReceipt(BaseModel):
    """What a service write answers instead of the service: where it wrote, not what.

    Attributes:
        setup_id: The written service.
        setup_version_id: The version the write cut: the one the service serves, or the staged one.
        version: That version's label.
        path: The written element's key path (``products[3]`` for an item pushed onto
            ``products``); empty for a write of the whole configuration.
        content_chars: Size of the saved configuration, in JSON characters.
    """

    setup_id: str
    setup_version_id: str
    version: str
    path: str
    content_chars: int

    @classmethod
    def of(
        cls, setup: SetupData, path: str, content: dict[str, Any], version: SetupVersionData | None = None
    ) -> ServiceReceipt:
        """Describe a saved write.

        Args:
            setup: The setup the write returned.
            path: The written element's key path; empty for the whole configuration.
            content: The configuration the write saved.
            version: The version the write cut, when it is not the one the setup serves (a staged write).

        Returns:
            The receipt.
        """
        version = version or setup.current_setup_version
        return cls(
            setup_id=setup.id,
            setup_version_id=version.id,
            version=version.version,
            path=path,
            content_chars=len(json.dumps(content, ensure_ascii=False)),
        )


class LiteralEscapes(BaseModel):
    """Refuse a written text a model escaped twice, unless the call says its backslashes are meant."""

    literal_escapes: bool = Field(
        default=False,
        description="True only when the written text means its backslash sequences: code (print('a\\nb')), "
        "a Windows path, a regex. Leave false for prose: a text holding \\n or \\u00e9 written out as "
        "characters, with no real line break or accent, is then refused as escaped twice.",
    )

    @model_validator(mode="before")
    @classmethod
    def _text_is_not_escaped(cls, data: Any) -> Any:
        r"""Refuse a ``content``, ``value`` or ``items`` text escaped twice (``\\n`` written out).

        Returns:
            The call unchanged when no written text looks escaped twice or ``literal_escapes`` is set.
        """
        if isinstance(data, dict):
            if data.get("literal_escapes"):
                logger.info("literal_escapes set: written backslash sequences kept as sent")
            else:
                for name in ("content", "value", "items"):
                    if name in data:
                        SetupContentValidator.reject_escaped_text(data[name])
        return data


class GetServiceAction(GetAction):
    """Fetch one service by id, with its current version, status and visibility, without its configuration."""

    structure: bool = Field(
        default=True,
        description="Include the structure map (true by default). A map too large to return is refused: then "
        "call get with structure=false, and read the map with the structure action, paged, or search_structure.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Read the service and replace its configuration by its size, and its map by its count on request.

        A large service's configuration would fill the model's context; ``load`` reads it, by key or by page.

        Returns:
            The setup as ``get`` answers it, its ``current_setup_version`` carrying ``content_chars``
            instead of ``content``, and ``structure_entries`` instead of ``structure`` when ``structure``
            is false.

        Raises:
            ValueError: ``structure`` is true and the map is larger than ``structure_max_chars``.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        version = setup.current_setup_version
        stored = version.structure or {}
        if self.structure:
            chars = len(json.dumps(stored, ensure_ascii=False))
            budget = get_services_manager_settings().structure_max_chars
            if chars > budget:
                msg = (
                    f"The structure has {len(stored)} entries, {chars} characters, too large to return with get "
                    f"(limit {budget}): call get with structure=false, then the structure action with offset and "
                    "limit (or search_structure)."
                )
                raise ValueError(msg)
            data = setup.model_dump(mode="json", exclude={"current_setup_version": {"content"}})
        else:
            data = setup.model_dump(mode="json", exclude={"current_setup_version": {"content", "structure"}})
            data["current_setup_version"]["structure_entries"] = len(stored)
        data["current_setup_version"]["content_chars"] = len(json.dumps(version.content, ensure_ascii=False))
        return data


class CreateServiceAction(RegistryAction, LiteralEscapes):
    """Create a shareable service other kins can discover.

    Only a name and the configuration JSON are needed — owner, organisation and kind
    are derived server-side. Once created it is discoverable via ``search`` and
    readable via ``load``.
    The service is always created *private* (owner only): visibility is not a creation
    parameter. Widening it to ``internal`` (whole organisation) or ``public`` (everyone)
    requires a separate ``change_visibility`` call.

    Scope: creates a new service: its name, its documentation, its whole configuration and its structure map, as
    its first version.
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
            The receipt of the created service.
        """
        setup = await ctx.setup.create_service_setup(
            self.name,
            self.content,
            documentation=self.documentation,
            structure={JsonStructure.canonical(self.content, path): text for path, text in self.structure.items()},
        )
        return ServiceReceipt.of(setup, "", self.content)


class StructureServiceAction(RegistryAction):
    """Show a service's shape — key paths and what lives at each — without its content.

    Use it on a setup id you already hold; ``search`` already returns the same map on
    every row, so there is no need to call this straight after a search. Feed a key from
    it to ``load`` to read just that part of the configuration. A large map is read page by
    page, or one branch at a time with ``prefix``.
    """

    display_name = "Structure"
    action: Literal["structure"] = "structure"
    setup_id: str = Field(..., description="The service setup id whose shape to show.")
    prefix: str | None = Field(
        default=None,
        min_length=1,
        description="Show only one branch: the entry at this key path and every entry under it "
        "(e.g. 'catalog' keeps 'catalog', 'catalog.products', 'catalog[0]', not 'catalogs').",
    )
    offset: int | None = Field(
        default=None,
        ge=0,
        description="Page the map: the first entry to return, entries sorted by key path. The answer gives "
        "the total and the next offset. A map too large to return whole is refused unless you page it.",
    )
    limit: int | None = Field(
        default=None, ge=1, description="How many entries the page holds. Omit it for as many as fit in one page."
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Return the stored ``{key path: summary}`` map, one branch of it, or a page of it.

        The map is the one written by whoever created or updated the setup, read from the
        current version rather than from the search index, which may lag a write. A setup
        stored before the map existed has an empty one.

        Returns:
            The map (or its branch) when it fits ``structure_max_chars`` of ``ServicesManagerSettings``,
            else with ``offset`` / ``limit`` a ``{entries, offset, total, next_offset}`` page sorted by path.

        Raises:
            ValueError: An unpaged map (or branch) larger than one page.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        branch = self.prefix
        entries = {
            path: text
            for path, text in (setup.current_setup_version.structure or {}).items()
            if branch is None or path == branch or path.startswith((f"{branch}.", f"{branch}["))
        }
        budget = get_services_manager_settings().structure_max_chars
        if self.offset is None and self.limit is None:
            chars = len(json.dumps(entries, ensure_ascii=False))
            if chars <= budget:
                return entries
            msg = (
                f"The structure has {len(entries)} entries, {chars} characters, too large to return whole "
                f"(limit {budget}): page it with offset and limit, narrow it with prefix, or find entries "
                "with search_structure."
            )
            raise ValueError(msg)
        offset = self.offset or 0
        page, next_offset = JsonStructure.page(dict(sorted(entries.items())), offset, self.limit, budget)
        return {"entries": page, "offset": offset, "total": len(entries), "next_offset": next_offset}


class SearchStructureServiceAction(RegistryAction):
    """Find the entries of a service's structure map by the words of their key path or description."""

    display_name = "Search structure"
    action: Literal["search_structure"] = "search_structure"
    setup_id: str = Field(..., description="The service setup id whose structure to search.")
    query: str = Field(
        ...,
        pattern=r"\S",
        description="Words to find, case-insensitive; an entry matches when every word is in its key path "
        "or its description (e.g. 'products sku').",
    )
    offset: int = Field(default=0, ge=0, description="The first matching entry to return, matches sorted by key path.")
    limit: int | None = Field(
        default=None, ge=1, description="How many entries the page holds. Omit it for as many as fit in one page."
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Return one page of the map entries matching every word of ``query``.

        Returns:
            ``{entries, offset, total, next_offset}``, entries sorted by path, ``total`` the number of matches.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        words = self.query.lower().split()
        matches = dict(
            sorted(
                (path, text)
                for path, text in (setup.current_setup_version.structure or {}).items()
                if all(word in f"{path} {text}".lower() for word in words)
            )
        )
        budget = get_services_manager_settings().structure_max_chars
        page, next_offset = JsonStructure.page(matches, self.offset, self.limit, budget)
        return {"entries": page, "offset": self.offset, "total": len(matches), "next_offset": next_offset}


class LoadServiceAction(RegistryAction):
    """Load a service's configuration — one key from its structure, or the whole document — or a page of it."""

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
    offset: int | None = Field(
        default=None,
        ge=0,
        description="Page the list, the text or the object at key (the whole configuration without a key): the "
        "first item, character or key (keys sorted) to return. The answer gives the total and the next offset. "
        "A part too large to return whole is refused unless you page it.",
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="How many items, characters or keys the page holds. Omit it for as many as fit in one page.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:  # ruff: ignore[too-many-locals] - one paging path per kind
        """Return the service's configuration, the part ``key`` names, or a page of it.

        Guards the object type first: without it ``load`` would happily return a tool's
        internal configuration — the most dangerous type-confusion, since the response
        carries no field the caller could use to notice it read the wrong kind. The read
        itself passes ``key`` to the setup service, so the narrowing happens where the
        document lives; a key the configuration does not have is refused as not found,
        which is what the wire does. A part larger than one page (``load_page_chars`` of
        ``ServicesManagerSettings``) is refused unless the call pages it. A text page ends at
        the most natural boundary that fits (paragraph, sentence, word) and shares its end with
        the start of the next page (``text_overlap_chars``).

        Returns:
            The part of the configuration ``key`` names, the whole configuration without one, or a
            ``{page, offset, total, next_offset}`` page of a list or an object, or of a text with a
            ``part`` saying which characters it holds and what follows.

        Raises:
            ValueError: An unpaged part larger than one page, a list item or object value larger than
                one page, or ``offset`` / ``limit`` on a scalar.
        """
        await ctx.ensure_kind(self.setup_id)
        content = (
            await ctx.setup.get_setup({"setup_id": self.setup_id, "structure_key": self.key or ""})
        ).current_setup_version.content
        # The setup service answers a keyed read as ``{key: part}``, whatever the key's depth.
        part = content.get(self.key) if self.key else content
        page_chars, offset = get_services_manager_settings().load_page_chars, self.offset or 0
        if self.offset is None and self.limit is None:
            chars = len(json.dumps(content, ensure_ascii=False))
            if chars <= page_chars:
                return content
            msg = (
                f"{self.key or 'The configuration'} is "
                + (
                    f"a list of {len(part)} items"
                    if isinstance(part, list)
                    else f"a text of {len(part)} characters"
                    if isinstance(part, str)
                    else f"an object of {len(part)} keys"
                    if isinstance(part, dict)
                    else "a value"
                )
                + f" ({chars} characters as JSON), too large to return whole "
                f"(limit {page_chars}): page it with offset and limit, or load a narrower key from the structure."
            )
            raise ValueError(msg)
        if isinstance(part, str):
            capacity = min(page_chars, (self.limit or page_chars) + 2)
            lead = min(get_services_manager_settings().text_overlap_chars, capacity // 4) if offset < len(part) else 0
            # The page opens on the end of the previous one: at most ``lead`` characters that weigh at most a
            # quarter page as JSON (escapes count), from the first word inside them.
            low = offset - JsonStructure.fit(part[max(0, offset - lead) : offset], capacity // 4 + 2, tail=True)
            word = re.search(r"\s\S", part[low:offset])
            start = low + word.start() + 1 if word else low
            room = capacity - len(json.dumps(part[start:offset], ensure_ascii=False)) + 2
            # The splitter ends the page at the most natural boundary that fits (paragraph, sentence, word), sized
            # as JSON so escapes count; a window of two pages is enough to find it.
            chunk = next(
                iter(
                    TextSplitter.from_callback(
                        lambda piece: len(json.dumps(piece, ensure_ascii=False)), room, trim=False
                    ).chunks(part[offset : offset + 2 * capacity])
                ),
                "",
            )
            if len(json.dumps(chunk, ensure_ascii=False)) > room:
                # The splitter hands back a short text whole, whatever its JSON size: cut it to the room left,
                # on its last word boundary when there is one, and never below one character.
                low = max(1, JsonStructure.fit(chunk, room))
                space = max(chunk.rfind(" ", 0, low), chunk.rfind("\n", 0, low))
                cut = space + 1 if space >= low // 2 else low
                # TODO(validate): TEXT-PAGE-FIT no text page is larger than its budget as JSON
                logger.info(
                    "[VALIDATE TEXT-PAGE-FIT] text page at %d cut from %d to %d characters (JSON budget %d)",
                    offset,
                    len(chunk),
                    cut,
                    room,
                )
                chunk = chunk[:cut]
            end = offset + len(chunk)
            # A full page moves on by its capacity less the repeated lead; one that ends early on a boundary says
            # nothing about the next ones, so it never sizes the estimate.
            more = -(-(len(part) - end) // (capacity - lead))
            # TODO(validate): PAGE-ESTIMATE the "more pages" of a text page matches the pages read after it
            logger.info("[VALIDATE PAGE-ESTIMATE] text page %d-%d of %d: about %d more", start, end, len(part), more)
            return {
                "page": part[start:end],
                "part": f"characters {start}-{end} of {len(part)}"
                + (
                    f"; its first {offset - start} characters repeat the end of the previous page"
                    if start < offset
                    else ""
                )
                + ("; last page" if end >= len(part) else f"; about {more} more pages; the next page starts at {end}"),
                "offset": start,
                "total": len(part),
                "next_offset": end if end < len(part) else None,
            }
        if isinstance(part, list | dict):
            page, next_offset = JsonStructure.page(part, offset, self.limit, page_chars)
            # A page holds one item even when it alone is over the budget, so paging progresses; that item
            # is never handed back whole: the model is told which key to load and page instead.
            if len(page) == 1:
                name, value = next(iter(page.items())) if isinstance(page, dict) else (offset, page[0])
                chars = len(json.dumps(value, ensure_ascii=False))
                if chars > page_chars:
                    msg = (
                        f"{JsonStructure.child(self.key, name)} alone is {chars} characters, more than one page "
                        f"(limit {page_chars}): load key {JsonStructure.child(self.key, name)} and page it with "
                        f"offset and limit, then continue here at offset {offset + 1}."
                    )
                    raise ValueError(msg)
            return {"page": page, "offset": offset, "total": len(part), "next_offset": next_offset}
        # A ValueError, not a TypeError: it is the model's request that is wrong, answered as a plain refusal.
        msg = "offset and limit page a list, a text or an object; load a key holding one"
        raise ValueError(msg)


class UpdateServiceAction(UpdateAction, LiteralEscapes):
    """Update a service, refreshing the key map alongside the configuration.

    The map belongs to the content it describes, and an update rewrites all of it, so the call
    carries a new map, as create does; the stored one is replaced by it.

    Scope: rewrites the whole configuration and the whole structure map, and sets the name (required: send the
    current one to keep it); the documentation changes only when given. Cuts a new version; visibility and
    history are kept.
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
            The ``structure`` entry for the update payload, list indices in the bracket form.
        """
        return {
            "structure": {JsonStructure.canonical(self.content, path): text for path, text in self.structure.items()}
        }

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Cut the new version under the service's lock, then describe it instead of echoing it.

        Returns:
            The receipt of the new version.
        """
        async with ctx.lock(self.setup_id):
            saved = await super().execute(ctx)
            version = await ctx.written_version(saved, current=self.set_as_current)
        return ServiceReceipt.of(saved, "", self.content, version)


class ServiceRevisionAction(RegistryAction):
    """Base of every partial service write: change part of a version and cut a new version from it.

    The new version inherits everything the call does not change — the rest of the content,
    the name, the documentation and the key map entries the call does not touch — so the caller
    sends only what changes. Abstract: each subclass edits the content and the map its own way.
    """

    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The service setup id to edit.")
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
    def _bracket(self, content: dict[str, Any]) -> None:
        """Rewrite the call's paths with a dotted list index the grammar's way (``items.0`` -> ``items[0]``).

        Args:
            content: The base version's configuration, which says where a list is.
        """

    @abstractmethod
    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> tuple[str, dict[str, str]]:
        """Edit ``content`` in place and return where, with the map describing the result.

        Args:
            content: A copy of the base version's configuration.
            structure: The base version's stored map; left untouched.

        Returns:
            The edited element's path and the map for the new version.
        """

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Edit the base version and cut a new version from it.

        The read and the write run under the service's lock, so a parallel edit starts from this one's result.

        Returns:
            The receipt of the new version.

        Raises:
            ValueError: ``setup_version_id`` belongs to another setup, or the edit is refused.
        """
        async with ctx.lock(self.setup_id):
            setup = await ctx.ensure_kind(self.setup_id)
            base = setup.current_setup_version
            if self.setup_version_id:
                base = await ctx.setup.get_setup_version({"setup_version_id": self.setup_version_id})
                if base.setup_id != self.setup_id:
                    msg = f"{self.setup_version_id} is not a version of {self.setup_id}; refused"
                    raise ValueError(msg)
            content = copy.deepcopy(base.content)
            self._bracket(content)
            path, structure = self._edit(content, base.structure or {})
            content = SetupContentValidator.reject_unsafe_keys(content)
            saved = await ctx.setup.update_setup({
                "setup_id": self.setup_id,
                "name": setup.name,
                "content": content,
                "set_as_current": self.set_as_current,
                "documentation": base.documentation,
                "structure": structure,
            })
            version = await ctx.written_version(saved, current=self.set_as_current)
        return ServiceReceipt.of(saved, path, content, version)


class ServiceEditAction(ServiceRevisionAction):
    """Base of ``patch``, ``append`` and ``remove``: edit the one element at ``path``."""

    path: str = Field(
        ...,
        min_length=1,
        description="Key path of the element, in the structure map's syntax: nested keys joined "
        'with "." (llm.model), a key containing . [ ] " or \\\\ bracket-quoted (limits["max.tokens"]), '
        "a list element indexed from 0 (tools[0]).",
    )

    def _bracket(self, content: dict[str, Any]) -> None:
        """Rewrite ``path`` with its list indices in the bracket form.

        Args:
            content: The base version's configuration.
        """
        self.path = JsonStructure.canonical(content, self.path)


class ServiceWriteAction(ServiceEditAction, LiteralEscapes):
    """Base of ``patch`` and ``append``: write ``value`` at ``path``, with the map entries describing it.

    Abstract: each subclass decides how ``value`` lands in the content.
    """

    # Typed, not Any: an untyped field let a model send a text escaped a second time (DEV-949 run).
    value: str | int | float | bool | list[Any] | dict[str, Any] | None = Field(
        ...,
        description="The JSON value to write there; null is written as null. A text is sent as plain text, "
        "with real line breaks and accents, never as an escaped copy.",
    )
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

    def _bracket(self, content: dict[str, Any]) -> None:
        """Rewrite ``path`` and the ``structure`` keys with their list indices in the bracket form.

        Args:
            content: The base version's configuration.
        """
        super()._bracket(content)
        self.structure = {JsonStructure.canonical(content, path): text for path, text in self.structure.items()}

    @abstractmethod
    def _apply(self, content: dict[str, Any]) -> str:
        """Write ``value`` into ``content`` in place.

        Args:
            content: A copy of the base version's configuration.

        Returns:
            The path of the written element, whose stored map entries the call's own replace.
        """

    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> tuple[str, dict[str, str]]:
        """Write ``value``, then swap the element's stored entries for the call's own.

        Args:
            content: A copy of the base version's configuration.
            structure: The base version's stored map.

        Returns:
            The written element's path, and the stored entries outside it plus the call's entries.

        Raises:
            ValueError: A ``structure`` key does not name the written element or a key inside it.
        """
        written = self._apply(content)
        misplaced = JsonStructure.misplaced(self.structure, content, written)
        if misplaced:
            msg = (
                f"structure keys {misplaced} do not name the written element {written!r} or a key inside "
                "it; nothing was written. To describe other parts without rewriting them, use update_structure"
            )
            raise ValueError(msg)
        return written, {**JsonStructure.prune(structure, written), **self.structure}


class PatchServiceAction(ServiceWriteAction):
    """Change one existing element of a service's configuration, without resending the rest.

    ``path`` must already exist — a key, a whole section or a list item; it is replaced by
    ``value``. To add something new use ``append``; to rewrite the whole configuration use
    ``update``.

    Scope: the configuration at ``path`` only (the element is replaced) and the structure map entries at or
    under ``path``. The name, the documentation, the rest of the configuration and the other map entries are
    kept. Cuts a new version.
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
    """Add to a service's configuration, without resending the rest.

    A ``path`` naming a list adds ``value`` at its end, or each of ``items`` in order. A text
    ``value`` on a ``path`` holding a text is added at its end. Otherwise ``path`` must not exist
    yet: it is created, with any missing parent objects (a list holding ``items``). To change an
    element that already exists use ``patch``.

    Scope: the configuration at ``path`` only (a new key, list items, or text added at the end of a text) and
    the structure map entries describing what is added. Never changes an existing value; the name, the
    documentation, the rest of the configuration and the other map entries are kept. Cuts a new version.
    """

    display_name = "Append"
    action: Literal["append"] = "append"
    value: str | int | float | bool | list[Any] | dict[str, Any] | None = Field(
        default=None,
        description="The one JSON value to add (null is written as null), or a text to add at the end of the "
        "text at path, sent as plain text with real line breaks and accents. Omit it when you send items.",
    )
    items: list[Any] | None = Field(
        default=None,
        min_length=1,
        description="Several items to push, in order, onto the list at path (created when path does not "
        "exist yet), instead of one value. Describe the list once in structure, keyed by path, rather than "
        "each item.",
    )

    @model_validator(mode="after")
    def check_one_payload(self) -> AppendServiceAction:
        """Require exactly one of ``value`` and ``items``.

        Returns:
            The validated action.

        Raises:
            ValueError: Both or neither are given.
        """
        if ("value" in self.model_fields_set) == (self.items is not None):
            msg = "append takes either value or items, not both and not neither"
            raise ValueError(msg)
        return self

    def _apply(self, content: dict[str, Any]) -> str:
        """Add ``value`` at ``path``.

        Returns:
            The path of the added element (``tools[2]`` for an item pushed onto ``tools``).
        """
        return JsonStructure.append(content, self.path, self.value)

    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> tuple[str, dict[str, str]]:
        """Push ``items`` or extend a text, keeping every stored entry; otherwise add one element.

        Args:
            content: A copy of the base version's configuration.
            structure: The base version's stored map.

        Returns:
            The path of the last pushed item or of the extended text, and the stored map plus the call's
            entries.

        Raises:
            ValueError: ``items`` on a path holding something other than a list, or a ``structure`` key
                outside ``path``.
        """
        current = JsonStructure.resolve(content, [self.path]).get(self.path)
        if self.items is None:
            if not isinstance(current, str) or not isinstance(self.value, str):
                return super()._edit(content, structure)
            JsonStructure.patch(content, self.path, current + self.value)
            written = self.path
        elif isinstance(current, list):
            for item in self.items:
                written = JsonStructure.append(content, self.path, item)
        else:
            JsonStructure.append(content, self.path, list(self.items))
            written = f"{self.path}[{len(self.items) - 1}]"
        misplaced = JsonStructure.misplaced(self.structure, content, self.path)
        if misplaced:
            msg = f"structure keys {misplaced} do not name {self.path!r} or a key inside it; nothing was written"
            raise ValueError(msg)
        return written, {**structure, **self.structure}


class RemoveServiceAction(ServiceEditAction):
    """Remove one element of a service's configuration — a key or a list item — without resending the rest.

    ``path`` must exist. Its structure map entries go with it; removing a list item moves the
    later items down one index, and their entries follow. To remove the whole service use
    ``delete``.

    Scope: the configuration at ``path`` only (the key or list item is removed) and its structure map entries;
    the entries of the later items of a list are renumbered. The name, the documentation and the rest of the
    configuration are kept. Cuts a new version.
    """

    display_name = "Remove"
    action: Literal["remove"] = "remove"

    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> tuple[str, dict[str, str]]:
        """Remove the element at ``path`` and update the map to match.

        Args:
            content: A copy of the base version's configuration.
            structure: The base version's stored map.

        Returns:
            ``path``, and the map without the element's entries, later list items renumbered.
        """
        JsonStructure.remove(content, self.path)
        return self.path, JsonStructure.forget(structure, self.path)


class UpdateStructureServiceAction(ServiceRevisionAction):
    """Complete or correct a service's structure map without touching its configuration.

    The entries are merged into the stored map: a key already described gets the new
    description, the others are kept, and an empty description removes the entry. Every
    described path must exist in the configuration. Send large maps in batches of about 100
    entries; each call cuts a new version with the same content.

    Scope: only the structure map. The configuration, the name and the documentation are copied unchanged into
    the new version.
    """

    display_name = "Update structure"
    action: Literal["update_structure"] = "update_structure"
    entries: dict[str, str] = Field(
        ...,
        min_length=1,
        description="Map entries to merge: key path (copied from the configuration's own keys, in the "
        "structure map's syntax, e.g. products[12]) -> what lives there. An empty description removes "
        "that entry. Paths not given keep their entry.",
    )

    def _bracket(self, content: dict[str, Any]) -> None:
        """Rewrite the ``entries`` keys with their list indices in the bracket form.

        Args:
            content: The base version's configuration.
        """
        self.entries = {JsonStructure.canonical(content, path): text for path, text in self.entries.items()}

    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> tuple[str, dict[str, str]]:
        """Merge ``entries`` into the map, leaving the content as it is.

        Args:
            content: A copy of the base version's configuration; only read.
            structure: The base version's stored map.

        Returns:
            An empty path (the whole configuration) and the merged, clipped map.

        Raises:
            ValueError: A described path does not exist in the configuration; nothing is written.
        """
        described = [path for path, text in self.entries.items() if text]
        found = JsonStructure.resolve(content, described)
        unknown = [path for path in described if path not in found]
        if unknown:
            msg = (
                f"{len(unknown)} described paths are not in the configuration: {unknown[:20]}; copy paths from "
                "the configuration (load or structure) and send the call again; nothing was written"
            )
            raise ValueError(msg)
        merged = dict(structure)
        for path, text in self.entries.items():
            if text:
                merged[path] = text
            elif path in merged:
                logger.info("update_structure: map entry removed: %s = %r", path, merged.pop(path))
        return "", JsonStructure.clip(merged)


class ServiceSection(BaseModel):
    """One part of a configuration that ``generate`` has written by its own model call."""

    path: str = Field(
        ...,
        min_length=1,
        description="Where the part goes, in the structure-map grammar: a key that does not exist yet "
        "(created, with its parent objects), a list (the part's items are pushed onto it — several sections "
        "may fill one list), or an existing key (replaced).",
    )
    instructions: str = Field(
        ...,
        min_length=1,
        description="What this part must contain, precisely enough for a writer who sees only the brief, "
        "the outline of all parts and these instructions. Size it under 15 000 characters of JSON (about 12 "
        "catalog items of 1 000 characters).",
    )
    summary: str = Field(
        ...,
        description="The structure-map entry for path: one line on what lives there. A list that already has "
        "an entry keeps it, so describe the whole list, not this slice.",
    )
    at: int | None = Field(
        default=None,
        ge=0,
        description="Only to redo a failed slice of a list: the index where its items go, the `at` of its "
        "`failed` entry. Omit to push at the end of the list.",
    )


class GenerateReceipt(ServiceReceipt):
    """What ``generate`` answers: the receipt of the saved version, each written part, and each failed one.

    Attributes:
        sections: Per written part, its ``section`` index, ``path``, JSON ``chars`` and ``items`` (a list
            reply's length).
        failed: Per failed part, its ``section`` index, ``path``, ``instructions``, ``reason`` and ``at`` (where
            a list slice belonged, ``None`` for any other part), so the parts to redo are known without reading
            the service back.
        hint: What to do about the failed parts; ``None`` when none failed.
    """

    sections: list[dict[str, Any]]
    failed: list[dict[str, Any]]
    hint: str | None = None


class GenerateServiceAction(RegistryAction, LiteralEscapes):
    """Write a large configuration part by part, each by its own model call, and save it as one version.

    For a service too large for one call: describe it once in ``brief`` and split it into
    ``sections``. Each section is written by a separate model call that sees only the brief, the
    outline of all sections and its own instructions, so every call stays small; the parts are
    merged into the current version and saved once.

    Scope: the configuration at each section's ``path`` (pushed onto a list, replaced, or created) and one
    structure map entry per section. The name, the documentation, the rest of the configuration and the other
    map entries are kept. Cuts one new version for all sections.
    """

    display_name = "Generate"
    action: Literal["generate"] = "generate"
    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The service to write into; create a small skeleton first.")
    brief: str = Field(
        ...,
        min_length=1,
        description="What the whole configuration is for and the conventions every part shares (ids, units, "
        "language, field names). Every writer reads it.",
    )
    sections: list[ServiceSection] = Field(
        ...,
        min_length=1,
        description="The parts to write, in order. No part may lie inside another one; several parts may share "
        "the path of a list to fill it.",
    )
    gates: ClassVar[weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]] = (
        weakref.WeakKeyDictionary()
    )

    @model_validator(mode="after")
    def _sections_do_not_nest(self) -> GenerateServiceAction:
        """Refuse a section inside another one: the outer write would replace the inner one.

        Returns:
            The action unchanged.

        Raises:
            ValueError: A section path lies inside another section's path.
        """
        pair = JsonStructure.nested([section.path for section in self.sections])
        if pair is not None:
            msg = (
                f"section {pair[1]!r} lies inside section {pair[0]!r}, which would replace it: "
                "write the outer part alone, or split it into parts that do not contain each other"
            )
            raise ValueError(msg)
        return self

    async def _write(self, writer: Model, gate: asyncio.Semaphore, outline: str, section: ServiceSection) -> Any:
        """Have ``writer`` produce one section's JSON value, retrying once on a reply that is not JSON.

        The reply is streamed: the proxy cuts a call that sends nothing for 120 s, and a section written as
        one block arrives only at the end.

        Args:
            writer: The model that writes the section.
            gate: Bounds the concurrent writer calls.
            outline: Every section's path and summary, one per line.
            section: The section to write.

        Returns:
            The section's JSON value.

        Raises:
            ValueError: The writer replied twice with no valid JSON.
        """
        messages = [
            Message(
                role="system",
                content="You write ONE part of a larger JSON configuration. Reply with that part's JSON value "
                "only: no prose, no code fence. Stay under 15 000 characters.",
            ),
            Message(
                role="user",
                content=f"Brief of the whole configuration:\n{self.brief}\n\nOutline (other writers handle the "
                f"other parts):\n{outline}\n\nYour part goes at `{section.path}`.\n{section.instructions}",
            ),
        ]
        async with gate:
            started = time.monotonic()
            for _ in range(2):
                text = "".join([
                    chunk.content
                    async for chunk in writer.ainvoke_stream(list(messages), Message(role="assistant"))
                    if isinstance(chunk.content, str)
                ]).strip()
                text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
                try:
                    value = json.loads(text)
                except json.JSONDecodeError as error:
                    failure = error
                    messages += [
                        Message(role="assistant", content=text[-2000:]),
                        Message(role="user", content=f"That was not valid JSON ({error}). Reply with the JSON only."),
                    ]
                    continue
                # TODO(validate): WRITER-STREAM sections stream; with proxy keepalive no 504 at 120 s
                logger.info(
                    "[VALIDATE WRITER-STREAM] generate: section %s written in %.0fs (%d chars)",
                    section.path,
                    time.monotonic() - started,
                    len(text),
                )
                return value
        msg = f"the writer replied twice with no valid JSON ({failure})"
        raise ValueError(msg)

    def _merge(
        self, content: dict[str, Any], structure: dict[str, str], replies: list[Any]
    ) -> tuple[dict[str, str], list[dict[str, Any]], list[dict[str, Any]]]:
        """Place every written section into ``content``, in order, and describe each in the map.

        Args:
            content: A copy of the version current at save time; edited in place.
            structure: That version's map.
            replies: Per section, its JSON value or the error its writer raised.

        Returns:
            The map for the new version, each written section and each failed one.
        """
        written: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        # Per list path, the (at, item count) of each insertion so far: a later ``at`` counts them.
        inserted: dict[str, list[tuple[int, int]]] = {}
        for index, (section, value) in enumerate(zip(self.sections, replies, strict=True)):
            current = JsonStructure.resolve(content, [section.path]).get(section.path)
            at = (
                None
                if section.at is None
                else section.at + sum(count for start, count in inserted.get(section.path, []) if start <= section.at)
            )
            error = value if isinstance(value, BaseException) else None
            if (
                error is None
                and not isinstance(current, list)
                and any(item["path"] == section.path for item in written)
            ):
                error = ValueError(
                    f"another section of this call already wrote {section.path!r}; sections share a path only to "
                    "fill a list"
                )
            if error is None:
                try:
                    SetupContentValidator.reject_unsafe_keys({section.path: value})
                    if not self.literal_escapes:
                        SetupContentValidator.reject_escaped_text({section.path: value})
                    replaced = JsonStructure.place(content, section.path, value, at)
                except ValueError as refused:
                    error = refused
            if error is not None:
                reason = f"{type(error).__name__}: {error}"
                slot = at if at is not None else len(current) if isinstance(current, list) else None
                logger.warning(
                    "generate: section %d at %s failed (%s), list slot %s: %s",
                    index,
                    section.path,
                    reason,
                    slot,
                    section.instructions,
                )
                failed.append({
                    "section": index,
                    "path": section.path,
                    "instructions": section.instructions,
                    "reason": reason,
                    "at": slot,
                })
                continue
            if section.at is not None:
                inserted.setdefault(section.path, []).append((section.at, len(value) if isinstance(value, list) else 1))
                # TODO(validate): GENERATE-AT a redone list slice lands where it belonged
                logger.info("[VALIDATE GENERATE-AT] generate: section %d inserted at %s[%d]", index, section.path, at)
            if replaced:
                structure = {**JsonStructure.prune(structure, section.path), section.path: section.summary}
            else:
                # A section pushing one slice onto a described list keeps the list's description.
                structure.setdefault(section.path, section.summary)
            written.append({
                "section": index,
                "path": section.path,
                "chars": len(json.dumps(value, ensure_ascii=False)),
                "items": len(value) if isinstance(value, list) else None,
            })
        return structure, written, failed

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Write every section concurrently, then merge them into the current version and save it once.

        Sections are written at most ``DIGITALKIN_SERVICES_MANAGER_WRITER_CONCURRENCY`` at a time. The merge
        runs under the service's lock on the version current at save time, so an edit made meanwhile is kept. A failed
        section is reported and logged, never dropped in silence; a cancelled mission saves nothing.

        Returns:
            The receipt of the new version, each written section and each failed one.

        Raises:
            ValueError: No writer model is bound, or every section failed.
        """
        await ctx.ensure_kind(self.setup_id)
        if ctx.writer is None:
            msg = "generate needs a model to write the sections and none is bound; write the parts with append"
            raise ValueError(msg)
        loop = asyncio.get_running_loop()
        gate = self.gates.get(loop)
        if gate is None:
            gate = self.gates[loop] = asyncio.Semaphore(get_services_manager_settings().writer_concurrency)
        outline = "\n".join(f"- {section.path}: {section.summary}" for section in self.sections)
        replies = await asyncio.gather(
            *(self._write(ctx.writer, gate, outline, section) for section in self.sections), return_exceptions=True
        )
        cancelled = next(
            (reply for reply in replies if isinstance(reply, asyncio.CancelledError | RunCancelledException)), None
        )
        if cancelled is not None:
            raise cancelled
        async with ctx.lock(self.setup_id):
            setup = await ctx.ensure_kind(self.setup_id)
            base = setup.current_setup_version
            content = copy.deepcopy(base.content)
            structure = dict(base.structure or {})
            structure, written, failed = self._merge(content, structure, replies)
            if not written:
                msg = f"every section failed, nothing was saved: {failed}"
                raise ValueError(msg)
            saved = await ctx.setup.update_setup({
                "setup_id": self.setup_id,
                "name": setup.name,
                "content": content,
                "set_as_current": True,
                "documentation": base.documentation,
                "structure": JsonStructure.clip(structure),
            })
        return GenerateReceipt(
            **ServiceReceipt.of(saved, "", content).model_dump(),
            sections=written,
            failed=failed,
            hint="The configuration was saved without the failed sections. Call generate again with only those, "
            "reusing their instructions and passing each one's `at` (split a large one in two); no need to load "
            "the service to find them."
            if failed
            else None,
        )


class SetServiceVersionAction(SetVersionAction):
    """Activate one of a service's existing versions — the way to undo a bad write.

    Takes a ``setup_version_id`` from ``list_versions``; it creates nothing, so rolling forward again is just
    another ``set_version`` on the newer id. Nothing is lost either way.

    Scope: only which existing version is active. No version is created or edited.
    """

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Make an existing version the current one, then describe it instead of echoing its configuration.

        Returns:
            The receipt of the activated version.
        """
        saved = await super().execute(ctx)
        # TODO(validate): SET-VERSION-RECEIPT a service set_version answers a receipt, not the version's content
        logger.info(
            "[VALIDATE SET-VERSION-RECEIPT] set_version: %s now serves %s", self.setup_id, self.setup_version_id
        )
        return ServiceReceipt.of(saved, "", saved.current_setup_version.content)


ServiceActions = Annotated[
    GetServiceAction
    | CreateServiceAction
    | SearchAction
    | StructureServiceAction
    | SearchStructureServiceAction
    | LoadServiceAction
    | UpdateServiceAction
    | PatchServiceAction
    | AppendServiceAction
    | RemoveServiceAction
    | UpdateStructureServiceAction
    | GenerateServiceAction
    | DeleteAction
    | ChangeVisibilityAction
    | ListVersionsAction
    | SetServiceVersionAction,
    Field(discriminator="action"),
]
