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
import os
import time
from abc import abstractmethod
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal

from agno.exceptions import RunCancelledException
from agno.models.message import Message
from pydantic import BaseModel, Field, model_validator

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
from digitalkin.utils.json_structure import JsonStructure
from digitalkin.utils.setup_content_validator import SetupContentValidator

if TYPE_CHECKING:
    from agno.models.base import Model

    from digitalkin.community.agno.toolkits.registry.base import RegistryActionCtx
    from digitalkin.services.setup.setup_strategy import SetupData


class ServiceReceipt(BaseModel):
    """What a service write answers instead of the service: where it wrote, not what.

    Attributes:
        setup_id: The written service.
        setup_version_id: The version the service serves after the write.
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
    def of(cls, setup: SetupData, path: str, content: dict[str, Any]) -> ServiceReceipt:
        """Describe a saved write.

        Args:
            setup: The setup the write returned.
            path: The written element's key path; empty for the whole configuration.
            content: The configuration the write saved.

        Returns:
            The receipt.
        """
        version = setup.current_setup_version
        return cls(
            setup_id=setup.id,
            setup_version_id=version.id,
            version=version.version,
            path=path,
            content_chars=len(json.dumps(content, ensure_ascii=False)),
        )


class GetServiceAction(GetAction):
    """Fetch one service by id, with its current version, status and visibility, without its configuration."""

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Read the service and replace its configuration by its size.

        A large service's configuration would fill the model's context; ``load`` reads it, by key or by page.

        Returns:
            The setup as ``get`` answers it, its ``current_setup_version`` carrying ``content_chars``
            instead of ``content``.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        data = setup.model_dump(mode="json", exclude={"current_setup_version": {"content"}})
        data["current_setup_version"]["content_chars"] = len(
            json.dumps(setup.current_setup_version.content, ensure_ascii=False)
        )
        return data


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
            The receipt of the created service.
        """
        setup = await ctx.setup.create_service_setup(
            self.name, self.content, documentation=self.documentation, structure=self.structure
        )
        return ServiceReceipt.of(setup, "", self.content)


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
        description="Page the list or the text at key: the first item (or character) to return. The answer "
        "gives the total and the next offset.",
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        description="How many items (or characters) the page holds. Omit it for as many as fit in one page.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Return the service's configuration, the part ``key`` names, or a page of it.

        Guards the object type first: without it ``load`` would happily return a tool's
        internal configuration — the most dangerous type-confusion, since the response
        carries no field the caller could use to notice it read the wrong kind. The read
        itself passes ``key`` to the setup service, so the narrowing happens where the
        document lives; a key the configuration does not have is refused as not found,
        which is what the wire does. A part larger than one page (``DK_SERVICE_LOAD_PAGE_CHARS``)
        is never returned whole: a list or a text answers its first page, an object an outline.

        Returns:
            The part of the configuration ``key`` names, the whole configuration without one, a
            ``{page, offset, total, next_offset}`` page, or an ``{outline, hint}`` of a large object.

        Raises:
            TypeError: ``offset`` or ``limit`` on a part that is not a list or a text.
        """
        await ctx.ensure_kind(self.setup_id)
        setup = await ctx.setup.get_setup({"setup_id": self.setup_id, "structure_key": self.key or ""})
        content = setup.current_setup_version.content
        # The setup service answers a keyed read as ``{key: part}``, whatever the key's depth.
        part = content.get(self.key) if self.key else content
        page_chars = int(os.environ.get("DK_SERVICE_LOAD_PAGE_CHARS", "20000"))
        if self.offset is None and self.limit is None:
            chars = len(json.dumps(content, ensure_ascii=False))
            if chars <= page_chars:
                return content
            if isinstance(part, dict):
                outline: dict[str, dict[str, Any]] = {}
                used = 2
                for name, value in part.items():
                    entry = {
                        "type": type(value).__name__,
                        "chars": len(json.dumps(value, ensure_ascii=False)),
                        "items": len(value) if isinstance(value, list | dict) else None,
                    }
                    used += len(json.dumps({name: entry}, ensure_ascii=False))
                    if used > page_chars:
                        break
                    outline[name] = entry
                return {
                    "outline": outline,
                    "more_keys": len(part) - len(outline),
                    "hint": f"{chars} characters, too large to return whole: load one key of the structure map, "
                    "and page a large list or text with offset and limit.",
                }
        offset = self.offset or 0
        if isinstance(part, str):
            end = offset + min(self.limit or page_chars, page_chars)
        elif isinstance(part, list):
            last = len(part) if self.limit is None else min(len(part), offset + self.limit)
            end, used = offset, 2
            for item in part[offset:last]:
                used += len(json.dumps(item, ensure_ascii=False)) + 2 * (end > offset)
                if used > page_chars and end > offset:
                    break
                end += 1
        else:
            msg = "offset and limit page a list or a text; load a key holding one"
            raise TypeError(msg)
        return {
            "page": part[offset:end],
            "offset": offset,
            "total": len(part),
            "next_offset": end if end < len(part) else None,
        }


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

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Cut the new version under the service's lock, then describe it instead of echoing it.

        Returns:
            The receipt of the new version.
        """
        async with ctx.lock(self.setup_id):
            saved = await super().execute(ctx)
        return ServiceReceipt.of(saved, "", self.content)


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
    def _edit(self, content: dict[str, Any], structure: dict[str, str]) -> tuple[str, dict[str, str]]:
        """Edit ``content`` in place and return where, with the map describing the result.

        Args:
            content: A copy of the base version's configuration.
            structure: The base version's stored map; left untouched.

        Returns:
            The edited element's path and the map for the new version.
        """

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Edit the base version's content at ``path`` and cut a new version from it.

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
        return ServiceReceipt.of(saved, path, content)


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
                "it; nothing was written"
            )
            raise ValueError(msg)
        return written, {**JsonStructure.prune(structure, written), **self.structure}


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
    """Add to a service's configuration, without resending the rest.

    A ``path`` naming a list adds ``value`` at its end, or each of ``items`` in order. A text
    ``value`` on a ``path`` holding a text is added at its end. Otherwise ``path`` must not exist
    yet: it is created, with any missing parent objects (a list holding ``items``). To change an
    element that already exists use ``patch``.
    """

    display_name = "Append"
    action: Literal["append"] = "append"
    value: Any = Field(
        default=None,
        description="The one JSON value to add (null is written as null), or a text to add at the end of the "
        "text at path. Omit it when you send items.",
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
        if self.items is None and not (isinstance(current, str) and isinstance(self.value, str)):
            return super()._edit(content, structure)
        if self.items is None:
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


class GenerateReceipt(ServiceReceipt):
    """What ``generate`` answers: the receipt of the saved version, each written part, and each failed one.

    Attributes:
        sections: Per written part, its ``section`` index, ``path``, JSON ``chars`` and ``items`` (a list
            reply's length).
        failed: Per failed part, its ``section`` index, ``path``, ``instructions`` and ``reason``, so the
            parts to redo are known without reading the service back.
        hint: What to do about the failed parts; ``None`` when none failed.
    """

    sections: list[dict[str, Any]]
    failed: list[dict[str, Any]]
    hint: str | None = None


class GenerateServiceAction(RegistryAction):
    """Write a large configuration part by part, each by its own model call, and save it as one version.

    For a service too large for one call: describe it once in ``brief`` and split it into
    ``sections``. Each section is written by a separate model call that sees only the brief, the
    outline of all sections and its own instructions, so every call stays small; the parts are
    merged into the current version and saved once.
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
    sections: list[ServiceSection] = Field(..., min_length=1, description="The parts to write, in order.")

    async def _write(self, writer: Model, gate: asyncio.Semaphore, outline: str, section: ServiceSection) -> Any:
        """Have ``writer`` produce one section's JSON value, retrying once on a reply that is not JSON.

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
                response = await writer.aresponse(messages=list(messages))
                text = str(response.content or "").strip().removeprefix("```json").removeprefix("```")
                text = text.removesuffix("```").strip()
                try:
                    value = json.loads(text)
                except json.JSONDecodeError as error:
                    failure = error
                    messages += [
                        Message(role="assistant", content=text[-2000:]),
                        Message(role="user", content=f"That was not valid JSON ({error}). Reply with the JSON only."),
                    ]
                    continue
                logger.info(
                    "generate: section %s written in %.0fs (%d chars)",
                    section.path,
                    time.monotonic() - started,
                    len(text),
                )
                return value
        msg = f"the writer replied twice with no valid JSON ({failure})"
        raise ValueError(msg)

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Write every section concurrently, then merge them into the current version and save it once.

        Sections are written at most ``DK_SERVICE_WRITER_CONCURRENCY`` at a time. The merge runs under the
        service's lock on the version current at save time, so an edit made meanwhile is kept. A failed
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
        gate = asyncio.Semaphore(int(os.environ.get("DK_SERVICE_WRITER_CONCURRENCY", "4")))
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
            written: list[dict[str, Any]] = []
            failed: list[dict[str, Any]] = []
            for index, (section, value) in enumerate(zip(self.sections, replies, strict=True)):
                error = value if isinstance(value, BaseException) else None
                if error is None:
                    try:
                        SetupContentValidator.reject_unsafe_keys({section.path: value})
                        replaced = JsonStructure.place(content, section.path, value)
                    except ValueError as refused:
                        error = refused
                if error is not None:
                    reason = f"{type(error).__name__}: {error}"
                    logger.warning("generate: section %s failed (%s): %s", section.path, reason, section.instructions)
                    failed.append({
                        "section": index,
                        "path": section.path,
                        "instructions": section.instructions,
                        "reason": reason,
                    })
                    continue
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
            "reusing their instructions (split a large one in two); no need to load the service to find them."
            if failed
            else None,
        )


ServiceActions = Annotated[
    GetServiceAction
    | CreateServiceAction
    | SearchAction
    | StructureServiceAction
    | LoadServiceAction
    | UpdateServiceAction
    | PatchServiceAction
    | AppendServiceAction
    | RemoveServiceAction
    | GenerateServiceAction
    | DeleteAction
    | ChangeVisibilityAction
    | ListVersionsAction
    | SetVersionAction,
    Field(discriminator="action"),
]
