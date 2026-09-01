"""CRUD actions shared by the three Registry Toolkit managers (Tools / Services / Kins).

``SearchAction`` reads setups of the manager's ``module_type`` from the registry;
``UpdateAction`` / ``DeleteAction`` / ``ChangeVisibilityAction`` / ``SetVersionAction`` write
through the setup service, and ``ListVersionsAction`` reads its version history. Each manager
composes its own action union from these (plus type-specific actions such as service
create/load).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import Field

from digitalkin.community.agno.toolkits.registry.base import RegistryAction
from digitalkin.logger import logger
from digitalkin.models.services.registry import (
    RegistrySetupStatus,
    RegistrySortBy,
    RegistryVisibility,
)

if TYPE_CHECKING:
    from digitalkin.community.agno.toolkits.registry.base import RegistryActionCtx


class GetAction(RegistryAction):
    """Fetch one instance by id, with its current version content, status and visibility."""

    action: Literal["get"] = "get"
    setup_id: str = Field(..., description="The setup id to fetch.")

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Read the setup (always its current version), refusing a foreign object type.

        Returns:
            The setup with its current version, status and visibility.
        """
        return await ctx.ensure_kind(self.setup_id)


class SearchAction(RegistryAction):
    """Semantic search over ready-to-use instances of this object type (configured setups).

    This is a SEMANTIC (nearest-match) search, never exhaustive: it returns the closest setups
    regardless of how weak the match is, so a non-empty result does NOT mean anything matched the
    query, and an empty result only means the whole corpus is empty. Do NOT use it to test whether
    a specific setup exists — fetch it by id with ``get`` (which returns a clean not-found instead).
    The index is also eventually consistent: right after a write (create/update/delete) results
    may briefly lag, so to read a change you just made use ``get`` rather than re-searching.

    Every filter the registry accepts is exposed here except the object type, which is fixed to
    this manager's own kind. Filters combine with AND; within one filter the values are OR'd.
    """

    _DOC_PREVIEW_CHARS: ClassVar[int] = 300
    # The service ceiling itself (storage and registry both cap a page at 100), so the toolkit
    # no longer imposes a tighter one of its own.
    _MAX_RESULTS: ClassVar[int] = 100

    action: Literal["search"] = "search"
    query: str = Field(default="", description="Free text matched against name and documentation.")
    setup_ids: list[str] | None = Field(
        default=None, description="Restrict to these setup ids. Omit for no restriction."
    )
    module_ids: list[str] | None = Field(
        default=None, description="Restrict to setups backed by these module ids. Omit for no restriction."
    )
    statuses: list[RegistrySetupStatus] | None = Field(
        default=None,
        description="Filter by setup status. Omit for the invocable ones (ready, configuration_succeeded); "
        'pass e.g. ["failed"] to inspect broken setups.',
    )
    visibilities: list[RegistryVisibility] | None = Field(
        default=None, description="Filter by visibility (public / private / internal). Omit for no filter."
    )
    tags: list[str] | None = Field(
        default=None,
        description="Match setups carrying AT LEAST ONE of these tags (case-insensitive). Omit for no filter.",
    )
    sort_by: RegistrySortBy = Field(
        default=RegistrySortBy.UNSPECIFIED,
        description="Sort key. Omit to let the registry choose (relevance when a query is set).",
    )
    descending: bool = Field(default=False, description="Reverse the sort order.")
    limit: int = Field(default=10, description=f"Max results (default 10, max {_MAX_RESULTS}).", ge=1, le=_MAX_RESULTS)
    offset: int = Field(default=0, description="Skip this many matches before returning results.", ge=0)

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Search invocable setups of the manager's type and trim each row for the LLM.

        Returns:
            ``{"total_returned", "truncated", "offset", "setups": [...]}``.
        """
        cap = min(max(self.limit, 1), self._MAX_RESULTS)
        # Fetch one extra row so ``truncated`` can mean "a further row exists", not merely "this
        # page is full": a page holding exactly ``cap`` rows is only truncated if a next one
        # exists. ``_MAX_RESULTS`` is now the service ceiling, so at ``cap == _MAX_RESULTS`` there
        # is no room for that probe row — clamp, and ``truncated`` reads false on a full last page
        # rather than the call being refused as out of range.
        setups = await ctx.registry.search_setups(
            query=self.query,
            setup_ids=self.setup_ids,
            module_ids=self.module_ids,
            # ``module_types`` is the one filter the caller cannot set: it is this manager's type
            # boundary, the same one ``ensure_kind`` enforces on the id-targeting actions. Letting
            # it through would turn tools_manager into a way to enumerate kins.
            module_types=[ctx.module_type],
            statuses=self.statuses or [RegistrySetupStatus.READY, RegistrySetupStatus.CONFIGURATION_SUCCEEDED],
            visibilities=self.visibilities,
            tags=self.tags,
            sort_by=self.sort_by,
            limit=min(cap + 1, self._MAX_RESULTS),
            offset=self.offset,
            descending=self.descending,
        )
        # A setup without a version is a non-instantiable record (e.g. a create that failed
        # mid-write leaving a versionless entity): it is indexed but can't be read or loaded, so
        # drop it here rather than surface a ``version:null`` row the caller can't act on.
        usable = [setup for setup in setups if setup.setup_version]
        # ``truncated`` is a genuine next-page signal read on the RENDERED (post-filter) rows: true
        # only when a further usable row exists beyond the page. Tying it to the raw backend count
        # would both contradict ``total_returned`` and, at exactly ``cap`` rows, promise an empty
        # next page.
        truncated = len(usable) > cap
        rows = [
            {
                "setup_id": setup.setup_id,
                "name": setup.name,
                "module_name": setup.module_name,
                "version": setup.setup_version,
                # Echoed because they are filterable: a caller cannot use the ``tags``,
                # ``visibilities`` or ``statuses`` filters without first seeing the values in use.
                "tags": setup.tags,
                "visibility": setup.visibility.value if setup.visibility else None,
                "status": setup.status.value if setup.status else None,
                "description": (setup.documentation or "")[: self._DOC_PREVIEW_CHARS],
            }
            for setup in usable[:cap]
        ]
        return {"total_returned": len(rows), "truncated": truncated, "offset": self.offset, "setups": rows}


class UpdateAction(RegistryAction):
    """Update an instance: cut a new version of its configuration, and rename it.

    The previous configuration is not overwritten — it stays in the version history, so an
    update that turns out to be wrong can be undone with ``set_version``. Use
    ``list_versions`` to find the id to go back to.
    """

    action: Literal["update"] = "update"
    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The setup id to update.")
    name: str = Field(..., description="New name.")
    content: dict[str, Any] = Field(
        ...,
        min_length=1,
        description="The new configuration payload for the version being cut (a non-empty JSON "
        "object). Note: JSON numbers round-trip as floats over the wire.",
    )
    set_as_current: bool = Field(
        default=True,
        description="Activate the new version immediately (the default). Pass false to stage it "
        "without changing what the instance currently serves, then activate it later with "
        "``set_version``.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Cut a new version of the setup's content and rename it.

        Guards the object type first (which also refuses a deleted target, since the setup service
        excludes deleted ids), then validates ``content`` against the module's config schema so a
        missing/wrong field is refused with a correctable message before the write.

        Returns:
            The updated setup.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        await ctx.validate_content(setup.module_id, self.content)
        return await ctx.setup.update_setup({
            "setup_id": self.setup_id,
            "name": self.name,
            "content": self.content,
            "set_as_current": self.set_as_current,
        })


class DeleteAction(RegistryAction):
    """Delete an instance by id (a soft delete: it disappears from ``search``).

    Only instances of this manager's own object type can be deleted — deleting a
    setup of another type is refused before any destructive call. Two limits of the
    current backend to be aware of:

    - deleting a **non-existent** or **already-deleted** id returns "not found"
      (a deleted id is no longer resolvable, so re-deleting is not a silent no-op);
    - once deleted, the id is **no longer retrievable** via ``get`` or ``load``.

    A setup whose backing module the registry cannot resolve has no knowable type, so every
    other action refuses it. Delete accepts it anyway — otherwise such a record could never be
    removed by anyone.
    """

    action: Literal["delete"] = "delete"
    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The setup id to delete.")

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Delete the setup (soft delete via the setup service).

        Guards the object type first — a mutation must not cross the type boundary any
        more than a read does — then deletes. ``orphan_ok`` makes the one exception: a setup
        whose module cannot be resolved has no type to cross, and refusing it would strand the
        record permanently.

        Returns:
            ``True`` on success.
        """
        await ctx.ensure_kind(self.setup_id, orphan_ok=True)
        return await ctx.setup.delete_setup({"setup_id": self.setup_id})


class ChangeVisibilityAction(RegistryAction):
    """Change who can see and use an instance."""

    action: Literal["change_visibility"] = "change_visibility"
    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The setup id whose visibility to change.")
    visibility: Literal["public", "private", "internal"] = Field(
        ...,
        description='"public" (everyone), "private" (owner only) or "internal" (whole organisation).',
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Change the setup's visibility scope.

        Guards the object type first (which also refuses a deleted target), then writes and
        re-reads the committed state.

        Returns:
            The setup with its updated visibility.
        """
        await ctx.ensure_kind(self.setup_id)
        await ctx.setup.change_visibility({"setup_id": self.setup_id, "visibility": self.visibility})
        # change_visibility composes its response from a snapshot read before the write, so a
        # concurrent update makes it echo a stale version/content. Re-read the committed state so the
        # response reflects the write (and any concurrent one), not a pre-write in-memory object.
        return await ctx.setup.get_setup({"setup_id": self.setup_id})


class ListVersionsAction(RegistryAction):
    """List an instance's configuration history, most recent first.

    Every ``update`` cuts a new version rather than overwriting the old one, so this is how
    you find an earlier configuration to go back to — pair it with ``set_version``. Rows are
    metadata only: use ``get`` to read the configuration the instance currently serves.
    """

    _MAX_VERSIONS: ClassVar[int] = 100

    action: Literal["list_versions"] = "list_versions"
    setup_id: str = Field(..., description="The setup id whose version history to list.")
    limit: int = Field(
        default=20,
        description=f"Max versions to return (default 20, max {_MAX_VERSIONS}).",
        ge=1,
        le=_MAX_VERSIONS,
    )
    offset: int = Field(default=0, description="Skip this many versions before returning results.", ge=0)

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """List the setup's versions, refusing a foreign object type.

        Guards the object type first: a version history is as much this manager's business as
        the setup itself, so ``kins_manager`` must not enumerate a tool's revisions.

        Returns:
            ``{"total_count", "returned", "offset", "current_setup_version_id", "versions": [...]}``.
        """
        await ctx.ensure_kind(self.setup_id)
        page = await ctx.setup.list_setup_versions({
            "setup_id": self.setup_id,
            "limit": self.limit,
            "offset": self.offset,
        })
        # Metadata only — the payloads are full configurations, and dumping every historical
        # revision into the context window is exactly what this two-step surface exists to avoid.
        versions = [
            {
                "setup_version_id": version.id,
                "version": version.version,
                "created_at": version.creation_date.isoformat(),
                "is_current": version.id == page.current_setup_version_id,
            }
            for version in page.setup_versions
        ]
        return {
            "total_count": page.total_count,
            "returned": len(versions),
            "offset": self.offset,
            "current_setup_version_id": page.current_setup_version_id,
            "versions": versions,
        }


class SetVersionAction(RegistryAction):
    """Activate one of an instance's existing versions — the way to undo a bad ``update``.

    Takes a ``setup_version_id`` from ``list_versions``; it does not create anything, so
    rolling forward again is just another ``set_version`` on the newer id. Nothing is lost
    either way.
    """

    action: Literal["set_version"] = "set_version"
    # Marks the call as state-mutating so the dispatcher invalidates the servicer's setup cache.
    # Without it the rollback would commit while running jobs kept resolving the version this
    # replaced — succeeding server-side and appearing to have done nothing.
    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The setup id whose active version to change.")
    setup_version_id: str = Field(
        ..., description="The version to activate, from a ``list_versions`` ``setup_version_id``."
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Make an existing version the current one.

        Guards the object type first — a mutation must not cross the type boundary — then
        activates. A version id belonging to another setup is refused by the setup service.

        Returns:
            The setup with its newly activated version.
        """
        await ctx.ensure_kind(self.setup_id)
        return await ctx.setup.set_current_setup_version({
            "setup_id": self.setup_id,
            "setup_version_id": self.setup_version_id,
        })
