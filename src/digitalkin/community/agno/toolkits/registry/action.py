"""CRUD actions shared by the three Registry Toolkit managers (Tools / Services / Kins).

``SearchAction`` reads setups of the manager's ``module_type`` from the registry;
``UpdateAction`` / ``DeleteAction`` / ``ChangeVisibilityAction`` write through the setup
service. Each manager composes its own action union from these (plus type-specific
actions such as service create/load).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic import Field

from digitalkin.community.agno.toolkits.registry.base import RegistryAction
from digitalkin.logger import logger
from digitalkin.models.services.registry import RegistrySetupStatus

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
    """

    _DOC_PREVIEW_CHARS: ClassVar[int] = 300
    _MAX_RESULTS: ClassVar[int] = 25

    action: Literal["search"] = "search"
    query: str = Field(description="Free text matched against name and documentation.")
    limit: int = Field(default=10, description=f"Max results (default 10, max {_MAX_RESULTS}).", ge=1, le=25)

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Search invocable setups of the manager's type and trim each row for the LLM.

        Returns:
            ``{"total_returned", "truncated", "setups": [...]}``.
        """
        cap = min(max(self.limit, 1), self._MAX_RESULTS)
        # Fetch one extra row so ``truncated`` can mean "a further row exists", not merely "this
        # page is full": a page holding exactly ``cap`` rows is only truncated if a next one
        # exists. The backend accepts limit 1-100, so ``cap + 1`` (≤ 26) is always in range.
        setups = await ctx.registry.search_setups(
            query=self.query,
            module_types=[ctx.module_type],
            statuses=[RegistrySetupStatus.READY, RegistrySetupStatus.CONFIGURATION_SUCCEEDED],
            limit=cap + 1,
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
                "description": (setup.documentation or "")[: self._DOC_PREVIEW_CHARS],
            }
            for setup in usable[:cap]
        ]
        return {"total_returned": len(rows), "truncated": truncated, "setups": rows}


class UpdateAction(RegistryAction):
    """Update an existing instance's name and current version content."""

    action: Literal["update"] = "update"
    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The setup id to update.")
    name: str = Field(..., description="New name.")
    content: dict[str, Any] = Field(
        ...,
        min_length=1,
        description="The current version's new configuration payload (a non-empty JSON object). "
        "Note: JSON numbers round-trip as floats over the wire.",
    )

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Update the setup's name and current version content.

        Guards the object type first (which also refuses a deleted target, since the setup service
        excludes deleted ids), then validates ``content`` against the module's config schema so a
        missing/wrong field is refused with a correctable message before the write.

        Returns:
            The updated setup.
        """
        setup = await ctx.ensure_kind(self.setup_id)
        await ctx.validate_content(setup.module_id, self.content)
        return await ctx.setup.update_setup({"setup_id": self.setup_id, "name": self.name, "content": self.content})


class DeleteAction(RegistryAction):
    """Delete an instance by id (a soft delete: it disappears from ``search``).

    Only instances of this manager's own object type can be deleted — deleting a
    setup of another type is refused before any destructive call. Two limits of the
    current backend to be aware of:

    - deleting a **non-existent** or **already-deleted** id returns "not found"
      (a deleted id is no longer resolvable, so re-deleting is not a silent no-op);
    - once deleted, the id is **no longer retrievable** via ``get`` or ``load``.
    """

    action: Literal["delete"] = "delete"
    writes: ClassVar[bool] = True
    setup_id: str = Field(..., description="The setup id to delete.")

    async def execute(self, ctx: RegistryActionCtx) -> Any:
        """Delete the setup (soft delete via the setup service).

        Guards the object type first — a mutation must not cross the type boundary any
        more than a read does — then deletes.

        Returns:
            ``True`` on success.
        """
        await ctx.ensure_kind(self.setup_id)
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
