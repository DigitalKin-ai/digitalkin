"""Discriminated actions for the ``load_manager`` dispatcher.

Each concrete :class:`LoadAction` carries its parameters and implements :meth:`execute`, which
performs the actual load against the agent's live tool list (via :class:`LoadActionCtx`) and
returns a structured :class:`LoadOutcome` — the manager wraps it in the canonical response
envelope, exactly like the CRUD managers. The runner (:meth:`LoadManager.run_paused`)
validates a paused call into one of these and calls ``execute`` — so adding a new loader
(service, kin) is just a new action class, no new plumbing. For now the only action is ``tool``.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from digitalkin.community.agno.toolkits.registry.base import BaseAction, BaseActionCtx
from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.logger import logger
from digitalkin.models.services.registry import RegistryModuleType
from digitalkin.services.registry.exceptions import RegistryServiceError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from digitalkin.models.module import ModuleContext


@dataclass(frozen=True, slots=True)
class LoadActionCtx(BaseActionCtx):
    """What a load action needs to run.

    Attributes:
        context: The module context — supplies ``registry``/``resolve_tool`` and wraps the ModuleToolkit.
        base_tools: The live tool list the agent's factory closes over; loads append to it in place.
        notify: Best-effort AG-UI notifier (the toolkit's ``_notify``).
    """

    context: ModuleContext
    base_tools: list[Any]
    notify: Callable[[str, dict[str, Any]], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class LoadOutcome:
    """Structured result of a load action, enveloped by the manager.

    Attributes:
        ok: Whether the tool is now loaded (``True`` also for an idempotent re-load).
        message: The LLM-readable status / error line.
        status: On success, ``"loaded"`` or ``"already_loaded"``; empty on failure.
        tool_name: The loaded tool's display name, when known.
        loaded_functions: The now-callable function names (empty unless a fresh load).
    """

    ok: bool
    message: str
    status: str = ""
    tool_name: str | None = None
    loaded_functions: list[str] = field(default_factory=list)


class LoadAction(BaseAction[LoadActionCtx], ABC):
    """Base for the discriminated ``load_manager`` actions.

    Inherits the abstract :meth:`~BaseAction.execute` and binds it to :class:`LoadActionCtx`;
    each concrete action carries its parameters as fields and loads its object into the agent,
    returning a :class:`LoadOutcome` the manager envelopes. Still abstract, so it is never a valid
    discriminator target and is never instantiated.
    """


class LoadToolAction(LoadAction):
    """Load a discovered tool into the agent so you can call it right away."""

    action: Literal["tool"] = "tool"
    setup_id: str = Field(..., description="The tool's setup id (from a tools_manager search/get) to load.")

    async def _duplicate_outcome(self, ctx: LoadActionCtx, module_id: str) -> LoadOutcome | None:
        """Resolve the two "already there" cases, or ``None`` if this is a genuinely new load.

        Runs BEFORE ``resolve_tool``: ``module_id`` is already known from the registry lookup, so
        a repeat load costs no schema fetch. Reads ``base_tools`` — what is callable right now —
        which after rehydration also covers a tool loaded on an earlier turn of this mission.

        Args:
            ctx: The load context carrying the live tool list and the module context.
            module_id: The requested setup's module, from the registry lookup.

        Returns:
            An ``already_loaded`` success, a conflict failure, or ``None`` to continue loading.
        """
        # Imported here, not at module top: ModuleToolkit requires the optional agno dependency at
        # import time, while this module must stay importable without it (same convention as the
        # rest of community.agno).
        from digitalkin.community.agno.module_toolkit import ModuleToolkit

        loaded = [tool for tool in ctx.base_tools if isinstance(tool, ModuleToolkit)]
        existing = next((tool for tool in loaded if tool.tool_module_info.setup_id == self.setup_id), None)
        if existing is not None:
            # Re-persist rather than just report: the id is only durable if an earlier turn's write
            # actually landed, and that write is fail-soft. Without this, one failed persist makes
            # the tool re-loadable forever but never durable — the model keeps getting "already
            # loaded" and the record never appears. The upsert is a no-op when it did land.
            # Setup-declared tools are excluded: they need no mission record.
            if self.setup_id not in ctx.context.tool_cache.declared:
                await ctx.context.persist_loaded_tool(self.setup_id)
            info = existing.tool_module_info
            name = info.tool_name or info.module_name or info.slug
            # Name the callable functions (they ARE callable right now) so an already-loaded result
            # is as verifiable as a fresh load, instead of an empty list reading as "nothing to call".
            callable_names = sorted({*existing.functions, *existing.async_functions})
            listed = f" You can now call: {', '.join(callable_names)}." if callable_names else ""
            return LoadOutcome(
                ok=True,
                status="already_loaded",
                tool_name=name,
                loaded_functions=callable_names,
                message=f"'{name}' is already loaded; call it directly.{listed}",
            )
        # A *different* setup of an already-loaded module cannot rebind — the live binding wins
        # server-side, so appending it would only add duplicate tool names and confirm a change that
        # never takes effect. Refuse explicitly instead of lying.
        conflict = next((tool for tool in loaded if tool.tool_module_info.module_id == module_id), None)
        if conflict is not None:
            return LoadOutcome(
                ok=False,
                message=(
                    f"could not load setup {self.setup_id}: its tool module is already loaded via setup "
                    f"{conflict.tool_module_info.setup_id}, whose configuration stays in effect"
                ),
            )
        return None

    async def execute(self, ctx: LoadActionCtx) -> LoadOutcome:  # noqa: C901, PLR0911 — each return is a distinct, LLM-readable outcome
        """Resolve the setup into a ModuleToolkit and append it to the live tool list.

        Idempotent per ``setup_id``; never raises. The setup's family is read first so a
        service/kin setup gets a distinct "not a tool" message instead of the generic resolution
        error shared with a never-existed id. Every failure returns a distinct message the model
        can tell apart (bad family, already loaded, not found, …).

        Returns:
            A :class:`LoadOutcome`: on success it names the now-callable functions so the load is
            verifiable; otherwise a distinct failure ``message`` with ``ok=False``.
        """
        if not self.setup_id:
            return LoadOutcome(ok=False, message="could not load a tool: no setup id was provided")

        # Resolve the setup's family BEFORE resolve_tool. resolve_tool fetches the tool
        # schema and *raises* for a non-tool family (service), which the generic handler below would
        # report as "resolution failed" — indistinguishable from an absent id. A cheap registry
        # lookup (the gate ensure_kind uses) discriminates the family and reserves "resolution
        # failed" for a genuine failure on a confirmed tool module.
        registry = ctx.context.registry
        setup = None
        module = None
        try:
            setup = await registry.get_setup(self.setup_id)
            if setup is not None and setup.module_id:
                module = await registry.discover_by_id(setup.module_id)
        except PermissionDeniedError:
            return LoadOutcome(ok=False, message=f"permission denied: cannot load setup {self.setup_id}")
        except (RegistryServiceError, ServerError) as error:
            # get_setup/discover_by_id RAISE (not return None) on an unknown id (NOT_FOUND) or any
            # registry read failure — RegistryModuleNotFoundError subclasses RegistryServiceError, so
            # this one handler covers them all. Never let it escape: an uncaught error here crashes
            # the whole module through the HITL runner.
            logger.warning("LoadToolAction: cannot resolve setup %s: %s", self.setup_id, error)
            return LoadOutcome(ok=False, message=f"could not load setup {self.setup_id}: no setup with that id exists")
        if setup is None or not setup.module_id or module is None:
            return LoadOutcome(ok=False, message=f"could not load setup {self.setup_id}: no setup with that id exists")
        if module.module_type != RegistryModuleType.TOOL_MODULE:
            return LoadOutcome(
                ok=False,
                message=(
                    f"could not load setup {self.setup_id}: it is a '{module.module_type.value}' setup, not a "
                    "tool; only tool setups (found via a tools_manager search) can be loaded"
                ),
            )

        duplicate = await self._duplicate_outcome(ctx, setup.module_id)
        if duplicate is not None:
            return duplicate

        # Confirmed tool module, not already loaded — resolve it into a callable toolkit.
        try:
            info = await ctx.context.resolve_tool(self.setup_id)
        except PermissionDeniedError:
            return LoadOutcome(ok=False, message=f"permission denied: cannot load setup {self.setup_id}")
        except Exception as error:
            logger.warning("LoadToolAction: failed to resolve setup %s: %s", self.setup_id, error)
            return LoadOutcome(ok=False, message=f"could not load setup {self.setup_id}: resolution failed")
        if info is None:
            return LoadOutcome(ok=False, message=f"could not load setup {self.setup_id}: no setup with that id exists")
        if not info.tools:
            return LoadOutcome(
                ok=False, message=f"could not load setup {self.setup_id}: this tool module exposes no callable tools"
            )

        from digitalkin.community.agno.module_toolkit import ModuleToolkit

        name = info.tool_name or info.module_name or info.slug
        toolkit = ModuleToolkit(ctx.context, info)
        ctx.base_tools.append(toolkit)
        # Persist the id so the load outlives this turn. ``resolve_tool`` only reached the
        # mission-scoped ``dynamic`` cache layer, which is rebuilt from scratch on the next
        # user message; without this the tool would have to be re-loaded every turn.
        await ctx.context.persist_loaded_tool(self.setup_id)
        await ctx.notify("tool_loaded", {"setup_id": self.setup_id, "tool_name": name})
        # Name the now-callable functions so the model can verify and call them directly.
        callable_names = sorted({*toolkit.functions, *toolkit.async_functions})
        listed = f" You can now call: {', '.join(callable_names)}." if callable_names else ""
        return LoadOutcome(
            ok=True,
            status="loaded",
            tool_name=name,
            loaded_functions=callable_names,
            message=f"loaded '{name}' — call it directly to use it.{listed}",
        )


# Single load action for now; wrap in
# ``Annotated[LoadToolAction | OtherLoadAction, Field(discriminator="action")]``
# once a second loader (service/kin) exists.
LoadActions = LoadToolAction
