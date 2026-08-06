"""Toolkit exposing the DigitalKin registry to the agent (setup + module search)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.logger import logger
from digitalkin.models.services.registry import RegistryModuleType, RegistrySetupStatus
from digitalkin.services.registry.exceptions import RegistryServiceError

if TYPE_CHECKING:
    from digitalkin.models.module import ModuleContext
    from digitalkin.services.registry.registry_strategy import RegistryStrategy


class RegistryTools(DkToolkit):
    """Search the DigitalKin registry: invocable setups and the module catalog.

    ``search_setups`` returns configured, ready-to-use instances (carrying a
    ``setup_id``); ``search_modules`` browses raw module types. Results are trimmed
    for the LLM: no configuration, no network addresses, truncated documentation.
    """

    _DOC_PREVIEW_CHARS: ClassVar[int] = 300
    _MAX_RESULTS: ClassVar[int] = 25
    _UNAVAILABLE: ClassVar[str] = "registry search is temporarily unavailable, retry shortly"

    def __init__(self, registry: RegistryStrategy, context: ModuleContext | None = None) -> None:
        """Initialize toolkit with the setup and module search tools.

        Args:
            registry: The module's registry service strategy.
            context: Module context; enables AG-UI notifications via the base toolkit.
        """
        self._registry = registry
        super().__init__(
            name="registry_tools",
            tools=[self.search_setups, self.search_modules, self.get_service_setup],
            context=context,
        )

    @staticmethod
    def _invalid_kind(kind: str | None) -> str | None:
        """Return an error message for an invalid ``kind``, or None if valid.

        Args:
            kind: The requested kind filter.

        Returns:
            Error message when ``kind`` is not 'tool', 'kin' or 'service', else None.
        """
        if kind is not None and kind not in {"tool", "kin", "service"}:
            return f"invalid kind '{kind}'; use 'tool', 'kin' or 'service'"
        return None

    def _clamp(self, limit: int) -> int:
        """Clamp a requested result count to ``[1, _MAX_RESULTS]``.

        Args:
            limit: Requested max results.

        Returns:
            The clamped limit.
        """
        return min(max(limit, 1), self._MAX_RESULTS)

    async def search_setups(
        self, query: str | None = None, kind: Literal["tool", "kin", "service"] | None = None, limit: int = 10
    ) -> str:
        """Search ready-to-use setups (configured agent/tool/service instances you can actually invoke).

        A setup is an installed, configured instance of a module — the thing you can
        call. Use this to discover which tools, agents (kins) or services are available.

        Args:
            query: Free text matched against setup name and documentation. Omit to list all.
            kind: Optional filter: "tool" (invocable tools), "kin" (agents) or "service".
            limit: Max results (default 10, max 25).

        Returns:
            The canonical envelope; ``output`` = {"total_returned", "truncated", "setups": [...]}.
        """
        bad = self._invalid_kind(kind)
        if bad:
            return self._fail(bad, tool="search_setups")

        cap = self._clamp(limit)
        try:
            setups = await self._registry.search_setups(
                query=query,
                module_types=[
                    {
                        "tool": RegistryModuleType.TOOL_MODULE,
                        "kin": RegistryModuleType.ARCHETYPE,
                        "service": RegistryModuleType.SERVICE,
                    }[kind]
                ]
                if kind
                else None,
                statuses=[RegistrySetupStatus.READY, RegistrySetupStatus.CONFIGURATION_SUCCEEDED],
                limit=cap,
            )
        except PermissionDeniedError:
            return self._fail("permission denied: search_setups", tool="search_setups")
        except ValueError as error:
            # Enum encoding drift (fail-closed): permanent, retrying will not help.
            logger.error("RegistryTools: setup search filter rejected: %s", error)
            return self._fail("search filter not supported by this registry version", tool="search_setups")
        except RegistryServiceError as error:
            logger.warning("RegistryTools: setup search failed: %s", error)
            return self._fail(self._UNAVAILABLE, tool="search_setups")

        rows = [
            {
                "setup_id": setup.setup_id,
                "name": setup.name,
                "kind": setup.module_type.value if setup.module_type else None,
                "module_name": setup.module_name,
                "version": setup.setup_version,
                "description": (setup.documentation or "")[: self._DOC_PREVIEW_CHARS],
            }
            for setup in setups
        ]
        return self._ok(
            {"total_returned": len(rows), "truncated": len(rows) == cap, "setups": rows},
            tool="search_setups",
        )

    async def get_service_setup(self, setup_id: str) -> str:
        """Fetch a service's configuration content (a JSON document) by setup id.

        Use after discovering a service via ``search_setups`` and the user accepted it:
        pass the proposed setup's ``setup_id`` to read the service content. Always
        returns the latest version.

        Args:
            setup_id: The service setup id (from a ``search_setups`` result).

        Returns:
            The canonical envelope; ``output`` = the service configuration JSON object.
        """
        try:
            content = await self._registry.get_service_setup(setup_id)
        except PermissionDeniedError:
            return self._fail("permission denied: get_service_setup", tool="get_service_setup")
        except RegistryServiceError as error:
            logger.warning("RegistryTools: service setup fetch failed: %s", error)
            return self._fail(self._UNAVAILABLE, tool="get_service_setup")

        if content is None:
            return self._fail(f"service setup '{setup_id}' not found or has no content", tool="get_service_setup")
        return self._ok(content, tool="get_service_setup")

    async def search_modules(
        self, query: str | None = None, kind: Literal["tool", "kin", "service"] | None = None, limit: int = 10
    ) -> str:
        """Search the module catalog (module TYPES, not configured instances).

        A module is a blueprint — it needs a setup before it can be invoked. Use
        ``search_setups`` to find something you can actually call; use this to browse
        what exists in the mesh.

        Args:
            query: Free text matched against module names. Omit to list all.
            kind: Optional filter: "tool", "kin" (agents/archetypes) or "service".
            limit: Max results (default 10, max 25).

        Returns:
            The canonical envelope; ``output`` = {"total_returned", "truncated", "modules": [...]}.
        """
        bad = self._invalid_kind(kind)
        if bad:
            return self._fail(bad, tool="search_modules")

        cap = self._clamp(limit)
        if kind == "tool":
            pending = self._registry.search_tools(name=query, limit=cap)
        elif kind == "kin":
            pending = self._registry.search_kins(name=query, limit=cap)
        elif kind == "service":
            pending = self._registry.search_services(name=query, limit=cap)
        else:
            pending = self._registry.search(name=query, limit=cap)
        try:
            modules = await pending
        except PermissionDeniedError:
            return self._fail("permission denied: search_modules", tool="search_modules")
        except ValueError as error:
            # Enum encoding drift (fail-closed): permanent, retrying will not help.
            logger.error("RegistryTools: module search filter rejected: %s", error)
            return self._fail("search filter not supported by this registry version", tool="search_modules")
        except RegistryServiceError as error:
            logger.warning("RegistryTools: module search failed: %s", error)
            return self._fail(self._UNAVAILABLE, tool="search_modules")

        rows = [
            {
                "module_id": module.module_id,
                "name": module.module_name,
                "kind": module.module_type.value,
                "version": module.version,
                "status": module.status.value if module.status else None,
                "description": (module.documentation or "")[: self._DOC_PREVIEW_CHARS],
            }
            for module in modules
        ]
        return self._ok(
            {"total_returned": len(rows), "truncated": len(rows) == cap, "modules": rows},
            tool="search_modules",
        )
