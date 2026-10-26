"""One-call assembler for the default DigitalKin toolkits."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agno.agent import Agent
from agno.team import Team

from digitalkin.community.agno.toolkits.chat_history import ChatHistoryTools
from digitalkin.community.agno.toolkits.registry.kins.kit import KinsManager
from digitalkin.community.agno.toolkits.registry.loader.kit import LoadManager
from digitalkin.community.agno.toolkits.registry.services.kit import ServicesManager
from digitalkin.community.agno.toolkits.registry.tools.kit import ToolsManager
from digitalkin.community.agno.toolkits.user_profile import UserProfileTools

if TYPE_CHECKING:
    from collections.abc import Callable

    from digitalkin.models.module.module_context import ModuleContext


class DefaultToolkits:
    """Assemble the default DigitalKin toolkits (chat history, user profile, registry).

    Two-phase usage — ChatHistoryTools and LoadManager need the constructed Agent/Team::

        tools = DefaultToolkits.build(context, session_id=sid)
        agent = Agent(tools=AguiTools.make_tools_factory(tools), cache_callables=False, ...)
        DefaultToolkits.bind_host(tools, agent)

    In team mode call :meth:`bind_host` again with the ``Team`` so history reads
    target the team session rather than the bare head agent.
    """

    @staticmethod
    def build(
        context: ModuleContext,
        session_id: str | None = None,
        *,
        tools: list[Any] | None = None,
        chat_history: bool = True,
    ) -> list[Any]:
        """Build the default toolkits from a module context.

        ``UserProfileTools`` is added only when ``context.user_profile`` is wired, the three
        registry managers (Tools / Services / Kins) only when ``context.setup`` is. LoadManager
        is always added last and bound to the returned list so dynamically-loaded tools land in
        the exact list the agent's factory splats.

        Args:
            context: The module context carrying the services and (optional) setup service.
            session_id: The Agno session whose chat history should be readable.
            tools: Toolkits the defaults are appended to, in place; this same list is returned.
            chat_history: ``False`` for sub-agents (team members, workflow steps), which have
                no session of their own to read.

        Returns:
            [*tools, ChatHistoryTools?, UserProfileTools?, (ToolsManager, ServicesManager,
             KinsManager)?, LoadManager].
        """
        toolkits = [] if tools is None else tools
        if chat_history:
            toolkits.append(ChatHistoryTools(session_id=session_id, context=context))
        if context.user_profile is not None:
            toolkits.append(UserProfileTools(context.user_profile, context=context))
        if context.setup is not None:
            toolkits.extend((
                ToolsManager(context.setup, context.registry, context=context),
                ServicesManager(context.setup, context.registry, context=context),
                KinsManager(context.setup, context.registry, context=context),
            ))
        loader = LoadManager(context=context)
        toolkits.append(loader)
        loader.bind_tools(toolkits)
        return toolkits

    @staticmethod
    def bind_host(tools: list[Any] | Callable[..., list[Any]] | None, host: Any) -> None:
        """Late-bind the constructed Agent/Team into ChatHistoryTools and the LoadManager.

        The LoadManager is rebound to ``host.tools`` when the host holds a plain list (a
        sub-agent, which copies the list it is given); a tools factory keeps its binding.

        Args:
            tools: The tools list (or a make_tools_factory callable) containing the toolkits.
            host: The constructed Agent or Team to bind as the history source.
        """
        ChatHistoryTools.bind_host(tools, host)
        loader = LoadManager.find(tools)
        if loader is not None and isinstance(host, Agent | Team):
            loader.bind_to(host)
