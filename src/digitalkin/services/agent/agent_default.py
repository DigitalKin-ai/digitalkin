"""Default agent implementation for the agent service."""

from digitalkin.services.agent.agent_strategy import AgentStrategy


class DefaultAgent(AgentStrategy):
    """Default agent implementation for the agent service."""

    async def start(self) -> None:
        """Start the agent."""

    async def stop(self) -> None:
        """Stop the agent."""
