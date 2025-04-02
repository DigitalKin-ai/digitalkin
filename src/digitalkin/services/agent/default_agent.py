"""Default agent implementation for the agent service."""

from .agent_strategy import AgentStrategy


class DefaultAgent(AgentStrategy):
    """Default agent implementation for the agent service."""

    def start(self) -> None:
        """Start the agent."""

    def stop(self) -> None:
        """Stop the agent."""
