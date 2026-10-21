"""Book the cost of every LLM call routed through the LiteLLM proxy.

A single :class:`CostTrackingLogger` on ``litellm.callbacks`` sees every completed call exactly once
(agents, team members, workflow steps, agno's session summaries; a stream fires once with the
aggregated usage), which per-agent post-hooks cannot guarantee. The callback is process-wide while a
server runs several missions, so each request carries its mission id in the LiteLLM ``metadata``
(:attr:`CostTrackingLogger.MISSION_METADATA_KEY`, set by ``SafeLiteLLM.from_spec``) and the callback
resolves the mission's cost service and price multiplier through :class:`CostRegistry`.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

import litellm
from litellm.integrations.custom_logger import CustomLogger
from pydantic import BaseModel

from digitalkin.community.litellm.registry import LiteLLMRegistry
from digitalkin.logger import logger
from digitalkin.services.cost.cost_strategy import CostConfig

if TYPE_CHECKING:
    from digitalkin.community.litellm.model_specs import ModelSpec
    from digitalkin.services.cost.cost_strategy import CostStrategy


# Every LLM token category is billed at 1.3.
LLM_COST_CONFIG: dict[str, CostConfig] = {  # dk-allow-global (cost config fragment)
    "chatbot_input": CostConfig(
        cost_name="chatbot_input",
        cost_type="TOKEN_INPUT",
        description="input token from the chatbot",
        unit="tokens",
        rate=1.3,
    ),
    "chatbot_cached_input": CostConfig(
        cost_name="chatbot_cached_input",
        cost_type="TOKEN_INPUT",
        description="Cached input token from the chatbot",
        unit="tokens",
        rate=1.3,
    ),
    "chatbot_output": CostConfig(
        cost_name="chatbot_output",
        cost_type="TOKEN_OUTPUT",
        description="output token from the chatbot",
        unit="tokens",
        rate=1.3,
    ),
    "chatbot_reasoning": CostConfig(
        cost_name="chatbot_reasoning",
        cost_type="TOKEN_OUTPUT",
        description="Reasoning token from the chatbot",
        unit="tokens",
        rate=1.3,
    ),
    "chatbot_cache_creation_input": CostConfig(
        cost_name="chatbot_cache_creation_input",
        cost_type="TOKEN_INPUT",
        description="Cache creation input token from the chatbot",
        unit="tokens",
        rate=1.3,
    ),
}


class CostRegistry:
    """Process-wide map from mission id to the cost services of its running turns.

    Each turn registers its own entry and removes only that one: two turns can run on one mission
    (a HITL resume and a new message), and the first to end must not stop the other's billing.
    """

    _entries: ClassVar[dict[str, list[tuple[CostStrategy, Decimal]]]] = {}

    @classmethod
    def register(cls, mission_id: str, cost: CostStrategy, price_multiplier: Decimal | float) -> None:
        """Register a turn's cost context on its mission.

        Args:
            mission_id: Mission id used as the routing key.
            cost: The turn's cost service; pass the same object to :meth:`unregister`.
            price_multiplier: Coefficient applied to every token cost; clamped to >= 1.0.
        """
        multiplier = price_multiplier if isinstance(price_multiplier, Decimal) else Decimal(str(price_multiplier))
        if multiplier < 1:
            logger.warning("Price multiplier %s < 1.0, using 1.0 instead", multiplier, extra={"mission_id": mission_id})
            multiplier = Decimal("1.0")
        cls._entries.setdefault(mission_id, []).append((cost, multiplier))

    @classmethod
    def unregister(cls, mission_id: str, cost: CostStrategy) -> None:
        """Drop the cost context one turn registered, leaving the mission's other turns billed.

        Args:
            mission_id: Mission id the turn registered on.
            cost: The cost service the turn registered.
        """
        entries = [entry for entry in cls._entries.get(mission_id, []) if entry[0] is not cost]
        if entries:
            cls._entries[mission_id] = entries
        else:
            cls._entries.pop(mission_id, None)

    @classmethod
    def get(cls, mission_id: str) -> tuple[CostStrategy, Decimal] | None:
        """Resolve the cost service and multiplier of a mission's latest running turn.

        Args:
            mission_id: Mission id to look up.

        Returns:
            ``(cost, multiplier)``, or None if no turn of the mission is registered.
        """
        entries = cls._entries.get(mission_id)
        return entries[-1] if entries else None


class CostTrackingLogger(CustomLogger):
    """LiteLLM success callback booking per-category token costs to the mission's cost service."""

    MISSION_METADATA_KEY: ClassVar[str] = "dk_mission_id"

    @classmethod
    def register(cls) -> None:
        """Append one instance to ``litellm.callbacks`` unless one is already there."""
        callbacks = litellm.callbacks or []
        if not any(isinstance(callback, CostTrackingLogger) for callback in callbacks):
            litellm.callbacks = [*callbacks, cls()]

    @staticmethod
    def token_to_dk_credit(tokens: int, price: Decimal, price_multiplier: Decimal = Decimal("1.0")) -> float:
        """Convert tokens to DK$ credits (1 DK$ = 0.02 EUR).

        Args:
            tokens: Number of tokens.
            price: Price in dollars per million tokens.
            price_multiplier: Coefficient applied to the cost.

        Returns:
            The cost in DK$.
        """
        return float(Decimal(tokens) * (price / Decimal(20_000)) * price_multiplier)

    async def async_log_success_event(
        self,
        kwargs: dict[str, Any],
        response_obj: Any,
        start_time: Any,  # ruff: ignore[unused-method-argument]
        end_time: Any,  # ruff: ignore[unused-method-argument]
    ) -> None:
        """Book the token cost of one completed call; never raises.

        Args:
            kwargs: LiteLLM call details (model, litellm_params, metadata, ...).
            response_obj: The completed response (aggregated usage for streamed calls).
            start_time: Call start time (unused).
            end_time: Call end time (unused).
        """
        try:
            await self._track(kwargs, response_obj)
        except Exception:
            logger.exception("Cost tracking failed")

    async def _track(self, kwargs: dict[str, Any], response_obj: Any) -> None:
        """Resolve the call's mission and model, then book its usage.

        Args:
            kwargs: LiteLLM call details.
            response_obj: The completed response carrying ``usage``.
        """
        mission_id: str | None = None
        standard = kwargs.get("standard_logging_object")
        for container in (kwargs.get("litellm_params"), kwargs, standard if isinstance(standard, dict) else None):
            metadata = container.get("metadata") if isinstance(container, dict) else None
            if isinstance(metadata, dict) and metadata.get(self.MISSION_METADATA_KEY):
                mission_id = str(metadata[self.MISSION_METADATA_KEY])
                break
        if mission_id is None:
            return
        entry = CostRegistry.get(mission_id)
        if entry is None:
            # TODO(validate): LLM-COST-UNREGISTERED an unbilled LLM call is logged, never dropped silently
            logger.warning(
                "[VALIDATE LLM-COST-UNREGISTERED] LLM call of mission %s not billed: no cost service registered",
                mission_id,
            )
            return
        usage = response_obj.usage if response_obj is not None else None
        if usage is None:
            logger.debug("No usage on response; cost not tracked", extra={"mission_id": mission_id})
            return
        model = kwargs.get("model")
        model_name = model.removeprefix("litellm_proxy/") if isinstance(model, str) else None
        spec = LiteLLMRegistry.shared().try_get(model_name) if model_name else None
        if spec is None:
            logger.warning("No pricing for model %s; cost not tracked", model_name, extra={"mission_id": mission_id})
            return
        await self._book(usage, spec, entry[0], entry[1], mission_id)

    @staticmethod
    def _as_dict(details: Any) -> dict[str, Any]:
        """Normalize a usage-detail object (pydantic model, dict or None) to a dict.

        Fields some LiteLLM versions omit are then simply absent instead of raising.

        Args:
            details: The usage-detail value.

        Returns:
            The detail fields.
        """
        if details is None:
            return {}
        if isinstance(details, dict):
            return details
        return details.model_dump() if isinstance(details, BaseModel) else {}

    @staticmethod
    async def _book(usage: Any, spec: ModelSpec, cost: CostStrategy, multiplier: Decimal, mission_id: str) -> None:
        """Split usage into cost categories and add each to the cost service.

        ``prompt_tokens`` includes cache reads (not cache writes) and ``completion_tokens`` includes
        reasoning, so both are subtracted out and every category is priced once.

        Args:
            usage: LiteLLM ``Usage`` of the response.
            spec: Model spec carrying the prices.
            cost: The mission's cost service.
            multiplier: Price multiplier.
            mission_id: Mission id, for logging.
        """
        prompt_details = CostTrackingLogger._as_dict(usage.prompt_tokens_details)
        completion_details = CostTrackingLogger._as_dict(usage.completion_tokens_details)
        cached_read = prompt_details.get("cached_tokens") or 0
        cache_creation = prompt_details.get("cache_creation_tokens") or 0
        reasoning = completion_details.get("reasoning_tokens") or 0
        non_cached_input = max((usage.prompt_tokens or 0) - cached_read, 0)
        non_reasoning_output = max((usage.completion_tokens or 0) - reasoning, 0)

        costs = spec.costs
        credit = CostTrackingLogger.token_to_dk_credit
        if non_cached_input:
            await cost.add(
                "llm_input_tokens", "chatbot_input", credit(non_cached_input, costs.input_cost_per_token, multiplier)
            )
        if non_reasoning_output:
            await cost.add(
                "llm_output_tokens",
                "chatbot_output",
                credit(non_reasoning_output, costs.output_cost_per_token, multiplier),
            )
        if reasoning:
            await cost.add(
                "llm_reasoning_tokens", "chatbot_reasoning", credit(reasoning, costs.output_cost_per_token, multiplier)
            )
        if cached_read:
            await cost.add(
                "llm_cached_tokens",
                "chatbot_cached_input",
                credit(cached_read, costs.cache_read_input_token_cost or costs.input_cost_per_token, multiplier),
            )
        if cache_creation:
            await cost.add(
                "llm_cache_creation_tokens",
                "chatbot_cache_creation_input",
                credit(cache_creation, costs.cache_creation_input_token_cost or costs.input_cost_per_token, multiplier),
            )
        logger.info(
            "LLM cost tracked: model=%s input=%d output=%d reasoning=%d cache_read=%d cache_write=%d multiplier=%s",
            spec.name,
            non_cached_input,
            non_reasoning_output,
            reasoning,
            cached_read,
            cache_creation,
            multiplier,
            extra={"mission_id": mission_id},
        )
