"""LiteLLM proxy integration (optional ``litellm`` extra).

- :class:`LiteLLMSettings`, :func:`get_litellm_settings` — proxy endpoint and token.
- :class:`ModelSpec` and its parts, :class:`Provider`, :class:`ProviderMapper` — model specifications.
- :class:`LiteLLMRegistry` (``LiteLLMRegistry.shared()``), :class:`LiteLLMConfig`,
  :class:`LiteLLMClient`, :class:`ModelParser` — the proxy catalogue and the setup-form model picker.
- :class:`CostTrackingLogger`, :class:`CostRegistry`, :data:`LLM_COST_CONFIG` — per-mission cost
  booking of every proxied call.

The agno model routed through the proxy is ``digitalkin.community.agno.safe_litellm.SafeLiteLLM``.
"""

from digitalkin.community.litellm.cost import LLM_COST_CONFIG, CostRegistry, CostTrackingLogger
from digitalkin.community.litellm.model_specs import (
    CapacityInfo,
    CostInfo,
    ModelSpec,
    Provider,
    ProviderMapper,
    SupportInfo,
    ThoughtCapacity,
)
from digitalkin.community.litellm.registry import LiteLLMClient, LiteLLMConfig, LiteLLMRegistry, ModelParser
from digitalkin.community.litellm.settings import LiteLLMSettings, get_litellm_settings

__all__ = [
    "LLM_COST_CONFIG",
    "CapacityInfo",
    "CostInfo",
    "CostRegistry",
    "CostTrackingLogger",
    "LiteLLMClient",
    "LiteLLMConfig",
    "LiteLLMRegistry",
    "LiteLLMSettings",
    "ModelParser",
    "ModelSpec",
    "Provider",
    "ProviderMapper",
    "SupportInfo",
    "ThoughtCapacity",
    "get_litellm_settings",
]
