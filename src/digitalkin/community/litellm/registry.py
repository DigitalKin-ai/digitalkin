"""LiteLLM proxy catalogue: fetch, parse and serve model specifications."""

import asyncio
import re
from collections import defaultdict
from collections.abc import Iterable
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field

from digitalkin.community.litellm.model_specs import (
    CapacityInfo,
    CostInfo,
    ModelSpec,
    Provider,
    ProviderMapper,
    SupportInfo,
    ThoughtCapacity,
)
from digitalkin.community.litellm.settings import get_litellm_settings
from digitalkin.logger import logger
from digitalkin.utils.dynamic_schema import DynamicField


class LiteLLMConfig(BaseModel):
    """Connection to the LiteLLM proxy; defaults are read from the environment when built."""

    base_url: str = Field(
        default_factory=lambda: get_litellm_settings().base_url,
        description="LiteLLM server base URL",
    )
    timeout: int = Field(default=60, ge=1, description="Request timeout in seconds")
    bearer_token: str = Field(
        # The lambda defers the read to when a config is built, not import time.
        default_factory=lambda: get_litellm_settings().api_key.get_secret_value(),  # ruff: ignore[unnecessary-lambda]
        description="Bearer token for authentication",
    )


class LiteLLMClient:
    """HTTP client for the proxy's ``/model/info`` endpoint."""

    def __init__(self, config: LiteLLMConfig) -> None:
        """Initialize the client.

        Args:
            config: Proxy connection settings.
        """
        self._config = config

    async def fetch_raw_models(self, timeout: int | None = None) -> list[dict[str, Any]]:
        """Fetch every model entry the proxy serves.

        Args:
            timeout: Timeout override in seconds.

        Returns:
            Raw model entries.
        """
        headers = {"Accept": "application/json"}
        if self._config.bearer_token:
            headers["Authorization"] = f"Bearer {self._config.bearer_token}"
        async with httpx.AsyncClient(
            base_url=self._config.base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout or self._config.timeout),
            headers=headers,
            http2=True,
        ) as client:
            logger.info("Fetching all models from LiteLLM")
            response = await client.get("/model/info")
            response.raise_for_status()
            data = response.json()
        if isinstance(data, dict):
            return data.get("data", [data])
        if isinstance(data, list):
            return data
        return [data]

    async def fetch_model(self, model_name: str, timeout: int | None = None) -> dict[str, Any]:
        """Fetch one model entry.

        ``/model/info`` takes no filter, so the whole catalogue is fetched; prefer
        ``LiteLLMRegistry.initialize_many`` when several names are known.

        Args:
            model_name: Model group name.
            timeout: Timeout override in seconds.

        Returns:
            The raw model entry.

        Raises:
            ValueError: If the proxy does not serve the model.
        """
        logger.info("Fetching model '%s' from LiteLLM", model_name)
        for model_data in await self.fetch_raw_models(timeout=timeout):
            if model_data.get("model_name") == model_name:
                return model_data
        msg = f"Model '{model_name}' not found in LiteLLM"
        raise ValueError(msg)


class ModelParser:
    """Turns one ``/model/info`` entry into a :class:`ModelSpec`."""

    def __init__(self, data: dict[str, Any]) -> None:
        """Initialize the parser.

        Args:
            data: Raw entry carrying ``model_name``, ``model_info`` and ``litellm_params``.
        """
        self._data = data
        self._model_info: dict[str, Any] = data.get("model_info") or {}
        self._litellm_params: dict[str, Any] = data.get("litellm_params") or {}

    def parse_provider(self) -> Provider:
        """Normalize the provider prefix of the LiteLLM model path.

        Returns:
            The provider.
        """
        model_path = self._litellm_params.get("model", "")
        return ProviderMapper.normalize(model_path.split("/")[0] if model_path else "")

    def parse_name(self) -> str:
        """Read the model group name.

        Returns:
            The stripped name.

        Raises:
            TypeError: If the name is missing or not a string.
        """
        name = self._data.get("model_name")
        if not isinstance(name, str):
            msg = "Model name not found or invalid."
            raise TypeError(msg)
        return name.strip()

    def parse_model_id(self) -> str:
        """Read the unique model identifier.

        Returns:
            The identifier, empty when absent.
        """
        key = self._model_info.get("key", "")
        return key.strip() if isinstance(key, str) else ""

    def parse_capacity(self) -> CapacityInfo:
        """Read the token limits.

        The context size falls back through ``max_input_tokens``, ``max_tokens`` and
        ``context_window``, since proxy entries declare it under different keys.

        Returns:
            The capacity; zeros when the values are not integers.
        """
        info = self._model_info
        context = info.get("max_input_tokens") or info.get("max_tokens") or info.get("context_window") or 0
        output = info.get("max_output_tokens") or info.get("max_output") or info.get("max_completion_tokens") or 0
        try:
            return CapacityInfo(context_window=int(context), max_output=int(output))
        except (ValueError, TypeError):
            logger.warning(
                "Invalid capacity values for model %s: context=%s, output=%s",
                self._data.get("model_name"),
                context,
                output,
            )
            return CapacityInfo(context_window=0, max_output=0)

    @staticmethod
    def _to_decimal(value: Any) -> Decimal | None:
        """Convert a raw price to a Decimal quantized to 1e-9.

        Args:
            value: Raw value.

        Returns:
            The Decimal, or None when empty, negative or not a number.
        """
        if value is None or value == "":  # ruff: ignore[compare-to-empty-string]
            return None
        try:
            result = (value if isinstance(value, Decimal) else Decimal(str(value))).quantize(
                Decimal("1E-9"), rounding=ROUND_HALF_EVEN
            )
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.warning("Failed to convert %s to Decimal: %s", value, e)
            return None
        if result < 0:
            logger.warning("Negative cost value detected: %s", result)
            return None
        return result

    def parse_costs(self) -> CostInfo:
        """Read every price, converted from per-unit to per-million.

        Returns:
            The costs.
        """
        cost_data = {}
        for field_name in CostInfo.model_fields:
            decimal_value = self._to_decimal(self._model_info.get(field_name))
            if decimal_value is not None:
                cost_data[field_name] = decimal_value * 1_000_000
        return CostInfo(**cost_data)

    def parse_support(self) -> SupportInfo:
        """Read the feature flags; string flags are true for ``true``, ``yes`` and ``1``.

        Returns:
            The support flags.
        """
        support_data = {}
        for field_name in SupportInfo.model_fields:
            value = self._model_info.get(field_name)
            if value is not None:
                support_data[field_name] = (
                    value.lower() in {"true", "yes", "1"} if isinstance(value, str) else bool(value)
                )
        return SupportInfo(**support_data)

    def parse_thought_capacity(self, support: SupportInfo) -> ThoughtCapacity:
        """Derive the reasoning capacity.

        Args:
            support: Parsed support flags.

        Returns:
            ``none`` without reasoning support, ``high`` when reasoning tokens are priced, the
            declared ``reasoning_level`` when present, else ``medium``.
        """
        if not support.supports_reasoning:
            return ThoughtCapacity(type="level", value="none")
        if self._model_info.get("output_cost_per_reasoning_token"):
            return ThoughtCapacity(type="token_budget", value="high")
        reasoning_level = self._model_info.get("reasoning_level")
        if reasoning_level:
            value = {"advanced": "high", "high": "high", "medium": "medium", "low": "low", "basic": "low"}.get(
                str(reasoning_level).lower(), "none"
            )
            return ThoughtCapacity(type="level", value=value)  # type: ignore[arg-type]
        return ThoughtCapacity(type="level", value="medium")

    def to_model_spec(self) -> ModelSpec:
        """Build the full specification.

        Returns:
            The model spec.

        Raises:
            TypeError: If any part fails to parse.
        """
        provider: Provider = Provider.OTHER
        name = "unknown"
        try:
            provider = self.parse_provider()
            name = self.parse_name()
            support = self.parse_support()
            return ModelSpec(
                provider=provider,
                name=name,
                model_id=self.parse_model_id(),
                capacity=self.parse_capacity(),
                costs=self.parse_costs(),
                support=support,
                thought_capacity=self.parse_thought_capacity(support),
                multimodal=support.is_multimodal,
                documentation_url=(
                    self._model_info.get("documentation_url") or self._litellm_params.get("documentation_url")
                ),
            )
        except (ValueError, TypeError) as e:
            msg = f"Failed to parse model spec for {provider.value}/{name}: {e}"
            raise TypeError(msg) from e


class LiteLLMRegistry:
    """Model specifications served by the proxy, loaded on demand, plus the setup-form model picker."""

    PRIORITY_MODELS: ClassVar[frozenset[str]] = frozenset({
        "claude-sonnet-5",
        "claude-opus-5",
        "claude-haiku-5.5",
        "claude-haiku-4.5",
        "gpt-5.4-mini",
        "gpt-5.4-nano",
        "gemini-3.7-flash",
        "gemini-3-1-pro",
        "gpt-5",
        "gpt-5-mini",
        "claude-sonnet-4-5",
        "claude-haiku-4-5",
        "claude-sonnet-4",
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "mistral-medium-2505",
        "grok-4-fast-non-reasoning",
        "Llama-4-Maverick",
        "Llama-4-Scout",
    })

    # Preference order of the setup form's pre-selected model. Only a name the proxy currently serves
    # is emitted: a literal default rots the day the proxy drops that model.
    DEFAULT_MODEL_CANDIDATES: ClassVar[tuple[str, ...]] = (
        "claude-sonnet-5",
        "gpt-5.4-mini",
        "claude-haiku-5.5",
        "claude-haiku-4.5",
        "gemini-3.7-flash",
        "gpt-5-mini",
        "claude-sonnet-4-5",
    )
    FAST_TIMEOUT: ClassVar[int] = 5

    _shared: ClassVar["LiteLLMRegistry | None"] = None

    def __init__(self, config: LiteLLMConfig | None = None) -> None:
        """Initialize the registry.

        Args:
            config: Proxy connection settings; read from the environment when omitted.
        """
        self._config = config or LiteLLMConfig()
        self._client = LiteLLMClient(self._config)
        self._models: dict[str, ModelSpec] = {}
        self._lock = asyncio.Lock()
        self._initialized = False

    def __deepcopy__(self, memo: dict[int, Any]) -> "LiteLLMRegistry":
        """Return the registry itself: it is a shared resource, never copied.

        Setup resolution deep-copies field metadata, which holds the registry's bound fetchers
        (see :meth:`model_picker`); copying would duplicate its ``asyncio.Lock``.

        Args:
            memo: The deepcopy memo.

        Returns:
            This registry.
        """
        return self

    @classmethod
    def shared(cls) -> "LiteLLMRegistry":
        """Process-wide registry, built from the environment on first use.

        Returns:
            The shared instance.
        """
        if cls._shared is None:
            cls._shared = cls()
        return cls._shared

    @property
    def config(self) -> LiteLLMConfig:
        """Proxy connection settings."""
        return self._config

    async def initialize(self, model_name: str | None = None) -> None:
        """Load one model, or the whole catalogue once.

        Args:
            model_name: Model to load; None loads every model (unparseable entries are skipped).

        Raises:
            ValueError: If ``model_name`` is not served.
        """
        async with self._lock:
            if model_name and model_name in self._models:
                return
            if not model_name and self._initialized:
                return
            if model_name:
                logger.info("Loading model '%s'", model_name)
                self._parse_model(await self._client.fetch_model(model_name), strict=True)
            else:
                logger.info("Loading all models")
                for model_data in await self._client.fetch_raw_models():
                    self._parse_model(model_data, strict=False)
                self._initialized = True
            logger.info("LiteLLM registry now holds %d models", len(self._models))

    async def initialize_many(self, names: Iterable[str]) -> None:
        """Load several models in a single ``/model/info`` round trip.

        Args:
            names: Model names; empty and already loaded names are skipped.

        Raises:
            ValueError: If any requested name is not served.
        """
        async with self._lock:
            wanted = {n for n in names if n and n not in self._models}
            if not wanted:
                return
            logger.info("Loading %d models from LiteLLM: %s", len(wanted), sorted(wanted))
            for model_data in await self._client.fetch_raw_models():
                row_name = model_data.get("model_name")
                if isinstance(row_name, str) and row_name in wanted:
                    self._parse_model(model_data, strict=True)
                    wanted.discard(row_name)
            if wanted:
                msg = f"Models not found in LiteLLM: {sorted(wanted)}"
                raise ValueError(msg)
            logger.info("LiteLLM registry now holds %d models", len(self._models))

    def _parse_model(self, model_data: dict[str, Any], *, strict: bool) -> None:
        """Parse one entry into the registry.

        Args:
            model_data: Raw entry.
            strict: Raise on failure instead of logging and skipping.

        Raises:
            TypeError: If strict and parsing fails.
            ValueError: If strict and parsing fails.
        """
        try:
            spec = ModelParser(model_data).to_model_spec()
        except (TypeError, ValueError) as e:
            if strict:
                logger.exception("Failed to parse model")
                raise
            logger.warning("Failed to parse model: %s", e)
            return
        self._models[spec.name] = spec

    def get(self, name: str) -> ModelSpec:
        """Return a loaded model.

        Args:
            name: Model name.

        Returns:
            The model spec.

        Raises:
            ValueError: If the model is not loaded.
        """
        model = self._models.get(name)
        if model is None:
            msg = f"Model '{name}' not in registry. Call 'await registry.initialize_many([{name!r}, ...])' first."
            raise ValueError(msg)
        return model

    def try_get(self, name: str) -> ModelSpec | None:
        """Return a loaded model, or None.

        Args:
            name: Model name.

        Returns:
            The model spec, or None if not loaded.
        """
        return self._models.get(name)

    def __len__(self) -> int:
        """Number of loaded models.

        Returns:
            The count.
        """
        return len(self._models)

    async def fetch_model_names(self) -> list[str]:
        """Fetch the served model names, priority models first, each half sorted.

        Returns:
            The names.
        """
        model_names = {
            name.strip()
            for model_data in await self._client.fetch_raw_models(timeout=self.FAST_TIMEOUT)
            if isinstance(name := model_data.get("model_name"), str) and name.strip()
        }
        logger.info("Fetched %d model names from LiteLLM", len(model_names))
        return sorted(model_names & self.PRIORITY_MODELS) + sorted(model_names - self.PRIORITY_MODELS)

    async def fetch_default_model_name(self) -> str:
        """Pick the setup form's pre-selected model from the proxy's current catalogue.

        Returns:
            The first served ``DEFAULT_MODEL_CANDIDATES`` entry, else the first served name.

        Raises:
            LookupError: If the proxy serves no model.
        """
        names = await self.fetch_model_names()
        if not names:
            msg = "LiteLLM proxy serves no model; cannot pick a default"
            raise LookupError(msg)
        served = set(names)
        return next((c for c in self.DEFAULT_MODEL_CANDIDATES if c in served), names[0])

    @staticmethod
    def _format_model_label(model_name: str) -> str:
        """Build a display label from a model slug (``claude-opus-4-5`` -> ``Claude Opus 4.5``).

        Args:
            model_name: Raw model name.

        Returns:
            The label.
        """
        parts: list[str] = []
        after_digit = False
        for part in (p for p in model_name.strip().replace("_", "-").split("-") if p):
            if after_digit and part.isdigit():
                parts[-1] = f"{parts[-1]}.{part}"
            else:
                parts.append(part)
            after_digit = part.isdigit()
        head = parts[0].lower() if parts else ""
        if head == "gpt":
            return " ".join([
                "GPT",
                *(p.upper() if p == "api" else (p.capitalize() if p.isalpha() else p) for p in parts[1:]),
            ])
        if head == "claude":
            return " ".join(["Claude", *(p.capitalize() if p.isalpha() else p for p in parts[1:])])
        return " ".join(
            (p.upper() if len(p) <= 3 else p.capitalize()) if p.isalpha() else p  # ruff: ignore[magic-value-comparison]
            for p in parts
        )

    async def build_model_groups(self) -> list[dict[str, Any]]:
        """Group the served models by creator, with logos, for ``GroupedIconSelectWidget``.

        Returns:
            ``[{"label": "OpenAI", "options": [{"value": ..., "label": ..., "image": ...}]}]``.
        """
        known = {
            "openai": "OpenAI",
            "deepseek": "DeepSeek",
            "anthropic": "Anthropic",
            "google": "Google",
            "google deepmind": "Google DeepMind",
            "deepmind": "DeepMind",
            "meta": "Meta",
            "microsoft": "Microsoft",
            "azure": "Azure",
            "amazon": "Amazon",
            "bedrock": "Bedrock",
            "cohere": "Cohere",
            "mistral": "Mistral",
            "xai": "xAI",
        }
        variant_rank = {"none": 1, "low": 2, "medium": 3, "high": 4, "xhigh": 5}
        by_creator: dict[str, dict[str, str]] = defaultdict(dict)
        creator_slugs: dict[str, str] = {}
        for item in await self._client.fetch_raw_models(timeout=self.FAST_TIMEOUT) or []:
            model_name = (item or {}).get("model_name")
            creator = ((item or {}).get("model_info") or {}).get("metadata", {}).get("model_creator")
            if not model_name or not creator:
                continue
            label = known.get(creator.strip().lower(), creator.strip())
            slug = re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9-]", "", re.sub(r"[\s_]+", "-", creator.strip().lower())))
            creator_slugs[label] = slug.strip("-") or "unknown"
            by_creator[label].setdefault(model_name, self._format_model_label(model_name))

        groups: list[dict[str, Any]] = []
        for creator_label in sorted(by_creator):
            labels = by_creator[creator_label]
            # Base models before their variants; variants by level (None < Low < ... < Xhigh).
            sort_keys: dict[str, tuple[str, int, int]] = {}
            for model_name, label in labels.items():
                words = label.split()
                sort_keys[model_name] = next(
                    (
                        (" ".join(words[:i]), 1, variant_rank.get(words[i + 1].lower(), 99))
                        for i, word in enumerate(words)
                        if word.lower() in {"thinking", "reasoning"} and i + 1 < len(words)
                    ),
                    (label, 0, 0),
                )
            image = (
                "https://dk-hub-shared.s3.eu-north-1.amazonaws.com/public/models_creators_logo/"
                f"{creator_slugs[creator_label]}.png"
            )
            groups.append({
                "label": creator_label,
                "options": [
                    {"value": name, "label": labels[name], "image": image}
                    for name in sorted(labels, key=sort_keys.__getitem__)
                ],
            })
        return groups

    async def fetch_model_groups_options(self) -> dict[str, Any]:
        """``ui:options`` fetcher of the model picker.

        Returns:
            ``{"groups": build_model_groups()}``.
        """
        return {"groups": await self.build_model_groups()}

    @classmethod
    def model_picker(cls) -> DynamicField:
        """``DynamicField`` of a setup field choosing a proxy model.

        Use as ``Annotated[str, LiteLLMRegistry.model_picker()]``; the enum, the default and the
        grouped options are fetched from the proxy when the schema is resolved. Each fetch resolves
        :meth:`shared` then, so declaring the field at import time reads no proxy setting yet.

        Returns:
            The dynamic field metadata.
        """
        return DynamicField(
            enum=lambda: cls.shared().fetch_model_names(),
            default=lambda: cls.shared().fetch_default_model_name(),
            **{"ui:options": lambda: cls.shared().fetch_model_groups_options()},
        )
