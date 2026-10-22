"""Model specifications parsed from the LiteLLM proxy catalogue."""

from decimal import Decimal
from enum import Enum
from typing import ClassVar, Literal

from pydantic import BaseModel, Field, computed_field, field_serializer


class Provider(str, Enum):
    """Model providers."""

    OPENAI = "OpenAI"
    ANTHROPIC = "Anthropic"
    GOOGLE = "Google"
    XAI = "xAI"
    META = "Meta"
    COHERE = "Cohere"
    AZURE = "azure"
    BEDROCK = "bedrock"
    OTHER = "other"


class ProviderMapper:
    """Maps the provider prefix of a LiteLLM model path to a :class:`Provider`."""

    mapping: ClassVar[dict[str, Provider]] = {
        "anthropic": Provider.ANTHROPIC,
        "bedrock": Provider.BEDROCK,
        "openai": Provider.OPENAI,
        "azure": Provider.AZURE,
        "google": Provider.GOOGLE,
        "vertex": Provider.GOOGLE,
        "gemini": Provider.GOOGLE,
        "xai": Provider.XAI,
        "grok": Provider.XAI,
        "meta": Provider.META,
        "llama": Provider.META,
        "cohere": Provider.COHERE,
    }

    @classmethod
    def normalize(cls, provider_str: str | None) -> Provider:
        """Normalize a raw provider string.

        Args:
            provider_str: Raw provider string.

        Returns:
            The exact match, else the first key contained in the string, else ``Provider.OTHER``.
        """
        if not provider_str:
            return Provider.OTHER
        normalized = provider_str.lower().strip()
        if normalized in cls.mapping:
            return cls.mapping[normalized]
        return next((provider for key, provider in cls.mapping.items() if key in normalized), Provider.OTHER)


class ThoughtCapacity(BaseModel):
    """Reasoning capacity of a model."""

    type: Literal["token_budget", "level"] = Field(
        default="token_budget", description="Type of thought capacity specification"
    )
    value: Literal["high", "medium", "low", "none"] = Field(description="Capacity level")


class SupportInfo(BaseModel):
    """Feature support flags."""

    supports_system_messages: bool | None = None
    supports_response_schema: bool | None = None
    supports_vision: bool | None = None
    supports_function_calling: bool | None = None
    supports_tool_choice: bool | None = None
    supports_assistant_prefill: bool | None = None
    supports_prompt_caching: bool | None = None

    supports_audio_input: bool | None = None
    supports_audio_output: bool | None = None
    supports_video_input: bool | None = None
    supports_pdf_input: bool | None = None
    supports_embedding_image_input: bool | None = None

    supports_native_streaming: bool | None = None
    supports_web_search: bool | None = None
    supports_reasoning: bool | None = None

    @property
    def is_multimodal(self) -> bool:
        """Whether the model supports any multimodal input or output."""
        return any((
            self.supports_vision,
            self.supports_audio_input,
            self.supports_audio_output,
            self.supports_video_input,
            self.supports_pdf_input,
            self.supports_embedding_image_input,
        ))


class CapacityInfo(BaseModel):
    """Context and output token limits."""

    context_window: int = Field(description="Context window")
    max_output: int = Field(description="Max output tokens")

    @property
    def total_capacity(self) -> int:
        """Context window plus max output tokens."""
        return self.context_window + self.max_output

    @property
    def input_budget(self) -> int:
        """Tokens a request's input may use: the window minus the output a request may ask for.

        Providers such as Anthropic and OpenAI count ``max_tokens`` against the same window as the
        input. A max output as large as the window means a separate output budget (Gemini as the
        proxy reports it), so nothing is reserved then.

        Returns:
            The input budget; 0 when the window is unknown.
        """
        if self.max_output < self.context_window:
            return self.context_window - self.max_output
        return self.context_window


class CostInfo(BaseModel):
    """Prices, in dollars per million units (tokens unless the field says otherwise)."""

    input_cost_per_token: Decimal
    output_cost_per_token: Decimal

    cache_read_input_token_cost: Decimal | None = None
    cache_creation_input_token_cost: Decimal | None = None

    input_cost_per_character: Decimal | None = None
    output_cost_per_character: Decimal | None = None
    input_cost_per_audio_token: Decimal | None = None
    output_cost_per_audio_token: Decimal | None = None
    output_cost_per_image: Decimal | None = None
    input_cost_per_query: Decimal | None = None
    input_cost_per_second: Decimal | None = None
    output_cost_per_second: Decimal | None = None

    input_cost_per_token_above_128k_tokens: Decimal | None = None
    input_cost_per_token_above_200k_tokens: Decimal | None = None
    output_cost_per_token_above_128k_tokens: Decimal | None = None
    output_cost_per_token_above_200k_tokens: Decimal | None = None

    output_cost_per_reasoning_token: Decimal | None = None

    @property
    def has_tiered_pricing(self) -> bool:
        """Whether any above-128k/200k tier is priced."""
        return any((
            self.input_cost_per_token_above_128k_tokens,
            self.input_cost_per_token_above_200k_tokens,
            self.output_cost_per_token_above_128k_tokens,
            self.output_cost_per_token_above_200k_tokens,
        ))

    @property
    def cost_per_million_tokens(self) -> dict[str, Decimal | None]:
        """Input and output token prices scaled by one million."""
        return {
            "input": self.input_cost_per_token * 1_000_000 if self.input_cost_per_token else None,
            "output": self.output_cost_per_token * 1_000_000 if self.output_cost_per_token else None,
        }


class ModelSpec(BaseModel):
    """Specification of one LiteLLM proxy model group."""

    provider: Provider = Field(description="Model provider")
    name: str = Field(description="Model name")
    model_id: str = Field(description="Model identifier")

    capacity: CapacityInfo
    costs: CostInfo
    support: SupportInfo

    thought_capacity: ThoughtCapacity | None = None
    multimodal: bool = Field(default=False, description="Has multimodal capabilities")
    documentation_url: str | None = Field(None, description="Documentation URL")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_capacity(self) -> int:
        """Context window plus max output tokens."""
        return self.capacity.context_window + self.capacity.max_output

    @property
    def has_native_reasoning(self) -> bool:
        """Whether the model reasons natively (a reasoning capacity other than ``none``)."""
        return self.thought_capacity is not None and self.thought_capacity.value != "none"

    @field_serializer("provider")
    def serialize_provider(self, value: Provider) -> str:  # ruff: ignore[no-self-use]
        """Serialize the provider as its string value for protobuf compatibility.

        Args:
            value: The provider.

        Returns:
            The provider's value.
        """
        return value.value
