"""Gateway settings — stream management, backpressure, queues, reaper."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewayStreamSettings(BaseSettings):
    """Redis Stream configuration for gateway data flow."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_", case_sensitive=False)

    redis_stream_ttl: int = Field(default=60, description="Stream TTL in seconds after EOS")
    redis_stream_maxlen: int = Field(default=1000, description="Approximate max entries before trimming")
    redis_cursor_ttl: int = Field(default=360, description="Cursor key TTL in seconds")
    stream_read_block_ms: int = Field(default=50, description="XREAD block timeout in milliseconds")
    stream_batch_size: int = Field(default=20, description="Entries per pipeline flush")
    stream_flush_ms: int = Field(default=50, description="Max ms between adaptive flushes")
    from_seq_multiplier: int = Field(
        default=10,
        description=(
            "Upper bound on a client's resume `seq` value, expressed as a multiple "
            "of ``redis_stream_maxlen``. Seq values above ``redis_stream_maxlen * "
            "from_seq_multiplier`` are rejected as obviously out-of-range."
        ),
    )

    @property
    def from_seq_limit(self) -> int:
        """Hard ceiling on a client-supplied resume cursor."""
        return self.redis_stream_maxlen * self.from_seq_multiplier


class GatewayBackpressureSettings(BaseSettings):
    """Backpressure thresholds for stream writers."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_STREAM_", case_sensitive=False)

    backpressure_threshold: float = Field(default=0.8, description="Fraction of maxlen triggering throttle")
    backpressure_delay_ms: int = Field(default=50, description="Sleep ms when above threshold")
    backpressure_check_interval: int = Field(default=100, description="Check XLEN every N writes")
    backpressure_timeout_s: float = Field(default=30.0, description="Max seconds to wait on backpressure")


class GatewayM2MSettings(BaseSettings):
    """Resilience settings for in-module M2M outbound calls.

    Wraps every ``GrpcCommunication.call_module`` invocation with a
    TTL'd registry entry, a per-target circuit breaker, a process-wide
    concurrency cap, and per-call deadlines. All fields are
    env-overridable under the ``DIGITALKIN_M2M_`` prefix.
    """

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_M2M_", case_sensitive=False)

    call_ttl_s: float = Field(
        default=300.0,
        description=(
            "Maximum lifetime of an outbound registry entry. A periodic sweeper "
            "drops + signals entries past their TTL even if the call's finally "
            "block never ran."
        ),
    )
    call_sweeper_interval_s: float = Field(
        default=30.0,
        description="How often the TTL sweeper scans the outbound registry.",
    )
    call_timeout_s: float = Field(
        default=120.0,
        description=(
            "Per-output deadline on the queue. Trips on producers that go silent without emitting stream.end."
        ),
    )
    call_max_concurrent: int = Field(
        default=200,
        description="Process-local ceiling on in-flight outbound calls.",
    )
    call_acquire_timeout_s: float = Field(
        default=30.0,
        description="How long call_module blocks on the concurrency semaphore before raising.",
    )
    call_breaker_fail_max: int = Field(
        default=5,
        description="Failures (per target host:port) before the per-target circuit breaker opens.",
    )
    call_breaker_reset_timeout_s: float = Field(
        default=30.0,
        description="How long a breaker stays open before a half-open probe.",
    )
    call_cancel_signal_timeout_s: float = Field(
        default=2.0,
        description="Best-effort SendSignal(CANCEL) deadline when call_module is cancelled.",
    )
    call_queue_maxsize: int = Field(
        default=1024,
        description="Per-call output queue ceiling.",
    )


class GatewayQueueSettings(BaseSettings):
    """Queue and timeout settings for gateway sessions."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_", case_sensitive=False)

    output_queue_size: int = Field(default=512, description="Retired; kept for forward-compat (no effect)")
    input_queue_size: int = Field(default=512, description="Retired; kept for forward-compat (no effect)")
    enqueue_timeout_s: float = Field(
        default=5.0, description="Retired; kept for forward-compat (no effect)"
    )
    toolkit_cache_ttl_s: float = Field(
        default=600.0,
        description=(
            "TTL for the per-setup tool cache "
            "(``ModuleServicer._tool_cache_by_setup``). Entries older than "
            "this are recomputed on next lookup. The INVALIDATE_TOOLS "
            "SendSignal flushes the whole cache regardless of TTL."
        ),
    )


class GatewaySettings(BaseSettings):
    """Top-level gateway configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GATEWAY_", case_sensitive=False)

    max_streams: int = Field(default=20000, description="Max concurrent gateway sessions (per instance)")
    max_local_cache: int = Field(default=5000, description="Max local session cache entries")
    redis_health_timeout: float = Field(default=5.0, description="Redis health check timeout in seconds")
    dial_back_bidi_timeout_s: float = Field(
        default=300.0,
        description=(
            "Hard deadline on the gateway → consumer Stream BiDi. The dial-back "
            "task is cancelled if it has not completed within this window."
        ),
    )
    dial_back_close_grace_s: float = Field(
        default=2.0,
        description=(
            "Grace period after the gateway emits the terminal stream.end on the "
            "dial-back BiDi before forcibly closing the inbound side. Guards "
            "against a consumer that ignores stream.end and would otherwise hold "
            "the BiDi open until keepalive (~2 min) surfaces UNAVAILABLE."
        ),
    )

    stream: GatewayStreamSettings = Field(default_factory=GatewayStreamSettings)
    backpressure: GatewayBackpressureSettings = Field(default_factory=GatewayBackpressureSettings)
    queue: GatewayQueueSettings = Field(default_factory=GatewayQueueSettings)
    m2m: GatewayM2MSettings = Field(default_factory=GatewayM2MSettings)
