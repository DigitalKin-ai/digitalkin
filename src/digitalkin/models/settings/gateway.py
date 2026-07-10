"""Gateway settings — stream management, backpressure, queues, reaper."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewayStreamSettings(BaseSettings):
    """Redis Stream configuration for gateway data flow."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GATEWAY_STREAM_", case_sensitive=False)

    redis_stream_ttl: int = Field(
        default=360,
        description="Stream TTL (s) after EOS; >= reconnect window so a completed stream survives a reboot",
    )
    redis_stream_initial_ttl: int = Field(default=600, description="Stream TTL in seconds before EOS")
    redis_stream_maxlen: int = Field(default=1000, gt=0, description="Approximate max entries before trimming")
    redis_cursor_ttl: int = Field(default=360, description="Cursor key TTL in seconds")
    stream_read_block_ms: int = Field(default=50, description="XREAD block timeout in milliseconds")
    read_idle_timeout_s: float = Field(
        default=300.0,
        description=(
            "Max seconds a consumer's Stream read waits with no new entry before giving up. "
            "Backstops a producer that died without writing an EOS marker (module crash or "
            "cancellation) so the consumer's RPC can't hang forever."
        ),
    )
    from_seq_multiplier: int = Field(
        default=10,
        gt=0,
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
        gt=0,
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
    call_associate_timeout_s: float = Field(
        default=5.0,
        description="Per-call deadline on the backend AssociateTask mint (bounded, not the 30s default).",
    )
    call_queue_maxsize: int = Field(
        default=1024,
        gt=0,
        description="Per-call output queue ceiling.",
    )


class GatewayQueueSettings(BaseSettings):
    """Queue and timeout settings for gateway sessions."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GATEWAY_QUEUE_", case_sensitive=False)

    toolkit_cache_ttl_s: float = Field(
        default=600.0,
        description=(
            "TTL for the per-setup tool cache "
            "(``ModuleServicer._tool_cache_by_setup``). Entries older than "
            "this are recomputed on next lookup. The INVALIDATE_TOOLS "
            "SendSignal flushes the whole cache regardless of TTL."
        ),
    )


class GatewayDialReconnectSettings(BaseSettings):
    """Server-side dial-back auto-reconnect window and backoff."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GATEWAY_DIAL_BACK_RECONNECT_", case_sensitive=False)

    window_s: float = Field(
        default=300.0,
        description=(
            "Total time, measured from the first detected disconnect, that the gateway keeps "
            "re-dialing a dead consumer so a client that lost internet or rebooted can re-attach "
            "and resume from its cursor. Keep <= the post-EOS stream TTL."
        ),
    )
    backoff_base_s: float = Field(default=3.0, description="Base delay between re-dial attempts (full-jittered).")
    backoff_max_s: float = Field(default=5.0, description="Cap on the re-dial backoff delay.")


class GatewaySettings(BaseSettings):
    """Top-level gateway configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GATEWAY_", case_sensitive=False)

    max_streams: int = Field(default=20000, gt=0, description="Max concurrent gateway sessions (per instance)")
    redis_health_timeout: float = Field(default=5.0, description="Redis health check timeout in seconds")
    dial_back_idle_timeout_s: float = Field(
        default=300.0,
        description=(
            "Idle timeout on the gateway → consumer Stream BiDi. Resets on every "
            "outbound chunk; fires only when no module output flows for this many "
            "seconds. Replaces the former absolute `dial_back_bidi_timeout_s`."
        ),
    )
    dial_back_max_lifetime_s: float = Field(
        default=3600.0,
        description=(
            "Absolute safety ceiling on a single dial-back BiDi. Applied as the "
            "gRPC RPC deadline; guards against runaway streams even if the idle "
            "timeout keeps resetting."
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
    queue: GatewayQueueSettings = Field(default_factory=GatewayQueueSettings)
    m2m: GatewayM2MSettings = Field(default_factory=GatewayM2MSettings)
    dial_reconnect: GatewayDialReconnectSettings = Field(default_factory=GatewayDialReconnectSettings)


@lru_cache(maxsize=1)
def get_gateway_settings() -> GatewaySettings:
    """Process-wide ``GatewaySettings`` singleton.

    Nested settings accessed via composition: ``.stream``, ``.queue``, ``.m2m``.
    Tests must call ``get_gateway_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``GatewaySettings`` instance.
    """
    return GatewaySettings()
