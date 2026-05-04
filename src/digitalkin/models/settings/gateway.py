"""Gateway settings — stream management, backpressure, queues, reaper."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GatewayStreamSettings(BaseSettings):
    """Redis Stream configuration for gateway data flow."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_", case_sensitive=False)

    redis_stream_ttl: int = Field(default=60, description="Stream TTL in seconds after EOS")
    redis_stream_maxlen: int = Field(default=1000, description="Approximate max entries before trimming")
    redis_cursor_ttl: int = Field(default=360, description="Cursor key TTL in seconds")
    stream_read_block_ms: int = Field(default=5, description="XREAD block timeout in milliseconds")
    stream_batch_size: int = Field(default=20, description="Entries per pipeline flush")
    stream_flush_ms: int = Field(default=50, description="Max ms between adaptive flushes")


class GatewayBackpressureSettings(BaseSettings):
    """Backpressure thresholds for stream writers."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_STREAM_", case_sensitive=False)

    backpressure_threshold: float = Field(default=0.8, description="Fraction of maxlen triggering throttle")
    backpressure_delay_ms: int = Field(default=50, description="Sleep ms when above threshold")
    backpressure_check_interval: int = Field(default=100, description="Check XLEN every N writes")
    backpressure_timeout_s: float = Field(default=30.0, description="Max seconds to wait on backpressure")


class GatewayQueueSettings(BaseSettings):
    """Queue and timeout settings for gateway sessions."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_", case_sensitive=False)

    output_queue_size: int = Field(default=512, description="Output queue max size")
    input_queue_size: int = Field(default=512, description="Input queue max size")
    enqueue_timeout_s: float = Field(default=5.0, description="Queue enqueue timeout in seconds")
    dispatcher_input_wait_s: float = Field(
        default=60.0,
        description=(
            "Max seconds the dispatcher waits for the consumer's first input "
            "(via session.input_queue) before emitting INPUT_WAIT_TIMEOUT. "
            "Must stay below the dial-back BiDi ceiling (300s) so the dispatcher "
            "times out first with a meaningful error code."
        ),
    )


class GatewaySettings(BaseSettings):
    """Top-level gateway configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GATEWAY_", case_sensitive=False)

    max_streams: int = Field(default=20000, description="Max concurrent gateway sessions (cluster-wide)")
    max_local_cache: int = Field(default=5000, description="Max local session cache entries")
    heartbeat_ttl: int = Field(default=45, description="Heartbeat timeout in seconds")
    reaper_interval: int = Field(default=30, description="Reaper check interval in seconds")
    session_state_ttl: int = Field(default=3600, description="Session metadata TTL in seconds")
    redis_health_timeout: float = Field(default=5.0, description="Redis health check timeout in seconds")
    dial_back_bidi_timeout_s: float = Field(
        default=300.0,
        description=(
            "Hard deadline on the gateway → consumer Stream BiDi. The dial-back "
            "task is cancelled if it has not completed within this window."
        ),
    )

    stream: GatewayStreamSettings = Field(default_factory=GatewayStreamSettings)
    backpressure: GatewayBackpressureSettings = Field(default_factory=GatewayBackpressureSettings)
    queue: GatewayQueueSettings = Field(default_factory=GatewayQueueSettings)
