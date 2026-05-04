"""Consumer-side settings for the gateway dial-back protocol.

These are the env-var-backed defaults consumed by
:class:`digitalkin.services.communication.ConsumerConfig`. Any caller
that constructs a ``ConsumerConfig`` without overriding a field gets the
value sourced from these settings (and therefore from the environment).
"""

from pydantic import Field, NonNegativeInt, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsumerSettings(BaseSettings):
    """Defaults for the SDK-side dial-back consumer.

    Env prefix: ``DIGITALKIN_CONSUMER_``. Example overrides::

        DIGITALKIN_CONSUMER_PORT=50057
        DIGITALKIN_CONSUMER_LISTEN=0.0.0.0
        DIGITALKIN_CONSUMER_ADVERTISE_ADDRESS=ada-server:50057
        DIGITALKIN_CONSUMER_QUEUE_MAXSIZE=2048
    """

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_CONSUMER_", case_sensitive=False)

    listen: str = Field(default="[::]", description="Bind interface for the standalone dial-back server.")
    port: PositiveInt = Field(default=50057, description="Bind port for the standalone dial-back server.")
    advertise_address: str = Field(
        default="",
        description=(
            "host:port the gateway will dial back. Sent as x-client-address. "
            "When empty, ConsumerConfig.effective_advertise falls back to listen:port."
        ),
    )
    secure_mode: bool = Field(default=False, description="Use TLS for the outbound gateway channel.")
    cert_path: str = Field(default="", description="Directory containing ca.crt (when secure_mode).")
    queue_maxsize: NonNegativeInt = Field(
        default=1024,
        description="Per-task output backpressure ceiling. 0 disables the bound.",
    )
    dial_back_bidi_timeout_s: float = Field(
        default=300.0,
        description=(
            "gRPC BiDi deadline on the gateway → consumer Stream. Hard ceiling. "
            "Applies on the gateway side via GatewaySettings.dial_back_bidi_timeout_s; "
            "exposed here for symmetry / observability."
        ),
    )
