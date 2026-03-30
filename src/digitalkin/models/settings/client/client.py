from pydantic import Field, field_validator
from pydantic.v1 import IPvAnyAddress
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.grpc_servers.models import (
    ClientConfig,
    ClientCredentials,
    GrpcCompression,
)
from digitalkin.models.settings.utils.channel import CommunicationMode, SecurityMode


class ClientSettings(BaseSettings):
    """Settings for the gRPC client toward the services provider."""

    model_config = SettingsConfigDict(env_prefix="SERVICES_PROVIDER_", case_sensitive=False)

    host: str = Field(default="[::]", description="Service provider host.")
    port: int = Field(default=50151, description="Service provider port.")
    mode: CommunicationMode = Field(default=CommunicationMode.ASYNC, description="Client execution mode.")
    security: SecurityMode = Field(default=SecurityMode.INSECURE, description="Client security mode.")
    mtls: bool = Field(default=False, description="Enable mutual TLS")

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, v: str | CommunicationMode) -> CommunicationMode:
        """Normalize mode value.

        Returns:
            The normalized ServerMode.
        """
        if isinstance(v, CommunicationMode):
            return v
        return CommunicationMode.SYNC if str(v).lower() == "sync" else CommunicationMode.ASYNC

    @field_validator("security", mode="before")
    @classmethod
    def _normalize_security(cls, v: str | SecurityMode) -> SecurityMode:
        """Normalize security value.

        Returns:
            The normalized SecurityMode.
        """
        if isinstance(v, SecurityMode):
            return v
        return SecurityMode.SECURE if str(v).lower() == "secure" else SecurityMode.INSECURE

    def to_client_config(self) -> ClientConfig:
        """Convert to ClientConfig.

        Returns:
            The client configuration.
        """
        creds: ClientCredentials | None = None
        if self.security == SecurityMode.SECURE:
            certs = CertificateSettings()
            ck, cc, ca = certs.get_services_provider_client_certificate_paths(mtls=self.mtls)
            creds = ClientCredentials(
                client_key_path=ck,
                client_cert_path=cc,
                root_cert_path=ca,
            )

        return ClientConfig(
            host=self.host,
            port=self.port,
            mode=self.mode,
            security=self.security,
            credentials=creds,
            compression=GrpcCompression.GZIP,
        )
