"""TLS/mTLS certificate volume settings."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.grpc_servers.exceptions import SecurityError


class CertificateSettings(BaseSettings):
    """Directories holding the certificates a module serves and dials with.

    Files follow the deployment convention: ``server.key`` / ``server.crt`` for
    the module's own server, ``client.key`` / ``client.crt`` for its outgoing
    calls, and ``ca.crt`` for the root of trust in both directories.

    Attributes:
        cert_volume (Path): Directory holding the module's server certificates.
        services_provider_cert_volume (Path): Directory holding the client certificates
            used to dial the services provider.

    """

    model_config = SettingsConfigDict(env_prefix="CERTIFICATE_", case_sensitive=False)

    cert_volume: Path = Field(
        default=Path("/certificates"),
        description="Directory where the module's own server certificates are stored",
    )
    services_provider_cert_volume: Path = Field(
        default=Path("/certificates"),
        description="Directory where the services-provider client certificates are stored",
    )

    def get_server_certificate_paths(self, *, mtls: bool = False) -> tuple[Path, Path, Path | None]:
        """Resolve the module server's key, certificate and root CA.

        Args:
            mtls: Return the CA certificate so incoming clients can be authenticated.

        Returns:
            Tuple of (server key, server certificate, root CA or None).

        Raises:
            SecurityError: If the server key or certificate is missing.
        """
        key_path = self.cert_volume / "server.key"
        cert_path = self.cert_volume / "server.crt"
        if not key_path.exists() or not cert_path.exists():
            msg = f"Server key or certificate not found in {self.cert_volume}"
            raise SecurityError(msg)
        ca_path = self.cert_volume / "ca.crt"
        return key_path, cert_path, (ca_path if mtls and ca_path.exists() else None)

    def get_client_certificate_paths(self, *, mtls: bool = False) -> tuple[Path | None, Path | None, Path]:
        """Resolve the client key, certificate and root CA used to dial the services provider.

        Args:
            mtls: Return the client key and certificate so the module can present them.

        Returns:
            Tuple of (client key or None, client certificate or None, root CA).

        Raises:
            SecurityError: If a required file is missing.
        """
        key_path = self.services_provider_cert_volume / "client.key"
        cert_path = self.services_provider_cert_volume / "client.crt"
        ca_path = self.services_provider_cert_volume / "ca.crt"
        if mtls and (not key_path.exists() or not cert_path.exists()):
            msg = f"Missing client.key or client.crt in {self.services_provider_cert_volume}"
            raise SecurityError(msg)
        if not ca_path.exists():
            msg = f"Missing ca.crt in {self.services_provider_cert_volume}"
            raise SecurityError(msg)
        return (key_path if mtls else None, cert_path if mtls else None, ca_path)


@lru_cache(maxsize=1)
def get_certificate_settings() -> CertificateSettings:
    """Process-wide ``CertificateSettings`` singleton.

    Tests must call ``get_certificate_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``CertificateSettings`` instance.
    """
    return CertificateSettings()
