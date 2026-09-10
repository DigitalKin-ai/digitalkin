"""Turn a module process's environment into the runtime configuration it needs."""

from argparse import ArgumentParser
from typing import ClassVar

from pydantic import SecretStr
from pydantic_settings import BaseSettings

from digitalkin.models.grpc_servers.models import ClientConfig, ClientCredentials
from digitalkin.models.services.services import ServicesMode
from digitalkin.models.settings.certificate import get_certificate_settings
from digitalkin.models.settings.client.client import get_client_settings
from digitalkin.models.settings.module import get_module_settings
from digitalkin.models.settings.server.server import get_server_settings
from digitalkin.models.settings.utils.channel import Credentials, SecurityMode


class EnvManager:
    """Environment-driven configuration shared by every module.

    A module declares nothing: the SDK reads ``SERVICE_MODE``, the ``CLIENT_*``
    settings and, in secure mode, the ``CERTIFICATE_*`` volumes, then hands the
    resulting :class:`ClientConfig` to the module server and to every remote
    service strategy that did not receive one explicitly.

    The config is built once per process because it reads certificates from
    disk and is requested on every strategy instantiation.
    """

    _client_config: ClassVar[ClientConfig | None] = None

    @classmethod
    def services_mode(cls) -> ServicesMode:
        """Execution mode of the service strategies.

        The ``-d/--dev-mode`` flag wins over ``SERVICE_MODE`` — a module is
        usually launched with the flag — so the server and the servicer agree
        on the mode before the servicer parses its own arguments. An
        unrecognised flag value falls through to the environment and is
        reported by the servicer's own ``choices`` check.

        Returns:
            ``ServicesMode.REMOTE`` in remote mode, else ``ServicesMode.LOCAL``.
        """
        parser = ArgumentParser(add_help=False)
        parser.add_argument("-d", "--dev-mode", dest="services_mode", default=None)
        flag = parser.parse_known_args()[0].services_mode
        if flag is not None:
            try:
                return ServicesMode(str(flag).lower())
            except ValueError:
                pass
        return get_module_settings().services_mode

    @classmethod
    def client_config(cls) -> ClientConfig:
        """Process-wide ``ClientConfig`` built from the environment.

        Host, port, mode, security, compression, channel options and retry
        policy come from ``ClientSettings``. In secure mode the credentials are
        resolved from the services-provider certificate volume unless
        ``CLIENT_CHANNEL_CREDENTIALS__*`` already names them.

        Returns:
            The shared ``ClientConfig`` instance.
        """
        # Bound to EnvManager, not to cls: the config is process-wide, so a
        # module-specific subclass must not build a second one.
        if EnvManager._client_config is not None:
            return EnvManager._client_config

        channel = get_client_settings().channel
        credentials: ClientCredentials | None = None
        if channel.security == SecurityMode.SECURE:
            if channel.credentials is not None and channel.credentials.root_cert_path is not None:
                credentials = ClientCredentials(
                    root_cert_path=channel.credentials.root_cert_path,
                    client_key_path=channel.credentials.key_path,
                    client_cert_path=channel.credentials.cert_path,
                )
            else:
                key, cert, ca = get_certificate_settings().get_client_certificate_paths(mtls=channel.mtls)
                credentials = ClientCredentials(root_cert_path=ca, client_key_path=key, client_cert_path=cert)

        EnvManager._client_config = ClientConfig(credentials=credentials)
        return EnvManager._client_config

    @classmethod
    def server_credentials(cls) -> Credentials:
        """Key, certificate and root CA the module serves with in secure mode.

        Mirrors :meth:`client_config`: ``SERVER_CHANNEL_CREDENTIALS__*`` names
        the files explicitly, otherwise they are read from the
        ``CERTIFICATE_CERT_VOLUME`` directory. The root CA is returned only
        when ``SERVER_CHANNEL_MTLS`` is set, because handing it to gRPC turns
        on client authentication.

        Resolved on demand rather than cached: the server reads it once at
        startup.

        Returns:
            The server credentials.
        """
        channel = get_server_settings().channel
        if channel.credentials is not None and channel.credentials.key_path and channel.credentials.cert_path:
            return channel.credentials

        key, cert, ca = get_certificate_settings().get_server_certificate_paths(mtls=channel.mtls)
        return Credentials(key_path=key, cert_path=cert, root_cert_path=ca)

    @classmethod
    def unset_required(cls, *settings: BaseSettings) -> list[str]:
        """Names of the ``env_required`` variables left empty in the given settings.

        The same marker drives the minimum ``.env`` template, so what the
        template calls required is exactly what a module refuses to boot without.

        Args:
            settings: Settings instances to inspect.

        Returns:
            The environment variable names whose value is empty.
        """
        missing: list[str] = []
        for instance in settings:
            prefix = instance.model_config.get("env_prefix", "")
            fields = type(instance).model_fields
            for field_name, value in instance.model_dump().items():
                field = fields[field_name]
                extra = field.json_schema_extra if isinstance(field.json_schema_extra, dict) else {}
                secret = value.get_secret_value() if isinstance(value, SecretStr) else value
                if extra.get("env_required") and not secret:
                    missing.append(str(field.alias or field.validation_alias or f"{prefix}{field_name}").upper())
        return missing

    @classmethod
    def reset(cls) -> None:
        """Drop the cached ``ClientConfig``. Tests must call this after mutating env."""
        EnvManager._client_config = None
