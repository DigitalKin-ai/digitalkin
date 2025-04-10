"""Factory functions for creating gRPC servers."""

from pathlib import Path
from typing import Any

from digitalkin.grpc_servers.module_server import ModuleServer
from digitalkin.grpc_servers.registry_server import RegistryServer
from digitalkin.grpc_servers.utils.models import (
    ModuleServerConfig,
    RegistryServerConfig,
    SecurityMode,
    ServerCredentials,
    ServerMode,
)
from digitalkin.modules._base_module import BaseModule


def create_module_server(
    module: type[BaseModule],
    host: str = "0.0.0.0",  # noqa: S104
    port: int = 50051,
    max_workers: int = 10,
    mode: str = "sync",
    security: str = "insecure",
    registry_address: str | None = None,
    server_key_path: str | None = None,
    server_cert_path: str | None = None,
    root_cert_path: str | None = None,
    server_options: list[tuple[str, Any]] | None = None,
) -> ModuleServer:
    """Create a new module server.

    Args:
        module: The module to serve.
        host: The host address to bind to.
        port: The port to listen on.
        max_workers: Maximum number of workers for the thread pool (sync mode only).
        mode: Server mode ("sync" or "async").
        security: Security mode ("secure" or "insecure").
        registry_address: Optional address of a registry server for auto-registration.
        server_key_path: Path to server private key (required for secure mode).
        server_cert_path: Path to server certificate (required for secure mode).
        root_cert_path: Optional path to root certificate.
        server_options: Additional server options.

    Returns:
        A configured ModuleServer instance.

    Raises:
        ValueError: If secure mode is requested but credentials are missing.
    """
    # Create configuration with credentials if needed
    config = _create_server_config(
        ModuleServerConfig,
        host=host,
        port=port,
        max_workers=max_workers,
        mode=mode,
        security=security,
        server_key_path=server_key_path,
        server_cert_path=server_cert_path,
        root_cert_path=root_cert_path,
        server_options=server_options,
        registry_address=registry_address,
    )

    # Create and return the server
    return ModuleServer(module, config)


def create_registry_server(
    host: str = "0.0.0.0",  # noqa: S104
    port: int = 50052,
    max_workers: int = 10,
    mode: str = "sync",
    security: str = "insecure",
    database_url: str | None = None,
    server_key_path: str | None = None,
    server_cert_path: str | None = None,
    root_cert_path: str | None = None,
    server_options: list[tuple[str, Any]] | None = None,
) -> RegistryServer:
    """Create a new registry server.

    Args:
        host: The host address to bind to.
        port: The port to listen on.
        max_workers: Maximum number of workers for the thread pool (sync mode only).
        mode: Server mode ("sync" or "async").
        security: Security mode ("secure" or "insecure").
        database_url: Optional database URL for registry data storage.
        server_key_path: Path to server private key (required for secure mode).
        server_cert_path: Path to server certificate (required for secure mode).
        root_cert_path: Optional path to root certificate.
        server_options: Additional server options.

    Returns:
        A configured RegistryServer instance.

    Raises:
        ValueError: If secure mode is requested but credentials are missing.
    """
    # Create configuration with credentials if needed
    config = _create_server_config(
        RegistryServerConfig,
        host=host,
        port=port,
        max_workers=max_workers,
        mode=mode,
        security=security,
        server_key_path=server_key_path,
        server_cert_path=server_cert_path,
        root_cert_path=root_cert_path,
        server_options=server_options,
        database_url=database_url,
    )

    # Create and return the server
    return RegistryServer(config)


def _create_server_config(
    config_class: Any,
    host: str,
    port: int,
    max_workers: int,
    mode: str,
    security: str,
    server_key_path: str | None,
    server_cert_path: str | None,
    root_cert_path: str | None,
    server_options: list[tuple[str, Any]] | None,
    **kwargs,
) -> ModuleServerConfig | RegistryServerConfig:
    """Create a server configuration with appropriate settings.

    Args:
        config_class: The configuration class to instantiate.
        host: The host address.
        port: The port number.
        max_workers: Maximum number of workers.
        mode: Server mode.
        security: Security mode.
        server_key_path: Path to server key.
        server_cert_path: Path to server certificate.
        root_cert_path: Path to root certificate.
        server_options: Additional server options.
        **kwargs: Additional configuration parameters.

    Returns:
        A server configuration instance.

    Raises:
        ValueError: If secure mode is requested but credentials are missing.
    """
    # Create basic config
    config_params = {
        "host": host,
        "port": port,
        "max_workers": max_workers,
        "mode": ServerMode(mode),
        "security": SecurityMode(security),
        "server_options": server_options or [],
        **kwargs,
    }

    # Add credentials if secure mode
    if security == "secure":
        if not server_key_path or not server_cert_path:
            raise ValueError(
                "Server key and certificate paths are required for secure mode"
            )

        config_params["credentials"] = ServerCredentials(
            server_key_path=Path(server_key_path),
            server_cert_path=Path(server_cert_path),
            root_cert_path=Path(root_cert_path) if root_cert_path else None,
        )

    return config_class(**config_params)
