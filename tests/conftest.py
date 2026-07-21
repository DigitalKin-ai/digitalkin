"""Common fixtures for test suite."""

import logging

import grpc_testing
import pytest
from _pytest.fixtures import SubRequest

from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.models.settings.grpc_client import (
    get_circuit_breaker_settings,
    get_grpc_channel_settings,
    get_grpc_client_settings,
    get_grpc_retry_settings,
)
from digitalkin.models.settings.log import get_logging_settings
from digitalkin.models.settings.module import get_module_settings
from digitalkin.models.settings.profiling import get_profiling_settings
from digitalkin.models.settings.queue import get_queue_settings
from digitalkin.models.settings.redis import get_redis_settings
from digitalkin.models.settings.resilience import get_bulkhead_settings
from digitalkin.models.settings.server.channel import get_server_channel_settings
from digitalkin.models.settings.server.server import get_server_settings
from digitalkin.models.settings.server.servicer import get_module_servicer_settings
from digitalkin.models.settings.task_manager import get_job_manager_settings, get_task_manager_settings

# Register fixture plugins
pytest_plugins = [
    "tests.fixtures.core_fixtures",
]

# Configure logging for tests
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Silence some loggers during tests
logging.getLogger("grpc").setLevel(logging.WARNING)


_SETTINGS_FACTORIES = (
    get_bulkhead_settings,
    get_circuit_breaker_settings,
    get_gateway_settings,
    get_grpc_channel_settings,
    get_grpc_client_settings,
    get_grpc_retry_settings,
    get_job_manager_settings,
    get_logging_settings,
    get_module_servicer_settings,
    get_module_settings,
    get_profiling_settings,
    get_queue_settings,
    get_redis_settings,
    get_server_channel_settings,
    get_server_settings,
    get_task_manager_settings,
)


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> None:
    """Clear every ``@lru_cache get_*_settings()`` factory before each test.

    Settings are process-wide singletons; without this, env vars set via
    ``monkeypatch.setenv`` in one test would leak into the next via the cached
    factory instance.
    """
    for factory in _SETTINGS_FACTORIES:
        factory.cache_clear()


@pytest.fixture
def server_config_sync_insecure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SERVER_CHANNEL_HOST", "localhost")
    monkeypatch.setenv("SERVER_CHANNEL_PORT", "50051")
    monkeypatch.setenv("SERVER_CHANNEL_COMMUNICATION_MODE", "sync")
    monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "insecure")

    get_server_settings.cache_clear()

@pytest.fixture
def server_config_async_insecure(monkeypatch: pytest.MonkeyPatch):
    """Create an async insecure server configuration."""
    monkeypatch.setenv("SERVER_CHANNEL_HOST", "localhost")
    monkeypatch.setenv("SERVER_CHANNEL_PORT", "50052")
    monkeypatch.setenv("SERVER_CHANNEL_COMMUNICATION_MODE", "async")
    monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "insecure")

    get_server_settings.cache_clear()

@pytest.fixture
def dummy_certs(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """Create dummy certificates for testing secure connections."""
    # Create certificate files
    server_key = tmp_path / "server.key"
    server_cert = tmp_path / "server.crt"
    ca_cert = tmp_path / "ca.crt"

    # Write dummy content
    server_key.write_text("DUMMY KEY CONTENT")
    server_cert.write_text("DUMMY CERT CONTENT")
    ca_cert.write_text("DUMMY CA CERT CONTENT")

    monkeypatch.setenv("SERVER_CHANNEL_CREDENTIALS__KEY_PATH", str(server_key))
    monkeypatch.setenv("SERVER_CHANNEL_CREDENTIALS__CERT_PATH", str(server_cert))
    monkeypatch.setenv("SERVER_CHANNEL_CREDENTIALS__ROOT_CERT_PATH", str(ca_cert))

    return server_key, server_cert, ca_cert


@pytest.fixture
def server_config_sync_secure(dummy_certs, monkeypatch: pytest.MonkeyPatch):
    """Create a sync secure server configuration."""
    monkeypatch.setenv("SERVER_CHANNEL_HOST", "localhost")
    monkeypatch.setenv("SERVER_CHANNEL_PORT", "50053")
    monkeypatch.setenv("SERVER_CHANNEL_COMMUNICATION_MODE", "sync")
    monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "secure")

    get_server_settings.cache_clear()


@pytest.fixture
def server_config_async_secure(dummy_certs, monkeypatch: pytest.MonkeyPatch):
    """Create an async secure server configuration."""
    monkeypatch.setenv("SERVER_CHANNEL_HOST", "localhost")
    monkeypatch.setenv("SERVER_CHANNEL_PORT", "50054")
    monkeypatch.setenv("SERVER_CHANNEL_COMMUNICATION_MODE", "async")
    monkeypatch.setenv("SERVER_CHANNEL_SECURITY", "secure")

    get_server_settings.cache_clear()


@pytest.fixture(scope="module")
def grpc_test_server(request: SubRequest) -> grpc_testing.Server:
    """Generate a Test Server with associated servicers.

    Creates a gRPC testing server from service instances defined in the test module.
    The test module must define two variables:
      - service_instance: An instance of the service to be tested
      - service_name: The service descriptor from the generated protobuf

    Args:
        request: The pytest request object containing module information.

    Raises:
        RuntimeError: If service_instance or service_name is not defined in the test module.

    Returns:
        grpc_testing.Server: Instance of gRPC testing server with the associated service.
    """
    # Get the service instance from the test module
    service_instance = getattr(request.module, "service_instance", None)
    if service_instance is None:
        msg = "Test module must define a variable `service_instance`"
        raise RuntimeError(msg)

    # Get the service descriptor from the test module
    service_name = getattr(request.module, "service_name", None)
    if service_name is None:
        msg = "Test module must define a variable `service_name`"
        raise RuntimeError(msg)

    # Create and return the gRPC testing server
    servicers = {service_name: service_instance}
    return grpc_testing.server_from_dictionary(servicers, grpc_testing.strict_real_time())
