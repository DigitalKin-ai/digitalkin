"""Registry hardening: enum encode/decode symmetry + registry-scoped settings."""

from enum import Enum

import pytest
from agentic_mesh_protocol.registry.v1 import registry_enums_pb2

from digitalkin.models.services.registry import (
    RegistryModuleType,
    RegistrySetupStatus,
    RegistryVisibility,
)
from digitalkin.models.settings.registry import get_registry_settings
from digitalkin.services.registry.grpc_registry import GrpcRegistry

_ENUM_CASES = [
    (registry_enums_pb2.ModuleType, "MODULE_TYPE", RegistryModuleType),
    (registry_enums_pb2.SetupStatus, "SETUP_STATUS", RegistrySetupStatus),
    (registry_enums_pb2.Visibility, "VISIBILITY", RegistryVisibility),
]


@pytest.mark.parametrize(("proto_enum", "prefix", "py_enum"), _ENUM_CASES)
def test_every_member_encodes_to_valid_proto_name(proto_enum: object, prefix: str, py_enum: type[Enum]) -> None:
    """Every Python registry enum member maps to a proto member the server accepts.

    Regression for silent Python/proto enum-name drift, which would otherwise produce an
    unrecognized filter string and fail the invocable-only guard open.
    """
    for member in py_enum:
        name = GrpcRegistry._encode_enum(proto_enum, prefix, member)
        assert name == f"{prefix}_{member.name}"
        assert proto_enum.Name(proto_enum.Value(name)) == name


def test_unknown_member_fails_closed() -> None:
    """A member with no proto counterpart raises instead of sending a bogus filter."""

    class _Drifted(Enum):
        NONEXISTENT = "nonexistent"

    with pytest.raises(ValueError, match="NONEXISTENT"):
        GrpcRegistry._encode_enum(registry_enums_pb2.SetupStatus, "SETUP_STATUS", _Drifted.NONEXISTENT)


def test_registry_settings_default() -> None:
    """The agent-facing search deadline defaults below the global gRPC 30s."""
    get_registry_settings.cache_clear()
    try:
        assert get_registry_settings().search_timeout_s == pytest.approx(10.0)
    finally:
        get_registry_settings.cache_clear()


def test_registry_settings_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """search_timeout_s is tunable via DIGITALKIN_REGISTRY_SEARCH_TIMEOUT_S."""
    monkeypatch.setenv("DIGITALKIN_REGISTRY_SEARCH_TIMEOUT_S", "3.5")
    get_registry_settings.cache_clear()
    try:
        assert get_registry_settings().search_timeout_s == pytest.approx(3.5)
    finally:
        get_registry_settings.cache_clear()


async def test_search_setups_forwards_tuned_deadline() -> None:
    """search_setups forwards the registry-scoped deadline to exec_grpc_query."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from digitalkin.models.grpc_servers.models import ClientConfig
    from digitalkin.models.settings.utils.channel import SecurityMode

    get_registry_settings.cache_clear()
    client = GrpcRegistry(
        "missions:m", "setups:s", "v1", ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    )
    client.exec_grpc_query = AsyncMock(return_value=SimpleNamespace(setups=[]))
    assert await client.search_setups(query="x") == []
    assert client.exec_grpc_query.await_args.kwargs["timeout"] == pytest.approx(10.0)
    get_registry_settings.cache_clear()


async def test_register_forwards_documentation_to_request() -> None:
    """register() attaches documentation to the RegisterModuleRequest for index search."""
    import contextlib
    from unittest.mock import AsyncMock

    from digitalkin.models.grpc_servers.models import ClientConfig
    from digitalkin.models.services.registry import RegistryModuleType
    from digitalkin.models.settings.utils.channel import SecurityMode

    client = GrpcRegistry(
        "missions:m", "setups:s", "v1", ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    )
    client.exec_grpc_query = AsyncMock(return_value=None)
    # register() parses the (mocked) response afterward and raises; the request is already captured.
    with contextlib.suppress(Exception):
        await client.register(
            "modules:x", "h", 1, "1.0.0", RegistryModuleType.TOOL_MODULE, documentation="indexed docs"
        )
    request = client.exec_grpc_query.await_args.args[1]
    assert request.documentation == "indexed docs"
