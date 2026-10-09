"""Registry hardening: enum encode/decode symmetry + registry-scoped settings."""

from enum import Enum
from unittest.mock import AsyncMock

import pytest
from agentic_mesh_protocol.common.v1 import common_enums_pb2
from agentic_mesh_protocol.module.v1 import module_enums_pb2
from agentic_mesh_protocol.registry.v1 import registry_dto_pb2, registry_messages_pb2
from agentic_mesh_protocol.setup.v1 import setup_enums_pb2
from google.protobuf import json_format

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.registry import (
    RegistryModuleStatus,
    RegistryModuleType,
    RegistrySetupStatus,
    RegistrySortBy,
    RegistryVisibility,
)
from digitalkin.models.settings.registry import get_registry_settings
from digitalkin.models.settings.utils.channel import SecurityMode
from digitalkin.services.registry.grpc_registry import GrpcRegistry

_ENUM_CASES = [
    (module_enums_pb2.ModuleType, "MODULE_TYPE", RegistryModuleType),
    (module_enums_pb2.ModuleStatus, "MODULE_STATUS", RegistryModuleStatus),
    (setup_enums_pb2.SetupStatus, "SETUP_STATUS", RegistrySetupStatus),
    (common_enums_pb2.Visibility, "VISIBILITY", RegistryVisibility),
]


@pytest.mark.parametrize(("proto_enum", "prefix", "py_enum"), _ENUM_CASES)
def test_every_member_encodes_to_valid_proto_name(proto_enum: object, prefix: str, py_enum: type[Enum]) -> None:
    """Every Python registry enum member but UNSPECIFIED maps to a proto member the server accepts.

    Regression for silent Python/proto enum-name drift, which would otherwise produce an
    unrecognized filter string and fail the invocable-only guard open.
    """
    for member in py_enum:
        if member.name == "UNSPECIFIED":
            continue
        name = GrpcRegistry._encode_enum(proto_enum, member)
        assert proto_enum.Name(proto_enum.Value(name)) == member.name


@pytest.mark.parametrize(("proto_enum", "prefix", "py_enum"), _ENUM_CASES)
def test_every_proto_member_decodes(proto_enum: object, prefix: str, py_enum: type[Enum]) -> None:
    """Every proto member decodes by name, so a registry answer never fails the model (e.g. VALIDATING)."""
    for name in proto_enum.DESCRIPTOR.values_by_name:
        assert py_enum[name.removeprefix(f"{prefix}_")].name == name.removeprefix(f"{prefix}_")


@pytest.mark.parametrize(("proto_enum", "prefix", "py_enum"), _ENUM_CASES)
def test_unspecified_fails_closed(proto_enum: object, prefix: str, py_enum: type[Enum]) -> None:
    """UNSPECIFIED has no bare proto member and the registry refuses the zero value: it raises."""
    with pytest.raises(ValueError, match="UNSPECIFIED"):
        GrpcRegistry._encode_enum(proto_enum, py_enum.UNSPECIFIED)


def test_unknown_member_fails_closed() -> None:
    """A member with no proto counterpart raises instead of sending a bogus filter."""

    class _Drifted(Enum):
        NONEXISTENT = "nonexistent"

    with pytest.raises(ValueError, match="NONEXISTENT"):
        GrpcRegistry._encode_enum(setup_enums_pb2.SetupStatus, _Drifted.NONEXISTENT)


def test_sort_keys_match_the_registry_order_rule() -> None:
    """Every sort key is one the SearchModules/SearchSetups CEL rule accepts as ``pagination.order``."""
    assert {member.value for member in RegistrySortBy if member is not RegistrySortBy.UNSPECIFIED} <= {
        "name",
        "created_at",
        "updated_at",
    }


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
    get_registry_settings.cache_clear()
    client = GrpcRegistry(
        "missions:m", "setups:s", "v1", ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    )
    client.exec_grpc_query = AsyncMock(return_value=registry_dto_pb2.SearchSetupsResponse())
    assert await client.search_setups(query="x") == []
    assert client.exec_grpc_query.await_args.kwargs["timeout"] == pytest.approx(10.0)
    get_registry_settings.cache_clear()


async def test_register_forwards_documentation_and_type_to_request() -> None:
    """register() attaches documentation and the declared type to the RegisterModuleRequest."""
    client = GrpcRegistry(
        "missions:m", "setups:s", "v1", ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    )
    client.exec_grpc_query = AsyncMock(
        return_value=registry_dto_pb2.RegisterModuleResponse(
            result=registry_messages_pb2.RegistryResult(
                identifier="modules:x",
                module_descriptor=registry_messages_pb2.ModuleDescriptor(id="modules:x", type=module_enums_pb2.SERVICE),
            )
        )
    )

    info = await client.register("modules:x", "h", 1, "1.0.0", RegistryModuleType.SERVICE, documentation="indexed docs")

    request = client.exec_grpc_query.await_args.args[1]
    assert request.documentation == "indexed docs"
    assert request.type == module_enums_pb2.SERVICE
    assert info is not None
    assert info.module_type == RegistryModuleType.SERVICE


async def test_register_forwards_schemas_to_request() -> None:
    """register() merges every schema into its RegisterModuleRequest field, an empty one included."""
    client = GrpcRegistry(
        "missions:m", "setups:s", "v1", ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    )
    client.exec_grpc_query = AsyncMock(
        return_value=registry_dto_pb2.RegisterModuleResponse(
            result=registry_messages_pb2.RegistryResult(
                identifier="modules:x",
                module_descriptor=registry_messages_pb2.ModuleDescriptor(id="modules:x", type=module_enums_pb2.SERVICE),
            )
        )
    )

    await client.register(
        "modules:x",
        "h",
        1,
        "1.0.0",
        RegistryModuleType.SERVICE,
        schemas={"input_schema": {"type": "object", "title": "In"}, "secret_schema": {}},
    )

    request = client.exec_grpc_query.await_args.args[1]
    assert request.input_schema["title"] == "In"
    assert request.HasField("secret_schema")
    assert not request.HasField("output_schema")
    assert request.module_id == "modules:x"


async def test_register_refuses_unknown_schema_field_before_the_registry_is_contacted() -> None:
    """An unknown schema key is a permanent error raised as-is, never sent nor wrapped."""
    client = GrpcRegistry(
        "missions:m", "setups:s", "v1", ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    )
    client.exec_grpc_query = AsyncMock()

    with pytest.raises(json_format.ParseError, match="user_info_schema"):
        await client.register(
            "modules:x", "h", 1, "1.0.0", RegistryModuleType.SERVICE, schemas={"user_info_schema": {}}
        )

    client.exec_grpc_query.assert_not_awaited()
