"""Registry registration payload: enforced documentation with its trigger table, and the module schemas."""

import json
from typing import Literal
from unittest.mock import Mock

import protovalidate
import pytest
from agentic_mesh_protocol.registry.v1 import registry_dto_pb2
from google.protobuf import json_format
from pydantic import BaseModel

from digitalkin.models.module.base_types import DataModel, DataTrigger
from digitalkin.models.module.module_types import SetupModel
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.registry import DefaultRegistry
from digitalkin.utils.package_discover import ModuleDiscoverer


class _InputTrigger(DataTrigger):
    protocol: Literal["message"] = "message"
    text: str = ""


class _InputModel(DataModel[_InputTrigger]):
    pass


class _SetupModel(SetupModel):
    pass


class _SecretModel(BaseModel):
    pass


def _module(description: str = "Does a specific thing.", *, metadata_desc: str | None = None) -> type[BaseModule]:
    meta: dict = {"module_id": "modules:test"}
    if metadata_desc is not None:
        meta["description"] = metadata_desc

    class _Mod(BaseModule[_InputModel, _InputModel, _SetupModel, _SecretModel]):
        name = "test_mod"
        setup_format = _SetupModel
        input_format = _InputModel
        output_format = _InputModel
        secret_format = _SecretModel
        metadata = meta
        triggers_discoverer = ModuleDiscoverer("test")

        async def initialize(self, context, setup_data) -> None:
            pass

        async def cleanup(self) -> None:
            pass

    _Mod.description = description
    handler = Mock()
    handler.protocol = "message"
    handler.description = "Handle a chat message"
    handler.input_format = _InputTrigger
    _Mod.triggers_discoverer._trigger_handlers_cls["message"] = [handler]
    return _Mod


def test_documentation_has_description_and_trigger_table() -> None:
    doc = _module(description="Specialised summariser archetype.").build_registry_documentation()
    assert doc.startswith("Specialised summariser archetype.")
    assert "## Triggers" in doc
    assert "| Trigger | Description |" in doc
    assert "| message | Handle a chat message |" in doc


def test_empty_description_raises() -> None:
    with pytest.raises(ValueError, match="non-empty 'description'"):
        _module(description="").build_registry_documentation()


def test_metadata_description_fallback() -> None:
    doc = _module(description="", metadata_desc="Blurb from metadata.").build_registry_documentation()
    assert doc.startswith("Blurb from metadata.")


async def test_default_registry_stores_documentation() -> None:
    registry = DefaultRegistry("", "", "")
    info = await registry.register("modules:x", "localhost", 50051, "1.0.0", documentation="indexed docs")
    assert info is not None
    assert info.documentation == "indexed docs"


async def test_registry_schemas_cover_every_module_schema_but_user_info() -> None:
    mod = _module()
    schemas = await mod.build_registry_schemas()
    assert set(schemas) == {
        "input_schema",
        "select_input_schema",
        "output_schema",
        "setup_schema",
        "secret_schema",
        "config_setup_schema",
        "cost_schema",
    }
    assert schemas["input_schema"] == json.loads(await mod.get_input_format(llm_format=False))
    assert schemas["setup_schema"] == json.loads(await mod.get_setup_format(llm_format=False))


async def test_undeclared_registry_schema_is_sent_empty() -> None:
    mod = _module()
    mod.secret_format = None
    schemas = await mod.build_registry_schemas()
    assert schemas["secret_schema"] == {}


async def test_registry_schemas_satisfy_register_module_request_validation() -> None:
    mod = _module()
    mod.output_format = None
    request = registry_dto_pb2.RegisterModuleRequest(
        module_id="modules:test", address="localhost", port=50051, version="1.0.0", type="SERVICE"
    )
    json_format.ParseDict(await mod.build_registry_schemas(), request)
    assert protovalidate.collect_violations(request) == []


async def test_default_registry_accepts_schemas() -> None:
    registry = DefaultRegistry("", "", "")
    info = await registry.register("modules:x", "localhost", 50051, "1.0.0", schemas={"input_schema": {}})
    assert info is not None
    assert info.module_id == "modules:x"
