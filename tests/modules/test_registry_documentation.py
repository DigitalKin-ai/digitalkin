"""Registry documentation assembly: enforced author description + LLM trigger table."""

from typing import Literal
from unittest.mock import Mock

import pytest
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
