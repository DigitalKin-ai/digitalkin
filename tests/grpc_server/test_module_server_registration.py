"""ModuleServer registration guards: a module without a registry type fails before reaching the registry."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.grpc_servers.module_server import ModuleServer
from digitalkin.models.services.registry import RegistryModuleType

pytestmark = [pytest.mark.timeout(10), pytest.mark.validation]


class TestRegistryTypeGuard:
    """The registry refuses an UNSPECIFIED type, so startup names the fix instead of failing in register()."""

    @staticmethod
    def _server(registry_type: RegistryModuleType) -> ModuleServer:
        server = ModuleServer.__new__(ModuleServer)
        server.client_config = MagicMock()
        server.module_class = type("NoTypeModule", (), {"registry_type": registry_type})  # type: ignore[assignment]
        return server

    async def test_unspecified_registry_type_fails_before_the_registry_is_contacted(self) -> None:
        server = self._server(RegistryModuleType.UNSPECIFIED)

        with (
            patch("digitalkin.grpc_servers.module_server.GrpcRegistry") as registry_cls,
            pytest.raises(RuntimeError, match="NoTypeModule declares no registry_type"),
        ):
            await server._init_and_register()

        registry_cls.assert_not_called()

    async def test_declared_registry_type_goes_on_to_the_registry(self) -> None:
        server = self._server(RegistryModuleType.SERVICE)

        with patch("digitalkin.grpc_servers.module_server.GrpcRegistry") as registry_cls:
            registry_cls.return_value.wait_for_ready = AsyncMock(return_value=False)
            with pytest.raises(RuntimeError, match="Registry server is unreachable"):
                await server._init_and_register()

        registry_cls.assert_called_once()


class TestRegistrationPayload:
    """Registration carries the module documentation and schemas."""

    async def test_register_receives_the_module_schemas(self) -> None:
        server = ModuleServer.__new__(ModuleServer)
        server.client_config = MagicMock()
        schemas = {"input_schema": {"type": "object"}}
        module_class = MagicMock(registry_type=RegistryModuleType.SERVICE, metadata={"version": "1.0.0"})
        module_class.get_module_id.return_value = "modules:x"
        module_class.build_registry_documentation.return_value = "docs"
        module_class.build_registry_schemas = AsyncMock(return_value=schemas)
        server.module_class = module_class

        with patch("digitalkin.grpc_servers.module_server.GrpcRegistry") as registry_cls:
            registry_cls.return_value.wait_for_ready = AsyncMock(return_value=True)
            registry_cls.return_value.register = AsyncMock(return_value=MagicMock(module_id="modules:x"))
            await server._init_and_register()

        register_kwargs = registry_cls.return_value.register.await_args.kwargs
        assert register_kwargs["schemas"] is schemas
        assert register_kwargs["documentation"] == "docs"
