"""Default registry implementation."""

from typing import ClassVar

from digitalkin.services.registry.registry_exceptions import RegistryModuleNotFoundError
from digitalkin.services.registry.registry_models import ModuleInfo, ModuleStatus, ModuleType
from digitalkin.services.registry.registry_strategy import RegistryStrategy


class DefaultRegistry(RegistryStrategy):
    """Default registry strategy using in-memory storage."""

    _modules: ClassVar[dict[str, ModuleInfo]] = {}

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    def search(
        self,
        name: str | None = None,
            module_type: ModuleType | None = None,
        organization_id: str | None = None,  # noqa: ARG002
    ) -> list[ModuleInfo]:
        results = list(self._modules.values())

        if name:
            results = [m for m in results if name in m.name]

        if module_type:
            results = [m for m in results if m.type == module_type]

        return results

    def get(self, module_id: str) -> ModuleInfo:
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)
        return self._modules[module_id]

    def get_status(self, module_id: str) -> ModuleStatus:
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)

        module = self._modules[module_id]
        return module.status or ModuleStatus.UNSPECIFIED

    def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
    ) -> ModuleInfo | None:
        existing = self._modules.get(module_id)
        self._modules[module_id] = ModuleInfo(
            id=module_id,
            type=existing.type if existing else ModuleType.UNSPECIFIED,
            address=address,
            port=port,
            version=version,
            name=existing.name if existing else module_id,
            status=ModuleStatus.ACTIVE,
        )
        return self._modules[module_id]

    def heartbeat(self, module_id: str) -> ModuleStatus:
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)

        module = self._modules[module_id]
        # Update status to ACTIVE on heartbeat
        self._modules[module_id] = ModuleInfo(
            id=module.id,
            type=module.type,
            address=module.address,
            port=module.port,
            version=module.version,
            name=module.name,
            status=ModuleStatus.ACTIVE,
        )
        return ModuleStatus.ACTIVE
