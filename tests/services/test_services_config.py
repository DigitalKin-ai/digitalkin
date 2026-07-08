"""Tests for ServicesConfig singleton caching and strategy initialization."""

from unittest.mock import AsyncMock

import pytest

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.services import ServicesMode
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.secret.grpc_secret import GrpcSecret
from digitalkin.services.services_config import ServicesConfig


def _client_config(host: str = "[::]", port: int = 50051) -> ClientConfig:
    return ClientConfig(
        host=host, port=port, mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE, credentials=None
    )


class TestSingletonStrategies:
    """Stateless strategies (registry, communication) are cached as singletons."""

    def test_same_instance_returned_on_second_call(self) -> None:
        """init_strategy returns cached singleton for stateless strategies."""
        config = ServicesConfig(mode=ServicesMode.LOCAL)

        reg1 = config.init_strategy("registry", "m1", "s1", "v1")
        reg2 = config.init_strategy("registry", "m2", "s2", "v2")

        assert reg1 is reg2

    def test_stateful_strategy_creates_new_instance(self) -> None:
        """Non-stateless strategies create a new instance each call."""
        config = ServicesConfig(mode=ServicesMode.LOCAL)

        id1 = config.init_strategy("identity", "m1", "s1", "v1")
        id2 = config.init_strategy("identity", "m2", "s2", "v2")

        assert id1 is not id2

    def test_all_stateless_strategies_cached(self) -> None:
        """All stateless strategies are singletons."""
        config = ServicesConfig(mode=ServicesMode.LOCAL)

        for name in ("registry", "communication"):
            first = config.init_strategy(name, "m1", "s1", "v1")
            second = config.init_strategy(name, "m2", "s2", "v2")
            assert first is second, f"{name} should be a singleton"

    def test_mode_switch_clears_singletons(self) -> None:
        """update_mode clears singleton cache so subsequent calls get fresh instances."""
        config = ServicesConfig(mode=ServicesMode.LOCAL)

        reg_before = config.init_strategy("registry", "m1", "s1", "v1")

        # Switching mode (even to same) should invalidate cache
        config.update_mode(ServicesMode.REMOTE)
        config.update_mode(ServicesMode.LOCAL)

        reg_after = config.init_strategy("registry", "m1", "s1", "v1")
        assert reg_before is not reg_after, "Singleton cache should be cleared after mode switch"


class TestSecretConfigInheritance:
    """The secret service shares the UserProfileService backend → inherits its client_config."""

    def test_secret_inherits_user_profile_client_config(self) -> None:
        """Without a dedicated secret config, GrpcSecret builds from user_profile's client_config."""
        cfg = _client_config()
        config = ServicesConfig(
            services_config_params={"user_profile": {"client_config": cfg}}, mode=ServicesMode.REMOTE
        )

        assert config.get_strategy_config("secret") == {"client_config": cfg}
        secret = config.init_strategy("secret", "m", "s", "sv")
        assert isinstance(secret, GrpcSecret)

    def test_explicit_secret_config_wins(self) -> None:
        """An explicit secret config is not overridden by user_profile's."""
        up_cfg = _client_config(host="up", port=1)
        secret_cfg = _client_config(host="secret", port=2)
        config = ServicesConfig(
            services_config_params={
                "user_profile": {"client_config": up_cfg},
                "secret": {"client_config": secret_cfg},
            },
            mode=ServicesMode.REMOTE,
        )
        assert config.get_strategy_config("secret")["client_config"] is secret_cfg


class TestBorrowedCleanup:
    """ModuleContext.cleanup() skips .close() on borrowed strategies."""

    @pytest.mark.asyncio
    async def test_borrowed_strategies_not_closed(self) -> None:
        """Cleanup does not call .close() on borrowed strategy names."""
        from digitalkin.models.module.module_context import ModuleContext

        comm = AsyncMock()
        reg = AsyncMock()
        cost = AsyncMock()

        ctx = ModuleContext(
            communication=comm, cost=cost,
            filesystem=AsyncMock(), identity=AsyncMock(), registry=reg,
            secret=AsyncMock(),
            storage=AsyncMock(),
            user_profile=AsyncMock(),
            session={"job_id": "j1", "mission_id": "m1", "setup_id": "s1", "setup_version_id": "v1"},
            borrowed=frozenset({"registry", "communication"}),
        )

        await ctx.cleanup()

        # Borrowed: should NOT be closed
        reg.close.assert_not_awaited()
        comm.close.assert_not_awaited()

        # Owned: SHOULD be closed
        cost.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_borrowed_closes_all(self) -> None:
        """Without borrowed set, cleanup closes all strategies."""
        from digitalkin.models.module.module_context import ModuleContext

        comm = AsyncMock()
        reg = AsyncMock()

        ctx = ModuleContext(
            communication=comm, cost=AsyncMock(),
            filesystem=AsyncMock(), identity=AsyncMock(), registry=reg,
            secret=AsyncMock(),
            storage=AsyncMock(), task_manager=AsyncMock(),
            user_profile=AsyncMock(),
            session={"job_id": "j1", "mission_id": "m1", "setup_id": "s1", "setup_version_id": "v1"},
        )

        await ctx.cleanup()

        reg.close.assert_awaited_once()
        comm.close.assert_awaited_once()
