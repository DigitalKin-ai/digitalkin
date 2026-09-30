"""ModuleServer.stop_async ordering: stop accepting RPCs, cancel tasks, then close gateway/Redis/channels."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.grpc_servers.module_server import ModuleServer

pytestmark = pytest.mark.timeout(10)


def _server(order: list[str]) -> ModuleServer:
    server = object.__new__(ModuleServer)
    server.server = MagicMock()
    server.module_class = MagicMock()
    server.module_class.get_module_id.return_value = "modules:m"
    server.module_class.clear_shared.side_effect = lambda: order.append("clear_shared")
    server._health_servicer = MagicMock()
    server._health_servicer.enter_graceful_shutdown.side_effect = lambda: order.append("not_serving")
    server.registry = MagicMock()
    server.registry.deregister = AsyncMock(side_effect=lambda *_: order.append("deregister"))
    server.registry.close = AsyncMock(side_effect=lambda: order.append("registry_close"))
    server.module_servicer = MagicMock()
    server.module_servicer.job_manager.tasks = {}
    server.module_servicer.job_manager.stop = AsyncMock(side_effect=lambda: order.append("job_manager_stop"))
    server.module_servicer.shutdown = AsyncMock(side_effect=lambda: order.append("servicer_shutdown"))
    server._gateway_servicer = MagicMock()
    server._gateway_servicer.stop = AsyncMock(side_effect=lambda: order.append("gateway_stop"))
    server._gateway_redis_client = MagicMock()
    server._gateway_redis_client.close = AsyncMock(side_effect=lambda: order.append("gateway_redis_close"))
    return server


async def test_stop_async_order() -> None:
    order: list[str] = []
    server = _server(order)

    async def _stop(grace: float | None) -> None:  # noqa: RUF029
        order.append(f"server_stop:{grace}")

    with (
        patch.object(server, "_stop_async", side_effect=_stop),
        patch(
            "digitalkin.grpc_servers.module_server.GrpcClientWrapper.close_all_cached_channels",
            new=AsyncMock(side_effect=lambda: order.append("channels_close")),
        ),
    ):
        await server.stop_async(grace=2.5)

    assert order == [
        "not_serving",
        "deregister",
        "server_stop:2.5",
        "job_manager_stop",
        "gateway_stop",
        "servicer_shutdown",
        "gateway_redis_close",
        "clear_shared",
        "registry_close",
        "channels_close",
    ]
    assert server.server is None


async def test_stop_async_continues_when_job_manager_stop_fails() -> None:
    order: list[str] = []
    server = _server(order)
    server.module_servicer.job_manager.stop = AsyncMock(side_effect=RuntimeError("boom"))

    with (
        patch.object(server, "_stop_async", new=AsyncMock()),
        patch(
            "digitalkin.grpc_servers.module_server.GrpcClientWrapper.close_all_cached_channels",
            new=AsyncMock(side_effect=lambda: order.append("channels_close")),
        ),
    ):
        await server.stop_async()

    assert order[-1] == "channels_close"
    assert "gateway_redis_close" in order
