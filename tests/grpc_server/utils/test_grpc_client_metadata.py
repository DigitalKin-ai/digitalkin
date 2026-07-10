"""``GrpcClientWrapper.exec_grpc_query`` forwards per-call metadata (e.g. an idempotency key)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper

pytestmark = [pytest.mark.timeout(10)]


class _Wrapper(GrpcClientWrapper):
    service_name = "MetadataTestService"


async def test_exec_grpc_query_forwards_metadata() -> None:
    """Metadata passed to exec_grpc_query reaches the stub call unchanged."""
    w = _Wrapper()
    w.stub = AsyncMock()
    w.stub.Foo = AsyncMock(return_value="ok")

    md = (("x-idempotency-key", "abc123"),)
    result = await w.exec_grpc_query("Foo", request="req", timeout=1.0, metadata=md)

    assert result == "ok"
    w.stub.Foo.assert_awaited_once_with("req", timeout=1.0, metadata=md)


async def test_exec_grpc_query_metadata_defaults_none() -> None:
    """Omitting metadata forwards ``None`` (backward-compatible)."""
    w = _Wrapper()
    w.stub = AsyncMock()
    w.stub.Bar = AsyncMock(return_value="ok")

    await w.exec_grpc_query("Bar", request="req", timeout=1.0)

    w.stub.Bar.assert_awaited_once_with("req", timeout=1.0, metadata=None)
