"""Deterministic Redis tests using fakeredis.

Tests RedisStateManager against an ephemeral in-memory Redis (HSET/HGETALL
round-trips, status transitions, exception recording). No real Redis needed.
"""

from __future__ import annotations

from typing import Any

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [pytest.mark.timeout(15)]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")


class _FakeRedisClient:
    """Adapter wrapping fakeredis to match RedisClient interface.

    Avoids importing the real RedisClient (which has redis import guard).
    Exposes only the methods the core Redis classes actually call.
    """

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def hset(self, name: str, mapping: dict[str, str | bytes]) -> int:
        return await self._client.hset(name, mapping=mapping)  # type: ignore[return-value]

    async def hgetall(self, name: str) -> dict[bytes, bytes]:
        return await self._client.hgetall(name)  # type: ignore[return-value]

    async def expire(self, name: str, seconds: int) -> bool:
        return await self._client.expire(name, seconds)  # type: ignore[return-value]

    async def delete(self, *names: str) -> int:
        return await self._client.delete(*names)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    async def xadd(self, name: str, fields: dict[str, str | bytes], *, maxlen: int | None = None) -> bytes:
        kwargs: dict[str, Any] = {}
        if maxlen is not None:
            kwargs["maxlen"] = maxlen
            kwargs["approximate"] = True
        return await self._client.xadd(name, fields, **kwargs)  # type: ignore[return-value]

    async def xread(self, streams: dict[str, str | bytes], *, count: int = 50, block: int = 0) -> list:
        return await self._client.xread(streams, count=count, block=block)  # type: ignore[return-value]

    async def xlen(self, name: str) -> int:
        return await self._client.xlen(name)  # type: ignore[return-value]

    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any:
        return await self._client.eval(script, len(keys), *keys, *args)

    def pipeline(self) -> Any:
        return self._client.pipeline()

    async def close(self) -> None:
        await self._client.aclose()


# ===========================================================================
# RedisStateManager
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestRedisStateManagerDeterministic:
    """State persistence against fakeredis."""

    @pytest.fixture
    async def state_mgr(self) -> Any:
        from digitalkin.core.task_manager.redis.redis_state import RedisStateManager

        client = _FakeRedisClient()
        mgr = RedisStateManager(client)  # type: ignore[arg-type]
        yield mgr
        await client.close()

    async def test_set_and_get_status(self, state_mgr: Any) -> None:
        await state_mgr.set_status("task_1", "running", started_at="2025-01-01T00:00:00Z")
        result = await state_mgr.get_status("task_1")
        assert result["status"] == "running"
        assert result["started_at"] == "2025-01-01T00:00:00Z"

    async def test_status_transitions_overwrite(self, state_mgr: Any) -> None:
        await state_mgr.set_status("task_2", "pending")
        await state_mgr.set_status("task_2", "running")
        await state_mgr.set_status("task_2", "completed")
        result = await state_mgr.get_status("task_2")
        assert result["status"] == "completed"

    async def test_get_nonexistent_returns_empty(self, state_mgr: Any) -> None:
        result = await state_mgr.get_status("nonexistent")
        assert result == {}

    async def test_record_exception_persists(self, state_mgr: Any) -> None:
        await state_mgr.set_status("task_3", "failed")
        await state_mgr.record_exception("task_3", "boom", "traceback here")
        result = await state_mgr.get_status("task_3")
        assert result["error_message"] == "boom"
        assert result["exception_traceback"] == "traceback here"

    async def test_register_task_sets_pending(self, state_mgr: Any) -> None:
        await state_mgr.register_task("task_4", "missions:m1", "setups:s1", "setup_versions:sv1")
        result = await state_mgr.get_status("task_4")
        assert result["status"] == "pending"
        assert result["mission_id"] == "missions:m1"
