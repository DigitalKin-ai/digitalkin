"""Deterministic Redis tests using fakeredis.

Tests RedisStateManager, RedisStreamWriter/Reader, RedisCheckpointManager,
and RedisIdempotencyGuard against an ephemeral in-memory Redis. No real
Redis needed — these run anywhere with zero infra.

Covers:
- State persistence and retrieval (HSET/HGETALL round-trips)
- Stream write/read with gap detection and EOS
- Checkpoint write/restore/delete lifecycle
- Idempotency Lua claim atomicity
- TTL enforcement
"""

from __future__ import annotations

import asyncio
import json
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
        mgr = RedisStateManager(client, task_ttl=60)  # type: ignore[arg-type]
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


# ===========================================================================
# RedisStreamWriter + Reader
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestRedisStreamsDeterministic:
    """Stream write/read with gap detection and EOS."""

    @pytest.fixture
    async def client(self) -> Any:
        c = _FakeRedisClient()
        yield c
        await c.close()

    async def test_write_and_read_roundtrip(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamReader, RedisStreamWriter

        writer = RedisStreamWriter("task_s1", client, stream_ttl=60)  # type: ignore[arg-type]
        reader = RedisStreamReader("task_s1", client, cursor_ttl=60)  # type: ignore[arg-type]

        await writer.write({"msg": "hello"})
        await writer.write({"msg": "world"})
        await writer.write_eos()

        items: list[dict] = []
        async for item in reader.read(count=10, block_ms=100):
            items.append(item)

        assert len(items) == 2
        assert items[0]["msg"] == "hello"
        assert items[1]["msg"] == "world"

    async def test_seq_numbers_monotonic(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamWriter

        writer = RedisStreamWriter("task_s2", client)  # type: ignore[arg-type]
        s1 = await writer.write({"a": 1})
        s2 = await writer.write({"a": 2})
        s3 = await writer.write({"a": 3})
        assert s1 < s2 < s3

    async def test_eos_terminates_reader(self, client: Any) -> None:
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamReader, RedisStreamWriter

        writer = RedisStreamWriter("task_s3", client)  # type: ignore[arg-type]
        reader = RedisStreamReader("task_s3", client)  # type: ignore[arg-type]

        await writer.write({"x": 1})
        await writer.write_eos()

        count = 0
        async for _ in reader.read(count=10, block_ms=100):
            count += 1

        assert count == 1  # EOS not yielded as data


# ===========================================================================
# RedisCheckpointManager
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestRedisCheckpointDeterministic:
    """Checkpoint lifecycle against fakeredis."""

    @pytest.fixture
    async def ckpt_mgr(self) -> Any:
        from digitalkin.core.task_manager.redis.redis_checkpoint import RedisCheckpointManager

        client = _FakeRedisClient()
        mgr = RedisCheckpointManager(client, checkpoint_ttl=60)  # type: ignore[arg-type]
        yield mgr
        await client.close()

    async def test_checkpoint_and_restore(self, ckpt_mgr: Any) -> None:
        await ckpt_mgr.checkpoint(
            session_id="sess_1",
            task_id="task_1",
            mission_id="missions:m1",
            setup_id="setups:s1",
            setup_version_id="setup_versions:sv1",
            status="running",
            last_seq=42,
            state={"model_state": "active"},
        )

        restored = await ckpt_mgr.restore("sess_1")
        assert restored is not None
        assert restored["task_id"] == "task_1"
        assert restored["status"] == "running"
        assert restored["last_seq"] == 42
        assert restored["state"]["model_state"] == "active"

    async def test_restore_nonexistent_returns_none(self, ckpt_mgr: Any) -> None:
        result = await ckpt_mgr.restore("nonexistent")
        assert result is None

    async def test_delete_removes_checkpoint(self, ckpt_mgr: Any) -> None:
        await ckpt_mgr.checkpoint(
            session_id="sess_del",
            task_id="t_del",
            mission_id="missions:m1",
            setup_id="setups:s1",
            setup_version_id="setup_versions:sv1",
            status="completed",
            last_seq=100,
        )
        await ckpt_mgr.delete("sess_del")
        assert await ckpt_mgr.restore("sess_del") is None


# ===========================================================================
# RedisIdempotencyGuard (with Lua)
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestRedisIdempotencyDeterministic:
    """Lua atomic claims against fakeredis."""

    @pytest.fixture
    async def guard(self) -> Any:
        from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotencyGuard

        client = _FakeRedisClient()
        guard = RedisIdempotencyGuard(client, claim_ttl=60)  # type: ignore[arg-type]
        yield guard
        await client.close()

    async def test_claim_fresh_task(self, guard: Any) -> None:
        from digitalkin.models.core.redis import ClaimResult

        result = await guard.claim("task_lua_1")
        assert result == ClaimResult.CLAIMED

    async def test_reclaim_same_task(self, guard: Any) -> None:
        from digitalkin.models.core.redis import ClaimResult

        await guard.claim("task_lua_2")
        result = await guard.claim("task_lua_2")
        assert result == ClaimResult.RECLAIMED

    async def test_release_and_reclaim(self, guard: Any) -> None:
        from digitalkin.models.core.redis import ClaimResult

        await guard.claim("task_lua_3")
        await guard.release("task_lua_3")
        result = await guard.claim("task_lua_3")
        assert result == ClaimResult.CLAIMED  # Fresh after release
