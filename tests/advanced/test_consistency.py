"""Eventual consistency tests.

Validates state convergence across the signal, state, and stream paths.
Uses fakeredis to simulate real Redis behavior deterministically.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [pytest.mark.timeout(15)]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")


class _FakeClient:
    """Minimal fakeredis adapter matching RedisClient interface."""

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
# State + Stream convergence
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestStateStreamConsistency:
    """State and stream converge to the same final state."""

    async def test_state_reflects_stream_completion(self) -> None:
        """After write_eos, state should be 'completed' and stream should end."""
        from digitalkin.core.task_manager.redis.redis_state import RedisStateManager
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamReader, RedisStreamWriter

        client = _FakeClient()
        state_mgr = RedisStateManager(client, task_ttl=60)  # type: ignore[arg-type]
        writer = RedisStreamWriter("conv_task", client, stream_ttl=60)  # type: ignore[arg-type]
        reader = RedisStreamReader("conv_task", client, cursor_ttl=60)  # type: ignore[arg-type]

        # Simulate lifecycle: pending → running → write data → completed
        await state_mgr.set_status("conv_task", "pending")
        await state_mgr.set_status("conv_task", "running")

        await writer.write({"output": "chunk_1"})
        await writer.write({"output": "chunk_2"})
        await writer.write_eos()

        await state_mgr.set_status("conv_task", "completed")

        # Verify convergence
        state = await state_mgr.get_status("conv_task")
        assert state["status"] == "completed"

        items: list[dict] = []
        async for item in reader.read(count=10, block_ms=100):
            items.append(item)

        assert len(items) == 2
        assert items[0]["output"] == "chunk_1"

        await client.close()

    async def test_checkpoint_matches_stream_position(self) -> None:
        """Checkpoint's last_seq matches the writer's last sequence number."""
        from digitalkin.core.task_manager.redis.redis_checkpoint import RedisCheckpointManager
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamWriter

        client = _FakeClient()
        ckpt_mgr = RedisCheckpointManager(client, checkpoint_ttl=60)  # type: ignore[arg-type]
        writer = RedisStreamWriter("ckpt_task", client, stream_ttl=60)  # type: ignore[arg-type]

        await writer.write({"chunk": 1})
        await writer.write({"chunk": 2})
        await writer.write({"chunk": 3})

        await ckpt_mgr.checkpoint(
            session_id="sess_ckpt",
            task_id="ckpt_task",
            mission_id="missions:m1",
            setup_id="setups:s1",
            setup_version_id="setup_versions:sv1",
            status="running",
            last_seq=writer.last_seq,
        )

        restored = await ckpt_mgr.restore("sess_ckpt")
        assert restored is not None
        assert restored["last_seq"] == 3
        assert restored["last_seq"] == writer.last_seq

        await client.close()


@SKIP_NO_FAKEREDIS
class TestIdempotencyConsistency:
    """Idempotency claims are consistent after release cycles."""

    async def test_claim_release_reclaim_cycle(self) -> None:
        """Full claim → release → reclaim cycle produces correct results."""
        from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotencyGuard
        from digitalkin.models.core.redis import ClaimResult

        client = _FakeClient()
        guard = RedisIdempotencyGuard(client, claim_ttl=60)  # type: ignore[arg-type]

        # First claim
        assert await guard.claim("cycle_task") == ClaimResult.CLAIMED
        # Reclaim (same worker)
        assert await guard.claim("cycle_task") == ClaimResult.RECLAIMED
        # Release
        await guard.release("cycle_task")
        # Fresh claim after release
        assert await guard.claim("cycle_task") == ClaimResult.CLAIMED

        await client.close()
