"""L0 — RedisIdempotency claim semantics against fakeredis.

Paired with the real-Redis check in
``tests/integration/redis/test_managers_real.py::TestRedisIdempotencyReal``.
"""

from __future__ import annotations

from typing import Any

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotency
from digitalkin.models.core.redis import ClaimResult

pytestmark = [
    pytest.mark.timeout(15),
    pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed"),
]


class _FakeRedisClient:
    """Minimal adapter exposing the methods RedisIdempotency calls."""

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any:
        return await self._client.eval(script, len(keys), *keys, *args)

    async def delete(self, *names: str) -> int:
        return await self._client.delete(*names)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def close(self) -> None:
        await self._client.aclose()


@pytest.fixture
async def guard():
    client = _FakeRedisClient()
    yield RedisIdempotency(client), client  # type: ignore[arg-type]
    await client.close()


class TestRedisIdempotency:
    """CLAIMED / RECLAIMED / TAKEN transitions + release."""

    async def test_first_claim_is_claimed(self, guard) -> None:
        idem, _ = guard
        assert await idem.claim("t1", "inst_a") is ClaimResult.CLAIMED

    async def test_same_instance_reclaims(self, guard) -> None:
        idem, _ = guard
        await idem.claim("t2", "inst_a")
        assert await idem.claim("t2", "inst_a") is ClaimResult.RECLAIMED

    async def test_other_instance_taken(self, guard) -> None:
        idem, _ = guard
        await idem.claim("t3", "inst_a")
        assert await idem.claim("t3", "inst_b") is ClaimResult.TAKEN

    async def test_release_frees_the_claim(self, guard) -> None:
        idem, client = guard
        await idem.claim("t4", "inst_a")
        await idem.release("t4")
        assert await client.get("idem:t4") is None
        assert await idem.claim("t4", "inst_b") is ClaimResult.CLAIMED
