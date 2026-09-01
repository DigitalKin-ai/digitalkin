"""Eventual consistency tests.

Validates state convergence across the signal, state, and stream paths.
Uses fakeredis to simulate real Redis behavior deterministically.
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
