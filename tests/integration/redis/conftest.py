"""Fixtures for L1 integration tests against real Redis (docker-compose).

Requires: `docker compose --profile redis up -d` before running.
All tests are marked @pytest.mark.integration and skip if Redis is unreachable.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

REDIS_URL = os.environ.get("DIGITALKIN_REDIS_URL", "redis://localhost:6379/0")


@pytest_asyncio.fixture
async def redis_client():
    """Function-scoped RedisClient connected to real Redis.

    Skips test if Redis is unreachable.
    """
    from digitalkin.core.task_manager.redis.redis_client import RedisClient

    client = RedisClient(REDIS_URL, pool_size=20)
    reachable = await client.verify(timeout=3.0)
    if not reachable:
        await client.close()
        pytest.skip("Redis not reachable — start with: docker compose --profile redis up -d")
    await client._client.flushdb()
    yield client
    await client._client.flushdb()
    await client.close()
