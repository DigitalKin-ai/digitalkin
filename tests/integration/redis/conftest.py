"""Fixtures for L1 integration tests against real Redis (docker-compose).

Requires: `docker compose --profile redis up -d` before running.
All tests are marked @pytest.mark.integration and skip if Redis is unreachable.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

# Default matches docker-compose.yml `tests-redis` host port (${REDIS_PORT:-6399}).
# Override with DIGITALKIN_REDIS_URL to point elsewhere.
REDIS_URL = os.environ.get("DIGITALKIN_REDIS_URL", "redis://localhost:6399/0")


@pytest_asyncio.fixture
async def redis_client(monkeypatch: pytest.MonkeyPatch):
    """Function-scoped RedisClient connected to real Redis.

    Skips test if Redis is unreachable.
    """
    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.models.settings.redis import get_redis_settings

    monkeypatch.setenv("DIGITALKIN_REDIS_POOL_SIZE", "20")
    monkeypatch.setenv("DIGITALKIN_REDIS_HEALTH_CHECK_TIMEOUT", "3.0")
    get_redis_settings.cache_clear()
    client = RedisClient(REDIS_URL)
    reachable = await client.verify()
    if not reachable:
        await client.close()
        msg = (
            f"Redis not reachable at {REDIS_URL} — start with: docker compose up -d tests-redis "
            "(or set DIGITALKIN_REDIS_URL)"
        )
        # In CI the integration leg sets DIGITALKIN_REQUIRE_REDIS=1 so a broken Redis wiring
        # fails loudly instead of silently skipping the whole suite green.
        if os.environ.get("DIGITALKIN_REQUIRE_REDIS"):
            pytest.fail(msg)
        pytest.skip(msg)
    await client._client.flushdb()
    yield client
    await client._client.flushdb()
    await client.close()
