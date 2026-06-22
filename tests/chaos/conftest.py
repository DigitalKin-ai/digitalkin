"""Fixtures for L2 chaos tests via Toxiproxy.

Requires:
  docker compose --profile redis --profile chaos up -d

Toxiproxy sits between tests and Redis. Tests inject faults (latency,
bandwidth limits, connection resets) via the Toxiproxy REST API on :8474.
Proxy listens on :26379 and forwards to Redis :6379.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
import pytest_asyncio

TOXIPROXY_API = os.environ.get("TOXIPROXY_API", "http://localhost:8474")
# Upstream host as seen from Toxiproxy container (Docker network name)
REDIS_UPSTREAM_HOST = os.environ.get("REDIS_UPSTREAM_HOST", "digitalkin-tests-redis")
REDIS_UPSTREAM_PORT = int(os.environ.get("REDIS_UPSTREAM_PORT", "6379"))
PROXY_LISTEN_PORT = 26379
# Host the test process dials the proxy on: localhost when run from the host (ports
# published), the toxiproxy service name when run inside the compose `tests` container.
PROXY_CONNECT_HOST = os.environ.get("PROXY_CONNECT_HOST", "localhost")


class ToxiproxyClient:
    """Minimal REST client for Toxiproxy API using stdlib only."""

    def __init__(self, api_url: str) -> None:
        self._api = api_url
        self._proxy_name: str | None = None

    @staticmethod
    def _request(url: str, method: str = "GET", data: bytes | None = None) -> bytes:
        """Sync HTTP request (run in thread for async)."""
        import urllib.request

        req = urllib.request.Request(url, data=data, method=method)  # noqa: S310
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310
            return resp.read()

    async def _async_request(self, url: str, method: str = "GET", data: dict | None = None) -> dict:
        """Async HTTP request via thread pool."""
        import asyncio
        import json as _json

        body = _json.dumps(data).encode() if data else None
        raw = await asyncio.to_thread(self._request, url, method, body)
        return _json.loads(raw) if raw else {}

    async def create_proxy(self, name: str, listen: str, upstream: str) -> dict:
        """Create a proxy."""
        self._proxy_name = name
        return await self._async_request(
            f"{self._api}/proxies", "POST",
            {"name": name, "listen": listen, "upstream": upstream, "enabled": True},
        )

    async def add_toxic(self, toxic_type: str, attributes: dict, stream: str = "downstream") -> dict:
        """Add a toxic to the proxy."""
        return await self._async_request(
            f"{self._api}/proxies/{self._proxy_name}/toxics", "POST",
            {"type": toxic_type, "stream": stream, "attributes": attributes},
        )

    async def remove_toxic(self, toxic_name: str) -> None:
        """Remove a specific toxic."""
        await self._async_request(
            f"{self._api}/proxies/{self._proxy_name}/toxics/{toxic_name}", "DELETE",
        )

    async def disable_proxy(self) -> None:
        """Disable the proxy (simulates complete outage)."""
        await self._async_request(
            f"{self._api}/proxies/{self._proxy_name}", "POST", {"enabled": False},
        )

    async def enable_proxy(self) -> None:
        """Re-enable the proxy."""
        await self._async_request(
            f"{self._api}/proxies/{self._proxy_name}", "POST", {"enabled": True},
        )

    async def reset(self) -> None:
        """Remove all toxics from all proxies."""
        await self._async_request(f"{self._api}/reset", "POST")

    async def delete_proxy(self) -> None:
        """Delete the proxy."""
        if self._proxy_name:
            try:
                await self._async_request(f"{self._api}/proxies/{self._proxy_name}", "DELETE")
            except Exception:
                pass


def _toxiproxy_available() -> bool:
    """Check if Toxiproxy API is reachable (sync check for skip marker)."""
    import urllib.request

    try:
        urllib.request.urlopen(f"{TOXIPROXY_API}/version", timeout=2)  # noqa: S310
        return True
    except Exception:
        return False


SKIP_NO_TOXIPROXY = pytest.mark.skipif(
    not _toxiproxy_available(),
    reason="Toxiproxy not running — start with: docker compose --profile redis --profile chaos up -d",
)


@pytest_asyncio.fixture
async def toxiproxy():
    """Function-scoped Toxiproxy client with auto-cleanup."""
    client = ToxiproxyClient(TOXIPROXY_API)
    await client.reset()
    await client.create_proxy(
        name="redis_proxy",
        listen=f"0.0.0.0:{PROXY_LISTEN_PORT}",
        upstream=f"{REDIS_UPSTREAM_HOST}:{REDIS_UPSTREAM_PORT}",
    )
    yield client
    await client.reset()
    await client.delete_proxy()


@pytest_asyncio.fixture
async def redis_via_proxy(toxiproxy, monkeypatch: pytest.MonkeyPatch):
    """RedisClient connected through Toxiproxy (for fault injection)."""
    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.models.settings.redis import get_redis_settings

    monkeypatch.setenv("DIGITALKIN_REDIS_POOL_SIZE", "10")
    monkeypatch.setenv("DIGITALKIN_REDIS_HEALTH_CHECK_TIMEOUT", "3.0")
    get_redis_settings.cache_clear()
    client = RedisClient(f"redis://{PROXY_CONNECT_HOST}:{PROXY_LISTEN_PORT}/0")
    reachable = await client.verify()
    if not reachable:
        await client.close()
        pytest.skip("Redis via proxy not reachable")
    await client._client.flushdb()
    yield client
    await client.close()
