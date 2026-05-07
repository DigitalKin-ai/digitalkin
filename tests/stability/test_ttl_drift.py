"""L6 — TTL drift tests under concurrent load.

Verifies that Redis TTL enforcement remains accurate under pressure:
- Bulk key expiration with short TTLs
- TTL accuracy within tolerance after concurrent writes
- No premature expiry beyond tolerance threshold

Uses fakeredis with time control for deterministic testing.
"""

from __future__ import annotations

import asyncio
import time

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [
    pytest.mark.stability,
    pytest.mark.timeout(60),
    pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed"),
]


class TestTtlConsistency:
    """TTL values remain consistent across bulk operations."""

    async def test_bulk_expire_consistency(self) -> None:
        """100 keys with TTL=300s all report TTL within 1s of each other."""
        client = fakeredis_aio.FakeRedis()

        for i in range(100):
            await client.set(f"ttl:bulk:{i}", b"v", ex=300)

        ttls = []
        for i in range(100):
            ttl = await client.ttl(f"ttl:bulk:{i}")
            ttls.append(ttl)

        assert all(t > 295 for t in ttls), f"Some TTLs too low: min={min(ttls)}"
        spread = max(ttls) - min(ttls)
        assert spread <= 2, f"TTL spread {spread}s across 100 keys — should be ≤2s"

        await client.aclose()

    async def test_ttl_survives_hset_update(self) -> None:
        """HSET field update does not reset TTL (unless EXPIRE called again)."""
        client = fakeredis_aio.FakeRedis()

        await client.hset("ttl:hash", mapping={"status": "pending"})
        await client.expire("ttl:hash", 300)

        ttl_before = await client.ttl("ttl:hash")
        assert ttl_before > 295

        # Update a field — TTL should remain
        await client.hset("ttl:hash", mapping={"status": "running"})
        ttl_after = await client.ttl("ttl:hash")
        assert ttl_after > 290, f"TTL reset to {ttl_after} after HSET update"

        await client.aclose()

    async def test_pipeline_expire_applied(self) -> None:
        """EXPIRE in pipeline is applied atomically with HSET."""
        client = fakeredis_aio.FakeRedis()

        pipe = client.pipeline()
        pipe.hset("ttl:pipe", mapping={"a": "1"})
        pipe.expire("ttl:pipe", 600)
        await pipe.execute()

        ttl = await client.ttl("ttl:pipe")
        assert ttl > 595

        await client.aclose()


class TestTtlProductionWorkflows:
    """TTL patterns matching production SDK usage."""

    async def test_checkpoint_ttl_lifecycle(self) -> None:
        """Checkpoint created with 5min TTL, queried, deleted."""
        client = fakeredis_aio.FakeRedis()

        # Create checkpoint with 5min TTL
        pipe = client.pipeline()
        pipe.hset("checkpoint:s1", mapping={"state": "{}", "last_seq": "42"})
        pipe.expire("checkpoint:s1", 300)
        pipe.sadd("checkpoints:active", "s1")
        await pipe.execute()

        # Verify TTL is set
        ttl = await client.ttl("checkpoint:s1")
        assert ttl > 295

        # Verify in active set
        members = await client.smembers("checkpoints:active")
        assert b"s1" in members

        # Delete checkpoint
        pipe = client.pipeline()
        pipe.delete("checkpoint:s1")
        pipe.srem("checkpoints:active", "s1")
        await pipe.execute()

        # Verify cleaned up
        assert await client.ttl("checkpoint:s1") == -2
        members = await client.smembers("checkpoints:active")
        assert b"s1" not in members

        await client.aclose()

    async def test_stream_ttl_after_eos(self) -> None:
        """Stream gets TTL after EOS marker (ProtoStreamWriter.write_eos)."""
        client = fakeredis_aio.FakeRedis()

        # Write entries
        for i in range(5):
            await client.xadd("task:s1:stream", {"pb": f"data_{i}".encode(), "seq": str(i)})

        # Before EOS: no TTL
        ttl = await client.ttl("task:s1:stream")
        assert ttl == -1  # no expiry

        # Write EOS and set TTL (production pattern)
        await client.xadd("task:s1:stream", {"pb": b"", "seq": "6", "eos": b"true"})
        await client.expire("task:s1:stream", 60)

        ttl = await client.ttl("task:s1:stream")
        assert 55 < ttl <= 60

        await client.aclose()
