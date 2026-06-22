"""Tests for resilience components.

Covers Bulkhead.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator

import pytest

pytestmark = [pytest.mark.timeout(15)]


# ===========================================================================
# Bulkhead
# ===========================================================================


class TestBulkhead:
    """Per-service concurrency limiting."""

    @pytest.fixture(autouse=True)
    def _clear(self) -> Generator[None]:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        Bulkhead._instances.clear()
        yield
        Bulkhead._instances.clear()

    async def test_allows_within_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        monkeypatch.setenv("DIGITALKIN_BULKHEAD_TEST_SVC_MAX", "3")
        bh = Bulkhead.for_service("test_svc")
        async with bh:
            assert bh.active == 1
        assert bh.active == 0

    async def test_concurrent_within_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        monkeypatch.setenv("DIGITALKIN_BULKHEAD_CONC_SVC_MAX", "5")
        bh = Bulkhead.for_service("conc_svc")
        results: list[int] = []

        async def work(i: int) -> None:
            async with bh:
                results.append(i)
                await asyncio.sleep(0.01)

        await asyncio.gather(*[work(i) for i in range(5)])
        assert len(results) == 5

    async def test_raises_when_full(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.core.exceptions import BulkheadFullError
        from digitalkin.core.resilience.bulkhead import Bulkhead

        monkeypatch.setenv("DIGITALKIN_BULKHEAD_FULL_SVC_MAX", "1")
        monkeypatch.setenv("DIGITALKIN_BULKHEAD_TIMEOUT", "0.05")
        bh = Bulkhead.for_service("full_svc")
        barrier = asyncio.Event()

        async def hold_slot() -> None:
            async with bh:
                barrier.set()
                await asyncio.sleep(1.0)

        task = asyncio.create_task(hold_slot())
        await barrier.wait()

        with pytest.raises(BulkheadFullError):
            async with bh:
                pass  # Should not reach here

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_singleton_per_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        monkeypatch.setenv("DIGITALKIN_BULKHEAD_SINGLETON_SVC_MAX", "10")
        a = Bulkhead.for_service("singleton_svc")
        b = Bulkhead.for_service("singleton_svc")
        assert a is b

    async def test_different_services_independent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        monkeypatch.setenv("DIGITALKIN_BULKHEAD_SVC_A_MAX", "1")
        monkeypatch.setenv("DIGITALKIN_BULKHEAD_SVC_B_MAX", "1")
        monkeypatch.setenv("DIGITALKIN_BULKHEAD_TIMEOUT", "0.05")
        a = Bulkhead.for_service("svc_a")
        b = Bulkhead.for_service("svc_b")

        async with a:
            # a is full, but b should still be available
            async with b:
                assert a.active == 1
                assert b.active == 1

    async def test_available_property(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        monkeypatch.setenv("DIGITALKIN_BULKHEAD_AVAIL_SVC_MAX", "3")
        bh = Bulkhead.for_service("avail_svc")
        assert bh.available == 3
        async with bh:
            assert bh.available == 2
