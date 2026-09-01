"""Coverage for M2MCallRegistry CRUD, breaker, slots, and sweeper lifecycle."""

from __future__ import annotations

import asyncio
import time

import pytest
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.exceptions import M2MAtCapacityError
from digitalkin.grpc_servers.m2m_call_registry import M2MCallRegistry
from digitalkin.models.grpc_servers.m2m import _M2MCallEntry
from digitalkin.models.settings.gateway import get_gateway_settings


def _entry(task_id: str = "t1", target_key: str = "tgt:1", expires_in: float = 60.0) -> _M2MCallEntry:
    return _M2MCallEntry(
        task_id=task_id,
        query=struct_pb2.Struct(),
        output_queue=asyncio.Queue(),
        expires_at=time.monotonic() + expires_in,
        target_key=target_key,
    )


class TestM2MCallRegistryCrud:
    def test_register_get_has(self) -> None:
        reg = M2MCallRegistry()
        entry = _entry("t1")
        reg.register(entry)
        assert reg.has("t1")
        assert reg.get("t1") is entry
        assert "t1" in reg.entries

    def test_unregister_returns_and_removes(self) -> None:
        reg = M2MCallRegistry()
        entry = _entry("t2")
        reg.register(entry)
        assert reg.unregister("t2") is entry
        assert not reg.has("t2")
        assert reg.unregister("t2") is None

    def test_get_missing_returns_none(self) -> None:
        assert M2MCallRegistry().get("absent") is None


class TestM2MBreaker:
    def test_breaker_for_lazy_creates_and_caches(self) -> None:
        reg = M2MCallRegistry()
        first = reg.breaker_for("svc:1")
        assert reg.breaker_for("svc:1") is first
        assert first.service_id == "m2m:svc:1"


class TestM2MSlots:
    async def test_acquire_then_release(self) -> None:
        reg = M2MCallRegistry()
        await reg.acquire_slot()
        reg.release_slot()

    async def test_acquire_at_capacity_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_MAX_CONCURRENT", "1")
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_ACQUIRE_TIMEOUT_S", "0.05")
        get_gateway_settings.cache_clear()
        reg = M2MCallRegistry()
        await reg.acquire_slot()
        with pytest.raises(M2MAtCapacityError):
            await reg.acquire_slot()


class TestM2MSweeperLifecycle:
    async def test_start_stop_idempotent(self) -> None:
        reg = M2MCallRegistry()
        await reg.start()
        await reg.start()
        await reg.stop()
        await reg.stop()
