"""Fault injection / chaos tests.

Simulates failures in Redis and gRPC to verify degraded-mode behavior:
- Redis connection failure during signal send
- Redis connection failure during stream write
- gRPC UNAVAILABLE during module call
- Circuit breaker tripping under sustained failure
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = [pytest.mark.chaos, pytest.mark.timeout(15)]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_singletons() -> Generator[None]:
    from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer, SharedRedisListener
    from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()
    RedisSendBuffer._instances.clear()
    yield
    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()
    RedisSendBuffer._instances.clear()


# ===========================================================================
# Redis failure during signal send
# ===========================================================================


class TestRedisSignalFailure:
    """RedisSendBuffer handles pipeline failures gracefully."""

    async def test_pipeline_failure_rejects_all_pending_futures(self) -> None:
        """When pipe.execute() fails, all pending futures get the exception."""
        from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer

        client = MagicMock()
        pipe = MagicMock()
        pipe.hset.return_value = pipe
        pipe.expire.return_value = pipe
        pipe.publish.return_value = pipe
        pipe.execute = AsyncMock(side_effect=ConnectionError("Redis down"))
        client.pipeline.return_value = pipe

        buf = RedisSendBuffer(client, signal_ttl=3600)
        buf._max_batch_size = 3

        with pytest.raises(ConnectionError):
            await asyncio.gather(
                buf.send("t1", '{"a":1}'),
                buf.send("t2", '{"a":2}'),
                buf.send("t3", '{"a":3}'),
            )


# ===========================================================================
# Redis failure during stream write
# ===========================================================================


class TestRedisStreamWriteFailure:
    """RedisStreamWriter handles XADD failures."""

    async def test_xadd_failure_raises_to_caller(self) -> None:
        """When XADD fails, the exception propagates to the writer."""
        from digitalkin.core.task_manager.redis.redis_streams import RedisStreamWriter

        client = MagicMock()
        client.xadd = AsyncMock(side_effect=ConnectionError("Redis down"))

        writer = RedisStreamWriter("task_fail", client)
        with pytest.raises(ConnectionError):
            await writer.write({"data": "test"})


# ===========================================================================
# Circuit breaker under sustained failure
# ===========================================================================


class TestCircuitBreakerChaos:
    """CB behavior under sustained gRPC failure."""

    async def test_sustained_failure_opens_circuit(self) -> None:
        """5 consecutive failures open the circuit, subsequent calls fail fast."""
        from digitalkin.grpc_servers.exceptions import CircuitOpenError
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
        from digitalkin.models.grpc_servers.circuit_breaker import CBState

        cb = CircuitBreaker("chaos_svc", fail_max=5, reset_timeout=30.0)

        for _ in range(5):
            cb.record_failure()

        assert cb.state == CBState.OPEN

        with pytest.raises(CircuitOpenError):
            cb.check()

    async def test_circuit_open_prevents_grpc_call(self) -> None:
        """When circuit is open, exec_grpc_query raises ServerError immediately."""
        from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
        from digitalkin.grpc_servers.exceptions import ServerError
        from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper

        cb = CircuitBreaker.get_or_create("ChaosService", fail_max=1, reset_timeout=30.0)
        cb.record_failure()  # Trip the circuit

        wrapper = object.__new__(GrpcClientWrapper)
        wrapper.service_name = "ChaosService"
        wrapper.stub = MagicMock()

        with pytest.raises(ServerError, match="Circuit open"):
            await wrapper.exec_grpc_query("SomeMethod", MagicMock())

        # Verify the stub was NEVER called (fail fast, no network)
        wrapper.stub.SomeMethod.assert_not_called()


# ===========================================================================
# Degraded mode: Redis unavailable, in-memory fallback
# ===========================================================================


class TestDegradedMode:
    """System operates in degraded mode when Redis is unavailable."""

    async def test_add_to_queue_continues_without_redis(self) -> None:
        """SingleJobManager.add_to_queue works when stream writer fails."""
        from unittest.mock import Mock

        from digitalkin.core.job_manager.single_job_manager import SingleJobManager
        from digitalkin.core.task_manager.task_session import TaskSession
        from digitalkin.models.core.job_manager_models import BackpressureStrategy
        from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

        # Create manager with failing Redis writer
        mgr = object.__new__(SingleJobManager)
        mgr._backpressure_strategy = BackpressureStrategy.REJECT
        mgr._backpressure_timeout = 5.0

        mock_task_manager = Mock()
        mock_task_manager.tasks_sessions = {}
        mgr._task_manager = mock_task_manager

        # Mock stream writer that always fails
        failing_writer = MagicMock()
        failing_writer.write = AsyncMock(side_effect=ConnectionError("Redis down"))
        mgr._stream_writers = {"job_1": failing_writer}

        # Create session
        module = Mock()
        module.context = Mock()
        module.context.task_manager = Mock(spec=TaskManagerStrategy)
        module.context.session = Mock()
        module.context.session.setup_id = "s:1"
        module.context.session.setup_version_id = "sv:1"
        module.context.session.current_ids = Mock(return_value={})
        module.context.cleanup = AsyncMock()
        module.stop = AsyncMock()

        session = TaskSession("job_1", "missions:m1", module, queue_maxsize=10)
        mgr._task_manager.tasks_sessions["job_1"] = session

        # Create a minimal output model
        from pydantic import BaseModel

        class FakeOutput(BaseModel):
            value: str

        # Should not raise — Redis fails but in-memory queue still works
        await mgr.add_to_queue("job_1", FakeOutput(value="test"))

        # Verify item landed in queue despite Redis failure
        assert not session.queue.empty()
