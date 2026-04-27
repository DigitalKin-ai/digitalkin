"""Tests for SingleJobManager backpressure strategies.

Verifies BLOCK, DROP_OLDEST, and REJECT strategies in add_to_queue,
as well as env var configuration and closed-stream rejection.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import pytest_asyncio
from pydantic import BaseModel

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.job_manager_models import BackpressureStrategy
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

pytestmark = pytest.mark.timeout(30)


# ============================================================================
# Helpers
# ============================================================================


class _FakeOutput(BaseModel):
    """Minimal model that satisfies DataModel | ModuleCodeModel."""

    value: str


def _make_manager(
    strategy: BackpressureStrategy = BackpressureStrategy.BLOCK,
    timeout: float = 30.0,
) -> SingleJobManager:
    """Create a SingleJobManager with the given backpressure settings.

    Uses object.__new__ to skip __init__, then sets up a mock _task_manager
    so the tasks_sessions property (delegated from base class) works.
    """
    mgr = object.__new__(SingleJobManager)
    mgr._backpressure_strategy = strategy
    mgr._backpressure_timeout = timeout

    # tasks_sessions is a property on BaseJobManager that delegates to _task_manager
    mock_task_manager = Mock()
    mock_task_manager.tasks_sessions = {}
    mgr._task_manager = mock_task_manager
    return mgr


def _make_session(queue_maxsize: int = 2) -> TaskSession:
    """Create a TaskSession with a mock module and small queue."""
    module = Mock(spec=BaseModule)
    module.stop = AsyncMock()
    module.context = Mock()
    module.context.session = Mock()
    module.context.session.setup_id = "setup:test"
    module.context.session.setup_version_id = "sv:test"
    module.context.session.current_ids = Mock(return_value={"task_id": "t", "mission_id": "m"})
    module.context.task_manager = Mock(spec=TaskManagerStrategy)
    module.context.task_manager.subscribe_signals = AsyncMock()
    module.context.task_manager.unsubscribe_signals = AsyncMock()
    module.context.task_manager.send_signal = AsyncMock()
    module.context.cleanup = AsyncMock()
    return TaskSession("job-1", "mission-1", module, queue_maxsize=queue_maxsize)


# ============================================================================
# BLOCK strategy
# ============================================================================


@pytest.mark.asyncio
async def test_block_waits_and_succeeds() -> None:
    """BLOCK: queue full, consumer reads, put succeeds within timeout."""
    mgr = _make_manager(BackpressureStrategy.BLOCK, timeout=5.0)
    session = _make_session(queue_maxsize=1)
    mgr.tasks_sessions["job-1"] = session

    # Fill the queue
    await session.queue.put({"old": True})

    async def _consumer() -> None:
        await asyncio.sleep(0.1)
        session.queue.get_nowait()
        session.queue.task_done()

    consumer = asyncio.create_task(_consumer())
    await mgr.add_to_queue("job-1", _FakeOutput(value="new"))
    await consumer

    assert session.queue.qsize() == 1
    item = session.queue.get_nowait()
    assert item == {"value": "new"}


@pytest.mark.asyncio
async def test_block_timeout_raises() -> None:
    """BLOCK: queue full, no consumer, timeout raises asyncio.TimeoutError."""
    mgr = _make_manager(BackpressureStrategy.BLOCK, timeout=0.1)
    session = _make_session(queue_maxsize=1)
    mgr.tasks_sessions["job-1"] = session

    await session.queue.put({"old": True})

    with pytest.raises(asyncio.TimeoutError):
        await mgr.add_to_queue("job-1", _FakeOutput(value="new"))


# ============================================================================
# DROP_OLDEST strategy
# ============================================================================


@pytest.mark.asyncio
async def test_drop_oldest_preserves_current_behavior() -> None:
    """DROP_OLDEST: drops oldest message when queue is full."""
    mgr = _make_manager(BackpressureStrategy.DROP_OLDEST, timeout=30.0)
    session = _make_session(queue_maxsize=1)
    mgr.tasks_sessions["job-1"] = session

    await session.queue.put({"value": "oldest"})

    await mgr.add_to_queue("job-1", _FakeOutput(value="newest"))

    assert session.queue.qsize() == 1
    item = session.queue.get_nowait()
    assert item == {"value": "newest"}


# ============================================================================
# REJECT strategy
# ============================================================================


@pytest.mark.asyncio
async def test_reject_discards_new_message() -> None:
    """REJECT: queue unchanged, new message discarded."""
    mgr = _make_manager(BackpressureStrategy.REJECT)
    session = _make_session(queue_maxsize=1)
    mgr.tasks_sessions["job-1"] = session

    await session.queue.put({"value": "existing"})

    # Should not raise, just silently discard
    await mgr.add_to_queue("job-1", _FakeOutput(value="rejected"))

    assert session.queue.qsize() == 1
    item = session.queue.get_nowait()
    assert item == {"value": "existing"}


# ============================================================================
# Env var configuration
# ============================================================================


def _mock_module_class() -> Mock:
    """Create a mock module class with attributes needed by BaseJobManager.__init__."""
    cls = Mock(spec=BaseModule)
    cls.services_config_strategies = {}
    cls.services_config_params = {}
    return cls


def test_env_var_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strategy and timeout are read from env vars in __init__."""
    monkeypatch.setenv("DIGITALKIN_BACKPRESSURE_STRATEGY", "reject")
    monkeypatch.setenv("DIGITALKIN_BACKPRESSURE_TIMEOUT", "42.5")

    from digitalkin.services.services_models import ServicesMode

    mgr = SingleJobManager(_mock_module_class(), ServicesMode.LOCAL, MagicMock())

    assert mgr._backpressure_strategy == BackpressureStrategy.REJECT
    assert mgr._backpressure_timeout == 42.5


def test_env_var_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default strategy is BLOCK, default timeout is 30.0."""
    monkeypatch.delenv("DIGITALKIN_BACKPRESSURE_STRATEGY", raising=False)
    monkeypatch.delenv("DIGITALKIN_BACKPRESSURE_TIMEOUT", raising=False)

    from digitalkin.services.services_models import ServicesMode

    mgr = SingleJobManager(_mock_module_class(), ServicesMode.LOCAL, MagicMock())

    assert mgr._backpressure_strategy == BackpressureStrategy.BLOCK
    assert mgr._backpressure_timeout == 300.0


# ============================================================================
# Closed stream rejection (all strategies)
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", list(BackpressureStrategy))
async def test_closed_stream_rejects(strategy: BackpressureStrategy) -> None:
    """Write is rejected after stream is closed, regardless of strategy."""
    mgr = _make_manager(strategy)
    session = _make_session(queue_maxsize=10)
    session.close_stream()
    mgr.tasks_sessions["job-1"] = session

    await mgr.add_to_queue("job-1", _FakeOutput(value="ignored"))

    assert session.queue.empty()


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", list(BackpressureStrategy))
async def test_missing_session_rejects(strategy: BackpressureStrategy) -> None:
    """Write is rejected when session doesn't exist, regardless of strategy."""
    mgr = _make_manager(strategy)

    # Should not raise
    await mgr.add_to_queue("nonexistent", _FakeOutput(value="ignored"))
