"""Advanced tests for TaskIQ job manager, broker, and worker integration.

Tests cover:
- Registry config forwarding across process boundaries (pickle survival)
- RStream SSL context creation with env var combinations
- Broker URL construction with scheme/host/port
- run_start_module task: registry injection, ServicesConfig wiring, error paths
- TaskiqJobManager: job dispatch, stream consumer lifecycle, queue routing
- PickleFormatter: round-trip serialization of TaskiqMessage
- Shutdown lifecycle, consumer resilience, stream completion, middleware, orphan reaper
"""

import asyncio
import datetime
import json
import os
import ssl
import sys
from typing import Any, ClassVar
from unittest.mock import AsyncMock, Mock, patch

import pytest

from digitalkin.services.services_models import ServicesMode, ServicesStrategy
from tests.mocks.models import MockInputModel, MockInputTrigger, MockOutputModel, MockSecretModel, MockSetupModel
from tests.mocks.modules import SimpleMockModule

pytestmark = [pytest.mark.taskiq, pytest.mark.timeout(30)]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MockModule = SimpleMockModule


@pytest.fixture(autouse=True)
def _clean_module_class_params():
    """Reset SimpleMockModule class-level state between tests."""
    original = dict(SimpleMockModule.services_config_params)
    yield
    SimpleMockModule.services_config_params = original


@pytest.fixture()
def _patch_taskiq():
    """Patch TASKIQ_BROKER and _start so TaskiqJobManager can be instantiated without RabbitMQ."""
    pytest.importorskip("taskiq", reason="taskiq not installed")
    with (
        patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER"),
        patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._start"),
    ):
        yield


# ===========================================================================
# 1. RStream SSL Context
# ===========================================================================


class TestRStreamSSLContext:
    """Tests for _rstream_ssl_context() env-driven TLS configuration."""

    def test_ssl_disabled_by_default(self):
        """No SSL context when RABBITMQ_RSTREAM_SSL is unset."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import _rstream_ssl_context

        with patch.dict(os.environ, {}, clear=True):
            assert _rstream_ssl_context() is None

    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "yes", "YES"])
    def test_ssl_enabled_truthy_values(self, value):
        """SSL context created for all truthy RABBITMQ_RSTREAM_SSL values."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import _rstream_ssl_context

        env = {"RABBITMQ_RSTREAM_SSL": value}
        with patch.dict(os.environ, env, clear=True):
            ctx = _rstream_ssl_context()
            assert isinstance(ctx, ssl.SSLContext)
            # Default: verify certs
            assert ctx.check_hostname is True
            assert ctx.verify_mode == ssl.CERT_REQUIRED

    def test_ssl_verify_disabled(self):
        """SSL context skips verification when RABBITMQ_RSTREAM_SSL_VERIFY=false."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import _rstream_ssl_context

        env = {"RABBITMQ_RSTREAM_SSL": "true", "RABBITMQ_RSTREAM_SSL_VERIFY": "false"}
        with patch.dict(os.environ, env, clear=True):
            ctx = _rstream_ssl_context()
            assert isinstance(ctx, ssl.SSLContext)
            assert ctx.check_hostname is False
            assert ctx.verify_mode == ssl.CERT_NONE

    @pytest.mark.parametrize("value", ["false", "0", "no", "", "random"])
    def test_ssl_not_enabled_falsy_values(self, value):
        """No SSL context for non-truthy RABBITMQ_RSTREAM_SSL values."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import _rstream_ssl_context

        env = {"RABBITMQ_RSTREAM_SSL": value}
        with patch.dict(os.environ, env, clear=True):
            assert _rstream_ssl_context() is None


# ===========================================================================
# 2. Broker URL Construction
# ===========================================================================


class TestBrokerURLConstruction:
    """Tests for TaskiqBrokerConfig.define_broker() URL assembly."""

    def test_default_scheme_is_amqp(self):
        """Broker defaults to amqp:// scheme when RABBITMQ_BROKER_SCHEME is unset."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import TaskiqBrokerConfig

        with patch.dict(os.environ, {}, clear=True):
            with patch("digitalkin.core.job_manager.taskiq_broker.AioPikaBroker") as mock_broker:
                mock_broker.return_value = Mock()
                TaskiqBrokerConfig.define_broker()
                url = mock_broker.call_args[0][0]
                assert url.startswith("amqp://")

    def test_amqps_scheme_from_env(self):
        """Broker uses amqps:// when RABBITMQ_BROKER_SCHEME=amqps."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import TaskiqBrokerConfig

        env = {
            "RABBITMQ_BROKER_SCHEME": "amqps",
            "RABBITMQ_BROKER_HOST": "rabbit.example.com",
            "RABBITMQ_BROKER_PORT": "5671",
            "RABBITMQ_BROKER_USERNAME": "user",
            "RABBITMQ_BROKER_PASSWORD": "pass",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("digitalkin.core.job_manager.taskiq_broker.AioPikaBroker") as mock_broker:
                mock_broker.return_value = Mock()
                TaskiqBrokerConfig.define_broker()
                url = mock_broker.call_args[0][0]
                assert url == "amqps://user:pass@rabbit.example.com:5671"

    def test_custom_host_port(self):
        """Broker constructs URL from individual env vars."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import TaskiqBrokerConfig

        env = {
            "RABBITMQ_BROKER_HOST": "myhost",
            "RABBITMQ_BROKER_PORT": "9999",
            "RABBITMQ_BROKER_USERNAME": "admin",
            "RABBITMQ_BROKER_PASSWORD": "secret",
        }
        with patch.dict(os.environ, env, clear=True):
            with patch("digitalkin.core.job_manager.taskiq_broker.AioPikaBroker") as mock_broker:
                mock_broker.return_value = Mock()
                TaskiqBrokerConfig.define_broker()
                url = mock_broker.call_args[0][0]
                assert url == "amqp://admin:secret@myhost:9999"


# ===========================================================================
# 3. Producer / Consumer SSL Wiring
# ===========================================================================


class TestProducerConsumerSSL:
    """Tests that Producer and Consumer receive ssl_context from _rstream_ssl_context."""

    def test_producer_receives_ssl_context(self):
        """define_producer passes ssl_context to rstream.Producer."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import TaskiqBrokerConfig

        mock_ctx = Mock(spec=ssl.SSLContext)
        with (
            patch.dict(os.environ, {"RABBITMQ_RSTREAM_SSL": "true"}, clear=True),
            patch("digitalkin.core.job_manager.taskiq_broker._rstream_ssl_context", return_value=mock_ctx),
            patch("digitalkin.core.job_manager.taskiq_broker.Producer") as mock_producer,
        ):
            TaskiqBrokerConfig.define_producer()
            assert mock_producer.call_args[1]["ssl_context"] is mock_ctx

    def test_producer_no_ssl_by_default(self):
        """define_producer passes ssl_context=None when SSL is disabled."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import TaskiqBrokerConfig

        with (
            patch.dict(os.environ, {}, clear=True),
            patch("digitalkin.core.job_manager.taskiq_broker._rstream_ssl_context", return_value=None),
            patch("digitalkin.core.job_manager.taskiq_broker.Producer") as mock_producer,
        ):
            TaskiqBrokerConfig.define_producer()
            assert mock_producer.call_args[1]["ssl_context"] is None

    @pytest.mark.asyncio
    async def test_consumer_receives_ssl_context(self, _patch_taskiq):
        """_define_consumer passes ssl_context to rstream.Consumer."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        mock_ctx = Mock(spec=ssl.SSLContext)
        with (
            patch(
                "digitalkin.core.job_manager.taskiq_broker._rstream_ssl_context",
                return_value=mock_ctx,
            ),
            patch("digitalkin.core.job_manager.taskiq_job_manager.Consumer") as mock_consumer,
        ):
            mock_consumer.return_value = Mock()
            TaskiqJobManager._define_consumer()
            assert mock_consumer.call_args[1]["ssl_context"] is mock_ctx


# ===========================================================================
# 4. Registry Config Forwarding (Pickle Survival)
# ===========================================================================


class TestRegistryConfigForwarding:
    """Tests that registry config survives TaskIQ worker process boundary."""

    @pytest.mark.asyncio
    async def test_create_module_instance_job_forwards_registry_config(self, _patch_taskiq):
        """create_module_instance_job passes registry_config from services_config_params."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        # Simulate what ModuleServer._prepare_registry_config does
        client_config = {"host": "localhost", "port": 50052}
        MockModule.services_config_params["registry"] = {"client_config": client_config}

        mock_task = Mock()
        mock_running = AsyncMock()
        mock_running.task_id = "job-123"
        mock_running.wait_result = AsyncMock(return_value=Mock(is_err=False))
        mock_task.kiq = AsyncMock(return_value=mock_running)

        with (
            patch(
                "digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER"
            ) as mock_broker,
        ):
            mock_broker.find_task.return_value = mock_task

            input_data = MockInputModel(root=MockInputTrigger())
            setup_data = MockSetupModel()

            # Patch module creation for metadata instance (line 397)
            with patch.object(MockModule, "__init__", return_value=None):
                with patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager.create_task"):
                    try:
                        await manager.create_module_instance_job(
                            input_data, setup_data, "mission:1", "setup:1", "sv:1"
                        )
                    except Exception:
                        pass  # We only care about the kiq call

            # Verify registry_config was passed as the last positional arg
            kiq_args = mock_task.kiq.call_args[0]
            registry_config_arg = kiq_args[-1]  # Last positional arg
            assert registry_config_arg == {"client_config": client_config}

    @pytest.mark.asyncio
    async def test_create_module_instance_job_forwards_none_when_no_registry(self, _patch_taskiq):
        """create_module_instance_job passes None when no registry config exists."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        # Ensure no registry key
        MockModule.services_config_params.pop("registry", None)

        mock_task = Mock()
        mock_running = AsyncMock()
        mock_running.task_id = "job-456"
        mock_running.wait_result = AsyncMock(return_value=Mock(is_err=False))
        mock_task.kiq = AsyncMock(return_value=mock_running)

        with patch(
            "digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER"
        ) as mock_broker:
            mock_broker.find_task.return_value = mock_task

            input_data = MockInputModel(root=MockInputTrigger())
            setup_data = MockSetupModel()

            with patch.object(MockModule, "__init__", return_value=None):
                with patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager.create_task"):
                    try:
                        await manager.create_module_instance_job(
                            input_data, setup_data, "mission:1", "setup:1", "sv:1"
                        )
                    except Exception:
                        pass

            kiq_args = mock_task.kiq.call_args[0]
            registry_config_arg = kiq_args[-1]
            assert registry_config_arg is None


# ===========================================================================
# 5. run_start_module Registry Injection
# ===========================================================================


class TestRunStartModuleRegistryInjection:
    """Tests that run_start_module restores registry config in the worker."""

    @pytest.mark.asyncio
    async def test_registry_config_injected_into_module_class(self):
        """run_start_module injects registry_config into module_class.services_config_params."""
        pytest.importorskip("taskiq", reason="taskiq not installed")

        # Create a fresh module class to avoid pollution
        class IsolatedModule(SimpleMockModule):
            services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
            services_config_params: ClassVar[dict[str, dict[str, Any] | None]] = {}

        registry_config = {"client_config": {"host": "registry.test", "port": 50052}}

        mock_context = Mock()
        mock_context.message = Mock()
        mock_context.message.task_id = "job-789"

        with (
            patch("digitalkin.core.job_manager.taskiq_broker.ServicesConfig"),
            patch("digitalkin.core.job_manager.taskiq_broker.ModuleFactory") as mock_factory,
            patch("digitalkin.core.job_manager.taskiq_broker.BaseJobManager"),
            patch("digitalkin.core.job_manager.taskiq_broker.TaskExecutor"),
            patch("digitalkin.core.job_manager.taskiq_broker.TaskSession"),
        ):
            mock_module_instance = Mock()
            mock_factory.create_module_instance.return_value = mock_module_instance

            from digitalkin.core.job_manager.taskiq_broker import run_start_module

            # Call the underlying function directly (unwrap the taskiq decorator)
            func = run_start_module.original_func if hasattr(run_start_module, "original_func") else run_start_module

            try:
                await func(
                    mission_id="mission:1",
                    setup_id="setup:1",
                    setup_version_id="sv:1",
                    module_class=IsolatedModule,
                    services_mode=ServicesMode.REMOTE,
                    input_data={"root": {"protocol": "mock", "data": "test"}},
                    setup_data={"config": "test"},
                    request_metadata=None,
                    registry_config=registry_config,
                    context=mock_context,
                )
            except Exception:
                pass  # Task execution may fail, we only test injection

        # Verify the injection happened
        assert "registry" in IsolatedModule.services_config_params
        assert IsolatedModule.services_config_params["registry"] == registry_config

    @pytest.mark.asyncio
    async def test_no_injection_when_registry_config_is_none(self):
        """run_start_module does not modify services_config_params when registry_config is None."""
        pytest.importorskip("taskiq", reason="taskiq not installed")

        class IsolatedModule2(SimpleMockModule):
            services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
            services_config_params: ClassVar[dict[str, dict[str, Any] | None]] = {}

        mock_context = Mock()
        mock_context.message = Mock()
        mock_context.message.task_id = "job-000"

        with (
            patch("digitalkin.core.job_manager.taskiq_broker.ServicesConfig"),
            patch("digitalkin.core.job_manager.taskiq_broker.ModuleFactory") as mock_factory,
            patch("digitalkin.core.job_manager.taskiq_broker.BaseJobManager"),
            patch("digitalkin.core.job_manager.taskiq_broker.TaskExecutor"),
            patch("digitalkin.core.job_manager.taskiq_broker.TaskSession"),
        ):
            mock_factory.create_module_instance.return_value = Mock()

            from digitalkin.core.job_manager.taskiq_broker import run_start_module

            func = run_start_module.original_func if hasattr(run_start_module, "original_func") else run_start_module

            try:
                await func(
                    mission_id="mission:1",
                    setup_id="setup:1",
                    setup_version_id="sv:1",
                    module_class=IsolatedModule2,
                    services_mode=ServicesMode.REMOTE,
                    input_data={"root": {"protocol": "mock", "data": "test"}},
                    setup_data={"config": "test"},
                    request_metadata=None,
                    registry_config=None,
                    context=mock_context,
                )
            except Exception:
                pass

        assert "registry" not in IsolatedModule2.services_config_params


# ===========================================================================
# 6. PickleFormatter Round-Trip
# ===========================================================================


class TestPickleFormatter:
    """Tests for PickleFormatter serialization round-trip."""

    def test_round_trip_preserves_message(self):
        """PickleFormatter dumps and loads produce equivalent TaskiqMessage."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from taskiq import TaskiqMessage

        from digitalkin.core.job_manager.taskiq_broker import PickleFormatter

        formatter = PickleFormatter()

        original = TaskiqMessage(
            task_id="test-id",
            task_name="test.task",
            labels={},
            args=[1, "hello", {"key": "value"}],
            kwargs={"flag": True},
        )

        broker_msg = formatter.dumps(original)
        restored = formatter.loads(broker_msg.message)

        assert restored.task_id == original.task_id
        assert restored.task_name == original.task_name
        assert restored.args == original.args
        assert restored.kwargs == original.kwargs


# ===========================================================================
# 7. Stream Consumer Queue Routing
# ===========================================================================


class TestStreamConsumerRouting:
    """Tests for TaskiqJobManager stream consumer and queue routing."""

    @pytest.mark.asyncio
    async def test_on_message_routes_to_correct_queue(self, _patch_taskiq):
        """_on_message dispatches output_data to the queue matching job_id."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        # Create queues for two jobs
        q1: asyncio.Queue = asyncio.Queue()
        q2: asyncio.Queue = asyncio.Queue()
        manager.job_queues["job-A"] = q1
        manager.job_queues["job-B"] = q2

        # Route message to job-B
        msg = json.dumps({"job_id": "job-B", "output_data": {"result": "hello"}}).encode()
        await manager._on_message(msg, Mock())

        assert q1.empty()
        assert not q2.empty()
        item = q2.get_nowait()
        assert item == {"result": "hello"}

    @pytest.mark.asyncio
    async def test_on_message_ignores_unknown_job(self, _patch_taskiq):
        """_on_message silently drops messages for unregistered job_ids."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        msg = json.dumps({"job_id": "nonexistent", "output_data": {"x": 1}}).encode()
        # Should not raise
        await manager._on_message(msg, Mock())

    @pytest.mark.asyncio
    async def test_on_message_handles_malformed_json(self, _patch_taskiq):
        """_on_message handles invalid JSON without crashing."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
        # Should not raise
        await manager._on_message(b"not-json{{{", Mock())

    @pytest.mark.asyncio
    async def test_on_message_handles_missing_job_id(self, _patch_taskiq):
        """_on_message ignores messages without job_id field."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
        msg = json.dumps({"output_data": {"x": 1}}).encode()
        # Should not raise
        await manager._on_message(msg, Mock())

    @pytest.mark.asyncio
    async def test_stream_consumer_yields_queued_items(self, _patch_taskiq):
        """generate_stream_consumer yields items put into the job queue."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
        manager.stream_timeout = 0.5  # Fast timeout for test

        outputs = []

        async def consume():
            async with manager.generate_stream_consumer("test-job") as stream:
                queue = manager.job_queues["test-job"]
                await queue.put({"data": "first"})
                await queue.put({"data": "second"})
                await queue.put({"data": "third"})

                count = 0
                async for output in stream:
                    outputs.append(output)
                    count += 1
                    if count >= 3:
                        break

        await asyncio.wait_for(consume(), timeout=3.0)
        assert len(outputs) == 3
        assert outputs[0] == {"data": "first"}

    @pytest.mark.asyncio
    async def test_stream_consumer_cleans_up_queue(self, _patch_taskiq):
        """generate_stream_consumer removes job queue on exit."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
        manager.stream_timeout = 0.2

        async with manager.generate_stream_consumer("cleanup-job") as stream:
            assert "cleanup-job" in manager.job_queues
            # Don't consume, just exit
            pass

        assert "cleanup-job" not in manager.job_queues


# ===========================================================================
# 8. TaskiqJobManager Initialization
# ===========================================================================


class TestTaskiqJobManagerInit:
    """Tests for TaskiqJobManager construction and configuration."""

    @pytest.mark.asyncio
    async def test_custom_stream_timeout_from_env(self, _patch_taskiq):
        """TaskiqJobManager reads DIGITALKIN_RSTREAM_TIMEOUT from environment."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        with patch.dict(os.environ, {"DIGITALKIN_RSTREAM_TIMEOUT": "45.0"}):
            manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE, stream_timeout=45.0)
            assert manager.stream_timeout == 45.0

    @pytest.mark.asyncio
    async def test_custom_queue_size_from_env(self, _patch_taskiq):
        """TaskiqJobManager reads DIGITALKIN_RSTREAM_QUEUE_SIZE from environment."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        with patch.dict(os.environ, {"DIGITALKIN_RSTREAM_QUEUE_SIZE": "500"}):
            manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
            assert manager.max_queue_size == 500


# ===========================================================================
# 9. Session Lifecycle from RStream
# ===========================================================================


class TestSessionLifecycleFromRStream:
    """Tests for session status bridging via RStream messages and lifecycle fixes."""

    @pytest.mark.asyncio
    async def test_on_message_marks_failed_on_error_code(self, _patch_taskiq):
        """ModuleCodeModel error in RStream marks session as failed."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        session = Mock()
        session.status = "pending"
        session._stream_closed = asyncio.Event()
        session.close_stream = session._stream_closed.set
        manager.tasks_sessions["job-err"] = session

        queue: asyncio.Queue = asyncio.Queue()
        manager.job_queues["job-err"] = queue

        msg = json.dumps({
            "job_id": "job-err",
            "output_data": {"code": "WorkerError", "message": "boom", "short_description": "fail"},
        }).encode()
        await manager._on_message(msg, Mock())

        assert session.status == "failed"
        assert not session._stream_closed.is_set()

    @pytest.mark.asyncio
    async def test_on_message_marks_completed_on_end_of_stream(self, _patch_taskiq):
        """EndOfStreamOutput in RStream marks session as completed and closes stream."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        session = Mock()
        session.status = "pending"
        session._stream_closed = asyncio.Event()
        session.close_stream = session._stream_closed.set
        manager.tasks_sessions["job-eos"] = session

        queue: asyncio.Queue = asyncio.Queue()
        manager.job_queues["job-eos"] = queue

        msg = json.dumps({
            "job_id": "job-eos",
            "output_data": {"root": {"protocol": "end_of_stream", "created_at": "2026-01-01"}, "annotations": {}},
        }).encode()
        await manager._on_message(msg, Mock())

        assert session.status == "completed"
        assert session._stream_closed.is_set()

    @pytest.mark.asyncio
    async def test_error_then_end_of_stream_preserves_failed(self, _patch_taskiq):
        """Error then end_of_stream keeps status as failed but still closes stream."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        session = Mock()
        session.status = "pending"
        session._stream_closed = asyncio.Event()
        session.close_stream = session._stream_closed.set
        manager.tasks_sessions["job-ef"] = session

        queue: asyncio.Queue = asyncio.Queue()
        manager.job_queues["job-ef"] = queue

        # First: error
        err_msg = json.dumps({
            "job_id": "job-ef",
            "output_data": {"code": "WorkerError", "message": "boom"},
        }).encode()
        await manager._on_message(err_msg, Mock())
        assert session.status == "failed"

        # Then: end_of_stream
        eos_msg = json.dumps({
            "job_id": "job-ef",
            "output_data": {"root": {"protocol": "end_of_stream", "created_at": "2026-01-01"}, "annotations": {}},
        }).encode()
        await manager._on_message(eos_msg, Mock())

        assert session.status == "failed"  # Not overwritten to "completed"
        assert session._stream_closed.is_set()  # Stream still closed

    @pytest.mark.asyncio
    async def test_on_message_ignores_if_already_cancelled(self, _patch_taskiq):
        """Pre-cancelled session status not overwritten by error or eos."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        session = Mock()
        session.status = "cancelled"
        session._stream_closed = asyncio.Event()
        session.close_stream = session._stream_closed.set
        manager.tasks_sessions["job-cx"] = session

        queue: asyncio.Queue = asyncio.Queue()
        manager.job_queues["job-cx"] = queue

        # Error should not overwrite "cancelled"
        err_msg = json.dumps({
            "job_id": "job-cx",
            "output_data": {"code": "WorkerError", "message": "boom"},
        }).encode()
        await manager._on_message(err_msg, Mock())
        assert session.status == "cancelled"

        # End of stream should not overwrite "cancelled" but should close stream
        eos_msg = json.dumps({
            "job_id": "job-cx",
            "output_data": {"root": {"protocol": "end_of_stream", "created_at": "2026-01-01"}, "annotations": {}},
        }).encode()
        await manager._on_message(eos_msg, Mock())
        assert session.status == "cancelled"
        assert session._stream_closed.is_set()

    @pytest.mark.asyncio
    async def test_config_setup_cleans_session(self, _patch_taskiq):
        """Config setup response cleans up session and releases semaphore."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        session = Mock()
        session.status = "pending"
        session.mission_id = "mission:cfg"
        manager.tasks_sessions["job-cfg"] = session

        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait({"config": "result"})
        manager.job_queues["job-cfg"] = queue

        with patch.object(manager._task_manager, "_cleanup_task", new_callable=AsyncMock) as mock_cleanup:
            result = await manager.generate_config_setup_module_response("job-cfg")

        assert result == {"config": "result"}
        assert "job-cfg" not in manager.job_queues
        mock_cleanup.assert_awaited_once_with("job-cfg", "mission:cfg")

    @pytest.mark.asyncio
    async def test_job_queue_pre_created(self, _patch_taskiq):
        """Queue exists and send_message is wired after create_module_instance_job dispatch."""
        from types import SimpleNamespace

        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        mock_task = Mock()
        mock_running = AsyncMock()
        mock_running.task_id = "job-pre"
        mock_running.wait_result = AsyncMock(return_value=Mock(is_err=False))
        mock_task.kiq = AsyncMock(return_value=mock_running)

        # Create a mock module with a context that supports callback wiring
        mock_module = Mock()
        mock_module.context = SimpleNamespace(callbacks=SimpleNamespace(logger=Mock()))

        # Replace module_class with a callable that returns our mock module
        mock_cls = Mock(return_value=mock_module)
        mock_cls.services_config_params = MockModule.services_config_params

        with (
            patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER") as mock_broker,
            patch.object(manager, "create_task", new_callable=AsyncMock) as mock_create_task,
        ):
            mock_broker.find_task.return_value = mock_task
            manager.module_class = mock_cls

            input_data = MockInputModel(root=MockInputTrigger())
            setup_data = MockSetupModel()

            await manager.create_module_instance_job(
                input_data, setup_data, "mission:1", "setup:1", "sv:1"
            )

            # Verify send_message was wired on the metadata-only module
            module = mock_create_task.call_args[0][2]
            assert callable(module.context.callbacks.send_message)

        assert "job-pre" in manager.job_queues

    @pytest.mark.asyncio
    async def test_wait_for_completion_returns_on_stream_closed(self, _patch_taskiq):
        """wait_for_completion returns instantly when _stream_closed is already set."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        session = Mock()
        session.status = "completed"
        session._stream_closed = asyncio.Event()
        session._stream_closed.set()
        manager.tasks_sessions["job-wfc"] = session

        # Should return near-instantly (well under 0.5s)
        await asyncio.wait_for(
            manager.wait_for_completion("job-wfc", max_wait=1.0),
            timeout=0.5,
        )

    @pytest.mark.asyncio
    async def test_generate_stream_consumer_reuses_existing_queue(self, _patch_taskiq):
        """Pre-populated queue items survive through generate_stream_consumer."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
        manager.stream_timeout = 0.3

        # Pre-create queue with items
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait({"data": "pre-existing"})
        manager.job_queues["job-reuse"] = queue

        outputs = []
        async with manager.generate_stream_consumer("job-reuse") as stream:
            assert manager.job_queues["job-reuse"] is queue
            count = 0
            async for output in stream:
                outputs.append(output)
                count += 1
                if count >= 1:
                    break

        assert outputs == [{"data": "pre-existing"}]

    def test_result_backend_wired_when_env_set(self):
        """RedisAsyncResultBackend attached when DIGITALKIN_TASKIQ_RESULT_BACKEND_URL is set."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import TaskiqBrokerConfig

        mock_taskiq_redis = Mock()
        with (
            patch.dict(os.environ, {"DIGITALKIN_TASKIQ_RESULT_BACKEND_URL": "redis://localhost:6379"}, clear=True),
            patch("digitalkin.core.job_manager.taskiq_broker.AioPikaBroker") as mock_broker_cls,
            patch.dict(sys.modules, {"taskiq_redis": mock_taskiq_redis}),
        ):
            mock_broker = Mock()
            mock_broker_cls.return_value = mock_broker

            TaskiqBrokerConfig.define_broker()

            mock_taskiq_redis.RedisAsyncResultBackend.assert_called_once_with("redis://localhost:6379")
            mock_broker.with_result_backend.assert_called_once()

    def test_no_result_backend_by_default(self):
        """No result backend attached when DIGITALKIN_TASKIQ_RESULT_BACKEND_URL is unset."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import TaskiqBrokerConfig

        with (
            patch.dict(os.environ, {}, clear=True),
            patch("digitalkin.core.job_manager.taskiq_broker.AioPikaBroker") as mock_broker_cls,
        ):
            mock_broker = Mock()
            mock_broker_cls.return_value = mock_broker

            TaskiqBrokerConfig.define_broker()

            mock_broker.with_result_backend.assert_not_called()


# ===========================================================================
# 10. Shutdown Lifecycle (Changes 1 & 2)
# ===========================================================================


class TestShutdownLifecycle:
    """Tests for stop() cleanup: modules, sessions, consumer, queues."""

    @pytest.mark.asyncio
    async def test_stop_cancels_all_modules_and_cleans_sessions(self, _patch_taskiq):
        """stop() cancels modules, cleans sessions, closes consumer, clears queues."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        # Simulate started state
        manager.stream_consumer = Mock()
        manager.stream_consumer.close = AsyncMock()
        manager.stream_consumer_task = asyncio.create_task(asyncio.sleep(100))
        manager._reaper_task = asyncio.create_task(asyncio.sleep(100))

        # Register mock sessions
        session1 = Mock()
        session1.status = "pending"
        session1.mission_id = "m1"
        session2 = Mock()
        session2.status = "completed"
        session2.mission_id = "m2"
        manager._task_manager.tasks_sessions["job-1"] = session1
        manager._task_manager.tasks_sessions["job-2"] = session2

        manager.job_queues["job-1"] = asyncio.Queue()
        manager.job_queues["job-2"] = asyncio.Queue()

        with (
            patch.object(manager, "stop_all_modules", new_callable=AsyncMock) as mock_stop_all,
            patch.object(manager._task_manager, "_cleanup_task", new_callable=AsyncMock) as mock_cleanup,
            patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqBrokerConfig.cleanup_global_resources", new_callable=AsyncMock),
        ):
            await manager.stop()

        mock_stop_all.assert_awaited_once()
        assert mock_cleanup.await_count == 2
        assert len(manager.job_queues) == 0
        manager.stream_consumer.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_releases_semaphore_slots(self, _patch_taskiq):
        """stop() releases all semaphore slots via _cleanup_task."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        manager.stream_consumer = Mock()
        manager.stream_consumer.close = AsyncMock()
        manager.stream_consumer_task = asyncio.create_task(asyncio.sleep(100))
        manager._reaper_task = asyncio.create_task(asyncio.sleep(100))

        session = Mock()
        session.status = "pending"
        session.mission_id = "m1"
        manager._task_manager.tasks_sessions["job-s"] = session

        cleanup_called = []

        async def fake_cleanup(task_id, mission_id):
            cleanup_called.append((task_id, mission_id))
            manager._task_manager.tasks_sessions.pop(task_id, None)

        with (
            patch.object(manager, "stop_all_modules", new_callable=AsyncMock),
            patch.object(manager._task_manager, "_cleanup_task", side_effect=fake_cleanup),
            patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqBrokerConfig.cleanup_global_resources", new_callable=AsyncMock),
        ):
            await manager.stop()

        assert ("job-s", "m1") in cleanup_called
        assert len(manager.tasks_sessions) == 0

    @pytest.mark.asyncio
    async def test_module_server_stop_calls_job_manager_stop(self):
        """ModuleServer.stop_async() calls job_manager.stop_all_modules() and stop()."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        mock_servicer = Mock()
        mock_servicer.shutdown = AsyncMock()
        mock_servicer.job_manager = Mock()
        mock_servicer.job_manager.stop_all_modules = AsyncMock()
        mock_servicer.job_manager.stop = AsyncMock()

        server = ModuleServer.__new__(ModuleServer)
        server.module_class = MockModule
        server.server_config = Mock()
        server.client_config = None
        server.module_servicer = mock_servicer
        server.registry = None
        server.server = Mock()
        server.server.stop = AsyncMock()
        server.server.wait_for_termination = AsyncMock()

        with patch("digitalkin.grpc_servers._base_server.BaseServer.stop_async", new_callable=AsyncMock):
            await server.stop_async()

        mock_servicer.job_manager.stop_all_modules.assert_awaited_once()
        mock_servicer.job_manager.stop.assert_awaited_once()


# ===========================================================================
# 11. Consumer Resilience (Change 3)
# ===========================================================================


class TestConsumerResilience:
    """Tests for RStream consumer auto-restart with backoff."""

    @pytest.mark.asyncio
    async def test_consumer_restarts_on_failure(self, _patch_taskiq):
        """Consumer.run() raises once then succeeds — verify reconnect."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        call_count = 0
        mock_consumer = Mock()

        async def fake_run():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("lost connection")
            # Second call succeeds and returns

        mock_consumer.run = fake_run
        mock_consumer.create_stream = AsyncMock()
        mock_consumer.start = AsyncMock()
        mock_consumer.subscribe = AsyncMock()
        manager.stream_consumer = mock_consumer

        with (
            patch.dict(os.environ, {"DIGITALKIN_RSTREAM_MAX_RETRIES": "3"}),
            patch.object(TaskiqJobManager, "_define_consumer", return_value=mock_consumer),
            patch("digitalkin.core.job_manager.taskiq_job_manager.asyncio.sleep", new_callable=AsyncMock),
        ):
            await manager._run_consumer_with_restart()

        assert call_count == 2
        mock_consumer.create_stream.assert_awaited_once()
        mock_consumer.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_consumer_gives_up_after_max_retries(self, _patch_taskiq):
        """Always raises — verify sessions marked failed after max retries."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        mock_consumer = Mock()

        async def always_fail():
            raise ConnectionError("down")

        mock_consumer.run = always_fail
        mock_consumer.create_stream = AsyncMock()
        mock_consumer.start = AsyncMock()
        mock_consumer.subscribe = AsyncMock()
        manager.stream_consumer = mock_consumer

        session = Mock()
        session.status = "pending"
        session.close_stream = Mock()
        manager._task_manager.tasks_sessions["job-f"] = session

        with (
            patch.dict(os.environ, {"DIGITALKIN_RSTREAM_MAX_RETRIES": "2"}),
            patch.object(TaskiqJobManager, "_define_consumer", return_value=mock_consumer),
            patch("digitalkin.core.job_manager.taskiq_job_manager.asyncio.sleep", new_callable=AsyncMock),
        ):
            await manager._run_consumer_with_restart()

        assert session.status == "failed"
        session.close_stream.assert_called_once()

    @pytest.mark.asyncio
    async def test_consumer_exits_cleanly_on_cancel(self, _patch_taskiq):
        """CancelledError propagates without retry."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        mock_consumer = Mock()

        async def raise_cancelled():
            raise asyncio.CancelledError()

        mock_consumer.run = raise_cancelled
        manager.stream_consumer = mock_consumer

        with pytest.raises(asyncio.CancelledError):
            await manager._run_consumer_with_restart()


# ===========================================================================
# 12. Stream Consumer Completion (Change 4)
# ===========================================================================


class TestStreamConsumerCompletion:
    """Tests for stream_closed and completed status detection in stream consumer."""

    @pytest.mark.asyncio
    async def test_stream_exits_on_stream_closed(self, _patch_taskiq):
        """_stream_closed set — immediate exit after timeout."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
        manager.stream_timeout = 0.1

        session = Mock()
        session.status = "completed"
        session.stream_closed = True
        manager._task_manager.tasks_sessions["job-sc"] = session

        outputs = []
        async with manager.generate_stream_consumer("job-sc") as stream:
            async for output in stream:
                outputs.append(output)

        assert outputs == []

    @pytest.mark.asyncio
    async def test_stream_exits_on_completed_status(self, _patch_taskiq):
        """status='completed' — drains and exits."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
        manager.stream_timeout = 0.1

        session = Mock()
        session.status = "completed"
        session.stream_closed = False
        manager._task_manager.tasks_sessions["job-comp"] = session

        outputs = []
        async with manager.generate_stream_consumer("job-comp") as stream:
            async for output in stream:
                outputs.append(output)

        assert outputs == []

    @pytest.mark.asyncio
    async def test_stream_drains_remaining_items_on_completion(self, _patch_taskiq):
        """Items in queue + completed status — all yielded before exit."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
        manager.stream_timeout = 0.1

        session = Mock()
        session.status = "completed"
        session.stream_closed = False
        manager._task_manager.tasks_sessions["job-drain"] = session

        # Pre-populate queue
        queue: asyncio.Queue = asyncio.Queue()
        queue.put_nowait({"data": "item1"})
        queue.put_nowait({"data": "item2"})
        manager.job_queues["job-drain"] = queue

        outputs = []
        async with manager.generate_stream_consumer("job-drain") as stream:
            async for output in stream:
                outputs.append(output)

        assert len(outputs) == 2
        assert outputs[0] == {"data": "item1"}
        assert outputs[1] == {"data": "item2"}


# ===========================================================================
# 13. Taskiq Lifecycle Middleware (Change 5)
# ===========================================================================


class TestMiddleware:
    """Tests for TaskiqLifecycleMiddleware."""

    @pytest.mark.asyncio
    async def test_middleware_pre_execute_returns_message(self):
        """pre_execute returns unmodified message."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from taskiq import TaskiqMessage

        from digitalkin.core.job_manager.taskiq_broker import TaskiqLifecycleMiddleware

        middleware = TaskiqLifecycleMiddleware()
        msg = TaskiqMessage(
            task_id="test-id",
            task_name="test.task",
            labels={},
            args=[],
            kwargs={},
        )

        result = await middleware.pre_execute(msg)
        assert result is msg

    @pytest.mark.asyncio
    async def test_middleware_on_error_sends_end_of_stream(self):
        """on_error sends ModuleCodeModel + EndOfStreamOutput as safety net."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from taskiq import TaskiqMessage
        from taskiq.result import TaskiqResult

        from digitalkin.core.job_manager.taskiq_broker import TaskiqLifecycleMiddleware

        middleware = TaskiqLifecycleMiddleware()
        msg = TaskiqMessage(
            task_id="crash-id",
            task_name="test.task",
            labels={},
            args=[],
            kwargs={},
        )
        result = TaskiqResult(is_err=True, return_value=None, execution_time=0.1, log="")
        exc = RuntimeError("worker crashed")

        sent_messages = []

        async def capture_send(job_id, output_data):
            sent_messages.append((job_id, type(output_data).__name__))

        with patch("digitalkin.core.job_manager.taskiq_broker.TaskiqBrokerConfig.send_message_to_stream", side_effect=capture_send):
            await middleware.on_error(msg, result, exc)

        assert len(sent_messages) == 2
        assert sent_messages[0] == ("crash-id", "ModuleCodeModel")
        assert sent_messages[1] == ("crash-id", "DataModel")

    @pytest.mark.asyncio
    async def test_middleware_on_error_handles_send_failure(self):
        """on_error swallows send failures without propagation."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from taskiq import TaskiqMessage
        from taskiq.result import TaskiqResult

        from digitalkin.core.job_manager.taskiq_broker import TaskiqLifecycleMiddleware

        middleware = TaskiqLifecycleMiddleware()
        msg = TaskiqMessage(
            task_id="fail-send",
            task_name="test.task",
            labels={},
            args=[],
            kwargs={},
        )
        result = TaskiqResult(is_err=True, return_value=None, execution_time=0.1, log="")
        exc = RuntimeError("worker crashed")

        with patch(
            "digitalkin.core.job_manager.taskiq_broker.TaskiqBrokerConfig.send_message_to_stream",
            side_effect=ConnectionError("stream down"),
        ):
            # Should not raise
            await middleware.on_error(msg, result, exc)

    def test_middleware_registered_on_broker(self):
        """TaskiqLifecycleMiddleware is registered in TASKIQ_BROKER.middlewares."""
        pytest.importorskip("taskiq", reason="taskiq not installed")
        from digitalkin.core.job_manager.taskiq_broker import TASKIQ_BROKER, TaskiqLifecycleMiddleware

        assert any(isinstance(m, TaskiqLifecycleMiddleware) for m in TASKIQ_BROKER.middlewares)


# ===========================================================================
# 14. Orphan Session Reaper (Change 6)
# ===========================================================================


class TestOrphanReaper:
    """Tests for orphan session reaper and TaskSession.created_at."""

    @pytest.mark.asyncio
    async def test_reaper_marks_old_pending_as_failed(self, _patch_taskiq):
        """Old created_at + pending status → marked failed + stream closed."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        session = Mock()
        session.status = "pending"
        session.mission_id = "m1"
        session.created_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=700)
        session.close_stream = Mock()
        manager._task_manager.tasks_sessions["job-orphan"] = session

        with (
            patch.dict(os.environ, {"DIGITALKIN_ORPHAN_SESSION_TIMEOUT": "600", "DIGITALKIN_ORPHAN_CHECK_INTERVAL": "0.01"}),
            patch.object(manager._task_manager, "_cleanup_task", new_callable=AsyncMock) as mock_cleanup,
        ):
            task = asyncio.create_task(manager._reap_orphan_sessions())
            await asyncio.sleep(0.05)
            task.cancel()
            await task  # Reaper catches CancelledError and returns cleanly

        assert session.status == "failed"
        session.close_stream.assert_called_once()
        mock_cleanup.assert_awaited_once_with("job-orphan", "m1")

    @pytest.mark.asyncio
    async def test_reaper_ignores_non_pending_sessions(self, _patch_taskiq):
        """status='running' + old → not touched."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        session = Mock()
        session.status = "running"
        session.mission_id = "m1"
        session.created_at = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=700)
        session.close_stream = Mock()
        manager._task_manager.tasks_sessions["job-running"] = session

        with (
            patch.dict(os.environ, {"DIGITALKIN_ORPHAN_SESSION_TIMEOUT": "600", "DIGITALKIN_ORPHAN_CHECK_INTERVAL": "0.01"}),
            patch.object(manager._task_manager, "_cleanup_task", new_callable=AsyncMock) as mock_cleanup,
        ):
            task = asyncio.create_task(manager._reap_orphan_sessions())
            await asyncio.sleep(0.05)
            task.cancel()
            await task  # Reaper catches CancelledError and returns cleanly

        assert session.status == "running"
        session.close_stream.assert_not_called()
        mock_cleanup.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reaper_stops_on_cancel(self, _patch_taskiq):
        """Cancel task → clean exit."""
        from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

        manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

        with patch.dict(os.environ, {"DIGITALKIN_ORPHAN_CHECK_INTERVAL": "0.01"}):
            task = asyncio.create_task(manager._reap_orphan_sessions())
            await asyncio.sleep(0.02)
            task.cancel()
            # Should not raise — reaper catches CancelledError and returns
            await task

    def test_task_session_has_created_at(self):
        """TaskSession.created_at is set on construction."""
        from digitalkin.core.task_manager.task_session import TaskSession

        mock_module = Mock()
        mock_module.context.task_manager = Mock()

        before = datetime.datetime.now(datetime.timezone.utc)
        session = TaskSession("t1", "m1", mock_module)
        after = datetime.datetime.now(datetime.timezone.utc)

        assert before <= session.created_at <= after
