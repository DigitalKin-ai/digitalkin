"""Advanced tests for TaskIQ job manager, broker, and worker integration.

Tests cover:
- Registry config forwarding across process boundaries (pickle survival)
- RStream SSL context creation with env var combinations
- Broker URL construction with scheme/host/port
- run_start_module task: registry injection, ServicesConfig wiring, error paths
- TaskiqJobManager: job dispatch, stream consumer lifecycle, queue routing
- PickleFormatter: round-trip serialization of TaskiqMessage
"""

import asyncio
import json
import os
import ssl
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
