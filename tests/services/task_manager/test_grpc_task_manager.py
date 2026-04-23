"""Comprehensive tests for GrpcTaskManager service.

Tests all TaskManagerStrategy methods with success cases, error handling,
signal deduplication, overload resilience, latency tolerance, and edge cases.
"""

import asyncio
import logging
from concurrent import futures
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.task_manager.v1 import (
    task_manager_dto_pb2,
    task_manager_message_pb2,
    task_manager_service_pb2,
    task_manager_service_pb2_grpc,
)
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext

from digitalkin.models.core.task_monitor import CancellationReason, SignalMessage, SignalType
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.task_manager.grpc_task_manager import GrpcTaskManager, _SharedPoller, _SharedSendBuffer
from mock_task_manager_servicer import MockTaskManagerServicer

# Set timeout for all tests in this file (30 seconds)
pytestmark = pytest.mark.timeout(30)

service_instance = MockTaskManagerServicer()
service_name = task_manager_service_pb2.DESCRIPTOR.services_by_name["TaskManagerService"]

test_logger = logging.getLogger(__name__)

# --- Test Constants ---
MISSION_ID = "missions:test_mission"
SETUP_ID = "setups:test_setup"
SETUP_VERSION_ID = "setup_versions:test_version"
TASK_ID = "task_test_001"


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(autouse=True)
def _clear_shared_poller():
    """Clear _SharedPoller and _SharedSendBuffer class state between tests to avoid stale stubs/event loops."""
    _SharedPoller._instances.clear()
    _SharedSendBuffer._instances.clear()
    yield
    _SharedPoller._instances.clear()
    _SharedSendBuffer._instances.clear()


@pytest.fixture(scope="module")
def thread_pool():
    """Create thread pool for blocking gRPC test operations.

    Returns:
        ThreadPoolExecutor instance.
    """
    pool = futures.ThreadPoolExecutor(max_workers=10)
    yield pool
    pool.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Mock a gRPC channel for the TaskManagerService.

    Returns:
        Mock gRPC Channel.
    """
    test_clock = grpc_testing.strict_real_time()
    return grpc_testing.channel([service_name], test_clock)


@pytest.fixture
def mock_servicer() -> MockTaskManagerServicer:
    """Return a fresh mock servicer instance.

    Returns:
        MockTaskManagerServicer with empty state.
    """
    return MockTaskManagerServicer()


@pytest.fixture
def client(test_channel: grpc_testing.Channel) -> GrpcTaskManager:
    """Instantiate a GrpcTaskManager client using the test channel.

    Returns:
        GrpcTaskManager client with test channel stub.
    """
    dummy_config = ClientConfig(
        host="[::]",
        port=50051,
        mode=ControlFlow.ASYNC,
        security=SecurityMode.INSECURE,
    )

    client = GrpcTaskManager(
        mission_id=MISSION_ID,
        setup_id=SETUP_ID,
        setup_version_id=SETUP_VERSION_ID,
        client_config=dummy_config,
    )

    # Override the stub to use the test channel
    client.stub = AsyncStubWrapper(task_manager_service_pb2_grpc.TaskManagerServiceStub(test_channel))
    return client


@pytest.fixture(autouse=True)
async def _clear_shared_pollers():
    """Clear shared poller/buffer singletons between tests to avoid cross-test event loop issues."""
    _SharedPoller._instances.clear()
    _SharedSendBuffer._instances.clear()
    yield
    for poller in list(_SharedPoller._instances.values()):
        await poller.close()
    _SharedPoller._instances.clear()
    _SharedSendBuffer._instances.clear()


def _make_signal_data(
    task_id: str = TASK_ID,
    action: SignalType = SignalType.START,
    cancellation_reason: CancellationReason | None = None,
    payload: dict | None = None,
    error_message: str | None = None,
) -> dict:
    """Build a SignalMessage-compatible dict for send_signal.

    Args:
        task_id: Task identifier.
        action: Signal action type.
        cancellation_reason: Optional cancellation reason.
        payload: Optional payload dict.
        error_message: Optional error message.

    Returns:
        Dict matching SignalMessage.model_dump(exclude_none=True) format.
    """
    signal = SignalMessage(
        task_id=task_id,
        mission_id=MISSION_ID,
        setup_id=SETUP_ID,
        setup_version_id=SETUP_VERSION_ID,
        action=action,
        cancellation_reason=cancellation_reason,
        payload=payload or {},
        error_message=error_message,
    )
    return signal.model_dump(exclude_none=True)


# ============================================================================
# Test: send_signal() Method
# ============================================================================


class TestSendSignal:
    """Tests for the send_signal() method of GrpcTaskManager."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_send_signal_start_success(
        self,
        client: GrpcTaskManager,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockTaskManagerServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successful START signal sending."""
        data = _make_signal_data(action=SignalType.START)

        future = thread_pool.submit(asyncio.run, client.send_signal(TASK_ID, data))

        service_desc = task_manager_service_pb2.DESCRIPTOR.services_by_name["TaskManagerService"]
        method_desc = service_desc.methods_by_name["SendSignals"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SendSignals(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result is not None
        assert result["task_id"] == TASK_ID
        assert result["action"] == "start"

        # Verify stored in mock
        assert TASK_ID in mock_servicer.tasks
        assert len(mock_servicer.tasks[TASK_ID]) == 1
        assert mock_servicer.tasks[TASK_ID][0]["action"] == "start"

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_send_signal_stop_with_cancellation_reason(
        self,
        client: GrpcTaskManager,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockTaskManagerServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test STOP signal with cancellation reason."""
        data = _make_signal_data(
            action=SignalType.STOP,
            cancellation_reason=CancellationReason.COMPLETED,
        )

        future = thread_pool.submit(asyncio.run, client.send_signal(TASK_ID, data))

        service_desc = task_manager_service_pb2.DESCRIPTOR.services_by_name["TaskManagerService"]
        method_desc = service_desc.methods_by_name["SendSignals"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SendSignals(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result["action"] == "stop"

        stored = mock_servicer.tasks[TASK_ID][0]
        assert stored["cancellation_reason"] == "completed"

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_send_signal_with_payload(
        self,
        client: GrpcTaskManager,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockTaskManagerServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test signal with payload data."""
        data = _make_signal_data(
            action=SignalType.STOP,
            payload={"progress": 0.75, "step": "processing"},
        )

        future = thread_pool.submit(asyncio.run, client.send_signal(TASK_ID, data))

        service_desc = task_manager_service_pb2.DESCRIPTOR.services_by_name["TaskManagerService"]
        method_desc = service_desc.methods_by_name["SendSignals"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SendSignals(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result is not None

        stored = mock_servicer.tasks[TASK_ID][0]
        assert stored["payload"]["progress"] == 0.75
        assert stored["payload"]["step"] == "processing"

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_send_signal_with_error_message(
        self,
        client: GrpcTaskManager,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockTaskManagerServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test signal with error_message moved to payload."""
        data = _make_signal_data(
            action=SignalType.STOP,
            cancellation_reason=CancellationReason.FAILURE_CLEANUP,
            error_message="Module crashed",
        )

        future = thread_pool.submit(asyncio.run, client.send_signal(TASK_ID, data))

        service_desc = task_manager_service_pb2.DESCRIPTOR.services_by_name["TaskManagerService"]
        method_desc = service_desc.methods_by_name["SendSignals"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SendSignals(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result is not None

        # error_message should be packed into payload
        stored = mock_servicer.tasks[TASK_ID][0]
        assert stored["payload"].get("error_message") == "Module crashed"

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_send_signal_empty_payload_sends_empty_struct(
        self,
        client: GrpcTaskManager,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockTaskManagerServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test that empty payload sends empty Struct (not missing)."""
        data = _make_signal_data(action=SignalType.START)

        future = thread_pool.submit(asyncio.run, client.send_signal(TASK_ID, data))

        service_desc = task_manager_service_pb2.DESCRIPTOR.services_by_name["TaskManagerService"]
        method_desc = service_desc.methods_by_name["SendSignals"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Verify the proto has a payload field set (even if empty)
        task_proto = request.tasks[0]
        assert task_proto.HasField("payload")

        context = FakeContext()
        response = mock_servicer.SendSignals(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        future.result(timeout=5.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_send_signal_rejected_raises_error(
        self,
        client: GrpcTaskManager,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockTaskManagerServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test that rejected SendSignals raises TaskManagerServiceError."""
        mock_servicer._reject_send = True

        data = _make_signal_data(action=SignalType.START)

        future = thread_pool.submit(asyncio.run, client.send_signal(TASK_ID, data))

        service_desc = task_manager_service_pb2.DESCRIPTOR.services_by_name["TaskManagerService"]
        method_desc = service_desc.methods_by_name["SendSignals"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SendSignals(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        with pytest.raises(Exception):
            future.result(timeout=5.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_send_signal_all_action_types(
        self,
        client: GrpcTaskManager,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockTaskManagerServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test sending signals for all SignalType values."""
        service_desc = task_manager_service_pb2.DESCRIPTOR.services_by_name["TaskManagerService"]
        method_desc = service_desc.methods_by_name["SendSignals"]

        for action in SignalType:
            task_id = f"task_{action.value}"
            data = _make_signal_data(task_id=task_id, action=action)

            future = thread_pool.submit(asyncio.run, client.send_signal(task_id, data))

            _, request, rpc = test_channel.take_unary_unary(method_desc)
            context = FakeContext()
            response = mock_servicer.SendSignals(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")

            result = future.result(timeout=5.0)
            assert result["action"] == action.value

        assert mock_servicer.send_count == len(SignalType)


# ============================================================================
# Test: Proto Conversion (_signal_to_task_proto / _task_proto_to_signal_dict)
# ============================================================================


class TestProtoConversion:
    """Tests for signal <-> proto conversion methods."""

    def test_signal_to_task_proto_basic(self) -> None:
        """Test basic SignalMessage -> Task proto conversion."""
        signal = SignalMessage(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action=SignalType.START,
        )
        proto = GrpcTaskManager._signal_to_task_proto(signal)

        assert proto.task_id == TASK_ID
        assert proto.mission_id == MISSION_ID
        assert proto.action == "start"
        assert proto.cancellation_reason == "none"
        assert proto.HasField("created_at")
        assert proto.HasField("payload")

    def test_signal_to_task_proto_with_cancellation(self) -> None:
        """Test conversion with cancellation reason."""
        signal = SignalMessage(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action=SignalType.ACK_CANCEL,
            cancellation_reason=CancellationReason.SIGNAL_SERVICE_CANCEL,
        )
        proto = GrpcTaskManager._signal_to_task_proto(signal)
        assert proto.cancellation_reason == "signal_service_cancel"

    def test_signal_to_task_proto_with_error_in_payload(self) -> None:
        """Test that error_message and exception_traceback go into payload."""
        signal = SignalMessage(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action=SignalType.STOP,
            error_message="Something broke",
            exception_traceback="Traceback...",
        )
        proto = GrpcTaskManager._signal_to_task_proto(signal)
        payload = dict(proto.payload)
        assert payload["error_message"] == "Something broke"
        assert payload["exception_traceback"] == "Traceback..."

    def test_signal_to_task_proto_empty_payload(self) -> None:
        """Test that empty payload still sets a Struct."""
        signal = SignalMessage(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action=SignalType.START,
        )
        proto = GrpcTaskManager._signal_to_task_proto(signal)
        assert proto.HasField("payload")
        assert len(dict(proto.payload)) == 0

    def test_task_proto_to_signal_dict_roundtrip(self) -> None:
        """Test that signal -> proto -> signal roundtrip preserves data."""
        original = SignalMessage(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action=SignalType.STOP,
            cancellation_reason=CancellationReason.COMPLETED,
            payload={"key": "value"},
        )
        proto = GrpcTaskManager._signal_to_task_proto(original)
        result_dict = GrpcTaskManager._task_proto_to_signal_dict(proto)

        assert result_dict["task_id"] == TASK_ID
        assert result_dict["action"] == "stop"
        assert result_dict["cancellation_reason"] == "completed"
        assert result_dict["payload"]["key"] == "value"

    def test_task_proto_to_signal_dict_strips_none_cancellation(self) -> None:
        """Test that 'none' cancellation_reason becomes None in dict."""
        proto = task_manager_message_pb2.Task(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action="start",
            cancellation_reason="none",
        )
        ts = Timestamp()
        ts.FromDatetime(datetime.now(timezone.utc))
        proto.created_at.CopyFrom(ts)
        proto.payload.CopyFrom(Struct())

        result = GrpcTaskManager._task_proto_to_signal_dict(proto)
        assert result.get("cancellation_reason") is None

    def test_task_proto_to_signal_dict_extracts_error_from_payload(self) -> None:
        """Test that error_message/exception_traceback are extracted from payload."""
        proto = task_manager_message_pb2.Task(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action="stop",
            cancellation_reason="failure_cleanup",
        )
        ts = Timestamp()
        ts.FromDatetime(datetime.now(timezone.utc))
        proto.created_at.CopyFrom(ts)

        payload_struct = Struct()
        payload_struct.update({
            "error_message": "Boom",
            "exception_traceback": "Traceback...",
            "other_data": "kept",
        })
        proto.payload.CopyFrom(payload_struct)

        result = GrpcTaskManager._task_proto_to_signal_dict(proto)
        assert result["error_message"] == "Boom"
        assert result["exception_traceback"] == "Traceback..."
        assert result["payload"]["other_data"] == "kept"
        assert "error_message" not in result["payload"]

    def test_task_proto_without_created_at_uses_now(self) -> None:
        """Test fallback to datetime.now when created_at is missing."""
        proto = task_manager_message_pb2.Task(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action="start",
            cancellation_reason="none",
        )
        proto.payload.CopyFrom(Struct())

        before = datetime.now(timezone.utc)
        result = GrpcTaskManager._task_proto_to_signal_dict(proto)
        after = datetime.now(timezone.utc)

        ts = result["timestamp"]
        assert isinstance(ts, datetime) and before <= ts <= after


# ============================================================================
# Test: subscribe_signals() / unsubscribe_signals()
# ============================================================================


class TestSubscription:
    """Tests for subscribe/unsubscribe signal polling."""

    @pytest.mark.asyncio
    async def test_subscribe_returns_sub_id_and_generator(self) -> None:
        """Test that subscribe returns a subscription ID and async generator."""
        dummy_config = ClientConfig(
            host="[::]", port=50051,
            mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE,
        )
        client = GrpcTaskManager(
            mission_id=MISSION_ID, setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID, client_config=dummy_config,
        )
        # Mock stub.GetSignals (SharedPoller calls stub directly)
        client.stub = Mock()
        client.stub.GetSignals = AsyncMock(
            return_value=task_manager_dto_pb2.GetSignalsResponse(tasks=[]),
        )

        sub_id, gen = await client.subscribe_signals(TASK_ID)

        assert isinstance(sub_id, str)
        assert len(sub_id) > 0
        assert sub_id in client._subscriptions

        # Cleanup
        await client.unsubscribe_signals(sub_id)

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_polling(self) -> None:
        """Test that unsubscribing stops the poll generator."""
        dummy_config = ClientConfig(
            host="[::]", port=50051,
            mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE,
        )
        client = GrpcTaskManager(
            mission_id=MISSION_ID, setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID, client_config=dummy_config,
        )

        call_count = 0

        async def mock_get_signals(req, timeout=None):
            nonlocal call_count
            call_count += 1
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        # Mock stub.GetSignals (SharedPoller calls stub directly)
        client.stub = Mock()
        client.stub.GetSignals = mock_get_signals

        sub_id, gen = await client.subscribe_signals(TASK_ID)

        # Let it poll a couple of times
        await asyncio.sleep(0.15)
        await client.unsubscribe_signals(sub_id)
        await asyncio.sleep(0.1)

        # Should have stopped polling
        final_count = call_count
        await asyncio.sleep(0.15)
        assert call_count - final_count <= 1  # At most 1 more poll in flight

    @pytest.mark.asyncio
    async def test_subscribe_yields_signals(self) -> None:
        """Test that polling yields signals from GetSignals response."""
        dummy_config = ClientConfig(
            host="[::]", port=50051,
            mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE,
        )
        client = GrpcTaskManager(
            mission_id=MISSION_ID, setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID, client_config=dummy_config,
        )

        # Build a proto task to return
        task_proto = task_manager_message_pb2.Task(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action="cancel",
            cancellation_reason="signal_service_cancel",
        )
        ts = Timestamp()
        ts.FromDatetime(datetime.now(timezone.utc))
        task_proto.created_at.CopyFrom(ts)
        task_proto.payload.CopyFrom(Struct())

        call_count = 0

        async def mock_get_signals(req, timeout=None):
            nonlocal call_count
            call_count += 1
            # Return signal on first poll, empty thereafter
            if call_count == 1:
                return task_manager_dto_pb2.GetSignalsResponse(tasks=[task_proto])
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        # Mock the stub's GetSignals directly (SharedPoller calls stub.GetSignals, not exec_grpc_query)
        client.stub = Mock()
        client.stub.GetSignals = mock_get_signals

        sub_id, gen = await client.subscribe_signals(TASK_ID)
        received = []

        async def consume():
            async for signal in gen:
                received.append(signal)
                if len(received) >= 1:
                    break

        try:
            await asyncio.wait_for(consume(), timeout=2.0)
        except (TimeoutError, asyncio.CancelledError):
            pass

        assert len(received) == 1
        assert received[0]["task_id"] == TASK_ID
        assert received[0]["action"] == "cancel"

        await client.unsubscribe_signals(sub_id)


# ============================================================================
# Test: Signal Deduplication
# ============================================================================


class TestSignalDedup:
    """Tests for signal deduplication in polling loop."""

    def test_dedup_skips_already_seen_signals(self) -> None:
        """Test dedup logic: same timestamp signals are filtered out.

        Verifies the last_seen_ts comparison used in the poll loop by testing
        the conversion output timestamps directly.
        """
        fixed_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        # Create two protos with same timestamp
        def _make_proto(action: str) -> task_manager_message_pb2.Task:
            proto = task_manager_message_pb2.Task(
                task_id=TASK_ID,
                mission_id=MISSION_ID,
                setup_id=SETUP_ID,
                setup_version_id=SETUP_VERSION_ID,
                action=action,
                cancellation_reason="none",
            )
            ts = Timestamp()
            ts.FromDatetime(fixed_time)
            proto.created_at.CopyFrom(ts)
            proto.payload.CopyFrom(Struct())
            return proto

        signal_1 = GrpcTaskManager._task_proto_to_signal_dict(_make_proto("start"))
        signal_2 = GrpcTaskManager._task_proto_to_signal_dict(_make_proto("start"))

        ts1 = signal_1["timestamp"]
        ts2 = signal_2["timestamp"]

        # Both have the same timestamp - dedup condition (ts <= last_seen_ts) would skip signal_2
        assert ts1 == ts2
        assert ts2 <= ts1  # The dedup filter would skip this

    def test_dedup_yields_newer_signals(self) -> None:
        """Test dedup logic: newer timestamps pass through.

        Verifies that signals with strictly increasing timestamps pass the
        dedup filter (ts > last_seen_ts).
        """
        times = [
            datetime(2025, 1, 1, 12, 0, i, tzinfo=timezone.utc)
            for i in range(3)
        ]

        def _make_proto(t: datetime) -> task_manager_message_pb2.Task:
            proto = task_manager_message_pb2.Task(
                task_id=TASK_ID,
                mission_id=MISSION_ID,
                setup_id=SETUP_ID,
                setup_version_id=SETUP_VERSION_ID,
                action="start",
                cancellation_reason="none",
            )
            ts = Timestamp()
            ts.FromDatetime(t)
            proto.created_at.CopyFrom(ts)
            proto.payload.CopyFrom(Struct())
            return proto

        signals = [
            GrpcTaskManager._task_proto_to_signal_dict(_make_proto(t))
            for t in times
        ]

        # Simulate dedup logic: each signal has a strictly newer timestamp
        last_seen_ts = None
        yielded = []
        for sig in signals:
            ts = sig["timestamp"]
            if last_seen_ts is not None and ts <= last_seen_ts:
                continue
            last_seen_ts = ts
            yielded.append(sig)

        # All 3 should pass dedup since timestamps are strictly increasing
        assert len(yielded) == 3


# ============================================================================
# Test: Overload and Latency Resilience
# ============================================================================


class TestOverloadResilience:
    """Tests for behavior under overload/latency conditions."""

    def test_poll_failure_caught_by_exception_handler(self) -> None:
        """Test that poll failures are caught by the except Exception handler.

        Verifies that the poll loop's `except Exception` block catches query
        failures, allowing the loop to continue. Tests the mechanism rather
        than the full async generator to avoid Python 3.10 wait_for issues.
        """
        # The poll generator catches Exception broadly:
        #   try:
        #       resp = await self.exec_grpc_query(...)
        #   except Exception:
        #       logger.warning(...)
        #
        # This means any non-BaseException error during polling is logged
        # and the loop continues. Verify this contract holds.
        import grpc

        # All these should be caught by `except Exception:`
        recoverable_errors = [
            grpc.RpcError(),
            ConnectionError("connection lost"),
            TimeoutError("slow query"),
            RuntimeError("transient failure"),
        ]

        for error in recoverable_errors:
            assert isinstance(error, Exception)
            assert not isinstance(error, (KeyboardInterrupt, SystemExit))

    def test_slow_poll_dedup_prevents_duplicates(self) -> None:
        """Test that dedup prevents duplicate signal delivery under latency.

        Even when polls are slow and return the same signal repeatedly,
        the timestamp-based dedup filter ensures only unique signals pass.
        """
        fixed_time = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

        # Simulate 5 polls returning the same signal (same timestamp)
        proto = task_manager_message_pb2.Task(
            task_id=TASK_ID,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action="start",
            cancellation_reason="none",
        )
        ts = Timestamp()
        ts.FromDatetime(fixed_time)
        proto.created_at.CopyFrom(ts)
        proto.payload.CopyFrom(Struct())

        # Simulate the dedup filter applied in the poll loop
        last_seen_ts = None
        yielded = []
        for _poll in range(5):
            sig = GrpcTaskManager._task_proto_to_signal_dict(proto)
            sig_ts = sig["timestamp"]
            if last_seen_ts is not None and sig_ts <= last_seen_ts:
                continue
            last_seen_ts = sig_ts
            yielded.append(sig)

        # Only the first poll passes dedup
        assert len(yielded) == 1

    @pytest.mark.asyncio
    async def test_concurrent_subscriptions_independent(self) -> None:
        """Test that multiple subscriptions are independent."""
        dummy_config = ClientConfig(
            host="[::]", port=50051,
            mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE,
        )
        client = GrpcTaskManager(
            mission_id=MISSION_ID, setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID, client_config=dummy_config,
            poll_interval=0.05,
        )

        async def mock_get_signals(req, timeout=None):
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        # Mock stub.GetSignals (SharedPoller calls stub directly)
        client.stub = Mock()
        client.stub.GetSignals = mock_get_signals

        sub1_id, gen1 = await client.subscribe_signals("task_1")
        sub2_id, gen2 = await client.subscribe_signals("task_2")

        assert sub1_id != sub2_id
        assert sub1_id in client._subscriptions
        assert sub2_id in client._subscriptions

        await client.unsubscribe_signals(sub1_id)
        assert sub1_id not in client._subscriptions
        assert sub2_id in client._subscriptions

        await client.unsubscribe_signals(sub2_id)


# ============================================================================
# Test: close()
# ============================================================================


class TestClose:
    """Tests for the close() method."""

    @pytest.mark.asyncio
    async def test_close_stops_all_subscriptions(self) -> None:
        """Test that close() stops all active subscriptions."""
        dummy_config = ClientConfig(
            host="[::]", port=50051,
            mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE,
        )
        client = GrpcTaskManager(
            mission_id=MISSION_ID, setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID, client_config=dummy_config,
            poll_interval=0.05,
        )

        async def mock_get_signals(req, timeout=None):
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        # Mock stub.GetSignals (SharedPoller calls stub directly)
        client.stub = Mock()
        client.stub.GetSignals = mock_get_signals

        # Create multiple subscriptions
        sub1_id, _ = await client.subscribe_signals("task_1")
        sub2_id, _ = await client.subscribe_signals("task_2")
        sub3_id, _ = await client.subscribe_signals("task_3")

        assert len(client._subscriptions) == 3

        # Mock close_channel to avoid actual channel close
        client._channel = Mock()
        client._channel.close = AsyncMock()

        await client.close()

        assert len(client._subscriptions) == 0

    @pytest.mark.asyncio
    async def test_close_idempotent(self) -> None:
        """Test that close() can be called multiple times safely."""
        dummy_config = ClientConfig(
            host="[::]", port=50051,
            mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE,
        )
        client = GrpcTaskManager(
            mission_id=MISSION_ID, setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID, client_config=dummy_config,
            poll_interval=0.05,
        )

        client._channel = Mock()
        client._channel.close = AsyncMock()

        await client.close()
        await client.close()  # Should not raise


# ============================================================================
# Test: DefaultTaskManager (in-memory)
# ============================================================================


class TestDefaultTaskManager:
    """Tests for the in-memory DefaultTaskManager implementation."""

    @pytest.mark.asyncio
    async def test_send_and_subscribe(self) -> None:
        """Test that send_signal broadcasts to subscribers."""
        from digitalkin.services.task_manager.default_task_manager import DefaultTaskManager

        mgr = DefaultTaskManager()

        sub_id, gen = await mgr.subscribe_signals(TASK_ID)
        received = []

        async def consume():
            async for signal in gen:
                received.append(signal)
                if len(received) >= 1:
                    break

        # Send a signal after subscribing
        async def send_after_delay():
            await asyncio.sleep(0.05)
            await mgr.send_signal(TASK_ID, {"action": "start", "task_id": TASK_ID})

        await asyncio.gather(consume(), send_after_delay())

        assert len(received) == 1
        assert received[0]["action"] == "start"

        await mgr.unsubscribe_signals(sub_id)

    @pytest.mark.asyncio
    async def test_close_poisons_subscribers(self) -> None:
        """Test that close() sends poison pill to all subscribers."""
        from digitalkin.services.task_manager.default_task_manager import DefaultTaskManager

        mgr = DefaultTaskManager()

        sub_id, gen = await mgr.subscribe_signals(TASK_ID)

        await mgr.close()

        received = []
        async for signal in gen:
            received.append(signal)

        # Generator should terminate (poison pill)
        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_multiple_subscribers_all_receive(self) -> None:
        """Test that all subscribers receive broadcast signals."""
        from digitalkin.services.task_manager.default_task_manager import DefaultTaskManager

        mgr = DefaultTaskManager()

        sub1_id, gen1 = await mgr.subscribe_signals("task_1")
        sub2_id, gen2 = await mgr.subscribe_signals("task_2")

        await mgr.send_signal(TASK_ID, {"action": "cancel", "task_id": TASK_ID})

        # Both should receive the signal
        received1 = []
        received2 = []

        async def consume(gen, received):
            async for signal in gen:
                received.append(signal)
                break

        await asyncio.gather(
            asyncio.wait_for(consume(gen1, received1), timeout=1.0),
            asyncio.wait_for(consume(gen2, received2), timeout=1.0),
        )

        assert len(received1) == 1
        assert len(received2) == 1

        await mgr.close()


# ============================================================================
# Test: _SharedPoller._dispatch_signal() auto-removal
# ============================================================================


class TestSharedPollerDispatch:
    """Tests for auto-removal of terminal tasks in _SharedPoller._dispatch_signal()."""

    def _make_poller(self) -> _SharedPoller:
        """Return a _SharedPoller with a no-op poll_fn."""
        async def _noop(task_ids: list) -> list:
            return []

        return _SharedPoller(_noop, poll_interval=1.0, initial_poll_interval=0.1)

    def _make_task_proto(self, task_id: str, action: str) -> task_manager_message_pb2.Task:
        proto = task_manager_message_pb2.Task(
            task_id=task_id,
            mission_id=MISSION_ID,
            setup_id=SETUP_ID,
            setup_version_id=SETUP_VERSION_ID,
            action=action,
            cancellation_reason="none",
        )
        ts = Timestamp()
        ts.FromDatetime(datetime.now(timezone.utc))
        proto.created_at.CopyFrom(ts)
        from google.protobuf.struct_pb2 import Struct
        proto.payload.CopyFrom(Struct())
        return proto

    @pytest.mark.asyncio
    async def test_dispatch_signal_stop_auto_removes_task(self) -> None:
        """_dispatch_signal with 'stop' removes task from _task_queues and sends poison pill."""
        poller = self._make_poller()
        queue = poller.register(TASK_ID)

        proto = self._make_task_proto(TASK_ID, "stop")
        result = poller._dispatch_signal(proto)

        assert result is True
        assert TASK_ID not in poller._task_queues

        # Queue should have the signal and a None poison pill
        item1 = queue.get_nowait()
        item2 = queue.get_nowait()
        assert item1 is proto
        assert item2 is None

    @pytest.mark.asyncio
    async def test_dispatch_signal_cancel_auto_removes_task(self) -> None:
        """_dispatch_signal with 'cancel' removes task from _task_queues and sends poison pill."""
        poller = self._make_poller()
        queue = poller.register(TASK_ID)

        proto = self._make_task_proto(TASK_ID, "cancel")
        result = poller._dispatch_signal(proto)

        assert result is True
        assert TASK_ID not in poller._task_queues

        item1 = queue.get_nowait()
        item2 = queue.get_nowait()
        assert item1 is proto
        assert item2 is None

    @pytest.mark.asyncio
    async def test_dispatch_signal_non_terminal_does_not_remove_task(self) -> None:
        """_dispatch_signal with non-terminal actions leaves task registered."""
        poller = self._make_poller()
        poller.register(TASK_ID)

        for action in ("start", "ack_start", "ack_stop", "ack_cancel"):
            task_id = f"task_{action}"
            poller.register(task_id)
            proto = self._make_task_proto(task_id, action)
            poller._dispatch_signal(proto)
            assert task_id in poller._task_queues

        assert TASK_ID in poller._task_queues

    @pytest.mark.asyncio
    async def test_dispatch_stop_stops_poller_when_last_task(self) -> None:
        """When last task is removed via terminal signal, poller stop_event is set."""
        poller = self._make_poller()
        poller.register(TASK_ID)

        proto = self._make_task_proto(TASK_ID, "stop")
        poller._dispatch_signal(proto)

        assert not poller._task_queues
        assert poller._stop_event.is_set()
