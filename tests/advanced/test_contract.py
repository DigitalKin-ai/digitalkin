"""Contract tests for gRPC proto definitions.

Verify that generated proto stubs match expected message shapes,
field names, enum values, and service method signatures. Catches
proto/code drift early without running a server.

Gateway lifecycle is in-band (sentinel Structs in StreamOutput.data
keyed under data.root.protocol). The gateway exposes only the external
consumer surface: AssociateTask, StartStream, Stream, SendSignal.
"""

from __future__ import annotations

import pytest

try:
    from agentic_mesh_protocol.gateway.v1 import gateway_pb2 as _gw_pb2  # noqa: F401

    _HAS_GATEWAY_PROTO = True
except ImportError:
    _HAS_GATEWAY_PROTO = False

pytestmark = [pytest.mark.contract, pytest.mark.timeout(5)]

SKIP_NO_GATEWAY = pytest.mark.skipif(
    not _HAS_GATEWAY_PROTO, reason="Gateway proto not installed (needs local editable)",
)


# ===========================================================================
# GatewayService contract — 4 RPCs: AssociateTask, StartStream, Stream, SendSignal
# ===========================================================================


@SKIP_NO_GATEWAY
class TestGatewayServiceContract:
    """Verify GatewayService proto shape."""

    def test_service_has_four_rpcs(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_service_pb2_grpc

        servicer = gateway_service_pb2_grpc.GatewayServiceServicer
        methods = {m for m in dir(servicer) if not m.startswith("_")}
        assert methods == {"AssociateTask", "StartStream", "Stream", "SendSignal"}

    def test_deleted_rpcs_absent(self) -> None:
        """ProduceStream and ConsumeStream must be gone."""
        from agentic_mesh_protocol.gateway.v1 import gateway_service_pb2_grpc

        servicer = gateway_service_pb2_grpc.GatewayServiceServicer
        methods = dir(servicer)
        assert "ProduceStream" not in methods
        assert "ConsumeStream" not in methods

    def test_start_stream_request_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.StartStreamRequest()
        fields = {f.name for f in msg.DESCRIPTOR.fields}
        assert fields == {"task_id", "setup_id", "mission_id"}

    def test_start_stream_response_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.StartStreamResponse()
        fields = {f.name for f in msg.DESCRIPTOR.fields}
        assert fields == {"accepted", "task_id"}

    def test_associate_task_request_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.AssociateTaskRequest()
        fields = {f.name for f in msg.DESCRIPTOR.fields}
        assert fields == {"parent_task_id"}

    def test_associate_task_response_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.AssociateTaskResponse()
        fields = {f.name for f in msg.DESCRIPTOR.fields}
        assert fields == {"task_id", "parent_task_id"}

    def test_stream_request_is_flat_no_oneof(self) -> None:
        """StreamRequest is flat: task_id, from_seq, data — no oneof."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.StreamClient()
        assert len(msg.DESCRIPTOR.oneofs) == 0
        fields = {f.name for f in msg.DESCRIPTOR.fields}
        assert fields == {"task_id", "from_seq", "data"}

    def test_stream_server_fields(self) -> None:
        """StreamServer carries seq + task_id + data."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.StreamServer()
        fields = {f.name for f in msg.DESCRIPTOR.fields}
        assert fields == {"seq", "task_id", "data"}

    def test_deleted_messages_absent(self) -> None:
        """Envelope, lifecycle status, errors, heartbeat, checkpoint, oneof shells — all gone."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        for name in (
            "GatewayResponse",
            "StreamStatus",
            "StreamError",
            "ServerHeartbeat",
            "Checkpoint",
            "ProduceStreamRequest",
            "ProduceStreamInit",
            "ProduceStreamResponse",
            "ProduceStreamData",
            "ConsumeStreamRequest",
            "ConsumeStreamInit",
            "ConsumeStreamData",
        ):
            assert not hasattr(gateway_pb2, name), f"{name} should be deleted"

    def test_stream_state_enum_absent(self) -> None:
        """StreamState enum was orphaned with StreamStatus and removed."""
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        assert not hasattr(gateway_pb2, "StreamState")

    def test_signal_action_enum_values(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        names = {v.name for v in gateway_pb2.SignalAction.DESCRIPTOR.values}
        # Cache invalidation set + cancel; explicit unprefixed names per design.
        assert names >= {
            "UNSPECIFIED",
            "CANCEL",
            "INVALIDATE_ALL",
            "INVALIDATE_CHANNELS",
            "INVALIDATE_MODELS",
            "INVALIDATE_SETUP",
            "INVALIDATE_TOOLS",
            "INVALIDATE_SHARED",
        }

    def test_client_signal_request_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.ClientSignalRequest()
        fields = {f.name for f in msg.DESCRIPTOR.fields}
        assert fields == {"task_id", "action"}


# ===========================================================================
# Sentinel protocol contract — in-band lifecycle via data.root.protocol
# ===========================================================================


class TestSentinelProtocolContract:
    """Verify the SDK utility models carry the renamed sentinels."""

    def test_end_of_stream_renamed_to_stream_end(self) -> None:
        """EndOfStreamOutput.protocol must be 'stream.end' (not 'end_of_stream')."""
        from digitalkin.models.module.utility import EndOfStreamOutput

        assert EndOfStreamOutput().protocol == "stream.end"

    def test_sentinel_namespace_is_stream_dot(self) -> None:
        """All gateway-emitted control sentinels live under the 'stream.' namespace."""
        from digitalkin.models.module.utility import EndOfStreamOutput

        assert EndOfStreamOutput().protocol.startswith("stream.")


# ===========================================================================
# ModuleService contract (unchanged, verify no regression)
# ===========================================================================


class TestModuleServiceContract:
    """Verify ModuleService proto shape is unchanged."""

    def test_start_module_is_server_streaming(self) -> None:
        from agentic_mesh_protocol.module.v1 import module_service_pb2_grpc

        servicer = module_service_pb2_grpc.ModuleServiceServicer
        assert "StartModule" in dir(servicer)

    def test_no_stream_module_rpc(self) -> None:
        """StreamModule BiDi was removed — verify it stays removed."""
        from agentic_mesh_protocol.module.v1 import module_service_pb2_grpc

        servicer = module_service_pb2_grpc.ModuleServiceServicer
        assert "StreamModule" not in dir(servicer)

    def test_start_module_request_fields(self) -> None:
        from agentic_mesh_protocol.module.v1 import lifecycle_pb2

        msg = lifecycle_pb2.StartModuleRequest()
        fields = [f.name for f in msg.DESCRIPTOR.fields]
        assert "input" in fields
        assert "setup_id" in fields
        assert "mission_id" in fields

    def test_start_module_response_fields(self) -> None:
        from agentic_mesh_protocol.module.v1 import lifecycle_pb2

        msg = lifecycle_pb2.StartModuleResponse()
        fields = [f.name for f in msg.DESCRIPTOR.fields]
        assert "success" in fields
        assert "output" in fields
        assert "job_id" in fields


# ===========================================================================
# Proto serialization round-trip — flat StreamOutput
# ===========================================================================


@SKIP_NO_GATEWAY
class TestProtoSerialization:
    """Verify proto messages serialize and deserialize correctly."""

    def test_stream_output_roundtrip(self) -> None:
        from google.protobuf import json_format, struct_pb2

        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        data = struct_pb2.Struct()
        data.update({"root": {"protocol": "message", "content": "hello"}})

        out = gateway_pb2.StreamServer(seq=42, data=data)
        serialized = out.SerializeToString()
        restored = gateway_pb2.StreamServer()
        restored.ParseFromString(serialized)

        assert restored.seq == 42
        d = json_format.MessageToDict(restored.data)
        assert d["root"]["content"] == "hello"
        assert d["root"]["protocol"] == "message"

    def test_stream_request_init_roundtrip(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        req = gateway_pb2.StreamClient(task_id="t1", from_seq=10)
        serialized = req.SerializeToString()
        restored = gateway_pb2.StreamClient()
        restored.ParseFromString(serialized)

        assert restored.task_id == "t1"
        assert restored.from_seq == 10
        # Empty data Struct: no fields
        assert len(restored.data.fields) == 0

    def test_stream_request_data_roundtrip(self) -> None:
        from google.protobuf import struct_pb2

        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        data = struct_pb2.Struct()
        data.update({"upstream": "input"})
        req = gateway_pb2.StreamClient(data=data)
        serialized = req.SerializeToString()
        restored = gateway_pb2.StreamClient()
        restored.ParseFromString(serialized)

        assert restored.data.fields["upstream"].string_value == "input"
