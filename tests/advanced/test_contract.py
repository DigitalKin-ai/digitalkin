"""Contract tests for gRPC proto definitions.

Verify that generated proto stubs match expected message shapes,
field names, enum values, and service method signatures. Catches
proto/code drift early without running a server.
"""

from __future__ import annotations

import pytest

try:
    from agentic_mesh_protocol.gateway.v1 import gateway_pb2 as _gw_pb2

    _HAS_GATEWAY_PROTO = True
except ImportError:
    _HAS_GATEWAY_PROTO = False

pytestmark = [pytest.mark.contract, pytest.mark.timeout(5)]

SKIP_NO_GATEWAY = pytest.mark.skipif(not _HAS_GATEWAY_PROTO, reason="Gateway proto not installed (needs local editable)")


# ===========================================================================
# GatewayService contract
# ===========================================================================


@SKIP_NO_GATEWAY
class TestGatewayServiceContract:
    """Verify GatewayService proto shape."""

    def test_service_has_four_rpcs(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_service_pb2_grpc

        servicer = gateway_service_pb2_grpc.GatewayServiceServicer
        methods = [m for m in dir(servicer) if not m.startswith("_")]
        assert "StartStream" in methods
        assert "ProduceStream" in methods
        assert "ConsumeStream" in methods
        assert "SendSignal" in methods

    def test_start_stream_request_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.StartStreamRequest()
        fields = [f.name for f in msg.DESCRIPTOR.fields]
        assert "task_id" in fields
        assert "input" in fields
        assert "setup_id" in fields
        assert "mission_id" in fields

    def test_start_stream_response_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.StartStreamResponse()
        fields = [f.name for f in msg.DESCRIPTOR.fields]
        assert "task_id" in fields
        assert "accepted" in fields

    def test_consume_stream_request_oneof(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.ConsumeStreamRequest()
        oneof = msg.DESCRIPTOR.oneofs
        assert len(oneof) == 1
        field_names = [f.name for f in oneof[0].fields]
        assert "init" in field_names
        assert "data" in field_names

    def test_produce_stream_request_oneof(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.ProduceStreamRequest()
        oneof = msg.DESCRIPTOR.oneofs
        assert len(oneof) == 1
        field_names = [f.name for f in oneof[0].fields]
        assert "init" in field_names
        assert "output" in field_names

    def test_gateway_response_oneof(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.GatewayResponse()
        oneof = msg.DESCRIPTOR.oneofs
        assert len(oneof) == 1
        assert oneof[0].name == "payload"
        field_names = [f.name for f in oneof[0].fields]
        assert "output" in field_names
        assert "status" in field_names
        assert "error" in field_names
        assert "heartbeat" in field_names

    def test_stream_state_enum_values(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        names = [v.name for v in gateway_pb2.StreamState.DESCRIPTOR.values]
        assert "STREAM_STATE_UNSPECIFIED" in names
        assert "STREAM_STATE_STARTING" in names
        assert "STREAM_STATE_RUNNING" in names
        assert "STREAM_STATE_COMPLETED" in names
        assert "STREAM_STATE_FAILED" in names
        assert "STREAM_STATE_CANCELLED" in names

    def test_signal_action_enum_values(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        names = [v.name for v in gateway_pb2.SignalAction.DESCRIPTOR.values]
        assert "SIGNAL_ACTION_UNSPECIFIED" in names
        assert "SIGNAL_ACTION_CANCEL" in names
        assert "SIGNAL_ACTION_PAUSE" in names

    def test_client_signal_request_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.ClientSignalRequest()
        fields = [f.name for f in msg.DESCRIPTOR.fields]
        assert "task_id" in fields
        assert "action" in fields

    def test_checkpoint_fields(self) -> None:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        msg = gateway_pb2.Checkpoint()
        fields = [f.name for f in msg.DESCRIPTOR.fields]
        assert "task_id" in fields
        assert "mission_id" in fields
        assert "status" in fields
        assert "last_seq" in fields
        assert "state" in fields
        assert "created_at" in fields


# ===========================================================================
# ModuleService contract (unchanged, but verify no regression)
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
# Proto serialization round-trip
# ===========================================================================


@SKIP_NO_GATEWAY
class TestProtoSerialization:
    """Verify proto messages serialize and deserialize correctly."""

    def test_gateway_response_output_roundtrip(self) -> None:
        from google.protobuf import json_format, struct_pb2

        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        data = struct_pb2.Struct()
        data.update({"root": {"protocol": "message", "content": "hello"}})

        resp = gateway_pb2.GatewayResponse(
            output=gateway_pb2.StreamOutput(
                task_id="t1",
                job_id="j1",
                data=data,
                seq=42,
            ),
        )

        serialized = resp.SerializeToString()
        deserialized = gateway_pb2.GatewayResponse()
        deserialized.ParseFromString(serialized)

        assert deserialized.output.task_id == "t1"
        assert deserialized.output.seq == 42
        output_dict = json_format.MessageToDict(deserialized.output.data)
        assert output_dict["root"]["content"] == "hello"

    def test_checkpoint_roundtrip(self) -> None:
        from google.protobuf import struct_pb2, timestamp_pb2

        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        state = struct_pb2.Struct()
        state.update({"model": "active", "tokens": 500})

        ts = timestamp_pb2.Timestamp()
        ts.GetCurrentTime()

        ckpt = gateway_pb2.Checkpoint(
            task_id="t_ckpt",
            mission_id="missions:m1",
            setup_id="setups:s1",
            setup_version_id="setup_versions:sv1",
            status="running",
            last_seq=100,
            state=state,
            created_at=ts,
        )

        serialized = ckpt.SerializeToString()
        restored = gateway_pb2.Checkpoint()
        restored.ParseFromString(serialized)

        assert restored.task_id == "t_ckpt"
        assert restored.last_seq == 100
        assert restored.status == "running"
