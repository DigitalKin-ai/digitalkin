"""Contract tests for gRPC proto definitions.

Verify that generated proto stubs match expected message shapes,
field names, enum values, and service method signatures. Catches
proto/code drift early without running a server.

Gateway lifecycle is in-band (sentinel Structs in StreamResponse.data
keyed under data.root.protocol). The gateway exposes only the external
consumer surface: AssociateTask, StartStream, Stream, SendSignal.
"""

from __future__ import annotations

import importlib

import pytest
from agentic_mesh_protocol.gateway.v1 import (
    gateway_dto_pb2,
    gateway_enums_pb2,
    gateway_messages_pb2,
    gateway_service_pb2,
    gateway_service_pb2_grpc,
)
from agentic_mesh_protocol.module.v1 import module_dto_pb2, module_service_pb2_grpc
from google.protobuf import json_format, struct_pb2

pytestmark = [pytest.mark.contract, pytest.mark.timeout(5)]


# ===========================================================================
# GatewayService contract — 4 RPCs: AssociateTask, StartStream, Stream, SendSignal
# ===========================================================================


class TestGatewayServiceContract:
    """Verify GatewayService proto shape."""

    def test_service_has_four_rpcs(self) -> None:
        servicer = gateway_service_pb2_grpc.GatewayServiceServicer
        methods = {m for m in dir(servicer) if not m.startswith("_")}
        assert methods == {"AssociateTask", "StartStream", "Stream", "SendSignal"}

    def test_deleted_rpcs_absent(self) -> None:
        """ProduceStream and ConsumeStream must be gone."""
        methods = dir(gateway_service_pb2_grpc.GatewayServiceServicer)
        assert "ProduceStream" not in methods
        assert "ConsumeStream" not in methods

    def test_stream_frames_follow_their_direction(self) -> None:
        """The client sends StreamRequest; the gateway answers StreamResponse."""
        stream = gateway_service_pb2.DESCRIPTOR.services_by_name["GatewayService"].methods_by_name["Stream"]
        assert stream.input_type.name == "StreamRequest"
        assert stream.output_type.name == "StreamResponse"
        assert stream.client_streaming
        assert stream.server_streaming

    def test_send_signal_types(self) -> None:
        send = gateway_service_pb2.DESCRIPTOR.services_by_name["GatewayService"].methods_by_name["SendSignal"]
        assert send.input_type.name == "SendSignalRequest"
        assert send.output_type.name == "SendSignalResponse"

    def test_start_stream_request_fields(self) -> None:
        fields = {f.name for f in gateway_dto_pb2.StartStreamRequest.DESCRIPTOR.fields}
        assert fields == {"task_id", "setup_id", "mission_id"}

    def test_start_stream_response_fields(self) -> None:
        fields = {f.name for f in gateway_dto_pb2.StartStreamResponse.DESCRIPTOR.fields}
        assert fields == {"accepted", "task_id"}

    def test_associate_task_request_fields(self) -> None:
        fields = {f.name for f in gateway_dto_pb2.AssociateTaskRequest.DESCRIPTOR.fields}
        assert fields == {"parent_task_id"}

    def test_associate_task_response_fields(self) -> None:
        fields = {f.name for f in gateway_dto_pb2.AssociateTaskResponse.DESCRIPTOR.fields}
        assert fields == {"task_id", "parent_task_id"}

    def test_stream_request_is_flat_no_oneof(self) -> None:
        """StreamRequest is flat: from_seq=1, task_id=2, data=3 — no oneof."""
        descriptor = gateway_messages_pb2.StreamRequest.DESCRIPTOR
        assert len(descriptor.oneofs) == 0
        assert {f.name: f.number for f in descriptor.fields} == {"from_seq": 1, "task_id": 2, "data": 3}

    def test_stream_response_fields(self) -> None:
        """StreamResponse carries seq=1, task_id=2, data=3."""
        descriptor = gateway_messages_pb2.StreamResponse.DESCRIPTOR
        assert {f.name: f.number for f in descriptor.fields} == {"seq": 1, "task_id": 2, "data": 3}

    def test_legacy_gateway_module_absent(self) -> None:
        """``gateway_pb2`` (StreamServer / StreamClient / ClientSignalRequest) is gone."""
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module("agentic_mesh_protocol.gateway.v1.gateway_pb2")

    def test_deleted_messages_absent(self) -> None:
        """Envelope, lifecycle status, heartbeat, legacy frame and signal shells — all gone."""
        names = set(gateway_messages_pb2.DESCRIPTOR.message_types_by_name) | set(
            gateway_dto_pb2.DESCRIPTOR.message_types_by_name
        )
        for name in (
            "GatewayResponse",
            "StreamStatus",
            "StreamError",
            "ServerHeartbeat",
            "Checkpoint",
            "ProduceStreamRequest",
            "ConsumeStreamRequest",
            "StreamServer",
            "StreamClient",
            "ClientSignalRequest",
            "ClientSignalResponse",
        ):
            assert name not in names, f"{name} should be deleted"

    def test_signal_action_enum_absent(self) -> None:
        assert "SignalAction" not in gateway_enums_pb2.DESCRIPTOR.enum_types_by_name

    def test_cache_scope_enum_values(self) -> None:
        names = {v.name for v in gateway_enums_pb2.CacheScope.DESCRIPTOR.values}
        assert names == {"CACHE_SCOPE_UNSPECIFIED", "ALL", "CHANNELS", "MODELS", "SETUP", "TOOLS", "SHARED"}

    def test_send_signal_request_is_a_oneof(self) -> None:
        descriptor = gateway_dto_pb2.SendSignalRequest.DESCRIPTOR
        assert [f.name for f in descriptor.oneofs_by_name["signal"].fields] == ["cancel", "invalidate"]

    def test_signal_payload_fields(self) -> None:
        assert {f.name for f in gateway_messages_pb2.CancelSignal.DESCRIPTOR.fields} == {"task_id"}
        assert {f.name for f in gateway_messages_pb2.InvalidateSignal.DESCRIPTOR.fields} == {"scope"}
        assert {f.name for f in gateway_dto_pb2.SendSignalResponse.DESCRIPTOR.fields} == {"success", "task_id"}


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
# ModuleService contract
# ===========================================================================


class TestModuleServiceContract:
    """Verify ModuleService proto shape."""

    def test_start_module_is_server_streaming(self) -> None:
        assert "StartModule" in dir(module_service_pb2_grpc.ModuleServiceServicer)

    def test_no_stream_module_rpc(self) -> None:
        """StreamModule BiDi was removed — verify it stays removed."""
        assert "StreamModule" not in dir(module_service_pb2_grpc.ModuleServiceServicer)

    def test_start_module_request_fields(self) -> None:
        fields = {f.name for f in module_dto_pb2.StartModuleRequest.DESCRIPTOR.fields}
        assert {"input", "setup_id", "mission_id"} <= fields

    def test_start_module_response_fields(self) -> None:
        """``success`` / ``output`` moved into the ``ModuleResult result`` envelope."""
        fields = {f.name for f in module_dto_pb2.StartModuleResponse.DESCRIPTOR.fields}
        assert fields == {"job_id", "result"}


# ===========================================================================
# Proto serialization round-trip — flat Stream frames
# ===========================================================================


class TestProtoSerialization:
    """Verify proto messages serialize and deserialize correctly."""

    def test_stream_response_roundtrip(self) -> None:
        data = struct_pb2.Struct()
        data.update({"root": {"protocol": "message", "content": "hello"}})

        restored = gateway_messages_pb2.StreamResponse()
        restored.ParseFromString(
            gateway_messages_pb2.StreamResponse(seq=42, task_id="t1", data=data).SerializeToString()
        )

        assert restored.seq == 42
        d = json_format.MessageToDict(restored.data)
        assert d["root"]["content"] == "hello"
        assert d["root"]["protocol"] == "message"

    def test_stream_request_init_roundtrip(self) -> None:
        restored = gateway_messages_pb2.StreamRequest()
        restored.ParseFromString(gateway_messages_pb2.StreamRequest(task_id="t1", from_seq=10).SerializeToString())

        assert restored.task_id == "t1"
        assert restored.from_seq == 10
        assert len(restored.data.fields) == 0

    def test_stream_request_data_roundtrip(self) -> None:
        data = struct_pb2.Struct()
        data.update({"upstream": "input"})
        restored = gateway_messages_pb2.StreamRequest()
        restored.ParseFromString(gateway_messages_pb2.StreamRequest(data=data).SerializeToString())

        assert restored.data.fields["upstream"].string_value == "input"

    def test_frames_are_wire_compatible(self) -> None:
        """The dial-back re-wraps a StreamResponse as a StreamRequest: seq lands in from_seq."""
        data = struct_pb2.Struct()
        data.update({"root": {"protocol": "stream.end"}})
        as_request = gateway_messages_pb2.StreamRequest.FromString(
            gateway_messages_pb2.StreamResponse(seq=7, task_id="t1", data=data).SerializeToString()
        )

        assert as_request.from_seq == 7
        assert as_request.task_id == "t1"
        assert as_request.data == data
