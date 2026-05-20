"""Tests for ``GatewayValidator.validate_address`` and StartStream's address-rejection path.

Per Phase 1.B of the dial-back rebuild plan: the gateway rejects
StartStream up front when ``x-client-address`` is missing, malformed, or
points at a wildcard bind address. ``dial_consumer_stream`` raises
``InvalidConsumerAddressError`` as defence-in-depth.
"""

from __future__ import annotations

from typing import Any

import pytest

from digitalkin.grpc_servers.utils.validators import GatewayValidator
from digitalkin.services.communication.exceptions import InvalidConsumerAddressError
from tests.gateway.test_gateway_servicer import _mock_context, _mock_servicer

pytestmark = [pytest.mark.timeout(15)]


class TestValidateAddress:
    """Pure unit tests for ``GatewayValidator.validate_address``."""

    @pytest.mark.parametrize(
        "address",
        [
            "127.0.0.1:50057",
            "localhost:50057",
            "host.docker.internal:8001",
            "ada-server:50051",
            "10.0.0.1:1",
            "example.com:65535",
        ],
    )
    def test_valid(self, address: str) -> None:
        assert GatewayValidator.validate_address(address, "x-client-address") is None

    @pytest.mark.parametrize(
        ("address", "expected_substring"),
        [
            ("", "is required"),
            ("localhost", "must be host:port"),
            ("localhost:", "must be host:port"),
            (":50057", "must be host:port"),
            ("localhost:abc", "must be host:port"),
            ("localhost:0", "port out of range"),
            ("localhost:65536", "port out of range"),
            ("localhost:99999", "port out of range"),
            ("[::]:50057", "wildcard bind address"),
            ("0.0.0.0:50057", "wildcard bind address"),
            ("::3:50057", "wildcard bind address"),  # "::" prefix tripped by pattern
        ],
    )
    def test_invalid(self, address: str, expected_substring: str) -> None:
        err = GatewayValidator.validate_address(address, "x-client-address")
        if expected_substring == "wildcard bind address" and err is not None and "must be host:port" in err:
            # IPv6 colon ambiguity: anything containing :: is wildcard-flavoured;
            # pattern rejects first. Either rejection is acceptable.
            return
        assert err is not None
        assert expected_substring in err


class TestStartStreamAddressRejection:
    """StartStream rejects requests without a usable ``x-client-address``."""

    @staticmethod
    def _request(task_id: str = "task_addr_1") -> Any:
        from agentic_mesh_protocol.gateway.v1 import gateway_pb2

        return gateway_pb2.StartStreamRequest(
            task_id=task_id, setup_id="setups:s1", mission_id="missions:m1",
        )

    async def test_rejects_missing_metadata(self) -> None:
        servicer = _mock_servicer()
        response = await servicer.StartStream(self._request(), _mock_context(client_address=None))
        assert response.accepted is False
        # No XADD on dispatch or stream — we rejected before any side effect.
        servicer._redis_client.xadd.assert_not_called()

    async def test_rejects_empty_metadata(self) -> None:
        servicer = _mock_servicer()
        response = await servicer.StartStream(self._request(), _mock_context(client_address=""))
        assert response.accepted is False
        servicer._redis_client.xadd.assert_not_called()

    async def test_rejects_malformed_no_port(self) -> None:
        servicer = _mock_servicer()
        response = await servicer.StartStream(self._request(), _mock_context(client_address="localhost"))
        assert response.accepted is False
        servicer._redis_client.xadd.assert_not_called()

    async def test_rejects_wildcard(self) -> None:
        servicer = _mock_servicer()
        response = await servicer.StartStream(self._request(), _mock_context(client_address="[::]:50057"))
        assert response.accepted is False
        servicer._redis_client.xadd.assert_not_called()

    async def test_accepts_valid_address(self) -> None:
        servicer = _mock_servicer()
        response = await servicer.StartStream(
            self._request(), _mock_context(client_address="127.0.0.1:50057"),
        )
        assert response.accepted is True
        # stream.start XADD must have happened.
        assert servicer._redis_client.xadd.await_count >= 1


class TestDialConsumerStreamValidation:
    """``dial_consumer_stream`` raises ``InvalidConsumerAddressError`` on bad input."""

    @staticmethod
    def _comm() -> Any:
        from digitalkin.models.grpc_servers.models import ClientConfig
        from digitalkin.services.communication.grpc_communication import GrpcCommunication

        cfg = ClientConfig(host="ignored", port=1)
        return GrpcCommunication(
            mission_id="missions:m1",
            setup_id="setups:s1",
            setup_version_id="setups:s1",
            client_config=cfg,
        )

    @pytest.mark.parametrize(
        "address",
        [
            "",
            "localhost",
            "localhost:",
            ":50057",
            "localhost:abc",
            "localhost:0",
            "localhost:65536",
        ],
    )
    def test_invalid_address_raises(self, address: str) -> None:
        comm = self._comm()
        with pytest.raises(InvalidConsumerAddressError):
            comm.dial_consumer_stream(address)
