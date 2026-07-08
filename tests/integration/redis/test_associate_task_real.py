"""Real-Redis integration for ``GatewayServicer.AssociateTask``.

Paired with the mocked-Redis unit tests in
``tests/gateway/test_gateway_servicer.py::TestAssociateTask``: proves the
parent→child association keys are actually written with a TTL against real Redis.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2

from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.models.settings.gateway import get_gateway_settings

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


class TestAssociateTaskReal:
    async def test_mint_persists_parent_child_links_with_ttl(self, redis_client) -> None:
        """A minted sub-task is recorded in the parent's children set and a reverse pointer, both TTL'd."""
        servicer = GatewayServicer(redis_client=redis_client)
        parent = "task:parent-real"

        response = await servicer.AssociateTask(gateway_pb2.AssociateTaskRequest(parent_task_id=parent), MagicMock())
        child = response.task_id
        assert child  # gateway-minted, not a client uuid
        assert response.parent_task_id == parent

        ttl = get_gateway_settings().stream.redis_stream_initial_ttl
        members = await redis_client.smembers(f"task:{parent}:children")
        assert child.encode() in members
        assert await redis_client.get(f"task:{child}:parent") == parent.encode()
        assert 0 < await redis_client._client.ttl(f"task:{parent}:children") <= ttl
        assert 0 < await redis_client._client.ttl(f"task:{child}:parent") <= ttl
