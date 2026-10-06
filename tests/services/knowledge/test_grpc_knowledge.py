"""Tests for GrpcKnowledge, the client onto the Service Provider's ContextService.

What matters here is the request that reaches the wire and the citation that comes back:
the clamping the protocol expects, the enum and Struct mapping, and the way a failure is
reported. The search carries no identity by construction, which these tests pin too.
"""

import asyncio
import types
from concurrent import futures

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.context.v1 import (
    context_enums_pb2,
    context_service_pb2,
    context_service_pb2_grpc,
)

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.knowledge import Modality
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.knowledge.exceptions import KnowledgeServiceError
from digitalkin.services.knowledge.grpc_knowledge import GrpcKnowledge
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext
from tests.services.knowledge.mock_context_servicer import MockContextServicer

pytestmark = pytest.mark.timeout(20)

MISSION_ID = "missions:test_mission"
SETUP_ID = "setups:test_setup"
SETUP_VERSION_ID = "setup_versions:test_version"
SEARCH = context_service_pb2.DESCRIPTOR.services_by_name["ContextService"].methods_by_name["Search"]


@pytest.fixture
def thread_pool():
    """Create a thread pool and ensure cleanup.

    Returns:
        ThreadPoolExecutor instance.
    """
    pool = futures.ThreadPoolExecutor(max_workers=1)
    yield pool
    pool.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Create a test gRPC channel.

    Returns:
        A testing channel intercepting ContextService calls.
    """
    return grpc_testing.channel(
        service_descriptors=[context_service_pb2.DESCRIPTOR.services_by_name["ContextService"]],
        time=grpc_testing.strict_real_time(),
    )


@pytest.fixture
def mock_servicer() -> MockContextServicer:
    """Create a mock ContextService servicer.

    Returns:
        Mock servicer instance.
    """
    return MockContextServicer()


@pytest.fixture
def dummy_client_config() -> ClientConfig:
    """Create a dummy ClientConfig.

    Returns:
        ClientConfig with test values.
    """
    return ClientConfig(
        host="localhost",
        port=50053,
        mode=ControlFlow.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )


@pytest.fixture
def client(test_channel: grpc_testing.Channel, dummy_client_config: ClientConfig) -> GrpcKnowledge:
    """Create a GrpcKnowledge client wired onto the test channel.

    Args:
        test_channel: Test gRPC channel.
        dummy_client_config: Dummy client configuration.

    Returns:
        GrpcKnowledge configured for testing.
    """
    knowledge = GrpcKnowledge(MISSION_ID, SETUP_ID, SETUP_VERSION_ID, dummy_client_config)
    knowledge.stub = AsyncStubWrapper(context_service_pb2_grpc.ContextServiceStub(test_channel))

    async def _test_exec_grpc_query(self, query_endpoint, request, timeout=None, metadata=None):
        response = getattr(self.stub, query_endpoint)(request)
        return await response if asyncio.iscoroutine(response) else response

    knowledge.exec_grpc_query = types.MethodType(_test_exec_grpc_query, knowledge)
    return knowledge


def _answer(
    client: GrpcKnowledge,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockContextServicer,
    thread_pool: futures.ThreadPoolExecutor,
    **kwargs,
):
    """Run one search against the mock servicer.

    Args:
        client: The knowledge client under test.
        test_channel: The intercepting channel.
        mock_servicer: The servicer answering the call.
        thread_pool: Pool the client call runs in.
        kwargs: Arguments forwarded to ``search``.

    Returns:
        The citations the client produced, and the request that reached the wire.
    """
    future = thread_pool.submit(asyncio.run, client.search(**kwargs))
    _, request, rpc = test_channel.take_unary_unary(SEARCH)
    response = mock_servicer.Search(request, FakeContext())
    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")
    return future.result(timeout=2.0), request


class TestSearch:
    @pytest.mark.grpc
    @pytest.mark.integration
    def test_a_citation_is_mapped_whole(
        self,
        client: GrpcKnowledge,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockContextServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        mock_servicer.add_document(
            document_id="doc_pricing",
            title="Apollo pricing",
            snippet="Apollo moved to a per-seat model.",
            score=0.032,
            source="google_drive",
            source_url="https://docs.google.com/document/d/pricing",
            modality=context_enums_pb2.MODALITY_PAGE,
            locator={"index": 1.0, "heading_path": ["Pricing"]},
            metadata={"revision": 4.0},
        )

        citations, _ = _answer(client, test_channel, mock_servicer, thread_pool, query="apollo pricing")

        assert len(citations) == 1
        citation = citations[0]
        assert citation.document_id == "doc_pricing"
        assert citation.title == "Apollo pricing"
        assert citation.snippet == "Apollo moved to a per-seat model."
        assert citation.score == pytest.approx(0.032)
        assert citation.source == "google_drive"
        assert citation.source_url == "https://docs.google.com/document/d/pricing"
        assert citation.modality is Modality.PAGE
        assert citation.heading_path == ["Pricing"]
        assert citation.metadata == {"revision": 4.0}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_the_request_carries_no_identity(
        self,
        client: GrpcKnowledge,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockContextServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        _, request = _answer(client, test_channel, mock_servicer, thread_pool, query="apollo")

        # The mission travels as ambient metadata; a field here would let a module choose
        # whose documents it searches.
        assert {field.name for field in request.DESCRIPTOR.fields} == {"query", "sources", "limit"}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_sources_and_limit_reach_the_wire(
        self,
        client: GrpcKnowledge,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockContextServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        _, request = _answer(
            client,
            test_channel,
            mock_servicer,
            thread_pool,
            query="  apollo pricing  ",
            sources=["google_drive", "notion"],
            limit=3,
        )

        assert request.query == "apollo pricing"
        assert list(request.sources) == ["google_drive", "notion"]
        assert request.limit == 3

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_a_limit_over_the_protocol_bound_is_clamped_before_sending(
        self,
        client: GrpcKnowledge,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockContextServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        _, request = _answer(client, test_channel, mock_servicer, thread_pool, query="apollo", limit=500)

        # protovalidate would reject 500 with INVALID_ARGUMENT; clamping spends no round-trip.
        assert request.limit == 50

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_nothing_found_is_an_empty_list_not_an_error(
        self,
        client: GrpcKnowledge,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockContextServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        citations, _ = _answer(client, test_channel, mock_servicer, thread_pool, query="nothing here")

        assert citations == []

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_an_unknown_modality_maps_to_unspecified(
        self,
        client: GrpcKnowledge,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockContextServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        mock_servicer.add_document(document_id="doc_1", modality=context_enums_pb2.MODALITY_UNSPECIFIED)

        citations, _ = _answer(client, test_channel, mock_servicer, thread_pool, query="apollo")

        assert citations[0].modality is Modality.UNSPECIFIED


class TestWhatItRefuses:
    async def test_an_empty_query_never_reaches_the_server(self, client: GrpcKnowledge) -> None:
        # A permanent condition: raised as-is, not wrapped as a (retryable) service error.
        with pytest.raises(ValueError, match="query must not be empty"):
            await client.search("  ")

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_a_server_failure_becomes_a_knowledge_error(
        self,
        client: GrpcKnowledge,
        test_channel: grpc_testing.Channel,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        future = thread_pool.submit(asyncio.run, client.search("apollo"))

        _, _, rpc = test_channel.take_unary_unary(SEARCH)
        rpc.send_initial_metadata(())
        rpc.terminate(None, (), grpc.StatusCode.INTERNAL, "index unreachable")

        with pytest.raises(KnowledgeServiceError):
            future.result(timeout=2.0)
