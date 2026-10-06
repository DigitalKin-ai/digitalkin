"""gRPC knowledge client implementation.

Searches the enterprise knowledge base through the Service Provider's ContextService,
which relays to the Context Platform.

``SearchRequest`` carries no identity: *who* is searching travels as the ambient
``x-mission-id`` metadata every outbound call already gets
(:class:`~digitalkin.grpc_servers.interceptors.request_ids.RequestIdClientInterceptor`),
and the Service Provider resolves it into the user and organization the search runs for.
A module therefore cannot widen its own scope, and cannot forget to narrow it.
"""

from typing import Any

import grpc
from agentic_mesh_protocol.context.v1 import (
    context_enums_pb2,
    context_models_pb2,
    context_requests_pb2,
    context_service_pb2_grpc,
)
from google.protobuf import json_format
from grpc_health.v1 import health_pb2, health_pb2_grpc

from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.knowledge import Citation, Modality
from digitalkin.models.settings.knowledge import get_knowledge_settings
from digitalkin.services.knowledge.exceptions import KnowledgeServiceError
from digitalkin.services.knowledge.knowledge_strategy import KnowledgeStrategy


class GrpcKnowledge(KnowledgeStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC-based knowledge client talking to the Service Provider's ContextService."""

    service_name: str = "ContextService"

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        client_config: ClientConfig,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the gRPC knowledge client."""
        KnowledgeStrategy.__init__(self, mission_id, setup_id, setup_version_id, config)
        self.service_name = "ContextService"
        self._init_channel(client_config)
        self.stub = self._get_or_create_stub(context_service_pb2_grpc.ContextServiceStub)
        logger.debug("Channel client 'Knowledge' initialized successfully")

    async def close(self) -> None:
        """Release this instance's pooled gRPC channel ref."""
        await self.close_channel()

    async def wait_for_ready(self, timeout: float = 1.0) -> bool:
        """Probe the backend via the standard gRPC Health Check service.

        Args:
            timeout: Max seconds for the round-trip.

        Returns:
            True if the server responded SERVING, False otherwise.
        """
        health_stub = health_pb2_grpc.HealthStub(self._channel)
        try:
            response = await health_stub.Check(  # type: ignore[attr-defined]  # grpc_health generated stub lacks typed Check
                health_pb2.HealthCheckRequest(service=""),
                timeout=timeout,
            )
        except grpc.aio.AioRpcError:
            return False
        return response.status == health_pb2.HealthCheckResponse.SERVING

    @staticmethod
    def _proto_to_citation(citation: context_models_pb2.Citation) -> Citation:
        """Convert a proto ``Citation`` to the SDK model.

        Args:
            citation: Proto Citation message.

        Returns:
            Citation with mapped fields.
        """
        modality_name = context_enums_pb2.Modality.Name(citation.modality).removeprefix("MODALITY_")
        return Citation(
            document_id=citation.document_id,
            title=citation.title,
            snippet=citation.snippet,
            score=citation.score,
            source=citation.source,
            source_url=citation.source_url,
            modality=Modality[modality_name],
            locator=json_format.MessageToDict(citation.locator),
            metadata=json_format.MessageToDict(citation.metadata),
        )

    async def search(
        self,
        query: str,
        sources: list[str] | None = None,
        limit: int | None = None,
    ) -> list[Citation]:
        """Search the knowledge base the current user may read.

        Args:
            query: What to look for, in natural language.
            sources: Restrict the search to these connectors; ``None`` searches every source.
            limit: Maximum number of documents to return; ``None`` uses the configured default.

        Returns:
            One citation per matching document, best first.

        Raises:
            ValueError: If the query holds nothing to search for.
            PermissionDeniedError: If the caller is not permitted.
            KnowledgeServiceError: If the gRPC call fails.
        """
        # Normalized before the error-handler scope: an empty query is a permanent condition
        # and must reach the caller as a ValueError, not be wrapped as a service error.
        text, wanted_sources, bounded = self.normalize(query, sources, limit)

        async with self.handle_grpc_errors("Search", KnowledgeServiceError):
            try:
                response = await self.exec_grpc_query(
                    "Search",
                    context_requests_pb2.SearchRequest(query=text, sources=wanted_sources, limit=bounded),
                    timeout=get_knowledge_settings().search_timeout_s,
                )
            except PermissionDeniedError:
                raise
            except ServerError as e:
                msg = f"Failed to search the knowledge base: {e}"
                logger.error(msg)
                raise KnowledgeServiceError(msg) from e

            logger.debug("Knowledge search returned %d document(s)", len(response.results))
            return [self._proto_to_citation(citation) for citation in response.results]
