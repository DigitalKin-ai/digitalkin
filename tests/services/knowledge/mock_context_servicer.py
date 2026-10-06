"""Mock ContextService servicer for testing the GrpcKnowledge client."""

from typing import Any

import grpc
from agentic_mesh_protocol.context.v1 import (
    context_enums_pb2,
    context_models_pb2,
    context_requests_pb2,
    context_service_pb2_grpc,
)
from google.protobuf.struct_pb2 import Struct


class MockContextServicer(context_service_pb2_grpc.ContextServiceServicer):
    """Answers ``Search`` from a canned corpus, filtering the way the real service does."""

    def __init__(self) -> None:
        """Initialize the mock servicer with an empty corpus."""
        super().__init__()
        self.documents: list[dict[str, Any]] = []
        self.last_request: context_requests_pb2.SearchRequest | None = None

    def add_document(self, **fields: Any) -> None:
        """Add one document the mock will cite.

        Args:
            fields: Citation fields; ``source`` drives the ``sources`` filter.
        """
        self.documents.append(fields)

    @staticmethod
    def _citation(fields: dict[str, Any]) -> context_models_pb2.Citation:
        """Build a proto citation from plain fields.

        Args:
            fields: The citation fields.

        Returns:
            The proto message.
        """
        locator = Struct()
        locator.update(fields.get("locator", {}))
        metadata = Struct()
        metadata.update(fields.get("metadata", {}))
        return context_models_pb2.Citation(
            document_id=fields.get("document_id", ""),
            title=fields.get("title", ""),
            snippet=fields.get("snippet", ""),
            score=fields.get("score", 0.0),
            source=fields.get("source", ""),
            source_url=fields.get("source_url", ""),
            modality=fields.get("modality", context_enums_pb2.MODALITY_TEXT),
            locator=locator,
            metadata=metadata,
        )

    def Search(  # noqa: N802  # gRPC method name
        self,
        request: context_requests_pb2.SearchRequest,
        context: grpc.ServicerContext,
    ) -> context_requests_pb2.SearchResponse:
        """Return the corpus documents matching the request.

        Args:
            request: The search request.
            context: gRPC context.

        Returns:
            The matching citations, capped at ``request.limit``.
        """
        self.last_request = request
        if not request.query:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("query is required")
            return context_requests_pb2.SearchResponse()

        wanted = set(request.sources)
        matching = [
            document
            for document in self.documents
            if not wanted or document.get("source", "") in wanted
        ]
        limit = request.limit or len(matching)
        return context_requests_pb2.SearchResponse(results=[self._citation(d) for d in matching[:limit]])
