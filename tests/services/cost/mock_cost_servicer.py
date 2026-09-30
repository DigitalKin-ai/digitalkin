"""Mock Cost Servicer for testing the GrpcCost service."""

from typing import Any

import grpc
import protovalidate
from agentic_mesh_protocol.cost.v1 import cost_dto_pb2, cost_enums_pb2, cost_messages_pb2, cost_service_pb2_grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from google.protobuf.message import Message

from digitalkin.logger import logger
from digitalkin.models.services.cost import CostType
from digitalkin.services.cost.cost_strategy import CostData


class MockCostServicer(cost_service_pb2_grpc.CostServiceServicer):
    """Mock implementation of the Cost Service Servicer for testing.

    Requests are checked against their ``buf.validate`` rules first, as the
    server-side validation interceptor does, and rejected with ``INVALID_ARGUMENT``.
    Listings honour ``PaginationRequest`` (server default page: 20 items).
    """

    def __init__(self) -> None:
        """Initialize the mock servicer with empty cost and config storage."""
        super().__init__()
        # mission_id -> list of CostData dumps
        self.costs: dict[str, list[dict[str, Any]]] = {}
        # setup_version_id -> list of CostConfig protos
        self.configs: dict[str, list[cost_messages_pb2.CostConfig]] = {}

    @staticmethod
    def _invalid(request: Message, context: grpc.ServicerContext) -> bool:
        """Flag a request breaking its ``buf.validate`` rules with ``INVALID_ARGUMENT``.

        Args:
            request: Incoming request.
            context: gRPC context

        Returns:
            True when the request is invalid.
        """
        try:
            protovalidate.validate(request)
        except protovalidate.ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"invalid {request.DESCRIPTOR.name}: {e}")
            return True
        return False

    @staticmethod
    def _page(
            items: list[Any], pagination: pagination_pb2.PaginationRequest
    ) -> tuple[list[Any], bulk_pb2.BulkResponse]:
        """Slice a listing to the requested page.

        Args:
            items: Every matching item.
            pagination: Requested page.

        Returns:
            The page items and the bulk summary describing them.
        """
        limit = pagination.limit or 20
        page = items[pagination.offset: pagination.offset + limit]
        return page, bulk_pb2.BulkResponse(
            total_processed=len(page),
            pagination=pagination_pb2.PaginationResponse(
                total_count=len(items),
                page=pagination.offset // limit,
                per_page=limit,
                total_pages=-(-len(items) // limit),
            ),
        )

    def _validate_and_store_cost(self, cost_dict: dict[str, Any]) -> None:
        """Validate cost data using Pydantic and store it.

        Args:
            cost_dict: Dictionary containing cost data

        Raises:
            ValidationError: If cost data is invalid
        """
        cost_data = CostData.model_validate(cost_dict)
        self.costs.setdefault(cost_data.mission_id, []).append(cost_data.model_dump())
        logger.debug("Stored cost: %s for mission %s", cost_data.name, cost_data.mission_id)

    @staticmethod
    def _cost_dict_to_proto(cost_dict: dict[str, Any]) -> cost_messages_pb2.Cost:
        """Convert a cost dictionary to a proto Cost message.

        Args:
            cost_dict: Dictionary containing cost data

        Returns:
            Proto cost message
        """
        return cost_messages_pb2.Cost(
            mission_id=cost_dict["mission_id"],
            setup_version_id=cost_dict["setup_version_id"],
            name=cost_dict["name"],
            cost=cost_dict["cost"],
            quantity=cost_dict["quantity"],
            rate=cost_dict["rate"],
            unit=cost_dict["unit"],
            type=cost_dict["cost_type"].name,
        )

    def CreateCost(
            self, request: cost_dto_pb2.CreateCostRequest, context: grpc.ServicerContext
    ) -> cost_dto_pb2.CreateCostResponse:
        """Record a cost in the mock database.

        Args:
            request: CreateCostRequest containing cost data
            context: gRPC context

        Returns:
            CreateCostResponse holding the recorded cost.
        """
        if self._invalid(request, context):
            return cost_dto_pb2.CreateCostResponse()
        cost_dict = {
            "cost": request.cost,
            "name": request.name,
            "unit": request.unit,
            "cost_type": CostType[cost_enums_pb2.CostType.Name(request.type)],
            "mission_id": request.mission_id,
            "rate": request.rate,
            "quantity": request.quantity,
            "setup_version_id": request.setup_version_id,
        }
        self._validate_and_store_cost(cost_dict)
        return cost_dto_pb2.CreateCostResponse(
            result=cost_messages_pb2.CostResult(identifier=request.name, cost=self._cost_dict_to_proto(cost_dict))
        )

    def ListCosts(
            self, request: cost_dto_pb2.ListCostsRequest, context: grpc.ServicerContext
    ) -> cost_dto_pb2.ListCostsResponse:
        """List a page of the costs of a mission matching every filter.

        Args:
            request: ListCostsRequest containing mission_id, filter and pagination
            context: gRPC context

        Returns:
            ListCostsResponse holding the page, its bulk summary and the total cost.
        """
        if self._invalid(request, context):
            return cost_dto_pb2.ListCostsResponse()
        matching = [
            cost
            for cost in self.costs.get(request.mission_id, [])
            if (not request.filter.names or cost["name"] in request.filter.names)
               and (
                       not request.filter.types
                       or cost_enums_pb2.CostType.Value(cost["cost_type"].name) in request.filter.types
            )
        ]
        page, bulk = self._page(matching, request.pagination)
        return cost_dto_pb2.ListCostsResponse(
            results=[
                cost_messages_pb2.CostResult(identifier=cost["name"], cost=self._cost_dict_to_proto(cost))
                for cost in page
            ],
            bulk=bulk,
            total_cost=sum(cost["cost"] for cost in matching),
        )

    def ListCostConfigs(
            self, request: cost_dto_pb2.ListCostConfigsRequest, context: grpc.ServicerContext
    ) -> cost_dto_pb2.ListCostConfigsResponse:
        """List a page of the cost configurations of a setup version.

        Args:
            request: ListCostConfigsRequest containing setup_version_id and pagination
            context: gRPC context

        Returns:
            ListCostConfigsResponse holding the page and its bulk summary.
        """
        if self._invalid(request, context):
            return cost_dto_pb2.ListCostConfigsResponse()
        page, bulk = self._page(self.configs.get(request.setup_version_id, []), request.pagination)
        return cost_dto_pb2.ListCostConfigsResponse(
            results=[cost_messages_pb2.CostResult(identifier=config.name, config=config) for config in page],
            bulk=bulk,
        )

    def SetCostConfig(
            self, request: cost_dto_pb2.SetCostConfigRequest, context: grpc.ServicerContext
    ) -> cost_dto_pb2.SetCostConfigResponse:
        """Replace the cost configurations of a setup version.

        Args:
            request: SetCostConfigRequest containing setup_version_id and configs
            context: gRPC context

        Returns:
            SetCostConfigResponse holding one result per stored configuration.
        """
        if self._invalid(request, context):
            return cost_dto_pb2.SetCostConfigResponse()
        self.configs[request.setup_version_id] = list(request.configs)
        return cost_dto_pb2.SetCostConfigResponse(
            results=[cost_messages_pb2.CostResult(identifier=config.name, config=config) for config in request.configs],
            bulk=bulk_pb2.BulkResponse(total_processed=len(request.configs)),
        )
