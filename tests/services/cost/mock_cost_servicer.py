"""Mock Cost Servicer for testing the GrpcCost service."""

from typing import Any

import grpc
from agentic_mesh_protocol.cost.v1 import cost_messages_pb2, cost_service_pb2_grpc, cost_dto_pb2
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.models.services.cost import CostType, CostData


class MockCostServicer(cost_service_pb2_grpc.CostServiceServicer):
    """Mock implementation of the Cost Service Servicer for testing."""

    def __init__(self) -> None:
        """Initialize the mock servicer with empty cost storage."""
        super().__init__()
        # mission_id -> list of CostData
        self.costs: dict[str, list[dict[str, Any]]] = {}

    def _validate_and_store_cost(self, cost_dict: dict[str, Any]) -> None:
        """Validate cost data using Pydantic and store it.

        Args:
            cost_dict: Dictionary containing cost data

        Raises:
            ValidationError: If cost data is invalid
        """
        # Validate using Pydantic
        cost_data = CostData.model_validate(cost_dict)

        # Store in mission-specific list
        mission_id = cost_data.mission_id
        if mission_id not in self.costs:
            self.costs[mission_id] = []

        self.costs[mission_id].append(cost_data.model_dump())
        logger.debug(f"Stored cost: {cost_data.name} for mission {mission_id}")

    def _cost_dict_to_proto(self, cost_dict: dict[str, Any]) -> cost_messages_pb2.Cost:
        """Convert a cost dictionary to a proto Cost message.

        Args:
            cost_dict: Dictionary containing cost data

        Returns:
            cost_pb2.Cost: Proto cost message
        """
        return cost_messages_pb2.Cost(
            cost=cost_dict["cost"],
            name=cost_dict["name"],
            unit=cost_dict["unit"],
            type=cost_dict["type"].to_proto(),
            mission_id=cost_dict["mission_id"],
            rate=cost_dict["rate"],
            quantity=cost_dict["quantity"],
            setup_version_id=cost_dict["setup_version_id"],
        )

    def CreateCost(self, request: cost_dto_pb2.CreateCostRequest, context: grpc.ServicerContext) -> cost_dto_pb2.CreateCostResponse:
        """Add a cost record to the mock database.

        Args:
            request: AddCostRequest containing cost data
            context: gRPC context

        Returns:
            AddCostResponse: Response indicating success or failure
        """
        try:
            # Validate required fields
            if not request.name:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Cost name is required")
                result = cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return cost_dto_pb2.CreateCostResponse(result=result)

            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                result = cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return cost_dto_pb2.CreateCostResponse(result=result)

            if request.quantity <= 0:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Quantity must be positive")
                result = cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return cost_dto_pb2.CreateCostResponse(result=result)

            if request.rate < 0:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Rate cannot be negative")
                result = cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return cost_dto_pb2.CreateCostResponse(result=result)

            # Validate cost type
            # Note: Protobuf enum values are integers, not strings
            # Validate that cost_type is one of the valid * enum values
            cost_type = CostType.from_proto(request.type)
            if cost_type not in CostType:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid cost type: {cost_type}")
                result = cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return cost_dto_pb2.CreateCostResponse(result=result)

            cost_data = CostData(
                cost=request.cost,
                name=request.name,
                unit=request.unit,
                type=cost_type,
                mission_id=request.mission_id,
                rate=request.rate,
                quantity=request.quantity,
                setup_version_id=request.setup_version_id
            )

            # Validate and store
            self._validate_and_store_cost(cost_data.dict())

            logger.info(f"Added cost: {request.name} for mission {request.mission_id}")

            # Create cost proto with proper type conversion
            cost_dict = cost_data.model_dump()
            cost_dict["type"] = cost_type.to_proto()

            result = cost_messages_pb2.CostResult(success=True, cost=cost_messages_pb2.Cost(**cost_dict))
            return cost_dto_pb2.CreateCostResponse(result=result)

        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Validation error: {e!s}")
            logger.error(f"Validation error in AddCost: {e}")
            result = cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
            return cost_dto_pb2.CreateCostResponse(result=result)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in AddCost: {e}", exc_info=True)
            result = cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)), success=False)
            return cost_dto_pb2.CreateCostResponse(result=result)

    def ListCosts(self, request: cost_dto_pb2.ListCostsRequest, context: grpc.ServicerContext) -> cost_dto_pb2.ListCostsResponse:
        """Get costs filtered by names and/or cost types.

        Args:
            request: GetCostsRequest containing mission_id and filter
            context: gRPC context

        Returns:
            GetCostsResponse: Response containing filtered costs
        """
        total_cost = None

        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                bulk = bulk_pb2.BulkResponse(total_process=0, total_failed=0)
                result = [cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)]
                return cost_dto_pb2.ListCostsRequest(result=result, bulk=bulk)

            # Get costs for this mission
            mission_costs = self.costs.get(request.mission_id, [])
            total_cost = len(mission_costs)

            # Apply filters
            filtered_costs = mission_costs

            # Filter by names if provided
            if request.filter and request.filter.names:
                filtered_costs = [c for c in filtered_costs if c["name"] in request.filter.names]

            # Filter by cost types if provided
            if request.filter and request.filter.types:
                filter_types = [CostType.from_proto(ct) for ct in request.filter.types]
                filtered_costs = [c for c in filtered_costs if c["type"] in filter_types]

            # Convert to proto messages
            cost_protos = [self._cost_dict_to_proto(cost) for cost in filtered_costs]
            items_results = [cost_messages_pb2.CostResult(cost=cost) for cost in cost_protos]

            logger.info(f"Retrieved {len(filtered_costs)} filtered costs for mission {request.mission_id}")
            pagination = pagination_pb2.PaginationResponse(total_count=len(items_results))
            bulk = bulk_pb2.BulkResponse(total_process=len(items_results), total_failed=0, pagination=pagination)
            return cost_dto_pb2.ListCostsResponse(bulk=bulk, result=items_results)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in GetCosts: {e}", exc_info=True)
            items_results = [cost_messages_pb2.CostResult(error=bulk_pb2.OperationError(code=grpc.StatusCode.INTERNAL, message="Error in GetCosts"))]
            bulk = bulk_pb2.BulkResponse(total_process=total_cost, total_failed=total_cost)
            return cost_dto_pb2.ListCostsResponse(bulk=bulk, result=items_results)
