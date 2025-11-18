"""Mock Cost Servicer for testing the GrpcCost service."""

from typing import Any

import grpc
from digitalkin_proto.agentic_mesh_protocol.cost.v1 import cost_pb2, cost_service_pb2_grpc
from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.services.cost.cost_strategy import CostData, CostType


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

    def _cost_dict_to_proto(self, cost_dict: dict[str, Any]) -> cost_pb2.Cost:
        """Convert a cost dictionary to a proto Cost message.

        Args:
            cost_dict: Dictionary containing cost data

        Returns:
            cost_pb2.Cost: Proto cost message
        """
        # Convert Python CostType enum to protobuf enum
        python_to_proto_cost_type = {
            CostType.TOKEN_INPUT: cost_pb2.TOKEN_INPUT,
            CostType.TOKEN_OUTPUT: cost_pb2.TOKEN_OUTPUT,
            CostType.API_CALL: cost_pb2.API_CALL,
            CostType.STORAGE: cost_pb2.STORAGE,
            CostType.TIME: cost_pb2.TIME,
            CostType.OTHER: cost_pb2.OTHER,
        }
        proto_cost_type = python_to_proto_cost_type.get(cost_dict["cost_type"], cost_pb2.OTHER)

        return cost_pb2.Cost(
            cost=cost_dict["cost"],
            name=cost_dict["name"],
            unit=cost_dict["unit"],
            cost_type=proto_cost_type,
            mission_id=cost_dict["mission_id"],
            rate=cost_dict["rate"],
            quantity=cost_dict["quantity"],
            setup_version_id=cost_dict["setup_version_id"],
        )

    def AddCost(self, request: cost_pb2.AddCostRequest, context: grpc.ServicerContext) -> cost_pb2.AddCostResponse:
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
                return cost_pb2.AddCostResponse(success=False)

            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                return cost_pb2.AddCostResponse(success=False)

            if request.quantity <= 0:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Quantity must be positive")
                return cost_pb2.AddCostResponse(success=False)

            if request.rate < 0:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Rate cannot be negative")
                return cost_pb2.AddCostResponse(success=False)

            # Validate cost type
            # Note: Protobuf enum values are integers, not strings
            # Validate that cost_type is one of the valid * enum values
            valid_values = [
                cost_pb2.TOKEN_INPUT,
                cost_pb2.TOKEN_OUTPUT,
                cost_pb2.API_CALL,
                cost_pb2.STORAGE,
                cost_pb2.TIME,
                cost_pb2.OTHER,
            ]
            if request.cost_type not in valid_values:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid cost type: {request.cost_type}")
                return cost_pb2.AddCostResponse(success=False)

            # Convert protobuf cost_type enum to Python CostType enum
            # Protobuf enums: TOKEN_INPUT=1, TOKEN_OUTPUT=2, etc.
            # Python enums: TOKEN_INPUT, TOKEN_OUTPUT, etc.
            proto_to_python_cost_type = {
                cost_pb2.TOKEN_INPUT: CostType.TOKEN_INPUT,
                cost_pb2.TOKEN_OUTPUT: CostType.TOKEN_OUTPUT,
                cost_pb2.API_CALL: CostType.API_CALL,
                cost_pb2.STORAGE: CostType.STORAGE,
                cost_pb2.TIME: CostType.TIME,
                cost_pb2.OTHER: CostType.OTHER,
            }
            python_cost_type = proto_to_python_cost_type.get(request.cost_type, CostType.OTHER)

            # Create cost dictionary
            cost_dict = {
                "cost": request.cost,
                "name": request.name,
                "unit": request.unit,
                "cost_type": python_cost_type,
                "mission_id": request.mission_id,
                "rate": request.rate,
                "quantity": request.quantity,
                "setup_version_id": request.setup_version_id,
            }

            # Validate and store
            self._validate_and_store_cost(cost_dict)

            logger.info(f"Added cost: {request.name} for mission {request.mission_id}")
            return cost_pb2.AddCostResponse(success=True)

        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Validation error: {e!s}")
            logger.error(f"Validation error in AddCost: {e}")
            return cost_pb2.AddCostResponse(success=False)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in AddCost: {e}", exc_info=True)
            return cost_pb2.AddCostResponse(success=False)

    def GetCost(self, request: cost_pb2.GetCostRequest, context: grpc.ServicerContext) -> cost_pb2.GetCostResponse:
        """Get costs by name for a specific mission.

        Args:
            request: GetCostRequest containing name and mission_id
            context: gRPC context

        Returns:
            GetCostResponse: Response containing matching costs
        """
        try:
            if not request.name:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Cost name is required")
                return cost_pb2.GetCostResponse(costs=[])

            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                return cost_pb2.GetCostResponse(costs=[])

            # Get costs for this mission
            mission_costs = self.costs.get(request.mission_id, [])

            # Filter by name
            matching_costs = [c for c in mission_costs if c["name"] == request.name]

            if not matching_costs:
                logger.debug(f"No costs found with name '{request.name}' for mission {request.mission_id}")
                return cost_pb2.GetCostResponse(costs=[])

            # Convert to proto messages
            cost_protos = [self._cost_dict_to_proto(cost) for cost in matching_costs]

            logger.info(
                f"Retrieved {len(matching_costs)} costs with name '{request.name}' for mission {request.mission_id}"
            )
            return cost_pb2.GetCostResponse(costs=cost_protos)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in GetCost: {e}", exc_info=True)
            return cost_pb2.GetCostResponse(costs=[])

    def GetCosts(self, request: cost_pb2.GetCostsRequest, context: grpc.ServicerContext) -> cost_pb2.GetCostsResponse:
        """Get costs filtered by names and/or cost types.

        Args:
            request: GetCostsRequest containing mission_id and filter
            context: gRPC context

        Returns:
            GetCostsResponse: Response containing filtered costs
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                return cost_pb2.GetCostsResponse(costs=[])

            # Get costs for this mission
            mission_costs = self.costs.get(request.mission_id, [])

            # Apply filters
            filtered_costs = mission_costs

            # Filter by names if provided
            if request.filter and request.filter.names:
                filtered_costs = [c for c in filtered_costs if c["name"] in request.filter.names]

            # Filter by cost types if provided
            if request.filter and request.filter.cost_types:
                # Convert protobuf enum integer values to Python CostType enums
                # Protobuf enum: 1 = TOKEN_INPUT -> Python: CostType.TOKEN_INPUT
                proto_to_python_cost_type = {
                    cost_pb2.TOKEN_INPUT: CostType.TOKEN_INPUT,
                    cost_pb2.TOKEN_OUTPUT: CostType.TOKEN_OUTPUT,
                    cost_pb2.API_CALL: CostType.API_CALL,
                    cost_pb2.STORAGE: CostType.STORAGE,
                    cost_pb2.TIME: CostType.TIME,
                    cost_pb2.OTHER: CostType.OTHER,
                }
                filter_types = [proto_to_python_cost_type.get(ct, CostType.OTHER) for ct in request.filter.cost_types]
                filtered_costs = [c for c in filtered_costs if c["cost_type"] in filter_types]

            # Convert to proto messages
            cost_protos = [self._cost_dict_to_proto(cost) for cost in filtered_costs]

            logger.info(f"Retrieved {len(filtered_costs)} filtered costs for mission {request.mission_id}")
            return cost_pb2.GetCostsResponse(costs=cost_protos)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in GetCosts: {e}", exc_info=True)
            return cost_pb2.GetCostsResponse(costs=[])
