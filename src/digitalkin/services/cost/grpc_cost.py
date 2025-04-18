"""This module implements the default Cost strategy."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Literal

from digitalkin_proto.digitalkin.cost.v1 import cost_pb2, cost_service_pb2_grpc
from google.protobuf import json_format

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.models import ServerConfig
from digitalkin.services.cost.cost_strategy import CostData, CostServiceError, CostStrategy

logger = logging.getLogger(__name__)


class GrpcCost(CostStrategy, GrpcClientWrapper):
    """This class implements the default Cost strategy."""

    @staticmethod
    @contextmanager
    def _handle_grpc_errors(operation: str) -> Generator[Any, Any, Any]:
        """Context manager for consistent gRPC error handling.

        Yields:
            Allow error handling in context.

        Args:
            operation: Description of the operation being performed.

        Raises:
            ValueError: Error with the model validation.
            ServerError: from gRPC Client.
            CostServiceError: Unexpected error.
        """
        try:
            yield
        except ServerError as e:
            msg = f"gRPC {operation} failed: {e}"
            logger.exception(msg)
            raise ServerError(msg) from e
        except Exception as e:
            msg = f"Unexpected error in {operation}"
            logger.exception(msg)
            raise CostServiceError(msg) from e

    def _get_costs_by_names(self, cost_dict: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByNamesRequest(
            mission_id=cost_dict["mission_id"],
            names=cost_dict["name"],
        )
        response = self.exec_grpc_query("GetCostsByNames", request)
        return [CostData.model_validate(json_format.MessageToDict(response))]

    def _get_costs_by_mission(self, cost_dict: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByMissionRequest(mission_id=cost_dict["mission_id"])
        response = self.exec_grpc_query("GetCostsByMission", request)
        return [CostData.model_validate(json_format.MessageToDict(cost)) for cost in response.costs]

    def _get_costs_by_type(self, cost_dict: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByCostTypeRequest(
            mission_id=cost_dict["mission_id"],
            cost_type=cost_dict["type"],
        )
        response = self.exec_grpc_query("GetCostsByCostType", request)
        return [CostData.model_validate(json_format.MessageToDict(cost)) for cost in response.costs]

    def __init__(self, mission_id: str, config: ServerConfig) -> None:
        """Initialize the cost."""
        super().__init__(mission_id)
        channel = self._init_channel(config)
        self.stub = cost_service_pb2_grpc.CostServiceStub(channel)
        logger.info("Channel client 'Cost' initialized succesfully")

    def add(
        self,
        name: str,
        cost: float,
        unit: str,
        cost_type: Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"],
    ) -> None:
        """Create a new record in the cost database.

        Required arguments:
            data: Object representation of CostData
        """
        with self._handle_grpc_errors("AddCost"):
            valid_data = CostData.model_validate({
                "name": name,
                "cost": cost,
                "unit": unit,
                "cost_type": cost_type,
                "mission_id": self.mission_id,
            })
            request = cost_pb2.AddCostRequest(**valid_data.model_dump())
            self.exec_grpc_query("AddCost", request)
            logger.debug("Cost added with cost_dict: %s", valid_data.model_dump())

    def get(
        self,
        names: list[str] | None = None,
        cost_type: Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"] | None = None,
    ) -> list[CostData]:
        """Get records from the database.

        Args:
            names: The names of the costs
            cost_type: The type of the costs

        Returns:
            list[CostData]: The list of records

        Raises:
            CostServiceError: If the cost data is invalid or if the cost already exists
        """
        with self._handle_grpc_errors("GetCost"):
            if names:
                request = cost_pb2.GetCostsByNamesRequest(
                    mission_id=self.mission_id,
                    names=names,
                )

                response = self.exec_grpc_query("GetCostsByNames", request)
                return [CostData.model_validate(json_format.MessageToDict(cost)) for cost in response.costs]

            if cost_type:
                request = cost_pb2.GetCostsByCostTypeRequest(
                    mission_id=self.mission_id,
                    cost_type=cost_type,
                )
                response = self.exec_grpc_query("GetCostsByCostType", request)
                return [CostData.model_validate(json_format.MessageToDict(cost)) for cost in response.costs]
        msg = "At least one of names or cost_type must be provided"
        logger.error(msg)
        raise CostServiceError(msg)

    def get_all(self) -> list[CostData]:
        """Get all costs for the mission.

        Returns:
            list[CostData]: The list of all costs
        """
        with self._handle_grpc_errors("GetCostsByMission"):
            request = cost_pb2.GetCostsByMissionRequest(
                mission_id=self.mission_id,
            )
            response = self.exec_grpc_query("GetCostsByMission", request)
            return [CostData.model_validate(json_format.MessageToDict(cost)) for cost in response.costs]
