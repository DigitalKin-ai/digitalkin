"""This module implements the default Cost strategy."""

import logging
from typing import Any

from digitalkin_proto.digitalkin.cost.v1 import cost_pb2, cost_service_pb2_grpc
from google.protobuf import json_format
from pydantic import ValidationError

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.models import ServerConfig
from digitalkin.services.cost.cost_strategy import CostData, CostStrategy

logger = logging.getLogger(__name__)


class GrpcCost(CostStrategy, GrpcClientWrapper):
    """This class implements the default Cost strategy."""

    def _get_costs_by_name(self, cost_dict: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByNameRequest(
            mission_id=cost_dict["mission_id"],
            name=cost_dict["name"],
        )
        response = self.exec_grpc_query("GetCostsByName", request)
        return [CostData(**json_format.MessageToDict(cost)) for cost in response.costs]

    def _get_costs_by_mission(self, cost_dict: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByMissionRequest(mission_id=cost_dict["mission_id"])
        response = self.exec_grpc_query("GetCostsByMission", request)
        return [CostData(**json_format.MessageToDict(cost)) for cost in response.costs]

    def _get_costs_by_type(self, cost_dict: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByTypeRequest(
            mission_id=cost_dict["mission_id"],
            type=cost_dict["type"],
        )
        response = self.exec_grpc_query("GetCostsBytype", request)
        return [CostData(**json_format.MessageToDict(cost)) for cost in response.costs]

    def __init__(self, mission_id: str, config: ServerConfig) -> None:
        """Initialize the cost."""
        super().__init__(mission_id)
        channel = self._init_channel(config)
        self.stub = cost_service_pb2_grpc.CostServiceStub(channel)
        logger.info("Channel client 'Cost' initialized succesfully")

    def add_cost(self, cost_dict: dict[str, Any]) -> str:
        """Create a new record in the cost database.

        Required arguments:
            data: Object representation of CostData

        Returns:
            str: The ID of the new record
        """
        try:
            valid_data = CostData.model_validate(cost_dict["data"])
        except ValidationError:
            logger.exception("Validation failed for model StorageData")
            return ""
        except KeyError:
            logger.exception("Missing mandatory 'data' in dict.")
            return ""

        request = cost_pb2.AddCostRequest(**valid_data.model_dump())
        return self.exec_grpc_query("AddCost", request)

    def get(self, cost_dict: dict[str, Any]) -> list[CostData]:
        """Get records from the database.

        Returns:
            list[CostData]: The list of records
        """
        if "mission_id" not in cost_dict:
            return []
        if "name" in cost_dict:
            return self._get_costs_by_name(cost_dict)
        if "type" in cost_dict:
            return self._get_costs_by_type(cost_dict)
        return self._get_costs_by_mission(cost_dict)
