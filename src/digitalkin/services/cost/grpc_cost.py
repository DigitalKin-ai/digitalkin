"""This module implements the default Cost strategy."""

import logging
from typing import Any

from digitalkin_proto.digitalkin.cost.v1 import cost_pb2, cost_service_pb2_grpc

from digitalkin.grpc.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc.utils.models import ServerConfig
from digitalkin.services.cost.cost_strategy import CostData, CostStrategy

logger = logging.getLogger(__name__)


class GrpcCost(CostStrategy, GrpcClientWrapper):
    """This class implements the default Cost strategy."""

    def __post_init__(self, config: ServerConfig, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        channel = self._init_channel(config)
        self.stub = cost_service_pb2_grpc.CostServiceStub(channel)
        logger.info("Channel client 'Cost' initialized succesfully")

    def add_cost(self, data: dict[str, Any]) -> str:
        """Create a new record in the cost database.

        Returns:
            str: The ID of the new record
        """
        # validate the incoming data
        cost = CostData(**data)

        request = cost_pb2.AddCostRequest(**cost)
        return self.exec_grpc_query("AddCost", request)

    def _get_costs_by_name(self, data: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByNameRequest(mission_id=data["mission_id"], name=data["name"])
        response = self.exec_grpc_query("GetCostsByName", request)
        return [
            CostData(
                cost=data.cost,
                mission_id=data.mission_id,
                name=data.name,
                type=data.type,
                unit=data.unit,
            )
            for data in response.costs
        ]

    def _get_costs_by_mission(self, data: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByMissionRequest(mission_id=data["mission_id"])
        response = self.exec_grpc_query("GetCostsByMission", request)
        return [
            CostData(
                cost=data.cost,
                mission_id=data.mission_id,
                name=data.name,
                type=data.type,
                unit=data.unit,
            )
            for data in response.costs
        ]

    def _get_costs_by_type(self, data: dict[str, Any]) -> list[CostData]:
        request = cost_pb2.GetCostsByTypeRequest(mission_id=data["mission_id"], type=data["type"])
        response = self.exec_grpc_query("GetCostsBytype", request)
        return [
            CostData(
                cost=data.cost,
                mission_id=data.mission_id,
                name=data.name,
                type=data.type,
                unit=data.unit,
            )
            for data in response.costs
        ]

    def get(self, data: dict[str, Any]) -> list[CostData]:
        """Get records from the database.

        Returns:
            list[CostData]: The list of records
        """
        if "mission_id" not in data:
            return []
        if "name" in data:
            return self._get_costs_by_name(data)
        if "type" in data:
            return self._get_costs_by_type(data)
        return self._get_costs_by_mission(data)
