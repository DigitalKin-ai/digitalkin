"""This module implements the gRPC Cost strategy."""

from typing import Literal

from agentic_mesh_protocol.cost.v1 import cost_pb2, cost_service_pb2_grpc
from google.protobuf import json_format

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.cost import AmountLimit, QuantityLimit
from digitalkin.services.cost.cost_strategy import (
    CostConfig,
    CostData,
    CostServiceError,
    CostStrategy,
    CostType,
)


class GrpcCost(CostStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC client implementation for the Cost service."""

    service_name: str = "CostService"

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, CostConfig],
        client_config: ClientConfig,
    ) -> None:
        """Initialize the cost."""
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id)
        self.config = config
        self._limits: dict[str, QuantityLimit | AmountLimit] = {}
        self._accumulated: dict[str, float] = {}
        channel = self._init_channel(client_config)
        self.stub = cost_service_pb2_grpc.CostServiceStub(channel)
        logger.debug("Channel client 'Cost' initialized successfully")

    def set_limits(self, limits: list[QuantityLimit | AmountLimit]) -> None:
        """Set cost limits for this session.

        Args:
            limits: List of CostLimit objects to enforce.
        """
        self._limits = {limit.name: limit for limit in limits}
        self._accumulated = {}

    def check_limit(self, cost_config_name: str, quantity: float) -> bool:
        """Check if adding this cost would exceed any limits.

        Args:
            cost_config_name: Name of the cost config.
            quantity: Quantity to add.

        Returns:
            True if within limits, False if would exceed.
        """
        limit = self._limits.get(cost_config_name)
        if limit is None:
            return True

        cost_config = self.config.get(cost_config_name)
        if cost_config is None:
            return True

        if limit.limit_type == "quantity":
            current = self._accumulated.get(f"{cost_config_name}_quantity", 0)
            return current + quantity <= limit.max_value

        current = self._accumulated.get(f"{cost_config_name}_amount", 0)
        projected = cost_config.rate * quantity
        return current + projected <= limit.max_value

    def add(
        self,
        name: str,
        cost_config_name: str,
        quantity: float,
    ) -> None:
        """Create a new record in the cost database.

        Args:
            name: The name of the cost
            cost_config_name: The name of the cost config
            quantity: The quantity of the cost

        Raises:
            CostServiceError: If the cost config is invalid
        """
        with self.handle_grpc_errors("AddCost", CostServiceError):
            cost_config = self.config.get(cost_config_name)
            if cost_config is None:
                msg = f"Cost config {cost_config_name} not found in the configuration."
                logger.error(msg)
                raise CostServiceError(msg)
            valid_data = CostData.model_validate({
                "name": name,
                "cost": cost_config.rate * quantity,
                "unit": cost_config.unit,
                "cost_type": CostType[cost_config.cost_type],
                "mission_id": self.mission_id,
                "rate": cost_config.rate,
                "quantity": quantity,
                "setup_version_id": self.setup_version_id,
            })
            request = cost_pb2.AddCostRequest(
                cost=valid_data.cost,
                name=valid_data.name,
                unit=valid_data.unit,
                cost_type=valid_data.cost_type.name,
                mission_id=valid_data.mission_id,
                rate=valid_data.rate,
                quantity=valid_data.quantity,
                setup_version_id=valid_data.setup_version_id,
            )
            self.exec_grpc_query("AddCost", request)
            logger.debug("Cost added with cost_dict: %s", valid_data.model_dump())

    def get(self, name: str) -> list[CostData]:
        """Get a record from the database.

        Args:
            name: The name of the cost

        Returns:
            CostData: The cost data
        """
        with self.handle_grpc_errors("GetCost", CostServiceError):
            request = cost_pb2.GetCostRequest(name=name, mission_id=self.mission_id)
            response: cost_pb2.GetCostResponse = self.exec_grpc_query("GetCost", request)
            cost_data_list = [
                json_format.MessageToDict(
                    cost,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                for cost in response.costs
            ]
            logger.debug("Costs retrieved with cost_dict: %s", cost_data_list)
            return [CostData.model_validate(cost_data) for cost_data in cost_data_list]

    def get_filtered(
        self,
        names: list[str] | None = None,
        cost_types: list[Literal["TOKEN_INPUT", "TOKEN_OUTPUT", "API_CALL", "STORAGE", "TIME", "OTHER"]] | None = None,
    ) -> list[CostData]:
        """Get a list of records from the database.

        Args:
            names: The names of the costs
            cost_types: The types of the costs

        Returns:
            list[CostData]: The cost data
        """
        with self.handle_grpc_errors("GetCosts", CostServiceError):
            request = cost_pb2.GetCostsRequest(
                mission_id=self.mission_id,
                filter=cost_pb2.CostFilter(
                    names=names or [],
                    cost_types=cost_types or [],
                ),
            )
            response: cost_pb2.GetCostsResponse = self.exec_grpc_query("GetCosts", request)
            cost_data_list = [
                json_format.MessageToDict(
                    cost,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                for cost in response.costs
            ]
            logger.debug("Filtered costs retrieved with cost_dict: %s", cost_data_list)
            return [CostData.model_validate(cost_data) for cost_data in cost_data_list]

    def get_cost_config(self) -> list[CostConfig]:
        """Get cost configuration from the database.

        Returns:
            List of CostConfig objects from the database.
        """
        with self.handle_grpc_errors("GetCostConfig", CostServiceError):
            request = cost_pb2.GetCostConfigRequest(setup_version_id=self.setup_version_id)
            response: cost_pb2.GetCostConfigResponse = self.exec_grpc_query("GetCostConfig", request)
            config_list = []
            for config in response.configs:
                config_dict = json_format.MessageToDict(
                    config,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                # Map proto field names to CostConfig field names
                config_list.append(
                    CostConfig(
                        cost_name=config_dict.get("name", ""),
                        cost_type=config_dict.get("cost_type", "OTHER"),
                        description=config_dict.get("description"),
                        unit=config_dict.get("unit", ""),
                        rate=config_dict.get("rate", 0.0),
                    )
                )
            logger.debug("Cost configs retrieved: %s", config_list)
            return config_list

    def set_cost_config(self, configs: list[CostConfig]) -> bool:
        """Store cost configuration in the database.

        Args:
            configs: List of CostConfig objects to store.

        Returns:
            True if successfully stored.
        """
        with self.handle_grpc_errors("SetCostConfig", CostServiceError):
            proto_configs = [
                cost_pb2.CostConfig(
                    name=config.cost_name,
                    cost_type=config.cost_type,
                    description=config.description or "",
                    unit=config.unit,
                    rate=config.rate,
                )
                for config in configs
            ]
            request = cost_pb2.SetCostConfigRequest(
                setup_version_id=self.setup_version_id,
                configs=proto_configs,
            )
            response: cost_pb2.SetCostConfigResponse = self.exec_grpc_query("SetCostConfig", request)
            logger.debug("Cost configs stored, success: %s", response.success)
            return response.success
