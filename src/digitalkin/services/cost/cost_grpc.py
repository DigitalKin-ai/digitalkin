"""This module implements the gRPC Cost strategy."""

from agentic_mesh_protocol.cost.v1 import cost_dto_pb2, cost_messages_pb2, cost_service_pb2_grpc
from google.protobuf import json_format

from digitalkin.exception.cost import CostServiceError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.cost import AmountLimit, CostConfig, CostData, CostType, QuantityLimit
from digitalkin.services.cost.cost_strategy import (
    CostStrategy,
)
from digitalkin.utils.proto_utils import proto_to_dict


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
        super().__init__(
            mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=client_config
        )
        self.config = config
        self._limits: dict[str, QuantityLimit | AmountLimit] = {}
        self._accumulated: dict[str, float] = {}
        channel = self._init_channel(client_config)
        self.stub = cost_service_pb2_grpc.CostServiceStub(channel)
        logger.debug("Channel client 'Cost' initialized successfully")

    # ══════════════════════════════════ Publics Methods ═══════════════════════════════════ #

    async def create(
        self,
        name: str,
        cost_config_name: str,
        quantity: float,
    ) -> None:
        """Create a cost record via gRPC.

        Raises:
            CostServiceError: If cost config not found or gRPC error.
        """
        async with self.handle_grpc_errors("CreateCost", CostServiceError):
            cost_config = self.config.get(cost_config_name)
            if cost_config is None:
                msg = f"Cost config {cost_config_name} not found in the configuration."
                logger.error(msg)
                raise CostServiceError(msg)
            valid_data = CostData.model_validate({
                "name": name,
                "cost": cost_config.rate * quantity,
                "unit": cost_config.unit,
                "type": cost_config.type,
                "mission_id": self.mission_id,
                "rate": cost_config.rate,
                "quantity": quantity,
                "setup_version_id": self.setup_version_id,
            })
            request = cost_dto_pb2.CreateCostRequest(
                cost=valid_data.cost,
                name=valid_data.name,
                unit=valid_data.unit,
                type=valid_data.type.name,
                mission_id=valid_data.mission_id,
                rate=valid_data.rate,
                quantity=valid_data.quantity,
                setup_version_id=valid_data.setup_version_id,
            )
            await self.exec_grpc_query("CreateCost", request)
            logger.debug("Cost added with cost_dict: %s", valid_data.model_dump())

    async def list_config(self) -> list[CostConfig]:
        """Retrieve cost configs via gRPC.

        Returns:
            List of cost configurations.
        """
        async with self.handle_grpc_errors("ListCostConfig", CostServiceError):
            request = cost_dto_pb2.ListCostConfigRequest(setup_version_id=self.setup_version_id)
            response: cost_dto_pb2.ListCostConfigResponse = await self.exec_grpc_query("ListCostConfig", request)
            config_list = []
            for config in response.result:
                config_dict = json_format.MessageToDict(
                    config.config,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                # Map proto field names to CostConfig field names
                config_list.append(
                    CostConfig(
                        name=config_dict.get("name", ""),
                        type=config_dict.get("type", CostType.OTHER),
                        description=config_dict.get("description"),
                        unit=config_dict.get("unit", ""),
                        rate=config_dict.get("rate", 0.0),
                    )
                )
            logger.debug("Cost configs retrieved: %s", config_list)
            return config_list

    async def set_config(self, configs: list[CostConfig]) -> bool:
        """Store cost configs via gRPC.

        Returns:
            True if all configs stored successfully.
        """
        async with self.handle_grpc_errors("SetCostConfig", CostServiceError):
            proto_configs = [
                cost_messages_pb2.CostConfig(
                    name=config.name,
                    cost_type=config.type,
                    description=config.description or "",
                    unit=config.unit,
                    rate=config.rate,
                )
                for config in configs
            ]
            request = cost_dto_pb2.SetCostConfigRequest(
                setup_version_id=self.setup_version_id,
                configs=proto_configs,
            )
            response: cost_dto_pb2.SetCostConfigResponse = await self.exec_grpc_query("SetCostConfig", request)
            if not response.result:
                success = False
            else:
                success = all(getattr(result, "success", False) for result in response.result)
            logger.debug("Cost configs stored, success: %s", success)
            return success

    async def set_limits(self, limits: list[QuantityLimit | AmountLimit]) -> None:
        """Store cost limits in memory."""
        self._limits = {limit.name: limit for limit in limits}
        self._accumulated = {}

    async def check_limit(self, cost_config_name: str, quantity: float) -> bool:
        """Check if adding quantity would exceed configured limits.

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
            result = current + quantity <= limit.max_value
            logger.debug("debug:check_limit cost_config_name=%s type=quantity result=%s", cost_config_name, result)
            return result

        current = self._accumulated.get(f"{cost_config_name}_amount", 0)
        projected = cost_config.rate * quantity
        result = current + projected <= limit.max_value
        logger.debug("debug:check_limit cost_config_name=%s type=amount result=%s", cost_config_name, result)
        return result

    async def list(
        self,
        names: list[str] | None = None,
        cost_types: list[CostType] | None = None,
    ) -> list[CostData]:
        """List cost records filtered by names or types via gRPC.

        Returns:
            List of cost data matching filters.
        """
        async with self.handle_grpc_errors("ListCosts", CostServiceError):
            request = cost_dto_pb2.ListCostsRequest(
                mission_id=self.mission_id,
                filter=cost_messages_pb2.CostFilter(
                    names=names or [],
                    types=[cost_type.to_proto() for cost_type in (cost_types or [])],
                ),
            )
            response: cost_dto_pb2.ListCostsResponse = await self.exec_grpc_query("ListCosts", request)
            cost_data_list = [
                json_format.MessageToDict(
                    cost_result.cost,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                for cost_result in response.result
            ]
            logger.debug("Filtered costs retrieved with cost_dict: %s", cost_data_list)
            return [CostData.model_validate(cost_data) for cost_data in cost_data_list]
