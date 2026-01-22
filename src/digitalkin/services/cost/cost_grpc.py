"""This module implements the gRPC Cost strategy."""

from agentic_mesh_protocol.cost.v1 import cost_dto_pb2
from agentic_mesh_protocol.cost.v1.cost_messages_pb2 import CostFilter as CostFilterProto
from agentic_mesh_protocol.cost.v1.cost_service_pb2_grpc import CostServiceStub
from google.protobuf import json_format

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.cost.cost_models import CostConfig, CostData, CostType
from digitalkin.services.cost.cost_strategy import (
    CostServiceError,
    CostStrategy,
)


class GrpcCost(CostStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """This class implements the default Cost strategy."""

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, CostConfig],
        client_config: ClientConfig,
    ) -> None:
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=config)
        channel = self._init_channel(client_config)
        self.stub = CostServiceStub(channel)
        logger.debug("Channel client 'Cost' initialized successfully")

    # ══════════════════════════════════ Publics Methods ═══════════════════════════════════ #

    def create(
        self,
        name: str,
        cost_config_name: str,
        quantity: float,
    ) -> None:
        with self.handle_grpc_errors("CreateCost", CostServiceError):
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
            self.exec_grpc_query("CreateCost", request)
            logger.debug("Cost added with cost_dict: %s", valid_data.model_dump())

    def list(
        self,
        names: list[str] | None = None,
            cost_types: list[CostType] | None = None,
    ) -> list[CostData]:
        with self.handle_grpc_errors("ListCosts", CostServiceError):
            request = cost_dto_pb2.ListCostsRequest(
                mission_id=self.mission_id,
                filter=CostFilterProto(
                    names=names or [],
                    types=[cost_type.to_proto() for cost_type in (cost_types or [])],
                ),
            )
            response: cost_dto_pb2.ListCostsResponse = self.exec_grpc_query("ListCosts", request)
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
