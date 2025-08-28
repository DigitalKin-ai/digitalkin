"""This module implements the gRPC Cost strategy."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Literal

from digitalkin_proto.digitalkin.cost.v1 import cost_pb2, cost_service_pb2_grpc
from google.protobuf import json_format

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.models import ClientConfig
from digitalkin.logger import logger
from digitalkin.services.cost.cost_strategy import (
    CostConfig,
    CostData,
    CostServiceError,
    CostStrategy,
    CostType,
)


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
        except CostServiceError as e:
            msg = f"CostServiceError in {operation}: {e}"
            logger.exception(msg)
            raise CostServiceError(msg) from e
        except ServerError as e:
            msg = f"gRPC {operation} failed: {e}"
            logger.exception(msg)
            raise ServerError(msg) from e
        except Exception as e:
            msg = f"Unexpected error in {operation}"
            logger.exception(msg)
            raise CostServiceError(msg) from e

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, CostConfig],
        client_config: ClientConfig,
    ) -> None:
        """Initialize the cost."""
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=config)
        channel = self._init_channel(client_config)
        self.stub = cost_service_pb2_grpc.CostServiceStub(channel)
        logger.debug("Channel client 'Cost' initialized succesfully")

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
        with self._handle_grpc_errors("AddCost"):
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
        with self._handle_grpc_errors("GetCost"):
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
        with self._handle_grpc_errors("GetCosts"):
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
