"""This module implements the default storage strategy."""

import datetime
import logging
from enum import Enum, auto
from typing import Any

import grpc
from digitalkin_proto.digitalkin.storage.v2 import data_pb2, storage_service_pb2_grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from digitalkin.grpc.utils.exceptions import ServerError
from digitalkin.grpc.utils.models import SecurityMode, ServerConfig
from digitalkin.services.storage.storage_strategy import StorageStrategy

logger = logging.getLogger(__name__)


class DataType(Enum):
    """."""

    OUTPUT = auto()
    VIEW = auto()


class StorageData(BaseModel):
    """."""

    data: dict[str, Any]
    mission_id: str
    name: str
    timestamp: datetime.datetime
    type: DataType


class GrpcStorage(StorageStrategy):
    """This class implements the default storage strategy."""

    def _init_channel(self, config: ServerConfig) -> grpc.Channel:
        """Create an appropriate channel to the registry server.

        Returns:
            A gRPC channel for communication with the registry.

        Raises:
            ValueError: If credentials are required but not provided.
        """
        if config.security == SecurityMode.SECURE and config.credentials:
            # Secure channel
            with open(config.credentials.server_cert_path, "rb") as cert_file:  # noqa: FURB101
                certificate_chain = cert_file.read()

            root_certificates = None
            if config.credentials.root_cert_path:
                with open(config.credentials.root_cert_path, "rb") as root_cert_file:  # noqa: FURB101
                    root_certificates = root_cert_file.read()

            # Create channel credentials
            channel_credentials = grpc.ssl_channel_credentials(root_certificates=root_certificates or certificate_chain)

            return grpc.secure_channel(f"{config.host}:{config.port}", channel_credentials)
        # Insecure channel
        return grpc.insecure_channel(f"{config.host}:{config.port}")

    def __post_init__(self, config: ServerConfig) -> None:
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        channel = self._init_channel(config)
        self.stub = storage_service_pb2_grpc.StorageServiceStub(channel)
        logger.info("Channel client 'storage' initialized succesfully")

    def exec_grpc_query(self, query_endpoint: str, request: Any) -> Any:
        """."""
        try:
            # Call the register method
            logger.warning("send request to %s", query_endpoint)
            response = getattr(self.stub, query_endpoint)(request)
            logger.warning("recive response from request to registry: %s", response)

            if response.success:
                logger.info("Module registered successfully")
            else:
                logger.error("Module registration failed")
                return response
        except grpc.RpcError:
            logger.exception("RPC error during registration:")
            raise ServerError

    def connect(self) -> bool:  # noqa: PLR6301
        """Establish connection to the database.

        Returns:
            bool: True if the connection is successful, False otherwise
        """
        return True

    def disconnect(self) -> bool:  # noqa: PLR6301
        """Close connection to the database.

        Returns:
            bool: True if the connection is closed, False otherwise
        """
        return True

    def create(self, table: str, data: dict[str, Any]) -> str:
        """Create a new record in the database.

        Returns:
            str: The ID of the new record
        """
        # Create a Struct for the data
        data_struct = Struct()
        if data.get("data"):
            data_struct.update(data["data"])

        request = data_pb2.StoreDataRequest(
            data=data_struct,
            mission_id=data["mission_id"],
            name=data["name"],
            type=data_pb2.DataType.Name(1),
        )
        return self.exec_grpc_query("StoreData", request)

    def get_data_by_mission(self, table: str, data: dict[str, Any]):
        request = data_pb2.GetDataByMissionRequest(mission_id=data["mission_id"])
        response = self.exec_grpc_query("GetDataByMission", request)
        return [
            StorageData(
                data=data.data,
                mission_id=data.mission_id,
                name=data.name,
                timestamp=data.timestamp,
                type=data.type,
            )
            for data in response.data_items
        ]

    def get_data_by_name(self, table: str, data: dict[str, Any]):
        request = data_pb2.GetDataByNameRequest(mission_id=data["mission_id"], name=data_pb2.DataType())
        response = self.exec_grpc_query("GetDataByName", request)
        return [
            StorageData(
                data=data.data,
                mission_id=data.mission_id,
                name=data.name,
                timestamp=data.timestamp,
                type=data.type,
            )
            for data in response.stored_data
        ]

    def get_data_by_type(self, table: str, data: dict[str, Any]):
        request = data_pb2.GetDataByTypeRequest(
            mission_id=data["mission_id"],
            type=data_pb2.DataType.Name(data["type"]),
        )
        response = self.exec_grpc_query("GetDataByType", request)
        return [
            StorageData(
                data=json_format.MessageToDict(data.data),
                mission_id=data.mission_id,
                name=data.name,
                timestamp=data.timestamp,
                type=data.type,
            )
            for data in response.stored_data
        ]

    def delete(self, table: str, data: dict[str, Any]) -> int:
        """Delete records from the database.

        Returns:
            int: The number of records deleted
        """
        request = data_pb2.DeleteDataRequest(
            mission_id=data["mission_id"],
            name=data_pb2.DataType.Name(data["name"]),
        )
        return self.exec_grpc_query("DeleteData", request)

    def get(self, table: str, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Get records from the database.

        Returns:
            list[dict[str, Any]]: The list of records
        """
        return []

    def update(self, table: str, data: dict[str, Any]) -> int:
        """Update records in the database.

        Returns:
            int: The number of records updated
        """
        return 1

    def get_all(self) -> dict[str, list[dict[str, Any]]]:
        """Get all records from the database.

        Returns:
            dict[str, list[dict[str, Any]]]: table with respective list of records
        """
        return {}
