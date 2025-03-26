"""This module implements the default storage strategy."""

import logging
from typing import Any

# import grpc
from digitalkin_proto.digitalkin.storage.v2 import data_pb2, storage_service_pb2_grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from digitalkin.grpc.utils.grpc_client_wrapper import GrpcClientWrapper

from digitalkin.grpc.utils.models import ServerConfig
from digitalkin.services.storage.storage_strategy import StorageData, StorageStrategy

logger = logging.getLogger(__name__)


class GrpcStorage(StorageStrategy, GrpcClientWrapper):
    """This class implements the default storage strategy."""

    def __post_init__(self, config: ServerConfig) -> None:
        """Init the channel from a config file.

        Need to be call if the user register a gRPC channel.
        """
        channel = self._init_channel(config)
        self.stub = storage_service_pb2_grpc.StorageServiceStub(channel)
        logger.info("Channel client 'storage' initialized succesfully")

    def create(self, data: dict[str, Any]) -> str:
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

    def _get_data_by_mission(self, data: dict[str, Any]) -> StorageData:
        request = data_pb2.GetDataByMissionRequest(mission_id=data["mission_id"])
        response = self.exec_grpc_query("GetDataByMission", request)
        return StorageData(**json_format.MessageToDict(response.stored_data))

    def _get_data_by_name(self, data: dict[str, Any]) -> StorageData:
        request = data_pb2.GetDataByNameRequest(mission_id=data["mission_id"], name=data["name"])
        response = self.exec_grpc_query("GetDataByName", request)
        return StorageData(**json_format.MessageToDict(response.stored_data))

    def _get_data_by_type(self, data: dict[str, Any]) -> StorageData:
        request = data_pb2.GetDataByTypeRequest(
            mission_id=data["mission_id"],
            type=data["type"]
        )
        response = self.exec_grpc_query("GetDataByType", request)
        return StorageData(**json_format.MessageToDict(response.stored_data))


    def get(self, data: dict[str, Any]) -> list[StorageData]:
        """Get records from the database.

        Returns:
            list[dict[str, Any]]: The list of records
        """
        if "mission_id" not in data:
            return []
        if "name" in data:
            return [self._get_data_by_name(data)]
        elif "type" in data:
            return [self._get_data_by_type(data)]
        return [self._get_data_by_mission(data)]

    def update(self, data: dict[str, Any]) -> int:
        """Update records in the database.

        Returns:
            int: The number of records updated
        """
        logger.warning("Not implemented in gRPC Storage.")
        return 1

    def delete(self, data: dict[str, Any]) -> int:
        """Delete records from the database.

        Returns:
            int: The number of records deleted
        """
        request = data_pb2.DeleteDataRequest(
            mission_id=data["mission_id"],
            name=data_pb2.DataType.Name(data["name"]),
        )
        return self.exec_grpc_query("DeleteData", request)
