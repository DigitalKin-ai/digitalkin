"""This module implements the default storage strategy."""

import logging
from typing import Any

from digitalkin_proto.digitalkin.storage.v2 import data_pb2, storage_service_pb2_grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from pydantic import ValidationError

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

    def create(self, storage_dict: dict[str, Any]) -> str:
        """Create a new record in the database.

        Required arguments:
            data: Object representation of StorageData

        Returns:
            str: The ID of the new record
        """
        try:
            valid_data = StorageData.model_validate(storage_dict["data"])  # Revalidates instance
        except ValidationError:
            logger.exception("Validation failed for model StorageData")
            return ""
        except KeyError:
            logger.exception("Missing mandatory 'data' in dict.")
            return ""

        # Create a Struct for the data
        data_struct = Struct()
        data_struct.update(valid_data.data)

        request = data_pb2.StoreDataRequest(
            data=data_struct,
            mission_id=storage_dict["data"].mission_id,
            name=storage_dict["data"].name,
            type=storage_dict["data"].type.name,
        )
        return self.exec_grpc_query("StoreData", request)

    def _get_data_by_mission(self, storage_dict: dict[str, Any]) -> StorageData:
        request = data_pb2.GetDataByMissionRequest(mission_id=storage_dict["mission_id"])
        response = self.exec_grpc_query("GetDataByMission", request)
        return StorageData(**json_format.MessageToDict(response.stored_data))

    def _get_data_by_name(self, storage_dict: dict[str, Any]) -> StorageData:
        request = data_pb2.GetDataByNameRequest(mission_id=storage_dict["mission_id"], name=storage_dict["name"])
        response = self.exec_grpc_query("GetDataByName", request)
        return StorageData(**json_format.MessageToDict(response.stored_data))

    def _get_data_by_type(self, storage_dict: dict[str, Any]) -> StorageData:
        request = data_pb2.GetDataByTypeRequest(mission_id=storage_dict["mission_id"], type=storage_dict["type"])
        response = self.exec_grpc_query("GetDataByType", request)
        return StorageData(**json_format.MessageToDict(response.stored_data))

    def get(self, storage_dict: dict[str, Any]) -> list[StorageData]:
        """Get records from the database.

        Returns:
            list[StorageData]: The list of records
        """
        if "mission_id" not in storage_dict:
            return []
        if "name" in storage_dict:
            return [self._get_data_by_name(storage_dict)]
        if "type" in storage_dict:
            return [self._get_data_by_type(storage_dict)]
        return [self._get_data_by_mission(storage_dict)]

    def update(self, storage_dict: dict[str, Any]) -> int:  # noqa: PLR6301
        """Update records in the database.

        Returns:
            int: The number of records updated
        """
        logger.warning("Not implemented in gRPC Storage. %s", storage_dict)
        return 1

    def delete(self, storage_dict: dict[str, Any]) -> int:
        """Delete records from the database.

        Returns:
            int: The number of records deleted
        """
        request = data_pb2.DeleteDataRequest(
            mission_id=storage_dict["mission_id"],
            name=data_pb2.DataType.Name(storage_dict["name"]),
        )
        return self.exec_grpc_query("DeleteData", request)
