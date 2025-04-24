"""This module implements the default storage strategy."""

import logging

from digitalkin_proto.digitalkin.storage.v2 import data_pb2, storage_service_pb2_grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.models import ClientConfig
from digitalkin.services.storage.storage_strategy import DataType, StorageRecord, StorageServiceError, StorageStrategy

logger = logging.getLogger(__name__)


class GrpcStorage(StorageStrategy, GrpcClientWrapper):
    """This class implements the default storage strategy."""

    def __init__(
        self,
        mission_id: str,
        config: dict[str, type[BaseModel]],
        client_config: ClientConfig,
        **kwargs,  # noqa: ANN003, ARG002
    ) -> None:
        """Initialize the storage."""
        super().__init__(mission_id=mission_id, config=config)

        channel = self._init_channel(client_config)
        self.stub = storage_service_pb2_grpc.StorageServiceStub(channel)
        logger.info("Channel client 'storage' initialized succesfully")

    def _store(self, record: StorageRecord) -> StorageRecord:
        """Create a new record in the database.

        Parameters:
            record: The record to store

        Returns:
            StorageRecord: The corresponding record

        Raises:
            StorageServiceError: If there is an error while storing the record
        """
        try:
            # Create a Struct for the data
            data_struct = Struct()
            data_struct.update(record.data.model_dump())

            request = data_pb2.StoreRecordRequest(
                data=data_struct,
                mission_id=record.mission_id,
                name=record.name,
                data_type=record.data_type.name,
            )
            return self.exec_grpc_query("StoreRecord", request)
        except Exception:
            msg = f"Error while storing record {record.name}"
            logger.exception(msg)
            raise StorageServiceError(msg)

    def _read(self, name: str) -> StorageRecord | None:
        """Get records from the database.

        Returns:
            list[StorageData]: The list of records
        """
        try:
            request = data_pb2.ReadRecordRequest(mission_id=self.mission_id, name=name)
            response: data_pb2.ReadRecordResponse = self.exec_grpc_query("ReadRecord", request)
            response_dict = json_format.MessageToDict(
                response.stored_data,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )
            return StorageRecord(
                mission_id=response_dict["mission_id"],
                name=response_dict["name"],
                data_type=response_dict["data_type"],
                data=self._validate_data(name, response_dict["data"]),
            )
        except Exception:
            msg = f"Error while reading record {name}"
            logger.exception(msg)
            return None

    def _modify(self, name: str, data: BaseModel) -> StorageRecord | None:
        """Update records in the database.

        Returns:
            int: The number of records updated
        """
        try:
            # Create a Struct for the data
            data_struct = Struct()
            data_struct.update(data.model_dump())

            request = data_pb2.ModifyRecordRequest(data=data_struct, mission_id=self.mission_id, name=name)
            response: data_pb2.ModifyRecordResponse = self.exec_grpc_query("ModifyRecord", request)
            return self._build_record_from_proto(response.stored_data, name)
        except Exception:
            msg = f"Error while modifing record {name}"
            logger.exception(msg)
            return None

    def _remove(self, name: str) -> bool:
        """Delete records from the database.

        Returns:
            int: The number of records deleted
        """
        try:
            request = data_pb2.RemoveRecordRequest(
                mission_id=self.mission_id,
                name=name,
            )
            self.exec_grpc_query("RemoveRecord", request)
        except Exception:
            msg = f"Error while removin record {name}"
            logger.exception(msg)
            return False
        return True

    def _build_record_from_proto(self, stored_data: data_pb2.StorageRecord, default_name: str) -> StorageRecord:
        """Helper to construire un StorageRecord complet à partir du message gRPC.

        Args:
            stored_data: Le message gRPC contenant les données stockées.
            default_name: Le nom par défaut à utiliser si le nom n'est pas présent dans les données.

        Returns:
            StorageRecord: Un objet StorageRecord construit à partir des données stockées.
        """
        # Converti en dict, avec tous les champs même s'ils sont absents
        raw = json_format.MessageToDict(
            stored_data,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        # On récupère ou on complète les champs obligatoires
        name = raw.get("name", default_name)
        dtype = raw.get("data_type", DataType.OUTPUT.name)
        payload = raw.get("data", {})

        # Valide le modèle pydantic pour le champ `data`
        validated = self._validate_data(name, payload)

        return StorageRecord(
            mission_id=self.mission_id,
            name=name,
            data_type=DataType[dtype],
            data=validated,
            creation_date=raw.get("creation_date"),
            update_date=raw.get("update_date"),
        )
