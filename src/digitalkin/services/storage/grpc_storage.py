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

    def _build_record_from_proto(self, proto: data_pb2.StorageRecord) -> StorageRecord:
        """Convert a protobuf StorageRecord message into our Pydantic model.

        Args:
            proto: gRPC StorageRecord

        Returns:
            A fully validated StorageRecord.
        """
        raw = json_format.MessageToDict(
            proto,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        mission = raw["mission_id"]
        coll = raw["collection"]
        rid = raw["record_id"]
        dtype = DataType[raw["data_type"]]
        payload = raw.get("data", {})

        validated = self._validate_data(coll, payload)
        return StorageRecord(
            mission_id=mission,
            collection=coll,
            record_id=rid,
            data=validated,
            data_type=dtype,
            creation_date=raw.get("creation_date"),
            update_date=raw.get("update_date"),
        )

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
            data_struct = Struct()
            data_struct.update(record.data.model_dump())
            req = data_pb2.StoreRecordRequest(
                data=data_struct,
                mission_id=record.mission_id,
                collection=record.collection,
                record_id=record.record_id,
                data_type=record.data_type.name,
            )
            resp = self.exec_grpc_query("StoreRecord", req)
            return self._build_record_from_proto(resp.stored_data)
        except Exception as e:
            logger.exception("gRPC StoreRecord failed for %s:%s", record.collection, record.record_id)
            raise StorageServiceError(str(e)) from e

    def _read(self, collection: str, record_id: str) -> StorageRecord | None:
        """Fetch a single document by collection + record_id.

        Returns:
            StorageData: The record
        """
        try:
            req = data_pb2.ReadRecordRequest(
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            resp = self.exec_grpc_query("ReadRecord", req)
            return self._build_record_from_proto(resp.stored_data)
        except Exception:
            logger.exception("gRPC ReadRecord failed for %s:%s", collection, record_id)
            return None

    def _update(self, collection: str, record_id: str, data: BaseModel) -> StorageRecord | None:
        """Overwrite a document via gRPC.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record
            data: The validated data model

        Returns:
            StorageRecord: The updated record
        """
        try:
            struct = Struct()
            struct.update(data.model_dump())
            req = data_pb2.UpdateRecordRequest(
                data=struct,
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            resp = self.exec_grpc_query("UpdateRecord", req)
            return self._build_record_from_proto(resp.stored_data)
        except Exception:
            logger.exception("gRPC UpdateRecord failed for %s:%s", collection, record_id)
            return None

    def _remove(self, collection: str, record_id: str) -> bool:
        """Delete a document via gRPC.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record

        Returns:
            bool: True if the record was deleted, False otherwise
        """
        try:
            req = data_pb2.RemoveRecordRequest(
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            self.exec_grpc_query("RemoveRecord", req)
        except Exception:
            logger.exception("gRPC RemoveRecord failed for %s:%s", collection, record_id)
            return False
        return True

    def _list(self, collection: str) -> list[StorageRecord]:
        """List all documents in a collection via gRPC.

        Args:
            collection: The unique name for the record type

        Returns:
            list[StorageRecord]: A list of storage records
        """
        try:
            req = data_pb2.ListRecordsRequest(
                mission_id=self.mission_id,
                collection=collection,
            )
            resp = self.exec_grpc_query("ListRecords", req)
            return [self._build_record_from_proto(r) for r in resp.records]
        except Exception:
            logger.exception("gRPC ListRecords failed for %s", collection)
            return []

    def _remove_collection(self, collection: str) -> bool:
        """Delete an entire collection via gRPC.

        Args:
            collection: The unique name for the record type

        Returns:
            bool: True if the collection was deleted, False otherwise
        """
        try:
            req = data_pb2.RemoveCollectionRequest(
                mission_id=self.mission_id,
                collection=collection,
            )
            self.exec_grpc_query("RemoveCollection", req)
        except Exception:
            logger.exception("gRPC RemoveCollection failed for %s", collection)
            return False
        return True

    def __init__(
        self,
        mission_id: str,
        setup_version_id: str,
        config: dict[str, type[BaseModel]],
        client_config: ClientConfig,
        **kwargs,  # noqa: ANN003, ARG002
    ) -> None:
        """Initialize the storage."""
        super().__init__(mission_id=mission_id, setup_version_id=setup_version_id, config=config)

        channel = self._init_channel(client_config)
        self.stub = storage_service_pb2_grpc.StorageServiceStub(channel)
        logger.debug("Channel client 'storage' initialized succesfully")
