"""This module implements the default storage strategy."""

from agentic_mesh_protocol.storage.v1 import data_pb2, storage_service_pb2_grpc
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.storage import DataType
from digitalkin.services.storage.exceptions import StorageServiceError
from digitalkin.services.storage.storage_strategy import (
    StorageRecord,
    StorageStrategy,
)
from digitalkin.utils.proto_utils import proto_to_dict


class GrpcStorage(StorageStrategy, GrpcClientWrapper):
    """gRPC client implementation for the Storage service."""

    service_name: str = "StorageService"

    def _build_record_from_proto(self, proto: data_pb2.StorageRecord) -> StorageRecord:
        """Convert a protobuf StorageRecord message into our Pydantic model.

        Uses direct field access for scalar fields and selective MessageToDict
        only for the nested Struct payload, avoiding full-message deserialization.

        Args:
            proto: gRPC StorageRecord

        Returns:
            A fully validated StorageRecord.
        """
        # Direct field access for scalars (avoids full MessageToDict overhead)
        mission = proto.mission_id
        coll = proto.collection
        rid = proto.record_id
        dtype = DataType[data_pb2.DataType.Name(proto.data_type)]

        # Selective deserialization: only the nested Struct payload
        payload = proto_to_dict(proto.data) if proto.HasField("data") else {}

        # Timestamp conversion
        creation_date = proto.creation_date.ToDatetime() if proto.HasField("creation_date") else None
        update_date = proto.update_date.ToDatetime() if proto.HasField("update_date") else None

        validated = self._validate_data(coll, payload)
        return StorageRecord(
            mission_id=mission,
            collection=coll,
            record_id=rid,
            data=validated,
            data_type=dtype,
            creation_date=creation_date,
            update_date=update_date,
        )

    async def _store(self, record: StorageRecord) -> StorageRecord:
        """Create a new record in the database.

        Parameters:
            record: The record to store

        Returns:
            StorageRecord: The corresponding record

        Raises:
            StorageServiceError: If there is an error while storing the record
        """
        logger.debug("debug:_store collection=%s id=%s", record.collection, record.record_id)
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
            resp = await self.exec_grpc_query("StoreRecord", req)
            return self._build_record_from_proto(resp.stored_data)
        except Exception as e:
            logger.exception(
                "gRPC StoreRecord failed for %s:%s",
                record.collection,
                record.record_id,
            )
            raise StorageServiceError(str(e)) from e

    async def _read(self, collection: str, record_id: str) -> StorageRecord | None:
        """Fetch a single document by collection + record_id.

        Returns:
            StorageData: The record
        """
        logger.debug("debug:_read collection=%s id=%s", collection, record_id)
        try:
            req = data_pb2.ReadRecordRequest(
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            resp = await self.exec_grpc_query("ReadRecord", req)
            return self._build_record_from_proto(resp.stored_data)
        except Exception:
            logger.debug("gRPC ReadRecord failed for %s:%s", collection, record_id)
            return None

    async def _update(
        self,
        collection: str,
        record_id: str,
        data: BaseModel,
    ) -> StorageRecord | None:
        """Overwrite a document via gRPC.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record
            data: The validated data model

        Returns:
            StorageRecord: The updated record
        """
        logger.debug("debug:_update collection=%s id=%s", collection, record_id)
        try:
            struct = Struct()
            struct.update(data.model_dump())
            req = data_pb2.UpdateRecordRequest(
                data=struct,
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            resp = await self.exec_grpc_query("UpdateRecord", req)
            return self._build_record_from_proto(resp.stored_data)
        except Exception:
            logger.warning("gRPC UpdateRecord failed for %s:%s", collection, record_id)
            return None

    async def _remove(self, collection: str, record_id: str) -> bool:
        """Delete a document via gRPC.

        Args:
            collection: The unique name for the record type
            record_id: The unique ID for the record

        Returns:
            bool: True if the record was deleted, False otherwise
        """
        logger.debug("debug:_remove collection=%s id=%s", collection, record_id)
        try:
            req = data_pb2.RemoveRecordRequest(
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            await self.exec_grpc_query("RemoveRecord", req)
        except Exception:
            logger.warning(
                "gRPC RemoveRecord failed for %s:%s",
                collection,
                record_id,
            )
            return False
        return True

    async def _list(self, collection: str) -> list[StorageRecord]:
        """List all documents in a collection via gRPC.

        Args:
            collection: The unique name for the record type

        Returns:
            list[StorageRecord]: A list of storage records
        """
        logger.debug("debug:_list collection=%s", collection)
        try:
            req = data_pb2.ListRecordsRequest(
                mission_id=self.mission_id,
                collection=collection,
            )
            resp = await self.exec_grpc_query("ListRecords", req)
            return [self._build_record_from_proto(r) for r in resp.records]
        except Exception:
            logger.warning("gRPC ListRecords failed for %s", collection)
            return []

    async def _remove_collection(self, collection: str) -> bool:
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
            await self.exec_grpc_query("RemoveCollection", req)
        except Exception:
            logger.warning("gRPC RemoveCollection failed for %s", collection)
            return False
        return True

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, type[BaseModel]],
        client_config: ClientConfig,
    ) -> None:
        """Initialize the storage."""
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=config)

        self._init_channel(client_config)
        self.stub = self._get_or_create_stub(storage_service_pb2_grpc.StorageServiceStub)
        logger.debug("Channel client 'storage' initialized successfully")
