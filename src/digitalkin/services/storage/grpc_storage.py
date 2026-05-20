"""This module implements the default storage strategy."""

from agentic_mesh_protocol.storage.v1 import data_pb2, storage_service_pb2_grpc
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.storage.storage_strategy import (
    DataType,
    StorageRecord,
    StorageServiceError,
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
        ctx = proto.context
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
            context=ctx,
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
                context=record.context,
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

    async def _read(self, collection: str, record_id: str, context: str) -> StorageRecord | None:
        """Fetch a single document scoped to a specific context.

        Returns:
            StorageData: The record
        """
        logger.debug("debug:_read context=%s collection=%s id=%s", context, collection, record_id)
        try:
            req = data_pb2.ReadRecordRequest(
                context=context,
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
        context: str,
    ) -> StorageRecord | None:
        """Overwrite a document via gRPC scoped to a specific context.

        Returns:
            StorageRecord: The updated record, or None on failure.
        """
        logger.debug("debug:_update context=%s collection=%s id=%s", context, collection, record_id)
        try:
            struct = Struct()
            struct.update(data.model_dump())
            req = data_pb2.UpdateRecordRequest(
                data=struct,
                context=context,
                collection=collection,
                record_id=record_id,
            )
            resp = await self.exec_grpc_query("UpdateRecord", req)
            return self._build_record_from_proto(resp.stored_data)
        except Exception:
            logger.warning("gRPC UpdateRecord failed for %s:%s", collection, record_id)
            return None

    async def _remove(self, collection: str, record_id: str, context: str) -> bool:
        """Delete a document via gRPC scoped to a specific context.

        Returns:
            bool: True if the record was deleted, False otherwise.
        """
        logger.debug("debug:_remove context=%s collection=%s id=%s", context, collection, record_id)
        try:
            req = data_pb2.RemoveRecordRequest(
                context=context,
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

    async def _list(self, collection: str, context: str) -> list[StorageRecord]:
        """List all documents in a collection via gRPC scoped to a specific context.

        Returns:
            list[StorageRecord]: The records found, or an empty list on failure.
        """
        logger.debug("debug:_list context=%s collection=%s", context, collection)
        try:
            req = data_pb2.ListRecordsRequest(
                context=context,
                collection=collection,
            )
            resp = await self.exec_grpc_query("ListRecords", req)
            return [self._build_record_from_proto(r) for r in resp.records]
        except Exception:
            logger.warning("gRPC ListRecords failed for %s", collection)
            return []

    async def _remove_collection(self, collection: str, context: str) -> bool:
        """Delete an entire collection via gRPC scoped to a specific context.

        Returns:
            bool: True if the collection was removed, False otherwise.
        """
        try:
            req = data_pb2.RemoveCollectionRequest(
                context=context,
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

        channel = self._init_channel(client_config)
        self.stub = storage_service_pb2_grpc.StorageServiceStub(channel)
        logger.debug("Channel client 'storage' initialized successfully")
