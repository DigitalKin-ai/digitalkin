"""This module implements the default storage strategy."""

from typing import Any
from uuid import uuid4

from agentic_mesh_protocol.storage.v1 import storage_dto_pb2, storage_messages_pb2, storage_service_pb2_grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from digitalkin.exception.storage import StorageServiceError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.storage import DataType, StorageRecord
from digitalkin.services.storage.storage_strategy import (
    StorageStrategy,
)


class GrpcStorage(StorageStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """This class implements the default storage strategy."""

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, type[BaseModel]],
        client_config: ClientConfig,
        **_kwargs: Any,
    ) -> None:
        """Initialize the storage."""
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id, config=config)

        channel = self._init_channel(client_config)
        self.stub = storage_service_pb2_grpc.StorageServiceStub(channel)
        logger.debug("Channel client 'storage' initialized successfully")

    def _build_record_from_proto(self, proto: storage_messages_pb2.StorageRecord) -> StorageRecord:
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
        dtype = raw["data_type"]
        payload = raw.get("data", {})

        validated = self._validate_data(coll, payload)
        return StorageRecord(
            mission_id=mission,
            collection=coll,
            record_id=rid,
            data=validated,
            data_type=dtype,
            created_at=raw.get("created_at"),
            updated_at=raw.get("updated_at"),
        )

    # ════════════════════════════════ Public Method ═════════════════════════════════ #

    async def update(self, collection: str, record_id: str, data: BaseModel) -> StorageRecord | None:
        """Update a record via gRPC.

        Returns:
            The updated record.
        """
        data = self._validate_data(collection, {**data, "mission_id": self.mission_id})
        async with self.handle_grpc_errors("UpdateRecord", StorageServiceError):
            struct = Struct()
            struct.update(data.model_dump())
            req = storage_dto_pb2.UpdateRecordRequest(
                data=struct,
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            resp = await self.exec_grpc_query("UpdateRecord", req)
            return self._build_record_from_proto(resp.result.record)

    async def delete(self, collection: str, record_id: str) -> bool:
        """Delete a record via gRPC.

        Returns:
            True if record was deleted.
        """
        async with self.handle_grpc_errors("DeleteRecord", StorageServiceError):
            req = storage_dto_pb2.DeleteRecordRequest(
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            response = await self.exec_grpc_query("DeleteRecord", req)
            logger.debug("Delete '%s' query sent successfully", self.mission_id)
            return response.result.success

    async def get(self, collection: str, record_id: str) -> StorageRecord | None:
        """Retrieve a record via gRPC.

        Returns:
            The record, or None.
        """
        async with self.handle_grpc_errors("GetRecord", StorageServiceError):
            req = storage_dto_pb2.GetRecordRequest(
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            resp = await self.exec_grpc_query("GetRecord", req)
            return self._build_record_from_proto(resp.result.record)

    async def list(self, collection: str) -> list[StorageRecord]:
        """List all records in a collection via gRPC.

        Returns:
            List of records.
        """
        async with self.handle_grpc_errors("ListRecords", StorageServiceError):
            req = storage_dto_pb2.ListRecordsRequest(
                mission_id=self.mission_id,
                collection=collection,
            )
            resp = await self.exec_grpc_query("ListRecords", req)
            return [self._build_record_from_proto(r.record) for r in resp.result]

    async def create(
        self, collection: str, record_id: str | None, data: BaseModel, data_type: DataType = DataType.OUTPUT
    ) -> StorageRecord:
        """Create a record via gRPC.

        Returns:
            The created record.

        Raises:
            TypeError: If invalid data type.
        """
        if not isinstance(data_type, DataType):
            msg = f"Invalid data type '{data_type}'. Must be one of {list(DataType.__members__.keys())}"
            raise TypeError(msg)
        validated_data = self._validate_data(collection, {**data, "mission_id": self.mission_id})
        async with self.handle_grpc_errors("CreateRecord", StorageServiceError):
            data_struct = Struct()
            record = self._create_storage_record(collection, record_id or uuid4().hex, validated_data, data_type)
            data_struct.update(record.data.model_dump())
            req = storage_dto_pb2.CreateRecordRequest(
                data=data_struct,
                mission_id=record.mission_id,
                collection=record.collection,
                record_id=record.record_id,
                data_type=record.data_type.name,
            )
            resp = await self.exec_grpc_query("CreateRecord", req)
            return self._build_record_from_proto(resp.result.record)

    async def delete_collection(self, collection: str) -> bool:
        """Delete all records in a collection via gRPC.

        Returns:
            True if collection was deleted.
        """
        async with self.handle_grpc_errors("DeleteCollection", StorageServiceError):
            req = storage_dto_pb2.DeleteCollectionRequest(
                mission_id=self.mission_id,
                collection=collection,
            )
            resp = await self.exec_grpc_query("DeleteCollection", req)
            return resp.result.success
