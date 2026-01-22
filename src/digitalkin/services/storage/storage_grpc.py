"""This module implements the default storage strategy."""

from uuid import uuid4

from agentic_mesh_protocol.storage.v1 import storage_dto_pb2, storage_messages_pb2, storage_service_pb2_grpc
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.storage.storage_models import DataType, StorageRecord
from digitalkin.services.storage.storage_strategy import (
    StorageServiceError,
    StorageStrategy,
)


class GrpcStorage(StorageStrategy, GrpcClientWrapper):
    """This class implements the default storage strategy."""

    def __init__(
            self,
            mission_id: str,
            setup_id: str,
            setup_version_id: str,
            config: dict[str, type[BaseModel]],
            client_config: ClientConfig,
            **kwargs,  # noqa: ANN003, ARG002
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

    def update(self, collection: str, record_id: str, data: BaseModel) -> StorageRecord | None:
        data = self._validate_data(collection, {**data, "mission_id": self.mission_id})
        try:
            struct = Struct()
            struct.update(data.model_dump())
            req = storage_dto_pb2.UpdateRecordRequest(
                data=struct,
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            resp = self.exec_grpc_query("UpdateRecord", req)
            return self._build_record_from_proto(resp.result.record)
        except Exception:
            logger.warning("gRPC UpdateRecord failed for %s:%s", collection, record_id)
            return None

    def delete(self, collection: str, record_id: str) -> bool:
        try:
            req = storage_dto_pb2.DeleteRecordRequest(
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            self.exec_grpc_query("DeleteRecord", req)
            return True
        except Exception:
            logger.warning(
                "gRPC DeleteRecord failed for %s:%s",
                collection,
                record_id,
            )
            return False

    def get(self, collection: str, record_id: str) -> StorageRecord | None:
        try:
            req = storage_dto_pb2.GetRecordRequest(
                mission_id=self.mission_id,
                collection=collection,
                record_id=record_id,
            )
            resp = self.exec_grpc_query("GetRecord", req)
            return self._build_record_from_proto(resp.result.record)
        except Exception:
            logger.warning("gRPC GetRecord failed for %s:%s", collection, record_id)
            return None

    def list(self, collection: str) -> list[StorageRecord]:
        try:
            req = storage_dto_pb2.ListRecordsRequest(
                mission_id=self.mission_id,
                collection=collection,
            )
            resp = self.exec_grpc_query("ListRecords", req)
            return [self._build_record_from_proto(r.record) for r in resp.result]
        except Exception:
            logger.warning("gRPC ListRecords failed for %s", collection)
            return []

    def create(
            self, collection: str, record_id: str | None, data: BaseModel, data_type: DataType = DataType.OUTPUT
    ) -> StorageRecord:
        if not isinstance(data_type, DataType):
            msg = f"Invalid data type '{data_type}'. Must be one of {list(DataType.__members__.keys())}"
            raise ValueError(msg)
        validated_data = self._validate_data(collection, {**data, "mission_id": self.mission_id})
        try:
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
            resp = self.exec_grpc_query("CreateRecord", req)
            return self._build_record_from_proto(resp.result.record)
        except Exception as e:
            logger.exception(
                "gRPC CreateRecord failed for %s:%s",
                record.collection,
                record.record_id,
            )
            raise StorageServiceError(str(e)) from e

    def delete_collection(self, collection: str) -> bool:
        try:
            req = storage_dto_pb2.DeleteCollectionRequest(
                mission_id=self.mission_id,
                collection=collection,
            )
            self.exec_grpc_query("DeleteCollection", req)
        except Exception:
            logger.warning("gRPC DeleteCollection failed for %s", collection)
            return False
        return True
