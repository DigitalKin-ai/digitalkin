"""Mock Storage Servicer for testing the GrpcStorage service."""

import datetime
from typing import Any

import grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from agentic_mesh_protocol.storage.v1 import (
    storage_dto_pb2,
    storage_enums_pb2,
    storage_messages_pb2,
    storage_service_pb2_grpc,
)
from google.protobuf import json_format, struct_pb2
from pydantic import BaseModel, ValidationError

from digitalkin.logger import logger


class MockStorageServicer(storage_service_pb2_grpc.StorageServiceServicer):
    """Mock implementation of the Storage Service Servicer for testing.

    Missing required fields surface as an ``INVALID_ARGUMENT`` status (what the
    validation interceptor does); domain failures (duplicate, absent record) come
    back as an ``OperationError`` in the ``StorageResult`` outcome.
    """

    def __init__(self, schema_config: dict[str, type[BaseModel]] | None = None) -> None:
        """Initialize the mock servicer with empty storage.

        Args:
            schema_config: Dictionary mapping collection names to Pydantic model classes
        """
        super().__init__()
        # context -> collection -> record_id -> record_data
        self.records: dict[int, dict[str, dict[str, dict[str, Any]]]] = {}
        self.schema_config = schema_config or {}

    @staticmethod
    def _invalid(context: grpc.ServicerContext, *required: object) -> bool:
        """Flag a request missing a required field as INVALID_ARGUMENT.

        Args:
            context: gRPC context
            *required: The required field values of the request.

        Returns:
            True if the request was rejected.
        """
        if all(required):
            return False
        context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
        context.set_details("context, collection and record_id are required")
        return True

    @staticmethod
    def _error(identifier: str, code: str, message: str) -> storage_messages_pb2.StorageResult:
        """Build a StorageResult holding an OperationError.

        Args:
            identifier: The record identifier.
            code: Machine-readable error code.
            message: Human-readable message.

        Returns:
            The error result.
        """
        return storage_messages_pb2.StorageResult(
            identifier=identifier, error=bulk_pb2.OperationError(code=code, message=message)
        )

    def _schema_error(self, collection: str, data: dict[str, Any]) -> str:
        """Validate data against the collection schema when one is configured.

        Args:
            collection: The collection name
            data: The data to validate

        Returns:
            The validation error message, or an empty string when valid.
        """
        model_cls = self.schema_config.get(collection)
        if model_cls is None:
            return ""
        try:
            model_cls.model_validate(data)
        except ValidationError as e:
            return str(e)
        return ""

    @staticmethod
    def _create_proto_record(
        ctx: int,
        collection: str,
        record_id: str,
        record_data: dict[str, Any],
    ) -> storage_messages_pb2.StorageRecord:
        """Convert internal record data to proto StorageRecord.

        The request carries only the context KIND; the concrete prefixed id is
        resolved server-side — here from the fixed test ids.

        Args:
            ctx: Context kind from the request (StorageContext enum value)
            collection: Collection name
            record_id: Record ID
            record_data: The record data dictionary

        Returns:
            Proto storage record
        """
        resolved = "setup_versions:test_version" if ctx == storage_enums_pb2.SETUP_VERSIONS else "missions:test_mission"
        return storage_messages_pb2.StorageRecord(
            storage_id=f"storage:{collection}-{record_id}",
            context=resolved,
            collection=collection,
            record_id=record_id,
            data_type=storage_enums_pb2.DataType.Value(record_data["data_type"]),
            visibility=record_data["visibility"],
            data=json_format.ParseDict(record_data["data"], struct_pb2.Struct()),
            created_at=record_data["created_at"],
            updated_at=record_data["updated_at"],
        )

    def CreateRecord(
            self, request: storage_dto_pb2.CreateRecordRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.CreateRecordResponse:
        """Store a new record in the mock database.

        Args:
            request: CreateRecordRequest containing record data
            context: gRPC context

        Returns:
            CreateRecordResponse holding the stored record or an OperationError.
        """
        if self._invalid(context, request.context, request.collection, request.record_id):
            return storage_dto_pb2.CreateRecordResponse()
        data_dict = json_format.MessageToDict(request.data, preserving_proto_field_name=True)
        if error := self._schema_error(request.collection, data_dict):
            return storage_dto_pb2.CreateRecordResponse(
                result=self._error(request.record_id, "INVALID_ARGUMENT", error[:2048])
            )
        collection_records = self.records.setdefault(request.context, {}).setdefault(request.collection, {})
        if request.record_id in collection_records:
            return storage_dto_pb2.CreateRecordResponse(
                result=self._error(request.record_id, "ALREADY_EXISTS", f"Record {request.record_id} already exists")
            )
        now = datetime.datetime.now(datetime.timezone.utc)
        record_data = {
            "data": data_dict,
            "data_type": storage_enums_pb2.DataType.Name(request.data_type),
            "visibility": request.visibility,
            "created_at": now,
            "updated_at": now,
        }
        collection_records[request.record_id] = record_data
        logger.info("Stored record %s in %s", request.record_id, request.collection)
        record = self._create_proto_record(request.context, request.collection, request.record_id, record_data)
        return storage_dto_pb2.CreateRecordResponse(
            result=storage_messages_pb2.StorageResult(identifier=request.record_id, record=record)
        )

    def GetRecord(
            self, request: storage_dto_pb2.GetRecordRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.GetRecordResponse:
        """Read a record from the mock database.

        Args:
            request: GetRecordRequest containing context, collection, record_id
            context: gRPC context

        Returns:
            GetRecordResponse holding the record, or a NOT_FOUND OperationError.
        """
        if self._invalid(context, request.context, request.collection, request.record_id):
            return storage_dto_pb2.GetRecordResponse()
        record_data = self.records.get(request.context, {}).get(request.collection, {}).get(request.record_id)
        if record_data is None:
            return storage_dto_pb2.GetRecordResponse(
                result=self._error(request.record_id, "NOT_FOUND", f"Record {request.record_id} not found")
            )
        record = self._create_proto_record(request.context, request.collection, request.record_id, record_data)
        return storage_dto_pb2.GetRecordResponse(
            result=storage_messages_pb2.StorageResult(identifier=request.record_id, record=record)
        )

    def UpdateRecord(
            self, request: storage_dto_pb2.UpdateRecordRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.UpdateRecordResponse:
        """Update an existing record in the mock database.

        Args:
            request: UpdateRecordRequest containing updated data
            context: gRPC context

        Returns:
            UpdateRecordResponse holding the updated record, or an OperationError.
        """
        if self._invalid(context, request.context, request.collection, request.record_id):
            return storage_dto_pb2.UpdateRecordResponse()
        record_data = self.records.get(request.context, {}).get(request.collection, {}).get(request.record_id)
        if record_data is None:
            return storage_dto_pb2.UpdateRecordResponse(
                result=self._error(request.record_id, "NOT_FOUND", f"Record {request.record_id} not found")
            )
        data_dict = json_format.MessageToDict(request.data, preserving_proto_field_name=True)
        if error := self._schema_error(request.collection, data_dict):
            return storage_dto_pb2.UpdateRecordResponse(
                result=self._error(request.record_id, "INVALID_ARGUMENT", error[:2048])
            )
        record_data["data"] = data_dict
        record_data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        if request.visibility:
            record_data["visibility"] = request.visibility
        record = self._create_proto_record(request.context, request.collection, request.record_id, record_data)
        return storage_dto_pb2.UpdateRecordResponse(
            result=storage_messages_pb2.StorageResult(identifier=request.record_id, record=record)
        )

    def DeleteRecord(
            self, request: storage_dto_pb2.DeleteRecordRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.DeleteRecordResponse:
        """Remove a record from the mock database.

        Args:
            request: DeleteRecordRequest containing context, collection, record_id
            context: gRPC context

        Returns:
            DeleteRecordResponse holding the deleted record, or a NOT_FOUND OperationError.
        """
        if self._invalid(context, request.context, request.collection, request.record_id):
            return storage_dto_pb2.DeleteRecordResponse()
        collection_records = self.records.get(request.context, {}).get(request.collection, {})
        record_data = collection_records.pop(request.record_id, None)
        if record_data is None:
            return storage_dto_pb2.DeleteRecordResponse(
                result=self._error(request.record_id, "NOT_FOUND", f"Record {request.record_id} not found")
            )
        record = self._create_proto_record(request.context, request.collection, request.record_id, record_data)
        return storage_dto_pb2.DeleteRecordResponse(
            result=storage_messages_pb2.StorageResult(identifier=request.record_id, record=record)
        )

    def ListRecords(
            self, request: storage_dto_pb2.ListRecordsRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.ListRecordsResponse:
        """List the records of a collection.

        Args:
            request: ListRecordsRequest containing context and collection
            context: gRPC context

        Returns:
            ListRecordsResponse with one StorageResult per record and the bulk summary.
        """
        if self._invalid(context, request.context, request.collection):
            return storage_dto_pb2.ListRecordsResponse()
        results = [
            storage_messages_pb2.StorageResult(
                identifier=record_id,
                record=self._create_proto_record(request.context, request.collection, record_id, record_data),
            )
            for record_id, record_data in self.records.get(request.context, {}).get(request.collection, {}).items()
            if not request.HasField("record_id") or record_id == request.record_id
        ]
        return storage_dto_pb2.ListRecordsResponse(
            results=results,
            bulk=bulk_pb2.BulkResponse(
                total_processed=len(results),
                pagination=pagination_pb2.PaginationResponse(total_count=len(results)),
            ),
        )

    def DeleteCollection(
            self, request: storage_dto_pb2.DeleteCollectionRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.DeleteCollectionResponse:
        """Remove all records in a collection.

        Args:
            request: DeleteCollectionRequest containing context and collection
            context: gRPC context

        Returns:
            DeleteCollectionResponse whose bulk counts the deleted records.
        """
        if self._invalid(context, request.context, request.collection):
            return storage_dto_pb2.DeleteCollectionResponse()
        removed = self.records.get(request.context, {}).pop(request.collection, {})
        return storage_dto_pb2.DeleteCollectionResponse(bulk=bulk_pb2.BulkResponse(total_processed=len(removed)))
