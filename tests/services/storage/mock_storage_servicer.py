"""Mock Storage Servicer for testing the GrpcStorage service."""

import datetime
from typing import Any

import grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from agentic_mesh_protocol.storage.v1 import storage_dto_pb2, storage_service_pb2_grpc, storage_messages_pb2
from google.protobuf import json_format, struct_pb2
from pydantic import BaseModel, ValidationError

from digitalkin.logger import logger
from digitalkin.services.storage.storage_models import DataType


class MockStorageServicer(storage_service_pb2_grpc.StorageServiceServicer):
    """Mock implementation of the Storage Service Servicer for testing."""

    def __init__(self, schema_config: dict[str, type[BaseModel]] | None = None) -> None:
        """Initialize the mock servicer with empty storage.

        Args:
            schema_config: Dictionary mapping collection names to Pydantic model classes
        """
        super().__init__()
        # mission_id -> collection -> record_id -> record_data
        self.records: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        # Schema configuration for validation
        self.schema_config = schema_config or {}

    def _validate_schema(self, collection: str, data: dict[str, Any]) -> None:
        """Validate data against the schema for the collection.

        Args:
            collection: The collection name
            data: The data to validate

        Raises:
            ValidationError: If validation fails
            ValueError: If no schema is registered for the collection
        """
        if collection not in self.schema_config:
            msg = f"No schema registered for collection '{collection}'"
            raise ValueError(msg)

        model_cls = self.schema_config[collection]
        # This will raise ValidationError if invalid
        model_cls.model_validate(data)

    @staticmethod
    def __create_proto_record(
            mission_id: str,
        collection: str,
        record_id: str,
        record_data: dict[str, Any],
    ) -> storage_messages_pb2.StorageRecord:
        """Convert internal record data to proto StorageRecord.

        Args:
            mission_id: Mission ID
            collection: Collection name
            record_id: Record ID
            record_data: The record data dictionary

        Returns:
            storage_pb2.StorageRecord: Proto storage record
        """
        # Convert data dict to Struct
        data_struct = json_format.ParseDict(
            record_data["data"],
            struct_pb2.Struct(),
        )

        # Convert ISO timestamp strings to datetime objects for protobuf Timestamp
        from google.protobuf.timestamp_pb2 import Timestamp

        creation_ts = Timestamp()
        update_ts = Timestamp()

        if record_data.get("created_at"):
            creation_dt = datetime.datetime.fromisoformat(record_data["created_at"])
            creation_ts.FromDatetime(creation_dt)

        if record_data.get("updated_at"):
            update_dt = datetime.datetime.fromisoformat(record_data["updated_at"])
            update_ts.FromDatetime(update_dt)

        return storage_messages_pb2.StorageRecord(
            mission_id=mission_id,
            collection=collection,
            record_id=record_id,
            data_type=record_data["data_type"],
            data=data_struct,
            created_at=creation_ts,
            updated_at=update_ts,
        )

    def CreateRecord(
            self, request: storage_dto_pb2.CreateRecordRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.CreateRecordResponse:
        """Store a new record in the mock database.

        Args:
            request: CreateRecordRequest containing record data
            context: gRPC context

        Returns:
            CreateRecordResponse: Response containing stored record
        """
        try:
            # Validate required fields
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.CreateRecordResponse(result=result)

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.CreateRecordResponse(result=result)

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.CreateRecordResponse(result=result)

            if DataType.from_proto(request.data_type) not in DataType:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid data type: {request.data_type}")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.CreateRecordResponse(result=result)

            # Convert Struct to dict
            data_dict = json_format.MessageToDict(request.data, preserving_proto_field_name=True)

            # Validate against schema if configured
            if request.collection in self.schema_config:
                try:
                    self._validate_schema(request.collection, data_dict)
                except (ValidationError, ValueError) as e:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Schema validation failed: {e!s}")
                    result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)),
                                                                success=False)
                    return storage_dto_pb2.CreateRecordResponse(result=result)

            # Check if record already exists
            mission_records = self.records.setdefault(request.mission_id, {})
            collection_records = mission_records.setdefault(request.collection, {})

            if request.record_id in collection_records:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(f"Record {request.record_id} already exists in collection {request.collection}")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.ALREADY_EXISTS)), success=False)
                return storage_dto_pb2.CreateRecordResponse(result=result)

            # Store the record
            # Convert protobuf enum integer value to string name for storage
            # storage_pb2.OUTPUT -> "OUTPUT"
            data_type = request.data_type
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            record_data = {
                "data": data_dict,
                "data_type": data_type,
                "created_at": now,
                "updated_at": now,
            }
            collection_records[request.record_id] = record_data

            # Create response
            stored_record = self.__create_proto_record(
                request.mission_id, request.collection, request.record_id, record_data
            )

            logger.info(f"Stored record: {request.record_id} in {request.collection} for mission {request.mission_id}")
            result = storage_messages_pb2.StorageResult(record=stored_record, success=True)
            return storage_dto_pb2.CreateRecordResponse(result=result)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in StoreRecord: {e}", exc_info=True)
            result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)), success=False)
            return storage_dto_pb2.CreateRecordResponse(result=result)

    def GetRecord(
            self, request: storage_dto_pb2.GetRecordRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.GetRecordResponse:
        """Read a record from the mock database.

        Args:
            request: GetRecordRequest containing mission_id, collection, record_id
            context: gRPC context

        Returns:
            GetRecordResponse: Response containing the record or empty if not found
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.GetRecordResponse(result=result)

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.GetRecordResponse(result=result)

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.GetRecordResponse(result=result)

            # Try to find the record
            mission_records = self.records.get(request.mission_id, {})
            collection_records = mission_records.get(request.collection, {})
            record_data = collection_records.get(request.record_id)

            if not record_data:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Record {request.record_id} not found in collection {request.collection}")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND)), success=False)
                return storage_dto_pb2.GetRecordResponse(result=result)

            # Create response
            stored_record = self.__create_proto_record(
                request.mission_id, request.collection, request.record_id, record_data
            )

            logger.info(f"Read record: {request.record_id} from {request.collection}")
            result = storage_messages_pb2.StorageResult(record=stored_record, success=True)
            return storage_dto_pb2.GetRecordResponse(result=result)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in ReadRecord: {e}", exc_info=True)
            result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)), success=False)
            return storage_dto_pb2.GetRecordResponse(result=result)

    def UpdateRecord(
            self, request: storage_dto_pb2.UpdateRecordRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.UpdateRecordResponse:
        """Update an existing record in the mock database.

        Args:
            request: UpdateRecordRequest containing updated data
            context: gRPC context

        Returns:
            UpdateRecordResponse: Response containing updated record
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.UpdateRecordResponse(result=result)

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.UpdateRecordResponse(result=result)

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.UpdateRecordResponse(result=result)

            # Try to find the record
            mission_records = self.records.get(request.mission_id, {})
            collection_records = mission_records.get(request.collection, {})
            record_data = collection_records.get(request.record_id)

            if not record_data:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Record {request.record_id} not found in collection {request.collection}")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND)), success=False)
                return storage_dto_pb2.UpdateRecordResponse(result=result)

            # Convert Struct to dict
            data_dict = json_format.MessageToDict(request.data, preserving_proto_field_name=True)

            # Validate against schema if configured
            if request.collection in self.schema_config:
                try:
                    self._validate_schema(request.collection, data_dict)
                except (ValidationError, ValueError) as e:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Schema validation failed: {e!s}")
                    result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)),
                                                                success=False)
                    return storage_dto_pb2.UpdateRecordResponse(result=result)

            # Update the record
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            record_data["data"] = data_dict
            record_data["updated_at"] = now

            # Create response
            stored_record = self.__create_proto_record(
                request.mission_id, request.collection, request.record_id, record_data
            )

            logger.info(f"Updated record: {request.record_id} in {request.collection}")
            result = storage_messages_pb2.StorageResult(record=stored_record, success=True)
            return storage_dto_pb2.UpdateRecordResponse(result=result)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in UpdateRecord: {e}", exc_info=True)
            result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)), success=False)
            return storage_dto_pb2.UpdateRecordResponse(result=result)

    def DeleteRecord(
            self, request: storage_dto_pb2.DeleteRecordRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.DeleteRecordResponse:
        """Remove a record from the mock database.

        Args:
            request: DeleteRecordRequest containing mission_id, collection, record_id
            context: gRPC context

        Returns:
            DeleteRecordResponse: Empty response
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.DeleteRecordResponse(result=result)

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.DeleteRecordResponse(result=result)

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.DeleteRecordResponse(result=result)

            # Try to find and remove the record
            mission_records = self.records.get(request.mission_id, {})
            collection_records = mission_records.get(request.collection, {})

            if request.record_id not in collection_records:
                # Not an error - idempotent delete
                msg = f"Record {request.record_id} not found for removal, already removed or never existed"
                logger.debug(msg)
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.CANCELLED), message=msg),
                                                            success=False)
                return storage_dto_pb2.DeleteRecordResponse(result=result)

            del collection_records[request.record_id]

            logger.info(f"Removed record: {request.record_id} from {request.collection}")
            result = storage_messages_pb2.StorageResult(success=True)
            return storage_dto_pb2.DeleteRecordResponse(result=result)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in RemoveRecord: {e}", exc_info=True)
            result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)), success=False)
            return storage_dto_pb2.DeleteRecordResponse(result=result)

    def ListRecords(
            self, request: storage_dto_pb2.ListRecordsRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.ListRecordsResponse:
        """List all records in a collection.

        Args:
            request: ListRecordsRequest containing mission_id and collection
            context: gRPC context

        Returns:
            ListRecordsResponse: Response containing list of records
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.ListRecordsResponse(result=result)

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.ListRecordsResponse(result=result)

            # Get all records in the collection
            mission_records = self.records.get(request.mission_id, {})
            collection_records = mission_records.get(request.collection, {})

            # Convert to proto records
            proto_records = []
            for record_id, record_data in collection_records.items():
                proto_record = self.__create_proto_record(request.mission_id, request.collection, record_id, record_data)
                proto_records.append(proto_record)

            logger.info(f"Listed {len(proto_records)} records from {request.collection}")
            result = [storage_messages_pb2.StorageResult(record=r, success=True) for r in proto_records]
            return storage_dto_pb2.ListRecordsResponse(result=result)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in ListRecords: {e}", exc_info=True)
            result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)), success=False)
            return storage_dto_pb2.ListRecordsResponse(result=[result])

    def DeleteCollection(
            self, request: storage_dto_pb2.DeleteCollectionRequest, context: grpc.ServicerContext
    ) -> storage_dto_pb2.DeleteCollectionResponse:
        """Remove all records in a collection.

        Args:
            request: DeleteCollectionRequest containing mission_id and collection
            context: gRPC context

        Returns:
            DeleteCollectionResponse: Empty response
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.DeleteCollectionResponse(result=result)

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT)), success=False)
                return storage_dto_pb2.DeleteCollectionResponse(result=result)

            # Remove the entire collection
            mission_records = self.records.get(request.mission_id, {})
            if request.collection in mission_records:
                del mission_records[request.collection]
                logger.info(f"Removed collection: {request.collection}")
            else:
                logger.debug(f"Collection {request.collection} not found, already removed or never existed")

            result = storage_messages_pb2.StorageResult(success=True)
            return storage_dto_pb2.DeleteCollectionResponse(result=result)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in RemoveCollection: {e}", exc_info=True)
            result = storage_messages_pb2.StorageResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)), success=False)
            return storage_dto_pb2.DeleteCollectionResponse(result=result)
