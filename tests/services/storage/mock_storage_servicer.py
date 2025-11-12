"""Mock Storage Servicer for testing the GrpcStorage service."""

import datetime
from typing import Any

import grpc
from digitalkin_proto.agentic_mesh_protocol.storage.v1 import data_pb2, storage_service_pb2_grpc
from google.protobuf import json_format
from pydantic import BaseModel, ValidationError

from digitalkin.logger import logger
from digitalkin.services.storage.storage_strategy import DataType


# --- Fake Context for Servicer ---
class FakeContext:
    """Fake gRPC context for testing."""

    def __init__(self) -> None:
        """Initialize with OK status."""
        self._code = grpc.StatusCode.OK
        self._details = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        """Set the gRPC status code.

        Args:
            code: The status code to set
        """
        self._code = code

    def set_details(self, details: str) -> None:
        """Set the error details.

        Args:
            details: The error message
        """
        self._details = details


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

    def _create_proto_record(
        self,
        mission_id: str,
        collection: str,
        record_id: str,
        record_data: dict[str, Any],
    ) -> data_pb2.StorageRecord:
        """Convert internal record data to proto StorageRecord.

        Args:
            mission_id: Mission ID
            collection: Collection name
            record_id: Record ID
            record_data: The record data dictionary

        Returns:
            data_pb2.StorageRecord: Proto storage record
        """
        # Convert data dict to Struct
        data_struct = json_format.ParseDict(record_data["data"], data_pb2.google_dot_protobuf_dot_struct__pb2.Struct())

        return data_pb2.StorageRecord(
            mission_id=mission_id,
            collection=collection,
            record_id=record_id,
            data_type=record_data["data_type"],
            data=data_struct,
            creation_date=record_data.get("creation_date", ""),
            update_date=record_data.get("update_date", ""),
        )

    def StoreRecord(
        self, request: data_pb2.StoreRecordRequest, context: grpc.ServicerContext
    ) -> data_pb2.StoreRecordResponse:
        """Store a new record in the mock database.

        Args:
            request: StoreRecordRequest containing record data
            context: gRPC context

        Returns:
            StoreRecordResponse: Response containing stored record
        """
        try:
            # Validate required fields
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                return data_pb2.StoreRecordResponse()

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.StoreRecordResponse()

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                return data_pb2.StoreRecordResponse()

            # Validate data type
            valid_data_types = [dt.name for dt in DataType]
            if request.data_type not in valid_data_types:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Invalid data type: {request.data_type}")
                return data_pb2.StoreRecordResponse()

            # Convert Struct to dict
            data_dict = json_format.MessageToDict(request.data, preserving_proto_field_name=True)

            # Validate against schema if configured
            if request.collection in self.schema_config:
                try:
                    self._validate_schema(request.collection, data_dict)
                except (ValidationError, ValueError) as e:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Schema validation failed: {str(e)}")
                    return data_pb2.StoreRecordResponse()

            # Check if record already exists
            mission_records = self.records.setdefault(request.mission_id, {})
            collection_records = mission_records.setdefault(request.collection, {})

            if request.record_id in collection_records:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(f"Record {request.record_id} already exists in collection {request.collection}")
                return data_pb2.StoreRecordResponse()

            # Store the record
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            record_data = {
                "data": data_dict,
                "data_type": request.data_type,
                "creation_date": now,
                "update_date": now,
            }
            collection_records[request.record_id] = record_data

            # Create response
            stored_record = self._create_proto_record(
                request.mission_id, request.collection, request.record_id, record_data
            )

            logger.info(f"Stored record: {request.record_id} in {request.collection} for mission {request.mission_id}")
            return data_pb2.StoreRecordResponse(stored_data=stored_record)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            logger.error(f"Error in StoreRecord: {e}", exc_info=True)
            return data_pb2.StoreRecordResponse()

    def ReadRecord(
        self, request: data_pb2.ReadRecordRequest, context: grpc.ServicerContext
    ) -> data_pb2.ReadRecordResponse:
        """Read a record from the mock database.

        Args:
            request: ReadRecordRequest containing mission_id, collection, record_id
            context: gRPC context

        Returns:
            ReadRecordResponse: Response containing the record or empty if not found
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                return data_pb2.ReadRecordResponse()

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.ReadRecordResponse()

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                return data_pb2.ReadRecordResponse()

            # Try to find the record
            mission_records = self.records.get(request.mission_id, {})
            collection_records = mission_records.get(request.collection, {})
            record_data = collection_records.get(request.record_id)

            if not record_data:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(
                    f"Record {request.record_id} not found in collection {request.collection}"
                )
                return data_pb2.ReadRecordResponse()

            # Create response
            stored_record = self._create_proto_record(
                request.mission_id, request.collection, request.record_id, record_data
            )

            logger.info(f"Read record: {request.record_id} from {request.collection}")
            return data_pb2.ReadRecordResponse(stored_data=stored_record)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            logger.error(f"Error in ReadRecord: {e}", exc_info=True)
            return data_pb2.ReadRecordResponse()

    def UpdateRecord(
        self, request: data_pb2.UpdateRecordRequest, context: grpc.ServicerContext
    ) -> data_pb2.UpdateRecordResponse:
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
                return data_pb2.UpdateRecordResponse()

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.UpdateRecordResponse()

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                return data_pb2.UpdateRecordResponse()

            # Try to find the record
            mission_records = self.records.get(request.mission_id, {})
            collection_records = mission_records.get(request.collection, {})
            record_data = collection_records.get(request.record_id)

            if not record_data:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(
                    f"Record {request.record_id} not found in collection {request.collection}"
                )
                return data_pb2.UpdateRecordResponse()

            # Convert Struct to dict
            data_dict = json_format.MessageToDict(request.data, preserving_proto_field_name=True)

            # Validate against schema if configured
            if request.collection in self.schema_config:
                try:
                    self._validate_schema(request.collection, data_dict)
                except (ValidationError, ValueError) as e:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Schema validation failed: {str(e)}")
                    return data_pb2.UpdateRecordResponse()

            # Update the record
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            record_data["data"] = data_dict
            record_data["update_date"] = now

            # Create response
            stored_record = self._create_proto_record(
                request.mission_id, request.collection, request.record_id, record_data
            )

            logger.info(f"Updated record: {request.record_id} in {request.collection}")
            return data_pb2.UpdateRecordResponse(stored_data=stored_record)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            logger.error(f"Error in UpdateRecord: {e}", exc_info=True)
            return data_pb2.UpdateRecordResponse()

    def RemoveRecord(
        self, request: data_pb2.RemoveRecordRequest, context: grpc.ServicerContext
    ) -> data_pb2.RemoveRecordResponse:
        """Remove a record from the mock database.

        Args:
            request: RemoveRecordRequest containing mission_id, collection, record_id
            context: gRPC context

        Returns:
            RemoveRecordResponse: Empty response
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                return data_pb2.RemoveRecordResponse()

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.RemoveRecordResponse()

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                return data_pb2.RemoveRecordResponse()

            # Try to find and remove the record
            mission_records = self.records.get(request.mission_id, {})
            collection_records = mission_records.get(request.collection, {})

            if request.record_id not in collection_records:
                # Not an error - idempotent delete
                logger.debug(f"Record {request.record_id} not found for removal, already removed or never existed")
                return data_pb2.RemoveRecordResponse()

            del collection_records[request.record_id]

            logger.info(f"Removed record: {request.record_id} from {request.collection}")
            return data_pb2.RemoveRecordResponse()

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            logger.error(f"Error in RemoveRecord: {e}", exc_info=True)
            return data_pb2.RemoveRecordResponse()

    def ListRecords(
        self, request: data_pb2.ListRecordsRequest, context: grpc.ServicerContext
    ) -> data_pb2.ListRecordsResponse:
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
                return data_pb2.ListRecordsResponse(records=[])

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.ListRecordsResponse(records=[])

            # Get all records in the collection
            mission_records = self.records.get(request.mission_id, {})
            collection_records = mission_records.get(request.collection, {})

            # Convert to proto records
            proto_records = []
            for record_id, record_data in collection_records.items():
                proto_record = self._create_proto_record(
                    request.mission_id, request.collection, record_id, record_data
                )
                proto_records.append(proto_record)

            logger.info(f"Listed {len(proto_records)} records from {request.collection}")
            return data_pb2.ListRecordsResponse(records=proto_records)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            logger.error(f"Error in ListRecords: {e}", exc_info=True)
            return data_pb2.ListRecordsResponse(records=[])

    def RemoveCollection(
        self, request: data_pb2.RemoveCollectionRequest, context: grpc.ServicerContext
    ) -> data_pb2.RemoveCollectionResponse:
        """Remove all records in a collection.

        Args:
            request: RemoveCollectionRequest containing mission_id and collection
            context: gRPC context

        Returns:
            RemoveCollectionResponse: Empty response
        """
        try:
            if not request.mission_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Mission ID is required")
                return data_pb2.RemoveCollectionResponse()

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.RemoveCollectionResponse()

            # Remove the entire collection
            mission_records = self.records.get(request.mission_id, {})
            if request.collection in mission_records:
                del mission_records[request.collection]
                logger.info(f"Removed collection: {request.collection}")
            else:
                logger.debug(f"Collection {request.collection} not found, already removed or never existed")

            return data_pb2.RemoveCollectionResponse()

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            logger.error(f"Error in RemoveCollection: {e}", exc_info=True)
            return data_pb2.RemoveCollectionResponse()
