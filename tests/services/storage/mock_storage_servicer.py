"""Mock Storage Servicer for testing the GrpcStorage service."""

import datetime
from typing import Any

import grpc
from agentic_mesh_protocol.storage.v1 import data_pb2, storage_service_pb2_grpc
from google.protobuf import json_format, struct_pb2
from pydantic import BaseModel, ValidationError

from digitalkin.logger import logger


class MockStorageServicer(storage_service_pb2_grpc.StorageServiceServicer):
    """Mock implementation of the Storage Service Servicer for testing."""

    def __init__(self, schema_config: dict[str, type[BaseModel]] | None = None) -> None:
        """Initialize the mock servicer with empty storage.

        Args:
            schema_config: Dictionary mapping collection names to Pydantic model classes
        """
        super().__init__()
        # context -> collection -> record_id -> record_data
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
        ctx: str,
        collection: str,
        record_id: str,
        record_data: dict[str, Any],
    ) -> data_pb2.StorageRecord:
        """Convert internal record data to proto StorageRecord.

        Args:
            ctx: Owner context (`missions:<id>` or `setup_versions:<id>`)
            collection: Collection name
            record_id: Record ID
            record_data: The record data dictionary

        Returns:
            data_pb2.StorageRecord: Proto storage record
        """
        # Convert data dict to Struct
        data_struct = json_format.ParseDict(
            record_data["data"],
            struct_pb2.Struct(),
        )

        # Convert stored string name back to protobuf enum value
        # "OUTPUT" -> data_pb2.OUTPUT (integer)
        value = getattr(data_pb2, record_data["data_type"])

        # Convert ISO timestamp strings to datetime objects for protobuf Timestamp
        from google.protobuf.timestamp_pb2 import Timestamp

        creation_ts = Timestamp()
        update_ts = Timestamp()

        if record_data.get("creation_date"):
            creation_dt = datetime.datetime.fromisoformat(record_data["creation_date"])
            creation_ts.FromDatetime(creation_dt)

        if record_data.get("update_date"):
            update_dt = datetime.datetime.fromisoformat(record_data["update_date"])
            update_ts.FromDatetime(update_dt)

        return data_pb2.StorageRecord(
            context=ctx,
            collection=collection,
            record_id=record_id,
            data_type=value,
            data=data_struct,
            creation_date=creation_ts,
            update_date=update_ts,
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
            if not request.context:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Context is required")
                return data_pb2.StoreRecordResponse()

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.StoreRecordResponse()

            if not request.record_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Record ID is required")
                return data_pb2.StoreRecordResponse()

            # Validate data type (request.data_type is a protobuf enum integer value)
            # Convert protobuf enum value to enum name for validation
            # data_pb2.OUTPUT (int) -> need to check if it's valid
            valid_values = [
                data_pb2.OUTPUT,
                data_pb2.VIEW,
                data_pb2.LOGS,
                data_pb2.OTHER,
            ]
            if request.data_type not in valid_values:
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
                    context.set_details(f"Schema validation failed: {e!s}")
                    return data_pb2.StoreRecordResponse()

            # Check if record already exists
            mission_records = self.records.setdefault(request.context, {})
            collection_records = mission_records.setdefault(request.collection, {})

            if request.record_id in collection_records:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(f"Record {request.record_id} already exists in collection {request.collection}")
                return data_pb2.StoreRecordResponse()

            # Store the record
            # Convert protobuf enum integer value to string name for storage
            # data_pb2.OUTPUT -> "OUTPUT"
            name = data_pb2.DataType.Name(request.data_type)
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            record_data = {
                "data": data_dict,
                "data_type": name,
                "creation_date": now,
                "update_date": now,
            }
            collection_records[request.record_id] = record_data

            # Create response
            stored_record = self._create_proto_record(
                request.context, request.collection, request.record_id, record_data
            )

            logger.info(f"Stored record: {request.record_id} in {request.collection} for context {request.context}")
            return data_pb2.StoreRecordResponse(stored_data=stored_record)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in StoreRecord: {e}", exc_info=True)
            return data_pb2.StoreRecordResponse()

    def ReadRecord(
        self, request: data_pb2.ReadRecordRequest, context: grpc.ServicerContext
    ) -> data_pb2.ReadRecordResponse:
        """Read a record from the mock database.

        Args:
            request: ReadRecordRequest containing context, collection, record_id
            context: gRPC context

        Returns:
            ReadRecordResponse: Response containing the record or empty if not found
        """
        try:
            if not request.context:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Context is required")
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
            mission_records = self.records.get(request.context, {})
            collection_records = mission_records.get(request.collection, {})
            record_data = collection_records.get(request.record_id)

            if not record_data:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Record {request.record_id} not found in collection {request.collection}")
                return data_pb2.ReadRecordResponse()

            # Create response
            stored_record = self._create_proto_record(
                request.context, request.collection, request.record_id, record_data
            )

            logger.info(f"Read record: {request.record_id} from {request.collection}")
            return data_pb2.ReadRecordResponse(stored_data=stored_record)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
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
            if not request.context:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Context is required")
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
            mission_records = self.records.get(request.context, {})
            collection_records = mission_records.get(request.collection, {})
            record_data = collection_records.get(request.record_id)

            if not record_data:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Record {request.record_id} not found in collection {request.collection}")
                return data_pb2.UpdateRecordResponse()

            # Convert Struct to dict
            data_dict = json_format.MessageToDict(request.data, preserving_proto_field_name=True)

            # Validate against schema if configured
            if request.collection in self.schema_config:
                try:
                    self._validate_schema(request.collection, data_dict)
                except (ValidationError, ValueError) as e:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Schema validation failed: {e!s}")
                    return data_pb2.UpdateRecordResponse()

            # Update the record
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            record_data["data"] = data_dict
            record_data["update_date"] = now

            # Create response
            stored_record = self._create_proto_record(
                request.context, request.collection, request.record_id, record_data
            )

            logger.info(f"Updated record: {request.record_id} in {request.collection}")
            return data_pb2.UpdateRecordResponse(stored_data=stored_record)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in UpdateRecord: {e}", exc_info=True)
            return data_pb2.UpdateRecordResponse()

    def RemoveRecord(
        self, request: data_pb2.RemoveRecordRequest, context: grpc.ServicerContext
    ) -> data_pb2.RemoveRecordResponse:
        """Remove a record from the mock database.

        Args:
            request: RemoveRecordRequest containing context, collection, record_id
            context: gRPC context

        Returns:
            RemoveRecordResponse: Empty response
        """
        try:
            if not request.context:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Context is required")
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
            mission_records = self.records.get(request.context, {})
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
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in RemoveRecord: {e}", exc_info=True)
            return data_pb2.RemoveRecordResponse()

    def ListRecords(
        self, request: data_pb2.ListRecordsRequest, context: grpc.ServicerContext
    ) -> data_pb2.ListRecordsResponse:
        """List all records in a collection.

        Args:
            request: ListRecordsRequest containing context and collection
            context: gRPC context

        Returns:
            ListRecordsResponse: Response containing list of records
        """
        try:
            if not request.context:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Context is required")
                return data_pb2.ListRecordsResponse(records=[])

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.ListRecordsResponse(records=[])

            # Get all records in the collection
            mission_records = self.records.get(request.context, {})
            collection_records = mission_records.get(request.collection, {})

            # Convert to proto records
            proto_records = []
            for record_id, record_data in collection_records.items():
                proto_record = self._create_proto_record(request.context, request.collection, record_id, record_data)
                proto_records.append(proto_record)

            logger.info(f"Listed {len(proto_records)} records from {request.collection}")
            return data_pb2.ListRecordsResponse(records=proto_records)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in ListRecords: {e}", exc_info=True)
            return data_pb2.ListRecordsResponse(records=[])

    def RemoveCollection(
        self, request: data_pb2.RemoveCollectionRequest, context: grpc.ServicerContext
    ) -> data_pb2.RemoveCollectionResponse:
        """Remove all records in a collection.

        Args:
            request: RemoveCollectionRequest containing context and collection
            context: gRPC context

        Returns:
            RemoveCollectionResponse: Empty response
        """
        try:
            if not request.context:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Context is required")
                return data_pb2.RemoveCollectionResponse()

            if not request.collection:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Collection is required")
                return data_pb2.RemoveCollectionResponse()

            # Remove the entire collection
            mission_records = self.records.get(request.context, {})
            if request.collection in mission_records:
                del mission_records[request.collection]
                logger.info(f"Removed collection: {request.collection}")
            else:
                logger.debug(f"Collection {request.collection} not found, already removed or never existed")

            return data_pb2.RemoveCollectionResponse()

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in RemoveCollection: {e}", exc_info=True)
            return data_pb2.RemoveCollectionResponse()
