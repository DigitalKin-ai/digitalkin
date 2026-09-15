"""This module implements the default storage strategy."""

from agentic_mesh_protocol.common.v1 import common_enums_pb2
from agentic_mesh_protocol.pagination.v1 import pagination_pb2
from agentic_mesh_protocol.storage.v1 import (
    storage_dto_pb2,
    storage_enums_pb2,
    storage_messages_pb2,
    storage_service_pb2_grpc,
)
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from digitalkin.grpc_servers.exceptions import CircuitOpenError, PermissionDeniedError
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.services import Context
from digitalkin.models.services.storage import DataType, Visibility
from digitalkin.services.storage.exceptions import StorageServiceError
from digitalkin.services.storage.storage_strategy import (
    StorageRecord,
    StorageStrategy,
)
from digitalkin.utils.proto_utils import ProtoUtils


class GrpcStorage(StorageStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC client implementation for the Storage service."""

    service_name: str = "StorageService"

    @staticmethod
    def _is_circuit_open(error: Exception) -> bool:
        """Whether ``error`` is a fast-fail from an open circuit breaker.

        An open breaker is an expected, already-logged condition (the
        CLOSED -> OPEN transition is logged once), so per-call rejections are
        logged quietly to avoid flooding logs during an outage window.

        Args:
            error: The exception raised by ``exec_grpc_query``.

        Returns:
            True if the error's cause is a ``CircuitOpenError``.
        """
        return isinstance(error.__cause__, CircuitOpenError)

    def _context_enum(self, context: str) -> storage_enums_pb2.StorageContext:
        """Map a resolved context string to the wire's context-kind enum.

        Since dev4 the request carries only the kind; the concrete id is resolved
        server-side from the request metadata stamped by ``RequestIdClientInterceptor``.
        USERS/ORGANIZATIONS are read-only cross-owner scopes — only the kind is sent;
        the server derives the owning user/organization from the request context (no id
        is transmitted by the client).

        Args:
            context: The resolved context string from ``_resolve_context``.

        Returns:
            The matching ``StorageContext`` wire enum.
        """
        # TODO(validate): remove after prod validation
        # [VALIDATE CTXENUM] server resolves the concrete id (incl. setup->current version) from metadata
        if context == self.setup_version_id or context.startswith("setup_versions:"):
            return storage_enums_pb2.SETUP_VERSIONS
        if context.startswith(f"{Context.USERS.value}:"):
            return storage_enums_pb2.USERS
        if context.startswith(f"{Context.ORGANIZATIONS.value}:"):
            return storage_enums_pb2.ORGANIZATIONS
        if context.startswith(f"{Context.UNSPECIFIED.value}:"):
            return storage_enums_pb2.STORAGE_CONTEXT_UNSPECIFIED
        return storage_enums_pb2.MISSIONS

    @staticmethod
    def _visibility_enum(visibility: Visibility) -> common_enums_pb2.Visibility:
        """Map an SDK ``Visibility`` to its common-proto wire enum.

        Args:
            visibility: The SDK visibility level.

        Returns:
            The matching wire enum (``VISIBILITY_UNSPECIFIED`` by default).
        """
        match visibility:
            case Visibility.PUBLIC:
                return common_enums_pb2.PUBLIC
            case Visibility.PRIVATE:
                return common_enums_pb2.PRIVATE
            case Visibility.INTERNAL:
                return common_enums_pb2.INTERNAL
            case _:
                return common_enums_pb2.VISIBILITY_UNSPECIFIED

    def _build_record_from_proto(self, proto: storage_messages_pb2.StorageRecord) -> StorageRecord:
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
        dtype = DataType[storage_enums_pb2.DataType.Name(proto.data_type)]
        visibility = Visibility[common_enums_pb2.Visibility.Name(proto.visibility).removeprefix("VISIBILITY_")]

        # Selective deserialization: only the nested Struct payload
        payload = ProtoUtils.proto_to_dict(proto.data) if proto.HasField("data") else {}

        # Timestamp conversion
        creation_date = proto.created_at.ToDatetime() if proto.HasField("created_at") else None
        update_date = proto.updated_at.ToDatetime() if proto.HasField("updated_at") else None

        validated = self._validate_data(coll, payload)
        return StorageRecord(
            context=ctx,
            collection=coll,
            record_id=rid,
            data=validated,
            data_type=dtype,
            visibility=visibility,
            creation_date=creation_date,
            update_date=update_date,
            storage_id=proto.storage_id,
        )

    def _build_record_or_skip(self, proto: storage_messages_pb2.StorageRecord) -> StorageRecord | None:
        """Convert a proto record, or log and return None if conversion/validation fails.

        Keeps one foreign-shaped record (e.g. written by another module) from
        failing an entire ListRecords result.

        Args:
            proto: gRPC StorageRecord

        Returns:
            The converted record, or None if it could not be validated.
        """
        try:
            return self._build_record_from_proto(proto)
        except Exception:
            logger.warning(
                "Skipping invalid record %s:%s in ListRecords", proto.collection, proto.record_id, exc_info=True
            )
            return None

    async def _store(self, record: StorageRecord) -> StorageRecord:
        """Create a new record in the database.

        Parameters:
            record: The record to store

        Returns:
            StorageRecord: The corresponding record

        Raises:
            PermissionDeniedError: If the service rejects the call with PERMISSION_DENIED.
            StorageServiceError: If the call fails or its result holds an OperationError.
        """
        logger.debug("debug:_store collection=%s id=%s", record.collection, record.record_id)
        data_struct = Struct()
        data_struct.update(record.data.model_dump())
        req = storage_dto_pb2.CreateRecordRequest(
            data=data_struct,
            context=self._context_enum(record.context),
            collection=record.collection,
            record_id=record.record_id,
            data_type=record.data_type.name,
            visibility=self._visibility_enum(record.visibility),
        )
        try:
            resp = await self.exec_grpc_query("CreateRecord", req)
            self.raise_on_error(resp.result, StorageServiceError)
            return self._build_record_from_proto(resp.result.record)
        except PermissionDeniedError:
            # TODO(validate): remove after prod validation
            logger.warning("[VALIDATE PD1] storage CreateRecord permission denied")
            raise
        except Exception as e:
            if self._is_circuit_open(e):
                logger.debug("gRPC CreateRecord skipped (circuit open) for %s:%s", record.collection, record.record_id)
            else:
                logger.exception("gRPC CreateRecord failed for %s:%s", record.collection, record.record_id)
            raise StorageServiceError(str(e)) from e

    async def _read(self, collection: str, record_id: str, context: str, storage_id: str = "") -> StorageRecord | None:
        """Fetch a single document scoped to a specific context.

        An absent record reads as None, whether the service answers with a NOT_FOUND
        status or with an OperationError result.

        Returns:
            StorageData: The record, or None if absent, invalid or on failure.

        Raises:
            PermissionDeniedError: If the service rejects the call with PERMISSION_DENIED.
        """
        logger.debug("debug:_read context=%s collection=%s id=%s", context, collection, record_id)
        try:
            req = storage_dto_pb2.GetRecordRequest(
                context=self._context_enum(context),
                collection=collection,
                record_id=record_id,
                storage_id=storage_id or None,
            )
            resp = await self.exec_grpc_query("GetRecord", req)
            self.raise_on_error(resp.result, StorageServiceError)
        except PermissionDeniedError:
            # TODO(validate): remove after prod validation
            logger.warning("[VALIDATE PD1] storage GetRecord permission denied")
            raise
        except Exception as e:
            if self._is_circuit_open(e):
                logger.debug("gRPC GetRecord skipped (circuit open) for %s:%s", collection, record_id)
            else:
                logger.info("gRPC GetRecord failed for %s:%s: %s", collection, record_id, e)
            return None

        try:
            return self._build_record_from_proto(resp.result.record)
        except Exception:
            logger.warning("Invalid record data for %s:%s in GetRecord", collection, record_id, exc_info=True)
            return None

    async def _update(
        self,
        collection: str,
        record_id: str,
        data: BaseModel,
        context: str,
        visibility: Visibility = Visibility.UNSPECIFIED,
    ) -> StorageRecord | None:
        """Overwrite a document via gRPC scoped to a specific context.

        Returns:
            StorageRecord: The updated record, or None on failure.

        Raises:
            PermissionDeniedError: If the service rejects the call with PERMISSION_DENIED.
        """
        logger.debug("debug:_update context=%s collection=%s id=%s", context, collection, record_id)
        struct = Struct()
        struct.update(data.model_dump())
        req = storage_dto_pb2.UpdateRecordRequest(
            data=struct,
            context=self._context_enum(context),
            collection=collection,
            record_id=record_id,
            visibility=self._visibility_enum(visibility),
        )
        try:
            resp = await self.exec_grpc_query("UpdateRecord", req)
            self.raise_on_error(resp.result, StorageServiceError)
            return self._build_record_from_proto(resp.result.record)
        except PermissionDeniedError:
            # TODO(validate): remove after prod validation
            logger.warning("[VALIDATE PD1] storage UpdateRecord permission denied")
            raise
        except Exception as e:
            if self._is_circuit_open(e):
                logger.debug("gRPC UpdateRecord skipped (circuit open) for %s:%s", collection, record_id)
            else:
                logger.warning("gRPC UpdateRecord failed for %s:%s: %s", collection, record_id, e)
            return None

    async def _remove(self, collection: str, record_id: str, context: str) -> bool:
        """Delete a document via gRPC scoped to a specific context.

        Returns:
            bool: True if the record was deleted, False otherwise.

        Raises:
            PermissionDeniedError: If the service rejects the call with PERMISSION_DENIED.
        """
        logger.debug("debug:_remove context=%s collection=%s id=%s", context, collection, record_id)
        try:
            req = storage_dto_pb2.DeleteRecordRequest(
                context=self._context_enum(context),
                collection=collection,
                record_id=record_id,
            )
            resp = await self.exec_grpc_query("DeleteRecord", req)
            self.raise_on_error(resp.result, StorageServiceError)
        except PermissionDeniedError:
            # TODO(validate): remove after prod validation
            logger.warning("[VALIDATE PD1] storage DeleteRecord permission denied")
            raise
        except Exception as e:
            if self._is_circuit_open(e):
                logger.debug("gRPC DeleteRecord skipped (circuit open) for %s:%s", collection, record_id)
            else:
                logger.warning("gRPC DeleteRecord failed for %s:%s: %s", collection, record_id, e)
            return False
        return True

    async def _list(
        self,
        collection: str,
        context: str,
        visibilities: list[Visibility] | None = None,
        record_id: str = "",
        limit: int = 0,
        offset: int = 0,
    ) -> list[StorageRecord]:
        """List all documents in a collection via gRPC scoped to a specific context.

        Results holding an OperationError, or a record failing validation, are logged and skipped.

        Returns:
            list[StorageRecord]: The records found, or an empty list on failure.

        Raises:
            PermissionDeniedError: If the service rejects the call with PERMISSION_DENIED.
        """
        logger.debug("debug:_list context=%s collection=%s", context, collection)
        req = storage_dto_pb2.ListRecordsRequest(
            context=self._context_enum(context),
            collection=collection,
            visibilities=[self._visibility_enum(v) for v in visibilities or ()],
            record_id=record_id or None,
            pagination=pagination_pb2.PaginationRequest(limit=min(limit or 20, 100), offset=offset)
            if limit or offset
            else None,
        )
        try:
            resp = await self.exec_grpc_query("ListRecords", req)
        except PermissionDeniedError:
            # TODO(validate): remove after prod validation
            logger.warning("[VALIDATE PD1] storage ListRecords permission denied")
            raise
        except Exception as e:
            if self._is_circuit_open(e):
                logger.debug("gRPC ListRecords skipped (circuit open) for %s", collection)
            else:
                logger.warning("gRPC ListRecords failed for %s: %s", collection, e)
            return []

        return [
            record
            for r in self.successful_results("ListRecords", resp.results)
            if (record := self._build_record_or_skip(r.record)) is not None
        ]

    async def _remove_collection(self, collection: str, context: str) -> bool:
        """Delete an entire collection via gRPC scoped to a specific context.

        Returns:
            bool: True if the collection was removed without any failed record, False otherwise.

        Raises:
            PermissionDeniedError: If the service rejects the call with PERMISSION_DENIED.
        """
        try:
            req = storage_dto_pb2.DeleteCollectionRequest(
                context=self._context_enum(context),
                collection=collection,
            )
            resp = await self.exec_grpc_query("DeleteCollection", req)
        except PermissionDeniedError:
            # TODO(validate): remove after prod validation
            logger.warning("[VALIDATE PD1] storage DeleteCollection permission denied")
            raise
        except Exception as e:
            if self._is_circuit_open(e):
                logger.debug("gRPC DeleteCollection skipped (circuit open) for %s", collection)
            else:
                logger.warning("gRPC DeleteCollection failed for %s: %s", collection, e)
            return False
        if resp.bulk.total_failed:
            logger.warning(
                "gRPC DeleteCollection for %s: %d of %d records failed",
                collection,
                resp.bulk.total_failed,
                resp.bulk.total_processed,
            )
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

    async def close(self) -> None:
        """Release this instance's pooled gRPC channel ref."""
        await self.close_channel()
