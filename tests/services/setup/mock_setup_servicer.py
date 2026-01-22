"""Test file for Module setup Servicer from the client side."""

import datetime
import secrets
import string

import grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from agentic_mesh_protocol.setup.v1 import (
    setup_messages_pb2,
    setup_service_pb2_grpc, setup_dto_pb2,
)
from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.models.services.setup import SetupData, SetupVersionData


class MockSetupServicer(setup_service_pb2_grpc.SetupServiceServicer):
    """Implementation of the MockSetupServicer."""

    alphabet = string.ascii_letters + string.digits

    setups: dict[str, SetupData]
    setup_versions: dict[str, dict[str, SetupVersionData]]

    def _generate_id(self) -> str:
        return "".join(secrets.choice(self.alphabet) for _ in range(16))

    def __init__(self) -> None:
        """Initialize the setup servicer with an empty setups."""
        super().__init__()
        self.setups = {}
        self.setup_versions = {}

    def CreateSetup(
            self, request: setup_dto_pb2.CreateSetupRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.CreateSetupResponse:
        try:
            setup_data_version = SetupVersionData(
                id=request.current_setup_version.id,
                setup_id=request.current_setup_version.setup_id,
                version=request.current_setup_version.version,
                created_at=request.current_setup_version.created_at.ToDatetime() or datetime.datetime.now(),  # noqa: DTZ005
                content=dict(request.current_setup_version.content),
            )
            setup_data = SetupData(
                id=self._generate_id(),
                name=request.name,
                organization_id=request.organization_id,
                module_id=request.module_id,
                owner_id=request.owner_id,
                current_setup_version=setup_data_version,
            )
        except ValidationError:
            msg = "Validation failed for model SetupData"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT),
                                                                                                 message=msg))
            return setup_dto_pb2.CreateSetupResponse(result=result)

        self.setups[setup_data.id] = setup_data
        logger.debug("CREATE SETUP DATA %s:%s succesfull", setup_data.id, setup_data)
        result = setup_messages_pb2.SetupResult(success=True, setup=setup_messages_pb2.Setup(**self.setups[setup_data.id].model_dump()))
        return setup_dto_pb2.CreateSetupResponse(result=result)

    def GetSetup(self, request: setup_dto_pb2.GetSetupRequest, context: grpc.ServicerContext) -> setup_dto_pb2.GetSetupResponse:
        logger.debug("GET SETUP setup_id = %s.", request.setup_id)
        if request.setup_id not in self.setups:
            msg = f"GET SETUP setup_id = {request.setup_id} | setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND),
                                                                                                 message=msg))
            return setup_dto_pb2.GetSetupResponse(result=result)
        result = setup_messages_pb2.SetupResult(setup=setup_messages_pb2.Setup(**self.setups[request.setup_id].model_dump()), success=True)
        return setup_dto_pb2.GetSetupResponse(result=result)

    def UpdateSetup(
            self, request: setup_dto_pb2.UpdateSetupRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.UpdateSetupResponse:
        if request.setup_id not in self.setups:
            msg = f"GET setup_id = {request.setup_id} | setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            result = setup_messages_pb2.SetupResult(error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND), message=msg), success=False)
            return setup_dto_pb2.UpdateSetupResponse(result=result)

        # Update only the fields that were explicitly set
        # For string fields, check if they're non-empty (proto3 default is empty string)
        if request.name:
            self.setups[request.setup_id].name = request.name
        if request.owner_id:
            self.setups[request.setup_id].owner_id = request.owner_id
        # For message fields, use HasField()
        if request.HasField("current_setup_version"):
            # Convert protobuf message to dict first, then validate
            setup_version_dict = {
                "id": request.current_setup_version.id,
                "setup_id": request.current_setup_version.setup_id,
                "version": request.current_setup_version.version,
                "created_at": request.current_setup_version.created_at.ToDatetime()
                if request.current_setup_version.HasField("created_at")
                else datetime.datetime.now(),  # noqa: DTZ005
                "content": dict(request.current_setup_version.content),
            }
            self.setups[request.setup_id].current_setup_version = SetupVersionData.model_validate(setup_version_dict)
        logger.debug("UPDATE SETUP DATA %s succesfull", request.setup_id)
        result = setup_messages_pb2.SetupResult(setup=setup_messages_pb2.Setup(**self.setups[request.setup_id].model_dump()), success=True)
        return setup_dto_pb2.UpdateSetupResponse(result=result)

    def DeleteSetup(
            self, request: setup_dto_pb2.DeleteSetupRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.DeleteSetupResponse:
        if request.setup_id not in self.setups:
            msg = f"DELETE setup_id = {request.setup_id} | setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND),
                                                                                                 message=msg))
            return setup_dto_pb2.DeleteSetupResponse(result=result)
        result = setup_messages_pb2.SetupResult(setup=setup_messages_pb2.Setup(**self.setups[request.setup_id].model_dump()), success=True)
        del self.setups[request.setup_id]
        return setup_dto_pb2.DeleteSetupResponse(result=result)

    def ListSetups(
            self, request: setup_dto_pb2.ListSetupsRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.ListSetupsResponse:
        """List setups with optional filtering and pagination.

        Args:
            request: ListSetupsRequest with organization_id, owner_id, limit, offset
            context: gRPC context

        Returns:
            ListSetupsResponse: Response containing setups and total_count
        """
        try:
            # Start with all setups
            filtered_setups = list(self.setups.values())

            # Apply filters
            if request.organization_id:
                filtered_setups = [s for s in filtered_setups if s.organization_id == request.organization_id]

            if request.owner_id:
                filtered_setups = [s for s in filtered_setups if s.owner_id == request.owner_id]

            # Get total count before pagination
            total_count = len(filtered_setups)

            # Apply pagination
            offset = max(0, request.pagination.offset)
            limit = request.pagination.limit if request.pagination.limit > 0 else len(filtered_setups)
            paginated_setups = filtered_setups[offset : offset + limit]

            # Convert to proto messages
            setup_protos = [setup_messages_pb2.Setup(**s.model_dump()) for s in paginated_setups]

            logger.info(f"Listed {len(setup_protos)} setups (total: {total_count})")
            result = [setup_messages_pb2.SetupResult(success=True, setup=setup) for setup in setup_protos]
            bulk = bulk_pb2.BulkResponse(total_process=total_count, total_failed=0)
            return setup_dto_pb2.ListSetupsResponse(result=result, bulk=bulk)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in ListSetups: {e}", exc_info=True)
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INTERNAL)))
            return setup_dto_pb2.ListSetupsResponse(result=result)
