"""Test file for Module setup Servicer from the client side."""

import datetime
import secrets
import string

import grpc
from agentic_mesh_protocol.setup.v1 import (
    setup_pb2,
    setup_service_pb2_grpc,
)
from google.protobuf import json_format
from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.services.setup.setup_strategy import SetupData, SetupVersionData


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
        self, request: setup_pb2.CreateSetupRequest, context: grpc.ServicerContext
    ) -> setup_pb2.CreateSetupResponse:
        try:
            setup_data_version = SetupVersionData(
                id=request.current_setup_version.id,
                setup_id=request.current_setup_version.setup_id,
                version=request.current_setup_version.version,
                creation_date=request.current_setup_version.creation_date.ToDatetime() or datetime.datetime.now(),  # noqa: DTZ005
                content=dict(request.current_setup_version.content),
            )
            setup_data = SetupData(
                id=self._generate_id(),
                name=request.name,
                organisation_id=request.organisation_id,
                module_id=request.module_id,
                owner_id=request.owner_id,
                current_setup_version=setup_data_version,
            )
        except ValidationError:
            msg = "Validation failed for model SetupData"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return setup_pb2.CreateSetupResponse(success=False)

        self.setups[setup_data.id] = setup_data
        logger.debug("CREATE SETUP DATA %s:%s succesfull", setup_data.id, setup_data)
        return setup_pb2.CreateSetupResponse(success=True)

    def GetSetup(self, request: setup_pb2.GetSetupRequest, context: grpc.ServicerContext) -> setup_pb2.GetSetupResponse:
        logger.debug("GET SETUP setup_id = %s.", request.setup_id)
        if request.setup_id not in self.setups:
            msg = f"GET SETUP setup_id = {request.setup_id} | setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.GetSetupResponse()
        return setup_pb2.GetSetupResponse(setup=setup_pb2.Setup(**self.setups[request.setup_id].model_dump()))

    def UpdateSetup(
        self, request: setup_pb2.UpdateSetupRequest, context: grpc.ServicerContext
    ) -> setup_pb2.UpdateSetupResponse:
        if request.setup_id not in self.setups:
            msg = f"GET setup_id = {request.setup_id} | setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.UpdateSetupResponse(success=False)

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
                "creation_date": request.current_setup_version.creation_date.ToDatetime()
                if request.current_setup_version.HasField("creation_date")
                else datetime.datetime.now(),  # noqa: DTZ005
                "content": dict(request.current_setup_version.content),
            }
            self.setups[request.setup_id].current_setup_version = SetupVersionData.model_validate(setup_version_dict)
        logger.debug("UPDATE SETUP DATA %s succesfull", request.setup_id)
        return setup_pb2.UpdateSetupResponse(success=True)

    def DeleteSetup(
        self, request: setup_pb2.DeleteSetupRequest, context: grpc.ServicerContext
    ) -> setup_pb2.DeleteSetupResponse:
        if request.setup_id not in self.setups:
            msg = f"DELETE setup_id = {request.setup_id} | setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.DeleteSetupResponse(success=False)

        del self.setups[request.setup_id]
        return setup_pb2.DeleteSetupResponse(success=True)

    def CreateSetupVersion(
        self, request: setup_pb2.CreateSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_pb2.CreateSetupVersionResponse:
        try:
            setup_data_version = SetupVersionData(
                id=self._generate_id(),
                setup_id=request.setup_id,
                version=request.version,
                creation_date=datetime.datetime.now(),  # noqa: DTZ005
                content=dict(request.content),
            )
        except ValidationError:
            msg = "Validation failed for model SetupVersionData"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return setup_pb2.CreateSetupVersionResponse(success=False)

        if request.setup_id not in self.setup_versions:
            self.setup_versions[request.setup_id] = {}
        self.setup_versions[request.setup_id][setup_data_version.version] = setup_data_version
        logger.debug("CREATE SETUP VERSION DATA %s:%s succesfull", request.setup_id, setup_data_version)
        return setup_pb2.CreateSetupVersionResponse(success=True)

    def GetSetupVersion(
        self, request: setup_pb2.GetSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_pb2.GetSetupVersionResponse:
        logger.debug("GET SETUP VERSION setup_version_id = %s.", request.setup_version_id)

        # Search for the setup version with the matching ID
        setup_version = None
        for setup_versions in self.setup_versions.values():
            for version_data in setup_versions.values():
                if version_data.id == request.setup_version_id:
                    setup_version = version_data
                    break
            if setup_version:
                break

        if setup_version is None:
            msg = f"GET SETUP VERSION setup_version_id = {request.setup_version_id} | name DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.GetSetupVersionResponse()

        return setup_pb2.GetSetupVersionResponse(setup_version=setup_pb2.SetupVersion(**setup_version.model_dump()))

    def SearchSetupVersions(
        self, request: setup_pb2.SearchSetupVersionsRequest, context: grpc.ServicerContext
    ) -> setup_pb2.SearchSetupVersionsResponse:
        if request.setup_id is None or request.setup_id not in self.setup_versions:
            msg = f"GET setup_id = {request.setup_id}: setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.SearchSetupVersionsResponse()

        query_setup_versions = self.setup_versions[request.setup_id]
        if request.version:
            query_setup_versions = {k: v for k, v in query_setup_versions.items() if request.version in k}

        return setup_pb2.SearchSetupVersionsResponse(
            setup_versions=[setup_pb2.SetupVersion(**value.model_dump()) for value in query_setup_versions.values()]
        )

    def UpdateSetupVersion(
        self, request: setup_pb2.UpdateSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_pb2.UpdateSetupVersionResponse:
        # Search for the setup version with the matching ID
        setup_version = None
        for setup_versions in self.setup_versions.values():
            for version_data in setup_versions.values():
                if version_data.id == request.setup_version_id:
                    setup_version = version_data
                    break
            if setup_version:
                break

        if setup_version is None:
            msg = "UPDATE setup_version_id = {request.setup_version_id}: setup_version_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.UpdateSetupVersionResponse(success=False)

        self.setup_versions[setup_version.setup_id][setup_version.version].content = json_format.MessageToDict(
            request.content
        )
        return setup_pb2.UpdateSetupVersionResponse(success=True)

    def DeleteSetupVersion(
        self, request: setup_pb2.DeleteSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_pb2.DeleteSetupVersionResponse:
        # Search for the setup version with the matching ID
        setup_version = None
        for setup_versions in self.setup_versions.values():
            for version_data in setup_versions.values():
                if version_data.id == request.setup_version_id:
                    setup_version = version_data
                    break
            if setup_version:
                break

        if setup_version is None:
            msg = f"DELETE name = {request.setup_version_id} | name DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.DeleteSetupVersionResponse(success=False)

        # Delete only the specific version, not all versions for this setup
        del self.setup_versions[setup_version.setup_id][setup_version.version]
        # If this was the last version for this setup, remove the setup entry as well
        if not self.setup_versions[setup_version.setup_id]:
            del self.setup_versions[setup_version.setup_id]
        return setup_pb2.DeleteSetupVersionResponse(success=True)

    def ListSetups(
        self, request: setup_pb2.ListSetupsRequest, context: grpc.ServicerContext
    ) -> setup_pb2.ListSetupsResponse:
        """List setups with optional filtering and pagination.

        Args:
            request: ListSetupsRequest with organisation_id, owner_id, limit, offset
            context: gRPC context

        Returns:
            ListSetupsResponse: Response containing setups and total_count
        """
        try:
            # Start with all setups
            filtered_setups = list(self.setups.values())

            # Apply filters
            if request.organisation_id:
                filtered_setups = [s for s in filtered_setups if s.organisation_id == request.organisation_id]

            if request.owner_id:
                filtered_setups = [s for s in filtered_setups if s.owner_id == request.owner_id]

            # Get total count before pagination
            total_count = len(filtered_setups)

            # Apply pagination
            offset = max(0, request.offset)
            limit = request.limit if request.limit > 0 else len(filtered_setups)
            paginated_setups = filtered_setups[offset : offset + limit]

            # Convert to proto messages
            setup_protos = [setup_pb2.Setup(**s.model_dump()) for s in paginated_setups]

            logger.info(f"Listed {len(setup_protos)} setups (total: {total_count})")
            return setup_pb2.ListSetupsResponse(setups=setup_protos, total_count=total_count)

        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {e!s}")
            logger.error(f"Error in ListSetups: {e}", exc_info=True)
            return setup_pb2.ListSetupsResponse(setups=[], total_count=0)
