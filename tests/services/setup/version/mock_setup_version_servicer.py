"""Test file for Module setup Servicer from the client side."""

import datetime
import secrets
import string

import grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from agentic_mesh_protocol.setup.v1 import (
    setup_version_service_pb2_grpc,
    setup_version_dto_pb2,
    setup_messages_pb2,
)
from google.protobuf import json_format
from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.models.services.setup import SetupData, SetupVersionData


class MockSetupVersionServicer(setup_version_service_pb2_grpc.SetupVersionServiceServicer):
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

    def CreateSetupVersion(
            self, request: setup_version_dto_pb2.CreateSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.CreateSetupVersionResponse:
        try:
            setup_data_version = SetupVersionData(
                id=self._generate_id(),
                setup_id=request.setup_id,
                version=request.version,
                created_at=datetime.datetime.now(),  # noqa: DTZ005
                content=dict(request.content),
            )
        except ValidationError:
            msg = "Validation failed for model SetupVersionData"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.INVALID_ARGUMENT),
                                                                                                 message=msg))
            return setup_version_dto_pb2.CreateSetupVersionResponse(result=result)

        if request.setup_id not in self.setup_versions:
            self.setup_versions[request.setup_id] = {}
        self.setup_versions[request.setup_id][setup_data_version.version] = setup_data_version
        logger.debug("CREATE SETUP VERSION DATA %s:%s succesfull", request.setup_id, setup_data_version)
        result = setup_messages_pb2.SetupResult(version=setup_messages_pb2.SetupVersion(**setup_data_version.model_dump()),
                                                success=True)
        return setup_version_dto_pb2.CreateSetupVersionResponse(result=result)

    def GetSetupVersion(
            self, request: setup_version_dto_pb2.GetSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.GetSetupVersionResponse:
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
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND),
                                                                                                 message=msg))
            return setup_version_dto_pb2.GetSetupVersionResponse(result=result)
        result = setup_messages_pb2.SetupResult(version=setup_messages_pb2.SetupVersion(**setup_version.model_dump()),
                                                success=True)
        return setup_version_dto_pb2.GetSetupVersionResponse(result=result)

    def SearchSetupVersions(
            self, request: setup_version_dto_pb2.SearchSetupVersionsRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.SearchSetupVersionsResponse:
        if request.setup_id is None or request.setup_id not in self.setup_versions:
            msg = f"GET setup_id = {request.setup_id}: setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND),
                                                                                                 message=msg))
            return setup_version_dto_pb2.SearchSetupVersionsResponse(result=[result])

        query_setup_versions = self.setup_versions[request.setup_id]
        if request.version:
            query_setup_versions = {k: v for k, v in query_setup_versions.items() if request.version in k}
        setup_versions = [setup_messages_pb2.SetupVersion(**value.model_dump()) for value in query_setup_versions.values()]
        result = [setup_messages_pb2.SetupResult(version=version, success=True) for version in setup_versions]
        return setup_version_dto_pb2.SearchSetupVersionsResponse(result=result)

    def UpdateSetupVersion(
            self, request: setup_version_dto_pb2.UpdateSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.UpdateSetupVersionResponse:
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
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND),
                                                                                                 message=msg))
            return setup_version_dto_pb2.UpdateSetupVersionResponse(result=result)

        self.setup_versions[setup_version.setup_id][setup_version.version].content = json_format.MessageToDict(
            request.content
        )
        version = setup_messages_pb2.SetupVersion(**setup_version.model_dump())
        result = setup_messages_pb2.SetupResult(version=version, success=True)
        return setup_version_dto_pb2.UpdateSetupVersionResponse(result=result)

    def DeleteSetupVersion(
            self, request: setup_version_dto_pb2.DeleteSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.DeleteSetupVersionResponse:
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
            result = setup_messages_pb2.SetupResult(success=False, error=bulk_pb2.OperationError(code=str(grpc.StatusCode.NOT_FOUND),
                                                                                                 message=msg))
            return setup_version_dto_pb2.DeleteSetupVersionResponse(result=result)

        # Delete only the specific version, not all versions for this setup
        version = setup_messages_pb2.SetupVersion(**setup_version.model_dump())
        del self.setup_versions[setup_version.setup_id][setup_version.version]
        # If this was the last version for this setup, remove the setup entry as well
        if not self.setup_versions[setup_version.setup_id]:
            del self.setup_versions[setup_version.setup_id]
        result = setup_messages_pb2.SetupResult(version=version, success=True)
        return setup_version_dto_pb2.DeleteSetupVersionResponse(result=result)
