"""Test file for Module setup Servicer from the client side."""

import datetime
import logging
import secrets
import string

import grpc
from digitalkin_proto.digitalkin.setup.v2 import (
    setup_pb2,
    setup_service_pb2_grpc,
)
from google.protobuf import json_format
from pydantic import ValidationError

from digitalkin.services.setup.setup_strategy import SetupData, SetupVersionData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# --- Fake Context for Servicer ---
class FakeContext:
    def __init__(self) -> None:
        self._code = grpc.StatusCode.OK
        self._details = ""

    def set_code(self, code) -> None:
        self._code = code

    def set_details(self, details) -> None:
        self._details = details


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
                name=request.current_setup_version.name,
                version=request.current_setup_version.version,
                creation_date=request.current_setup_version.creation_date.ToDatetime() or datetime.datetime.now(),
                content=dict(request.current_setup_version.content),
            )
            setup_data = SetupData(
                id=self._generate_id(),
                name=request.name,
                organisation_id=request.organisation_id,
                module_id=request.module_id,
                owner=request.owner,
                current_setup_version=setup_data_version,
            )
        except ValidationError:
            msg = "Validation failed for model SetupData"
            logger.exception(msg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return setup_pb2.CreateSetupResponse(success=False)

        self.setups[setup_data.id] = setup_data
        logger.info("CREATE SETUP DATA %s:%s succesfull", setup_data.id, setup_data)
        return setup_pb2.CreateSetupResponse(success=True)

    def GetSetup(self, request: setup_pb2.GetSetupRequest, context: grpc.ServicerContext) -> setup_pb2.GetSetupResponse:
        logger.info("GET SETUP setup_id = %s.", request.setup_id)
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

        if request.name is not None:
            self.setups[request.setup_id].name = request.name
        if request.owner is not None:
            self.setups[request.setup_id].owner = request.owner
        if request.current_setup_version is not None:
            self.setups[request.setup_id].current_setup_version = SetupVersionData.model_validate(
                request.current_setup_version
            )
        logger.info("UPDATE SETUP DATA %s succesfull", request.setup_id)
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
                name=request.name,
                version=request.version,
                creation_date=datetime.datetime.now(),
                content=dict(request.content),
            )
        except ValidationError:
            msg = "Validation failed for model SetupVersionData"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(msg)
            return setup_pb2.CreateSetupVersionResponse(success=False)

        if request.name not in self.setup_versions:
            self.setup_versions[request.name] = {}
        self.setup_versions[request.name][setup_data_version.version] = setup_data_version
        logger.info("CREATE SETUP VERSION DATA %s:%s succesfull", request.name, setup_data_version)
        return setup_pb2.CreateSetupVersionResponse(success=True)

    def GetSetupVersion(
        self, request: setup_pb2.GetSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_pb2.GetSetupVersionResponse:
        logger.info("GET SETUP VERSION name = %s.", request.name)
        if request.name not in self.setup_versions:
            msg = f"GET SETUP VERSION name = {request.name} | name DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.GetSetupVersionResponse()

        if request.version not in self.setup_versions[request.version]:
            msg = f"GET SETUP VERSION version = {request.version} | version DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.GetSetupVersionResponse()

        return setup_pb2.GetSetupVersionResponse(
            setup_version=setup_pb2.SetupVersion(**self.setup_versions[request.name][request.version].model_dump())
        )

    def SearchSetupVersions(
        self, request: setup_pb2.SearchSetupVersionsRequest, context: grpc.ServicerContext
    ) -> setup_pb2.SearchSetupVersionsResponse:
        if request.name not in self.setup_versions:
            msg = f"GET setup_id = {request.name}: setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.SearchSetupVersionsResponse()

        query_setup_versions = self.setup_versions[request.name].values()
        return setup_pb2.SearchSetupVersionsResponse(
            setup_versions=[
                setup_pb2.SetupVersion(**value.model_dump())
                for value in query_setup_versions
                if request.version in value.version
            ]
        )

    def UpdateSetupVersion(
        self, request: setup_pb2.UpdateSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_pb2.UpdateSetupVersionResponse:
        if request.name not in self.setup_versions:
            msg = "UPDATE name = {request.name}: name DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.UpdateSetupVersionResponse(success=False)

        if request.version not in self.setup_versions[request.name]:
            msg = "UPDATE version = {request.version}: version DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.UpdateSetupVersionResponse(success=False)

        self.setup_versions[request.name][request.version].content = json_format.MessageToDict(request.content)
        return setup_pb2.UpdateSetupVersionResponse(success=True)

    def DeleteSetupVersion(
        self, request: setup_pb2.DeleteSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_pb2.DeleteSetupVersionResponse:
        if request.name not in self.setup_versions:
            msg = f"DELETE name = {request.name} | name DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.DeleteSetupVersionResponse(success=False)

        del self.setup_versions[request.name]
        return setup_pb2.DeleteSetupVersionResponse(success=True)
