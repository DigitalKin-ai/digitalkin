"""Mock SetupService servicer implementing the 7-RPC protocol (client-side tests)."""

import datetime
import secrets
import string

import grpc
from agentic_mesh_protocol.setup.v1 import (
    setup_pb2,
    setup_service_pb2_grpc,
)

from digitalkin.logger import logger


class MockSetupServicer(setup_service_pb2_grpc.SetupServiceServicer):
    """In-memory SetupService double.

    Owner/organisation/module are "derived from the request context" the way the
    real server does — here hardcoded to ``ctx-*`` values so tests can assert the
    client never sends them.
    """

    alphabet = string.ascii_letters + string.digits

    setups: dict[str, setup_pb2.Setup]
    # Every version cut per setup, oldest first — what ListSetupVersions pages over.
    versions: dict[str, list[setup_pb2.SetupVersion]]

    def _generate_id(self) -> str:
        return "".join(secrets.choice(self.alphabet) for _ in range(16))

    def __init__(self) -> None:
        """Initialize the setup servicer with an empty store."""
        super().__init__()
        self.setups = {}
        self.versions = {}

    @staticmethod
    def _sibling_response_pair(setup: setup_pb2.Setup) -> tuple[setup_pb2.Setup, setup_pb2.SetupVersion]:
        """Split a stored setup into (setup without embedded version, sibling version).

        Exercises the client's fallback merge path (response-level ``setup_version``).
        """
        bare = setup_pb2.Setup()
        bare.CopyFrom(setup)
        version = setup_pb2.SetupVersion()
        version.CopyFrom(setup.current_setup_version)
        bare.ClearField("current_setup_version")
        return bare, version

    def CreateSetup(
        self, request: setup_pb2.CreateSetupRequest, context: grpc.ServicerContext
    ) -> setup_pb2.CreateSetupResponse:
        if not request.name:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("name is required")
            return setup_pb2.CreateSetupResponse(success=False)

        setup_id = self._generate_id()
        setup = setup_pb2.Setup(
            id=setup_id,
            name=request.name,
            organisation_id="ctx-org",
            owner_id="ctx-owner",
            module_id="ctx-module",
            status=setup_pb2.SetupStatus.READY,
            visibility=setup_pb2.Visibility.VISIBILITY_PRIVATE,
            current_setup_version=setup_pb2.SetupVersion(
                id=self._generate_id(),
                setup_id=setup_id,
                version="1.0.0",
                content=request.content,
                creation_date=datetime.datetime.now(datetime.timezone.utc),
            ),
        )
        self.setups[setup_id] = setup
        # Snapshot, not the live message: current_setup_version gets CopyFrom'd on every
        # update, which would rewrite this history entry in place if it were aliased.
        seed = setup_pb2.SetupVersion()
        seed.CopyFrom(setup.current_setup_version)
        self.versions[setup_id] = [seed]
        logger.debug("CREATE SETUP %s successful", setup_id)
        bare, version = self._sibling_response_pair(setup)
        return setup_pb2.CreateSetupResponse(success=True, setup=bare, setup_version=version)

    def GetSetup(self, request: setup_pb2.GetSetupRequest, context: grpc.ServicerContext) -> setup_pb2.GetSetupResponse:
        setup = self.setups.get(request.setup_id)
        if setup is None:
            msg = f"GET SETUP setup_id = {request.setup_id} | setup_id DOESN'T EXIST"
            logger.warning(msg)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(msg)
            return setup_pb2.GetSetupResponse()
        # Embedded current_setup_version populated: exercises the client's preferred path.
        return setup_pb2.GetSetupResponse(setup=setup, setup_version=setup.current_setup_version)

    def UpdateSetup(
        self, request: setup_pb2.UpdateSetupRequest, context: grpc.ServicerContext
    ) -> setup_pb2.UpdateSetupResponse:
        setup = self.setups.get(request.setup_id)
        if setup is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"setup_id = {request.setup_id} DOESN'T EXIST")
            return setup_pb2.UpdateSetupResponse(success=False)
        setup.name = request.name
        history = self.versions.setdefault(request.setup_id, [])
        version = setup_pb2.SetupVersion(
            id=self._generate_id(),
            setup_id=request.setup_id,
            version=f"1.0.{len(history)}",
            content=request.content,
        )
        version.creation_date.FromDatetime(datetime.datetime.now(datetime.timezone.utc))
        history.append(version)
        if request.set_as_current:
            setup.current_setup_version.CopyFrom(version)
        return setup_pb2.UpdateSetupResponse(
            success=True, setup=setup, setup_version=setup.current_setup_version
        )

    def DeleteSetup(
        self, request: setup_pb2.DeleteSetupRequest, context: grpc.ServicerContext
    ) -> setup_pb2.DeleteSetupResponse:
        if request.setup_id not in self.setups:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"setup_id = {request.setup_id} DOESN'T EXIST")
            return setup_pb2.DeleteSetupResponse(success=False)
        del self.setups[request.setup_id]
        return setup_pb2.DeleteSetupResponse(success=True)

    def ChangeVisibility(
        self, request: setup_pb2.ChangeVisibilityRequest, context: grpc.ServicerContext
    ) -> setup_pb2.ChangeVisibilityResponse:
        setup = self.setups.get(request.setup_id)
        if setup is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"setup_id = {request.setup_id} DOESN'T EXIST")
            return setup_pb2.ChangeVisibilityResponse(success=False)
        setup.visibility = request.visibility
        return setup_pb2.ChangeVisibilityResponse(
            success=True, setup=setup, setup_version=setup.current_setup_version
        )

    def ListSetupVersions(
        self, request: setup_pb2.ListSetupVersionsRequest, context: grpc.ServicerContext
    ) -> setup_pb2.ListSetupVersionsResponse:
        setup = self.setups.get(request.setup_id)
        if setup is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"setup_id = {request.setup_id} DOESN'T EXIST")
            return setup_pb2.ListSetupVersionsResponse()
        history = list(reversed(self.versions.get(request.setup_id, [])))
        window = history[request.offset : request.offset + request.limit]
        return setup_pb2.ListSetupVersionsResponse(
            setup_versions=window,
            total_count=len(history),
            current_setup_version_id=setup.current_setup_version.id,
        )

    def SetCurrentSetupVersion(
        self, request: setup_pb2.SetCurrentSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_pb2.SetCurrentSetupVersionResponse:
        setup = self.setups.get(request.setup_id)
        if setup is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"setup_id = {request.setup_id} DOESN'T EXIST")
            return setup_pb2.SetCurrentSetupVersionResponse(success=False)
        history = self.versions.get(request.setup_id, [])
        version = next((v for v in history if v.id == request.setup_version_id), None)
        if version is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"setup_version_id = {request.setup_version_id} DOESN'T EXIST")
            return setup_pb2.SetCurrentSetupVersionResponse(success=False)
        setup.current_setup_version.CopyFrom(version)
        return setup_pb2.SetCurrentSetupVersionResponse(
            success=True, setup=setup, setup_version=version
        )
