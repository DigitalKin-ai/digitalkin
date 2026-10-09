"""Mock SetupService + SetupVersionService servicer (client-side tests)."""

import datetime
import secrets
import string

import grpc
import protovalidate
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from agentic_mesh_protocol.setup.v1 import (
    setup_dto_pb2,
    setup_messages_pb2,
    setup_service_pb2_grpc,
    setup_version_dto_pb2,
    setup_version_service_pb2_grpc,
)
from google.protobuf.message import Message

from digitalkin.logger import logger
from digitalkin.utils.json_structure import JsonStructure
from digitalkin.utils.proto_utils import ProtoUtils


class MockSetupServicer(
    setup_service_pb2_grpc.SetupServiceServicer,
    setup_version_service_pb2_grpc.SetupVersionServiceServicer,
):
    """In-memory double of both setup services.

    Owner/organization/module are "derived from the request context" the way the
    real server does — here hardcoded to ``*:ctx`` values so tests can assert the
    client never sends them. An unknown id is answered with an ``OperationError``
    result, the protocol's per-item failure. Requests are first checked against their
    ``buf.validate`` rules, as the server-side validation interceptor does, and rejected
    with ``INVALID_ARGUMENT``.
    """

    alphabet = string.ascii_letters + string.digits

    setups: dict[str, setup_messages_pb2.Setup]
    # Every version cut per setup, oldest first — what ListSetupVersions pages over.
    versions: dict[str, list[setup_messages_pb2.SetupVersion]]

    def _generate_id(self, prefix: str) -> str:
        return prefix + "".join(secrets.choice(self.alphabet) for _ in range(16))

    def __init__(self) -> None:
        """Initialize the setup servicer with an empty store."""
        super().__init__()
        self.setups = {}
        self.versions = {}

    @staticmethod
    def _invalid(request: Message, context: grpc.ServicerContext) -> bool:
        """Flag a request breaking its ``buf.validate`` rules with ``INVALID_ARGUMENT``."""
        try:
            protovalidate.validate(request)
        except protovalidate.ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"invalid {request.DESCRIPTOR.name}: {e}")
            return True
        return False

    def _find_version(
            self, setup_version_id: str
    ) -> tuple[setup_messages_pb2.Setup, list[setup_messages_pb2.SetupVersion], setup_messages_pb2.SetupVersion] | None:
        return next(
            (
                (self.setups[setup_id], history, version)
                for setup_id, history in self.versions.items()
                for version in history
                if version.id == setup_version_id
            ),
            None,
        )

    @staticmethod
    def _not_found(identifier: str) -> setup_messages_pb2.SetupResult:
        return setup_messages_pb2.SetupResult(
            identifier=identifier,
            error=bulk_pb2.OperationError(code="NOT_FOUND", message=f"{identifier} DOESN'T EXIST"),
        )

    def _cut_version(
            self, setup_id: str, revision: setup_messages_pb2.SetupRevision
    ) -> setup_messages_pb2.SetupVersion:
        history = self.versions.setdefault(setup_id, [])
        version = setup_messages_pb2.SetupVersion(
            id=self._generate_id("setup_versions:"),
            setup_id=setup_id,
            version=f"1.0.{len(history)}",
            documentation=revision.documentation,
            content=revision.content,
            created_at=datetime.datetime.now(datetime.timezone.utc),
        )
        if revision.HasField("structure"):
            version.structure.CopyFrom(revision.structure)
        history.append(version)
        return version

    def CreateSetup(
            self, request: setup_dto_pb2.CreateSetupRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.CreateSetupResponse:
        if self._invalid(request, context):
            return setup_dto_pb2.CreateSetupResponse()

        setup_id = self._generate_id("setups:")
        setup = setup_messages_pb2.Setup(
            id=setup_id,
            name=request.name,
            organization_id="organizations:ctx",
            owner_id="users:ctx",
            module_id="modules:ctx",
            status="READY",
            visibility="PRIVATE",
        )
        setup.current_setup_version.CopyFrom(self._cut_version(setup_id, request.revision))
        self.setups[setup_id] = setup
        logger.debug("CREATE SETUP %s successful", setup_id)
        return setup_dto_pb2.CreateSetupResponse(
            result=setup_messages_pb2.SetupResult(identifier=setup_id, setup=setup)
        )

    def GetSetup(
            self, request: setup_dto_pb2.GetSetupRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.GetSetupResponse:
        if self._invalid(request, context):
            return setup_dto_pb2.GetSetupResponse()
        setup = self.setups.get(request.setup_id)
        if setup is None:
            return setup_dto_pb2.GetSetupResponse(result=self._not_found(request.setup_id))
        if not request.HasField("structure_key"):
            return setup_dto_pb2.GetSetupResponse(
                result=setup_messages_pb2.SetupResult(identifier=setup.id, setup=setup)
            )
        # Server-side projection: one key path, resolved against the stored document. A key
        # that does not resolve is NOT_FOUND, and DefaultSetup mirrors the same rule.
        content = ProtoUtils.proto_to_dict(setup.current_setup_version.content)
        values = JsonStructure.resolve(content, [request.structure_key])
        if not values:
            return setup_dto_pb2.GetSetupResponse(result=self._not_found(f"{setup.id}/{request.structure_key}"))
        projected = setup_messages_pb2.Setup()
        projected.CopyFrom(setup)
        projected.current_setup_version.content.Clear()
        projected.current_setup_version.content.update(values)
        return setup_dto_pb2.GetSetupResponse(
            result=setup_messages_pb2.SetupResult(identifier=setup.id, setup=projected)
        )

    def UpdateSetup(
            self, request: setup_dto_pb2.UpdateSetupRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.UpdateSetupResponse:
        if self._invalid(request, context):
            return setup_dto_pb2.UpdateSetupResponse()
        setup = self.setups.get(request.setup_id)
        if setup is None:
            return setup_dto_pb2.UpdateSetupResponse(result=self._not_found(request.setup_id))
        if request.HasField("name"):
            setup.name = request.name
        if request.HasField("revision"):
            version = self._cut_version(setup.id, request.revision)
            if request.set_as_current:
                setup.current_setup_version.CopyFrom(version)
        return setup_dto_pb2.UpdateSetupResponse(
            result=setup_messages_pb2.SetupResult(identifier=setup.id, setup=setup)
        )

    def DeleteSetup(
            self, request: setup_dto_pb2.DeleteSetupRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.DeleteSetupResponse:
        if self._invalid(request, context):
            return setup_dto_pb2.DeleteSetupResponse()
        setup = self.setups.pop(request.setup_id, None)
        if setup is None:
            return setup_dto_pb2.DeleteSetupResponse(result=self._not_found(request.setup_id))
        self.versions.pop(request.setup_id, None)
        return setup_dto_pb2.DeleteSetupResponse(
            result=setup_messages_pb2.SetupResult(identifier=setup.id, setup=setup)
        )

    def ChangeVisibility(
            self, request: setup_dto_pb2.ChangeVisibilityRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.ChangeVisibilityResponse:
        if self._invalid(request, context):
            return setup_dto_pb2.ChangeVisibilityResponse()
        setup = self.setups.get(request.setup_id)
        if setup is None:
            return setup_dto_pb2.ChangeVisibilityResponse(result=self._not_found(request.setup_id))
        setup.visibility = request.visibility
        return setup_dto_pb2.ChangeVisibilityResponse(
            result=setup_messages_pb2.SetupResult(identifier=setup.id, setup=setup)
        )

    def ListSetups(
            self, request: setup_dto_pb2.ListSetupsRequest, context: grpc.ServicerContext
    ) -> setup_dto_pb2.ListSetupsResponse:
        if self._invalid(request, context):
            return setup_dto_pb2.ListSetupsResponse()
        matching = [
            setup
            for setup in self.setups.values()
            if (not request.HasField("organization_id") or setup.organization_id == request.organization_id)
               and (not request.HasField("owner_id") or setup.owner_id == request.owner_id)
               and (not request.HasField("module_id") or setup.module_id == request.module_id)
               and (not request.statuses or setup.status in request.statuses)
        ]
        offset = request.pagination.offset
        window = matching[offset: offset + (request.pagination.limit or 20)]
        return setup_dto_pb2.ListSetupsResponse(
            results=[setup_messages_pb2.SetupResult(identifier=s.id, setup=s) for s in window],
            bulk=bulk_pb2.BulkResponse(
                total_processed=len(window),
                pagination=pagination_pb2.PaginationResponse(total_count=len(matching)),
            ),
        )

    def CreateSetupVersion(
            self, request: setup_version_dto_pb2.CreateSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.CreateSetupVersionResponse:
        if self._invalid(request, context):
            return setup_version_dto_pb2.CreateSetupVersionResponse()
        setup = self.setups.get(request.setup_id)
        if setup is None:
            return setup_version_dto_pb2.CreateSetupVersionResponse(result=self._not_found(request.setup_id))
        version = self._cut_version(setup.id, request.revision)
        version.version = request.version
        if request.set_as_current:
            setup.current_setup_version.CopyFrom(version)
        return setup_version_dto_pb2.CreateSetupVersionResponse(
            result=setup_messages_pb2.SetupResult(identifier=version.id, version=version)
        )

    def GetSetupVersion(
            self, request: setup_version_dto_pb2.GetSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.GetSetupVersionResponse:
        if self._invalid(request, context):
            return setup_version_dto_pb2.GetSetupVersionResponse()
        found = self._find_version(request.setup_version_id)
        if found is None:
            return setup_version_dto_pb2.GetSetupVersionResponse(result=self._not_found(request.setup_version_id))
        version = found[2]
        return setup_version_dto_pb2.GetSetupVersionResponse(
            result=setup_messages_pb2.SetupResult(identifier=version.id, version=version)
        )

    def UpdateSetupVersion(
            self, request: setup_version_dto_pb2.UpdateSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.UpdateSetupVersionResponse:
        if self._invalid(request, context):
            return setup_version_dto_pb2.UpdateSetupVersionResponse()
        found = self._find_version(request.setup_version_id)
        if found is None:
            return setup_version_dto_pb2.UpdateSetupVersionResponse(result=self._not_found(request.setup_version_id))
        setup, _, version = found
        if request.HasField("version"):
            version.version = request.version
        if request.HasField("content"):
            version.content.CopyFrom(request.content)
        if request.HasField("documentation"):
            version.documentation = request.documentation
        if request.HasField("structure"):
            version.structure.CopyFrom(request.structure)
        # Setup.current_setup_version holds a copy, so an edit of the active version is mirrored.
        if setup.current_setup_version.id == version.id:
            setup.current_setup_version.CopyFrom(version)
        return setup_version_dto_pb2.UpdateSetupVersionResponse(
            result=setup_messages_pb2.SetupResult(identifier=version.id, version=version)
        )

    def DeleteSetupVersion(
            self, request: setup_version_dto_pb2.DeleteSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.DeleteSetupVersionResponse:
        if self._invalid(request, context):
            return setup_version_dto_pb2.DeleteSetupVersionResponse()
        found = self._find_version(request.setup_version_id)
        if found is None:
            return setup_version_dto_pb2.DeleteSetupVersionResponse(result=self._not_found(request.setup_version_id))
        setup, history, version = found
        if setup.current_setup_version.id == version.id:
            return setup_version_dto_pb2.DeleteSetupVersionResponse(
                result=setup_messages_pb2.SetupResult(
                    identifier=version.id,
                    error=bulk_pb2.OperationError(code="FAILED_PRECONDITION", message="version is current"),
                )
            )
        history.remove(version)
        return setup_version_dto_pb2.DeleteSetupVersionResponse(
            result=setup_messages_pb2.SetupResult(identifier=version.id, version=version)
        )

    def ListSetupVersions(
            self, request: setup_version_dto_pb2.ListSetupVersionsRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.ListSetupVersionsResponse:
        if self._invalid(request, context):
            return setup_version_dto_pb2.ListSetupVersionsResponse()
        setup = self.setups.get(request.setup_id)
        if setup is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"setup_id = {request.setup_id} DOESN'T EXIST")
            return setup_version_dto_pb2.ListSetupVersionsResponse()
        history = list(reversed(self.versions.get(request.setup_id, [])))
        offset = request.pagination.offset
        window = history[offset: offset + request.pagination.limit]
        return setup_version_dto_pb2.ListSetupVersionsResponse(
            results=[setup_messages_pb2.SetupResult(identifier=v.id, version=v) for v in window],
            bulk=bulk_pb2.BulkResponse(
                total_processed=len(window),
                pagination=pagination_pb2.PaginationResponse(total_count=len(history)),
            ),
            current_setup_version_id=setup.current_setup_version.id,
        )

    def SetCurrentSetupVersion(
            self, request: setup_version_dto_pb2.SetCurrentSetupVersionRequest, context: grpc.ServicerContext
    ) -> setup_version_dto_pb2.SetCurrentSetupVersionResponse:
        if self._invalid(request, context):
            return setup_version_dto_pb2.SetCurrentSetupVersionResponse()
        setup = self.setups.get(request.setup_id)
        if setup is None:
            return setup_version_dto_pb2.SetCurrentSetupVersionResponse(result=self._not_found(request.setup_id))
        history = self.versions.get(request.setup_id, [])
        version = next((v for v in history if v.id == request.setup_version_id), None)
        if version is None:
            return setup_version_dto_pb2.SetCurrentSetupVersionResponse(
                result=self._not_found(request.setup_version_id)
            )
        setup.current_setup_version.CopyFrom(version)
        return setup_version_dto_pb2.SetCurrentSetupVersionResponse(
            result=setup_messages_pb2.SetupResult(identifier=setup.id, setup=setup)
        )
