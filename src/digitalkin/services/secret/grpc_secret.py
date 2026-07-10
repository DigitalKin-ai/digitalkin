"""Digital Kin Secret Service gRPC Client (wraps UserProfileService.GetSetupSecret)."""

from typing import Any

from agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2_grpc,
)

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.secret.exceptions import SecretServiceError
from digitalkin.services.secret.secret_strategy import SecretStrategy
from digitalkin.utils.proto_utils import ProtoUtils


class GrpcSecret(SecretStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC client for setup secrets (backed by the UserProfileService)."""

    service_name: str = "UserProfileService"

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        client_config: ClientConfig,
    ) -> None:
        """Initialize the secret service.

        Args:
            mission_id: The ID of the mission this strategy is associated with
            setup_id: The ID of the setup
            setup_version_id: The ID of the setup version
            client_config: Client configuration for gRPC connection
        """
        super().__init__(mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id)
        self._init_channel(client_config)
        self.stub = self._get_or_create_stub(user_profile_service_pb2_grpc.UserProfileServiceStub)
        logger.debug("Channel client 'Secret' initialized successfully")

    async def close(self) -> None:
        """Release this instance's pooled gRPC channel ref."""
        await self.close_channel()

    async def get_secret(self) -> dict[str, Any] | None:
        """Resolve the secret object attached to this setup.

        Returns:
            The secret values, or None if not found.

        Raises:
            SecretServiceError: If the gRPC operation fails.
        """
        async with self.handle_grpc_errors("GetSetupSecret", SecretServiceError):
            request = user_profile_pb2.GetSetupSecretRequest(setup_id=self.setup_id, mission_id=self.mission_id)
            response = await self.exec_grpc_query("GetSetupSecret", request)
            if not response.success:
                logger.info(
                    "[VALIDATE SC1] secret fetch: setup_id=%s mission_id=%s success=False",
                    self.setup_id,
                    self.mission_id,
                )  # TODO(validate): remove after prod validation
                return None
            secret = ProtoUtils.proto_to_dict(response.secret, with_defaults=True)
            logger.info(
                "[VALIDATE SC1] secret fetch: setup_id=%s mission_id=%s success=True keys=%d",
                self.setup_id,
                self.mission_id,
                len(secret),
            )  # TODO(validate): remove after prod validation
            return secret
