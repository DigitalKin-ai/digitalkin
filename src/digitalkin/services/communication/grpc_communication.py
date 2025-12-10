"""gRPC client implementation for Communication service."""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable

from agentic_mesh_protocol.module.v1 import (
    information_pb2,
    lifecycle_pb2,
    module_service_pb2_grpc,
)
from google.protobuf import json_format, struct_pb2

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.base_strategy import BaseStrategy
from digitalkin.services.communication.communication_strategy import CommunicationStrategy


class GrpcCommunication(CommunicationStrategy, GrpcClientWrapper):
    """gRPC client for module-to-module communication.

    This class provides methods to communicate with remote modules
    using the Module Service gRPC protocol.
    """

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        client_config: ClientConfig,
    ) -> None:
        """Initialize the gRPC communication client.

        Args:
            mission_id: Mission identifier
            setup_id: Setup identifier
            setup_version_id: Setup version identifier
            client_config: Client configuration for gRPC connection
        """
        BaseStrategy.__init__(self, mission_id, setup_id, setup_version_id)
        self.client_config = client_config

        logger.debug(
            "Initialized GrpcCommunication",
            extra={"security": client_config.security},
        )

    def _create_stub(self, module_address: str, module_port: int) -> module_service_pb2_grpc.ModuleServiceStub:
        """Create a new stub for the target module.

        Args:
            module_address: Module host address
            module_port: Module port

        Returns:
            ModuleServiceStub for the target module
        """
        logger.debug(
            "Creating connection",
            extra={"address": module_address, "port": module_port},
        )

        config = ClientConfig(
            host=module_address,
            port=module_port,
            mode=self.client_config.mode,
            security=self.client_config.security,
            credentials=self.client_config.credentials,
            channel_options=self.client_config.channel_options,
        )

        channel = self._init_channel(config)
        return module_service_pb2_grpc.ModuleServiceStub(channel)

    async def get_module_schemas(
        self,
        module_address: str,
        module_port: int,
        *,
        llm_format: bool = False,
    ) -> dict[str, dict]:
        """Get module schemas via gRPC.

        Args:
            module_address: Target module address
            module_port: Target module port
            llm_format: Return LLM-friendly format

        Returns:
            Dictionary containing schemas
        """
        stub = self._create_stub(module_address, module_port)

        # Create requests
        input_request = information_pb2.GetModuleInputRequest(llm_format=llm_format)
        output_request = information_pb2.GetModuleOutputRequest(llm_format=llm_format)
        setup_request = information_pb2.GetModuleSetupRequest(llm_format=llm_format)
        secret_request = information_pb2.GetModuleSecretRequest(llm_format=llm_format)

        # Get all schemas in parallel
        try:
            input_response, output_response, setup_response, secret_response = await asyncio.gather(
                asyncio.to_thread(stub.GetModuleInput, input_request),
                asyncio.to_thread(stub.GetModuleOutput, output_request),
                asyncio.to_thread(stub.GetModuleSetup, setup_request),
                asyncio.to_thread(stub.GetModuleSecret, secret_request),
            )

            logger.debug(
                "Retrieved module schemas",
                extra={
                    "module_address": module_address,
                    "module_port": module_port,
                    "llm_format": llm_format,
                },
            )

            return {
                "input": json_format.MessageToDict(input_response.input_schema),
                "output": json_format.MessageToDict(output_response.output_schema),
                "setup": json_format.MessageToDict(setup_response.setup_schema),
                "secret": json_format.MessageToDict(secret_response.secret_schema),
            }
        except Exception:
            logger.exception(
                "Failed to get module schemas",
                extra={
                    "module_address": module_address,
                    "module_port": module_port,
                },
            )
            raise

    async def call_module(
        self,
        module_address: str,
        module_port: int,
        input_data: dict,
        setup_id: str,
        mission_id: str,
        callback: Callable[[dict], Awaitable[None]] | None = None,
    ) -> AsyncGenerator[dict, None]:
        """Call a module and stream responses via gRPC.

        Args:
            module_address: Target module address
            module_port: Target module port
            input_data: Input data as dictionary
            setup_id: Setup configuration ID
            mission_id: Mission context ID
            callback: Optional callback for each response

        Yields:
            Streaming responses from module as dictionaries
        """
        stub = self._create_stub(module_address, module_port)

        # Convert input data to protobuf Struct
        input_struct = struct_pb2.Struct()
        input_struct.update(input_data)

        # Create request
        request = lifecycle_pb2.StartModuleRequest(
            input=input_struct,
            setup_id=setup_id,
            mission_id=mission_id,
        )

        logger.debug(
            "Calling module",
            extra={
                "module_address": module_address,
                "module_port": module_port,
                "setup_id": setup_id,
                "mission_id": mission_id,
            },
        )

        try:
            # Call StartModule with streaming response
            response_stream = stub.StartModule(request)

            # Stream responses
            for response in response_stream:
                # Convert protobuf Struct to dict
                output_dict = json_format.MessageToDict(response.output)

                # Add job_id and success flag
                response_dict = {
                    "success": response.success,
                    "job_id": response.job_id,
                    "output": output_dict,
                }

                logger.debug(
                    "Received module response",
                    extra={
                        "module_address": module_address,
                        "module_port": module_port,
                        "success": response.success,
                        "job_id": response.job_id,
                    },
                )

                # Call callback if provided
                if callback:
                    await callback(response_dict)

                yield response_dict

        except Exception:
            logger.exception(
                "Failed to call module",
                extra={
                    "module_address": module_address,
                    "module_port": module_port,
                    "setup_id": setup_id,
                    "mission_id": mission_id,
                },
            )
            raise
