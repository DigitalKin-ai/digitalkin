"""gRPC client implementation for Communication service."""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable

import grpc.aio
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

    service_name: str = "CommunicationService"

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
        # Track cache keys this instance owns refs on, for cleanup
        self._pool_keys: set[str] = set()

        logger.debug(
            "Initialized GrpcCommunication",
            extra={"security": client_config.security},
        )

    def _get_or_create_channel(self, module_address: str, module_port: int) -> grpc.aio.Channel:
        """Get or create a shared cached channel for the target module.

        Uses GrpcClientWrapper._channel_cache for ref-counted sharing so
        multiple tasks calling the same remote module reuse one HTTP/2 connection.

        Args:
            module_address: Module host address
            module_port: Module port

        Returns:
            Async gRPC channel for the target module
        """
        config = ClientConfig(
            host=module_address,
            port=module_port,
            mode=self.client_config.mode,
            security=self.client_config.security,
            credentials=self.client_config.credentials,
            compression=self.client_config.compression,
            channel_options=self.client_config.channel_options,
        )
        channel = self._init_channel(config)
        if self._channel_cache_key is not None:
            self._pool_keys.add(self._channel_cache_key)
        return channel

    async def close_all_channels(self) -> None:
        """Release refs on all pooled gRPC channels."""
        for key in self._pool_keys:
            await GrpcClientWrapper.release_cached_channel(key)
        self._pool_keys.clear()

    async def cleanup(self) -> None:
        """Clean up all gRPC channels."""
        await self.close_all_channels()

    def _create_stub(self, module_address: str, module_port: int) -> module_service_pb2_grpc.ModuleServiceStub:
        """Create a new stub for the target module.

        Args:
            module_address: Module host address
            module_port: Module port

        Returns:
            ModuleServiceStub for the target module
        """
        channel = self._get_or_create_channel(module_address, module_port)
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
            Dictionary containing schemas: input, output, setup, secret, cost
        """
        stub = self._create_stub(module_address, module_port)

        # Create requests
        # Note: cost always uses llm_format=False to get actual config data (rates, units)
        # No LLM are allowed to set costs
        input_request = information_pb2.GetModuleInputRequest(llm_format=llm_format)
        output_request = information_pb2.GetModuleOutputRequest(llm_format=llm_format)
        setup_request = information_pb2.GetModuleSetupRequest(llm_format=llm_format)
        secret_request = information_pb2.GetModuleSecretRequest(llm_format=llm_format)
        cost_request = information_pb2.GetModuleCostRequest(llm_format=False)

        # Get all schemas in parallel
        input_response, output_response, setup_response, secret_response, cost_response = await asyncio.gather(
            stub.GetModuleInput(input_request),
            stub.GetModuleOutput(output_request),
            stub.GetModuleSetup(setup_request),
            stub.GetModuleSecret(secret_request),
            stub.GetModuleCost(cost_request),
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
            "cost": json_format.MessageToDict(cost_response.cost_schema),
        }

    async def call_module(
        self,
        module_address: str,
        module_port: int,
        input_data: dict,
        setup_id: str,
        mission_id: str,
        callback: Callable[[dict], Awaitable[None]] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> AsyncGenerator[dict, None]:
        """Call a module and stream responses via gRPC.

        Args:
            module_address: Target module address
            module_port: Target module port
            input_data: Input data as dictionary
            setup_id: Setup configuration ID
            mission_id: Mission context ID
            callback: Optional callback for each response
            metadata: Optional gRPC metadata (headers) to send with the request.

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

        # Convert metadata dict to gRPC metadata format
        grpc_metadata = list(metadata.items()) if metadata else None

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
            # Call StartModule with streaming response and optional metadata
            response_stream = stub.StartModule(request, metadata=grpc_metadata)

            # Stream responses
            async for response in response_stream:
                # Convert protobuf Struct to dict
                output_dict = json_format.MessageToDict(response.output)

                # Check for end_of_stream signal
                if output_dict.get("root", {}).get("protocol") == "end_of_stream":
                    logger.debug(
                        "End of stream received",
                        extra={
                            "module_address": module_address,
                            "module_port": module_port,
                        },
                    )
                    break

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
