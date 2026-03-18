"""Taskiq broker & RSTREAM producer for the job manager."""

import asyncio
import logging
import os
import pickle
import ssl
from typing import Any

from rstream import Producer
from rstream.exceptions import PreconditionFailed
from taskiq import Context, TaskiqDepends, TaskiqMessage
from taskiq.abc.formatter import TaskiqFormatter
from taskiq.compat import model_validate
from taskiq.message import BrokerMessage
from taskiq_aio_pika import AioPikaBroker

from digitalkin.core.common import ModuleFactory
from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.task_manager.task_executor import TaskExecutor
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.module.module import ModuleCodeModel
from digitalkin.models.module.module_types import DataModel
from digitalkin.models.module.utility import EndOfStreamOutput
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode

logging.getLogger("taskiq").setLevel(logging.INFO)
logging.getLogger("aiormq").setLevel(logging.INFO)
logging.getLogger("aio_pika").setLevel(logging.INFO)
logging.getLogger("rstream").setLevel(logging.INFO)


class PickleFormatter(TaskiqFormatter):
    """Formatter that pickles the JSON-dumped TaskiqMessage.

    This lets you send arbitrary Python objects (classes, functions, etc.)
    by first converting to JSON-safe primitives, then pickling that string.
    """

    def dumps(self, message: TaskiqMessage) -> BrokerMessage:  # Required by TaskiqFormatter interface # noqa: PLR6301
        """Dumps message from python complex object to JSON.

        Args:
            message: TaskIQ message

        Returns:
            BrokerMessage with mandatory information for TaskIQ
        """
        payload: bytes = pickle.dumps(message)

        return BrokerMessage(
            task_id=message.task_id,
            task_name=message.task_name,
            message=payload,
            labels=message.labels,
        )

    def loads(self, message: bytes) -> TaskiqMessage:  # Required by TaskiqFormatter interface # noqa: PLR6301
        """Recreate Python object from bytes.

        Non-pickle messages (e.g. raw JSON left in the queue by other producers)
        are logged and converted to a no-op ``TaskiqMessage`` so that Taskiq
        acknowledges (consumes) them instead of nack-ing and re-delivering in a loop.

        Args:
            message: Broker message from bytes.

        Returns:
            message with TaskIQ format
        """
        try:
            json_str = pickle.loads(  # noqa: S301
                message
            )  # Pickle: required for Taskiq deserialization (internal broker messages only)
        except Exception as e:
            logger.warning(
                "Discarding non-pickle message (size=%d, preview=%r): %s",
                len(message),
                message[:80],
                e,
            )
            # Return a no-op message that Taskiq will ack and discard
            # (no task named "__discarded__" exists, so Taskiq logs a warning and moves on)
            return TaskiqMessage(
                task_id="__discarded__",
                task_name="__discarded__",
                labels={"_discarded": "true"},
                args=[],
                kwargs={},
            )
        return model_validate(TaskiqMessage, json_str)


def _rstream_ssl_context() -> ssl.SSLContext | None:
    """Create SSL context for RStream if TLS is enabled via RABBITMQ_RSTREAM_SSL=true.

    Returns:
        SSL context if TLS is enabled, None otherwise.
    """
    if os.environ.get("RABBITMQ_RSTREAM_SSL", "").lower() not in {"true", "1", "yes"}:
        return None
    ctx = ssl.create_default_context()
    # Allow self-signed certs in staging if RABBITMQ_RSTREAM_SSL_VERIFY=false
    if os.environ.get("RABBITMQ_RSTREAM_SSL_VERIFY", "true").lower() in {"false", "0", "no"}:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


class TaskiqBrokerConfig:
    """Configuration and lifecycle management for Taskiq broker and RStream producer."""

    STREAM = "taskiq_data"
    STREAM_RETENTION = 200_000

    @staticmethod
    async def _on_producer_closed(reason: Any) -> None:
        """Log RStream producer connection closure for diagnostics.

        Args:
            reason: Connection close reason from rstream.
        """
        logger.error("RStream producer connection closed: %s", reason)

    @staticmethod
    def define_producer() -> Producer:
        """Create RStream producer with tuned settings for sustained throughput.

        Tuning:
        - ``default_batch_publishing_delay``: Flush batches every 100ms (default 3s)
          for lower streaming latency during long-running tasks.
        - ``default_context_switch_value``: Yield to the event loop every 100 messages
          (default 1000) to keep concurrent coroutines responsive under heavy output.

        Returns:
            Producer connected to RabbitMQ.
        """
        host = os.environ.get("RABBITMQ_RSTREAM_HOST", "localhost")
        port = os.environ.get("RABBITMQ_RSTREAM_PORT", "5552")
        username = os.environ.get("RABBITMQ_RSTREAM_USERNAME", "guest")
        password = os.environ.get("RABBITMQ_RSTREAM_PASSWORD", "guest")

        logger.info("RStream producer connecting to %s:%s", host, port)
        return Producer(
            host=host,
            port=int(port),
            username=username,
            password=password,
            ssl_context=_rstream_ssl_context(),
            default_batch_publishing_delay=float(os.environ.get("DIGITALKIN_RSTREAM_BATCH_DELAY", "0.1")),
            default_context_switch_value=int(os.environ.get("DIGITALKIN_RSTREAM_CONTEXT_SWITCH", "100")),
            connection_name="digitalkin_producer",
            on_close_handler=TaskiqBrokerConfig._on_producer_closed,
        )

    @staticmethod
    def define_broker() -> AioPikaBroker:
        """Create AioPikaBroker with tuned QoS for worker prefetch control.

        Returns:
            Broker connected to RabbitMQ with custom formatter.
        """
        host = os.environ.get("RABBITMQ_BROKER_HOST", "localhost")
        port = os.environ.get("RABBITMQ_BROKER_PORT", "5672")
        username = os.environ.get("RABBITMQ_BROKER_USERNAME", "guest")
        password = os.environ.get("RABBITMQ_BROKER_PASSWORD", "guest")
        scheme = os.environ.get("RABBITMQ_BROKER_SCHEME", "amqp")

        broker = AioPikaBroker(
            f"{scheme}://{username}:{password}@{host}:{port}",
            qos=int(os.environ.get("DIGITALKIN_TASKIQ_PREFETCH", "10")),
            startup=[TaskiqBrokerConfig.init_rstream],
        )
        broker.formatter = PickleFormatter()
        return broker

    @staticmethod
    async def init_rstream() -> None:
        """Init a stream for every tasks."""
        try:
            await RSTREAM_PRODUCER.create_stream(
                TaskiqBrokerConfig.STREAM,
                exists_ok=True,
                arguments={"max-length-bytes": TaskiqBrokerConfig.STREAM_RETENTION},
            )
        except PreconditionFailed:
            logger.warning("stream already exist")

    @staticmethod
    async def cleanup_global_resources() -> None:
        """Clean up global resources (producer and broker connections).

        This should be called during shutdown to prevent connection leaks.
        """
        try:
            await RSTREAM_PRODUCER.close()
            logger.info("RStream producer closed successfully")
        except Exception as e:
            logger.warning("Failed to close RStream producer: %s", e)

        try:
            await TASKIQ_BROKER.shutdown()
            logger.info("Taskiq broker shut down successfully")
        except Exception as e:
            logger.warning("Failed to shutdown Taskiq broker: %s", e)

    @staticmethod
    async def send_message_to_stream(job_id: str, output_data: DataModel | ModuleCodeModel) -> None:
        """Add a message frame to the RStream.

        Uses Pydantic's Rust-based model_dump_json() and direct string embedding
        to avoid the overhead of model_dump() → dict → json.dumps() → encode().

        Args:
            job_id: ID of the job that sent the message.
            output_data: Message body as a OutputModelT or error / stream_code.
        """
        # job_id is always a UUID (hex + hyphens), safe to embed without escaping
        output_json = output_data.model_dump_json()
        body = f'{{"job_id":"{job_id}","output_data":{output_json}}}'.encode()
        await RSTREAM_PRODUCER.send(stream=TaskiqBrokerConfig.STREAM, message=body)


# Module-level globals required by Taskiq framework (decorator needs broker at import time)
RSTREAM_PRODUCER = TaskiqBrokerConfig.define_producer()
TASKIQ_BROKER = TaskiqBrokerConfig.define_broker()


@TASKIQ_BROKER.task(task_name="__discarded__")
async def _discarded_message() -> None:  # noqa: RUF029
    """No-op sink for poison messages consumed by PickleFormatter.

    Taskiq's receiver early-returns without acking when a task name is unknown,
    so we register this dummy task to ensure the message is executed (no-op),
    acked, and removed from the queue.
    """
    logger.debug("Poison message acknowledged and discarded")


@TASKIQ_BROKER.task
async def run_start_module(
    mission_id: str,
    setup_id: str,
    setup_version_id: str,
    module_class: type[BaseModule],
    services_mode: ServicesMode,
    input_data: dict,
    setup_data: dict,
    request_metadata: dict[str, str] | None = None,
    registry_config: dict[str, Any] | None = None,
    context: Context = TaskiqDepends(),
) -> None:
    """TaskIQ task allowing a module to compute in the background asynchronously.

    Args:
        mission_id: str,
        setup_id: The setup ID associated with the module.
        setup_version_id: The setup ID associated with the module.
        module_class: type[BaseModule],
        services_mode: ServicesMode,
        input_data: dict,
        setup_data: dict,
        request_metadata: gRPC request metadata (headers) to forward to the module.
        registry_config: Registry config (client_config) forwarded from the main process.
        context: Allow TaskIQ context access
    """
    logger.info("Starting module with services_mode: %s", services_mode)

    # Restore registry config lost during pickle (worker re-imports class without runtime mutations)
    if registry_config is not None:
        if "services_config_params" not in module_class.__dict__:
            module_class.services_config_params = dict(module_class.services_config_params)
        module_class.services_config_params["registry"] = registry_config

    services_config = ServicesConfig(
        services_config_strategies=module_class.services_config_strategies,
        services_config_params=module_class.services_config_params,
        mode=services_mode,
    )
    module_class.services_config = services_config
    logger.debug("Services config: %s | Module config: %s", services_config, module_class.services_config)
    module_class.discover()

    job_id = context.message.task_id
    callback = await BaseJobManager.job_specific_callback(TaskiqBrokerConfig.send_message_to_stream, job_id)
    module = ModuleFactory.create_module_instance(
        module_class, job_id, mission_id, setup_id, setup_version_id, request_metadata=request_metadata
    )

    try:
        # Create TaskExecutor and supporting components for worker execution
        executor = TaskExecutor()
        session = TaskSession(job_id, mission_id, module)

        # Execute the task using TaskExecutor
        async def send_end_of_stream(_: Any) -> None:
            try:
                await callback(DataModel(root=EndOfStreamOutput()))
            except Exception as e:
                logger.error("Error sending end of stream: %s", e, exc_info=True)

        # Reconstruct Pydantic models from dicts for type safety
        try:
            input_model = module_class.create_input_model(input_data)
            setup_model = await module_class.create_setup_model(setup_data)
        except Exception as e:
            logger.error("Failed to reconstruct models for job %s: %s", job_id, e, exc_info=True)
            try:
                await callback(
                    ModuleCodeModel(
                        code="ValidationError",
                        short_description="Model reconstruction failed",
                        message=str(e),
                    )
                )
                await callback(DataModel(root=EndOfStreamOutput()))
            except Exception:
                logger.exception("Failed to send error to stream for job %s", job_id)
            raise

        supervisor_task = await executor.execute_task(
            task_id=job_id,
            mission_id=mission_id,
            coro=module.start(
                input_model,
                setup_model,
                callback,
                done_callback=lambda result: asyncio.ensure_future(send_end_of_stream(result)),
            ),
            session=session,
        )

        # Wait for the supervisor task to complete
        await supervisor_task
        logger.info("Module task %s completed", job_id)
    except Exception as e:
        logger.exception("Error running module %s", job_id)
        try:
            await callback(
                ModuleCodeModel(
                    code="WorkerError",
                    short_description="Worker execution failed",
                    message=str(e),
                )
            )
            await callback(DataModel(root=EndOfStreamOutput()))
        except Exception:
            logger.exception("Failed to send error to stream for job %s", job_id)
        raise
    finally:
        # Cleanup via module context
        try:
            await module.context.cleanup()
        except Exception:
            logger.exception("Error cleaning up module context for job %s", job_id)


@TASKIQ_BROKER.task
async def run_config_module(
    mission_id: str,
    setup_id: str,
    setup_version_id: str,
    module_class: type[BaseModule],
    services_mode: ServicesMode,
    config_setup_data: dict,
    request_metadata: dict[str, str] | None = None,
    context: Context = TaskiqDepends(),
) -> None:
    """TaskIQ task allowing a module to compute in the background asynchronously.

    Args:
        mission_id: str,
        setup_id: The setup ID associated with the module.
        setup_version_id: The setup ID associated with the module.
        module_class: type[BaseModule],
        services_mode: ServicesMode,
        config_setup_data: dict,
        request_metadata: gRPC request metadata (headers) to forward to the module.
        context: Allow TaskIQ context access
    """
    logger.info("Starting config module with services_mode: %s", services_mode)
    services_config = ServicesConfig(
        services_config_strategies=module_class.services_config_strategies,
        services_config_params=module_class.services_config_params,
        mode=services_mode,
    )
    module_class.services_config = services_config
    logger.debug("Services config: %s | Module config: %s", services_config, module_class.services_config)

    job_id = context.message.task_id
    callback = await BaseJobManager.job_specific_callback(  # type: ignore[type-var]
        TaskiqBrokerConfig.send_message_to_stream, job_id
    )
    module = ModuleFactory.create_module_instance(
        module_class, job_id, mission_id, setup_id, setup_version_id, request_metadata=request_metadata
    )

    try:
        # Create TaskExecutor and supporting components for worker execution
        executor = TaskExecutor()
        session = TaskSession(job_id, mission_id, module)

        # Create and run the config setup task with TaskExecutor
        try:
            setup_model = module_class.create_config_setup_model(config_setup_data)
        except Exception as e:
            logger.error("Failed to reconstruct config setup model for job %s: %s", job_id, e, exc_info=True)
            try:
                await callback(
                    ModuleCodeModel(
                        code="ValidationError",
                        short_description="Config setup model reconstruction failed",
                        message=str(e),
                    )
                )
                await callback(DataModel(root=EndOfStreamOutput()))
            except Exception:
                logger.exception("Failed to send error to stream for job %s", job_id)
            raise

        supervisor_task = await executor.execute_task(
            task_id=job_id,
            mission_id=mission_id,
            coro=module.start_config_setup(setup_model, callback),
            session=session,
        )

        # Wait for the supervisor task to complete
        await supervisor_task
        logger.info("Config module task %s completed", job_id)
    except Exception as e:
        logger.exception("Error running config module %s", job_id)
        try:
            await callback(
                ModuleCodeModel(
                    code="WorkerError",
                    short_description="Config worker execution failed",
                    message=str(e),
                )
            )
            await callback(DataModel(root=EndOfStreamOutput()))
        except Exception:
            logger.exception("Failed to send error to stream for job %s", job_id)
        raise
    finally:
        # Cleanup via module context
        try:
            await module.context.cleanup()
        except Exception:
            logger.exception("Error cleaning up module context for job %s", job_id)
