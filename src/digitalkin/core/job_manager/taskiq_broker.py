"""Taskiq broker & RSTREAM producer for the job manager."""

import asyncio
import datetime
import json
import logging
import os
import pickle  # noqa: S403
from typing import Any

from rstream import Producer
from rstream.exceptions import PreconditionFailed
from taskiq import Context, TaskiqDepends, TaskiqMessage
from taskiq.abc.formatter import TaskiqFormatter
from taskiq.compat import model_validate
from taskiq.message import BrokerMessage
from taskiq_aio_pika import AioPikaBroker

from digitalkin.core.common import ConnectionFactory, ModuleFactory
from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.task_manager.task_executor import TaskExecutor
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.module.module import ModuleCodeModel
from digitalkin.models.module.module_types import DataModel, OutputModelT
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

    def dumps(self, message: TaskiqMessage) -> BrokerMessage:  # noqa: PLR6301
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

    def loads(self, message: bytes) -> TaskiqMessage:  # noqa: PLR6301
        """Recreate Python object from bytes.

        Args:
            message: Broker message from bytes.

        Returns:
            message with TaskIQ format
        """
        json_str = pickle.loads(message)  # noqa: S301
        return model_validate(TaskiqMessage, json_str)


def define_producer() -> Producer:
    """Get from the env the connection parameter to RabbitMQ.

    Returns:
        Producer
    """
    host: str = os.environ.get("RABBITMQ_RSTREAM_HOST", "localhost")
    port: str = os.environ.get("RABBITMQ_RSTREAM_PORT", "5552")
    username: str = os.environ.get("RABBITMQ_RSTREAM_USERNAME", "guest")
    password: str = os.environ.get("RABBITMQ_RSTREAM_PASSWORD", "guest")

    logger.info("Connection to RabbitMQ: %s:%s.", host, port)
    return Producer(host=host, port=int(port), username=username, password=password)


async def init_rstream() -> None:
    """Init a stream for every tasks."""
    try:
        await RSTREAM_PRODUCER.create_stream(
            STREAM,
            exists_ok=True,
            arguments={"max-length-bytes": STREAM_RETENTION},
        )
    except PreconditionFailed:
        logger.warning("stream already exist")


def define_broker() -> AioPikaBroker:
    """Define broker with from env paramter.

    Returns:
        Broker: connected to RabbitMQ and with custom formatter.
    """
    host: str = os.environ.get("RABBITMQ_BROKER_HOST", "localhost")
    port: str = os.environ.get("RABBITMQ_BROKER_PORT", "5672")
    username: str = os.environ.get("RABBITMQ_BROKER_USERNAME", "guest")
    password: str = os.environ.get("RABBITMQ_BROKER_PASSWORD", "guest")

    broker = AioPikaBroker(
        f"amqp://{username}:{password}@{host}:{port}",
        startup=[init_rstream],
    )
    broker.formatter = PickleFormatter()
    return broker


STREAM = "taskiq_data"
STREAM_RETENTION = 200_000
RSTREAM_PRODUCER = define_producer()
TASKIQ_BROKER = define_broker()


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


async def send_message_to_stream(job_id: str, output_data: OutputModelT | ModuleCodeModel) -> None:  # type: ignore[type-var]
    """Callback define to add a message frame to the Rstream.

    Args:
        job_id: id of the job that sent the message
        output_data: message body as a OutputModelT or error / stream_code
    """
    body = json.dumps({"job_id": job_id, "output_data": output_data.model_dump()}).encode("utf-8")
    await RSTREAM_PRODUCER.send(stream=STREAM, message=body)


@TASKIQ_BROKER.task
async def run_start_module(
    mission_id: str,
    setup_id: str,
    setup_version_id: str,
    module_class: type[BaseModule],
    services_mode: ServicesMode,
    input_data: dict,
    setup_data: dict,
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
        context: Allow TaskIQ context access
    """
    logger.info("Starting module with services_mode: %s", services_mode)
    services_config = ServicesConfig(
        services_config_strategies=module_class.services_config_strategies,
        services_config_params=module_class.services_config_params,
        mode=services_mode,
    )
    setattr(module_class, "services_config", services_config)
    logger.debug("Services config: %s | Module config: %s", services_config, module_class.services_config)
    module_class.discover()

    job_id = context.message.task_id
    callback = await BaseJobManager.job_specific_callback(send_message_to_stream, job_id)  # type: ignore[type-var]
    module = ModuleFactory.create_module_instance(module_class, job_id, mission_id, setup_id, setup_version_id)

    channel = None
    try:
        # Create TaskExecutor and supporting components for worker execution
        executor = TaskExecutor()
        # SurrealDB env vars are expected to be set in env.
        channel = await ConnectionFactory.create_surreal_connection("taskiq_worker", datetime.timedelta(seconds=5))
        session = TaskSession(job_id, mission_id, channel, module, datetime.timedelta(seconds=2))

        # Execute the task using TaskExecutor
        # Create a proper done callback that handles errors
        async def send_end_of_stream(_: Any) -> None:  # noqa: ANN401
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
            channel=channel,
        )

        # Wait for the supervisor task to complete
        await supervisor_task
        logger.info("Module task %s completed", job_id)
    except Exception:
        logger.exception("Error running module %s", job_id)
        raise
    finally:
        # Cleanup channel
        if channel is not None:
            try:
                await channel.close()
            except Exception:
                logger.exception("Error closing channel for job %s", job_id)


@TASKIQ_BROKER.task
async def run_config_module(
    mission_id: str,
    setup_id: str,
    setup_version_id: str,
    module_class: type[BaseModule],
    services_mode: ServicesMode,
    config_setup_data: dict,
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
        context: Allow TaskIQ context access
    """
    logger.info("Starting config module with services_mode: %s", services_mode)
    services_config = ServicesConfig(
        services_config_strategies=module_class.services_config_strategies,
        services_config_params=module_class.services_config_params,
        mode=services_mode,
    )
    setattr(module_class, "services_config", services_config)
    logger.debug("Services config: %s | Module config: %s", services_config, module_class.services_config)

    job_id = context.message.task_id
    callback = await BaseJobManager.job_specific_callback(send_message_to_stream, job_id)  # type: ignore[type-var]
    module = ModuleFactory.create_module_instance(module_class, job_id, mission_id, setup_id, setup_version_id)

    # Override environment variables temporarily to use manager's SurrealDB
    channel = None
    try:
        # Create TaskExecutor and supporting components for worker execution
        executor = TaskExecutor()
        # SurrealDB env vars are expected to be set in env.
        channel = await ConnectionFactory.create_surreal_connection("taskiq_worker", datetime.timedelta(seconds=5))
        session = TaskSession(job_id, mission_id, channel, module, datetime.timedelta(seconds=2))

        # Create and run the config setup task with TaskExecutor
        setup_model = module_class.create_config_setup_model(config_setup_data)

        supervisor_task = await executor.execute_task(
            task_id=job_id,
            mission_id=mission_id,
            coro=module.start_config_setup(setup_model, callback),
            session=session,
            channel=channel,
        )

        # Wait for the supervisor task to complete
        await supervisor_task
        logger.info("Config module task %s completed", job_id)
    except Exception:
        logger.exception("Error running config module %s", job_id)
        raise
    finally:
        # Cleanup channel
        if channel is not None:
            try:
                await channel.close()
            except Exception:
                logger.exception("Error closing channel for job %s", job_id)
