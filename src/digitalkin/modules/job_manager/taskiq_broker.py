"""Taskiq broker & RSTREAM producer for the job manager."""

import asyncio
import json
import logging
import os
import pickle  # noqa: S403

from rstream import Producer
from rstream.exceptions import PreconditionFailed
from taskiq import Context, TaskiqDepends, TaskiqMessage
from taskiq.abc.formatter import TaskiqFormatter
from taskiq.compat import model_validate
from taskiq.message import BrokerMessage
from taskiq_aio_pika import AioPikaBroker

from digitalkin.logger import logger
from digitalkin.models.module.module_types import OutputModelT
from digitalkin.modules._base_module import BaseModule
from digitalkin.modules.job_manager.base_job_manager import BaseJobManager
from digitalkin.modules.job_manager.job_manager_models import StreamCodeModel
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


async def send_message_to_stream(job_id: str, output_data: OutputModelT) -> None:  # type: ignore
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
    logger.warning("%s", services_mode)
    services_config = ServicesConfig(
        services_config_strategies=module_class.services_config_strategies,
        services_config_params=module_class.services_config_params,
        mode=services_mode,
    )
    setattr(module_class, "services_config", services_config)
    logger.warning("%s | %s", services_config, module_class.services_config)

    job_id = context.message.task_id
    callback = await BaseJobManager.job_specific_callback(send_message_to_stream, job_id)
    module = module_class(job_id, mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id)

    await module.start(
        input_data,
        setup_data,
        callback,
        # ensure that the callback is called when the task is done + allow asyncio to run
        # TODO: should define a BaseModel for stream code / error
        done_callback=lambda _: asyncio.create_task(callback(StreamCodeModel(code="__END_OF_STREAM__"))),
    )


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
        setup_data: dict,
        context: Allow TaskIQ context access
    """
    logger.warning("%s", services_mode)
    services_config = ServicesConfig(
        services_config_strategies=module_class.services_config_strategies,
        services_config_params=module_class.services_config_params,
        mode=services_mode,
    )
    setattr(module_class, "services_config", services_config)
    logger.warning("%s | %s", services_config, module_class.services_config)

    job_id = context.message.task_id
    callback = await BaseJobManager.job_specific_callback(send_message_to_stream, job_id)
    module = module_class(job_id, mission_id=mission_id, setup_id=setup_id, setup_version_id=setup_version_id)

    await module.start_config_setup(
        module_class.create_config_setup_model(config_setup_data),
        callback,
    )
