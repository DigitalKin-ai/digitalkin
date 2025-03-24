"""This module sets up a logger."""

import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.getLogger("grpc").setLevel(logging.DEBUG)
logging.getLogger("asyncio").setLevel(logging.DEBUG)


logger = logging.getLogger("digitalkin")
