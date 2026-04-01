"""Benchmark module server — EchoModule with embedded Gateway.

Pre-registers a default setup so LOCAL mode works without external services.
Server config comes from env vars via ServerSettings (pydantic-settings).
Gateway auto-enables via DIGITALKIN_REDIS_URL env var.
"""

import asyncio
import logging
import sys

sys.path.insert(0, "/app/bench_module")

import datetime

from echo_module import EchoToolModule

from digitalkin.grpc_servers.module_server import ModuleServer


async def main_async() -> int:
    """Run the benchmark module server.

    Returns:
        Exit code.
    """
    module_server = None
    try:
        module_server = ModuleServer(EchoToolModule)
        await module_server.start_async()

        # Pre-register default setup so LOCAL mode can resolve setup_id
        if module_server.module_servicer is not None:
            setup = module_server.module_servicer.setup
            now = datetime.datetime.now(datetime.timezone.utc)
            await setup.create_setup({
                "setup_id": "setups:echo_bench",
                "data": {
                    "id": "setups:echo_bench",
                    "name": "Echo Benchmark",
                    "organisation_id": "org:bench",
                    "owner_id": "user:bench",
                    "module_id": "modules:echo_bench",
                    "current_setup_version": {
                        "id": "v1",
                        "setup_id": "setups:echo_bench",
                        "version": "1.0.0",
                        "content": {"enabled": True},
                        "creation_date": now.isoformat(),
                    },
                },
            })
            await setup.create_setup_version({
                "setup_id": "setups:echo_bench",
                "data": {
                    "id": "v1",
                    "setup_id": "setups:echo_bench",
                    "version": "1.0.0",
                    "content": {"enabled": True},
                    "creation_date": now.isoformat(),
                },
            })
            logging.info("Pre-registered setup: setups:echo_bench")

        logging.info("Bench module server started on 0.0.0.0:50055")
        await module_server.await_termination()
    except KeyboardInterrupt:
        pass
    finally:
        if module_server is not None and module_server.server is not None:
            await module_server.stop_async()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main_async()))
