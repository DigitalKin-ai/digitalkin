"""Shared isolation for grpc-service unit tests.

Their sync ``client`` fixtures build a throwaway ``grpc.aio`` channel in the
strategy ``__init__``; under ``asyncio_mode=auto`` pytest-asyncio tears down each
async test's event loop, so the next file's sync fixtures run with no current
loop and ``grpc.aio.insecure_channel`` raises ``RuntimeError: no current event
loop``. Guarantee a current, open loop, and drop the class-level channel cache so
channels built on a now-closed loop are never reused across files.
"""

import asyncio

import pytest

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper


@pytest.fixture(autouse=True)
def _grpc_service_isolation():
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = None
    except RuntimeError:
        loop = None
    if loop is None:
        asyncio.set_event_loop(asyncio.new_event_loop())
    yield
    GrpcClientWrapper._channel_cache.clear()
    GrpcClientWrapper._ref_counts.clear()
    GrpcClientWrapper._stub_cache.clear()
