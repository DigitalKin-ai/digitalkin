"""Shared gateway-test teardown.

A test that issues ``StartStream`` schedules a background ``_dial_consumer`` task
(which may spawn ``module_runner`` / reap tasks). Tests that don't tear the
gateway down leave that task re-dialing a now-dead consumer; under load/random
ordering it bleeds into a later test (touching a closed redis / a foreign loop)
and surfaces as a spurious ERROR. This autouse fixture cancels any such stray
task after each test so leaks can't cross test boundaries.
"""

from __future__ import annotations

import asyncio

import pytest

_STRAY_PREFIXES = ("dial_consumer_", "module_runner_", "reap_")


@pytest.fixture(autouse=True)
async def _reap_gateway_background_tasks() -> object:
    yield
    current = asyncio.current_task()
    stray = [
        task
        for task in asyncio.all_tasks()
        if task is not current and not task.done() and task.get_name().startswith(_STRAY_PREFIXES)
    ]
    for task in stray:
        task.cancel()
    if stray:
        await asyncio.gather(*stray, return_exceptions=True)
