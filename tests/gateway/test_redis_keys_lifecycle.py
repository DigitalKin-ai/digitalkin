"""Gateway Redis key lifecycle: idem release on a failed dial, idem TTL after a fatal EOS (R3)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from digitalkin.models.core.redis import ClaimResult
from digitalkin.models.settings.gateway import get_gateway_settings
from tests.gateway.test_gateway_servicer import _mock_context, _mock_servicer


async def _dial(servicer: Any) -> None:
    await servicer._dial_consumer(
        task_id="t-dial",
        mission_id="missions:m",
        setup_id="setups:s",
        address="127.0.0.1:1",
    )


async def test_failed_dial_before_runner_spawn_releases_idem() -> None:
    servicer = _mock_servicer()
    servicer._idempotency.release = AsyncMock()

    await _dial(servicer)

    servicer._idempotency.release.assert_awaited_once_with("t-dial")


async def test_dial_that_spawned_the_runner_keeps_idem() -> None:
    servicer = _mock_servicer()
    servicer._idempotency.release = AsyncMock()

    async def _attempt(*, on_runner_spawn: Any, **_: Any) -> bool:  # noqa: RUF029
        on_runner_spawn()
        return False

    servicer._run_dial_attempt = _attempt

    await _dial(servicer)

    servicer._idempotency.release.assert_not_awaited()


async def test_fatal_eos_shortens_idem_ttl() -> None:
    get_gateway_settings.cache_clear()
    servicer = _mock_servicer()

    await servicer._emit_fatal_to_redis("t-fatal", "DIAL_BACK_UNREACHABLE", "unreachable", log_extra={})

    ttl = get_gateway_settings().stream.redis_stream_ttl
    servicer._redis_client.expire.assert_any_await("task:t-fatal:stream", ttl)
    servicer._redis_client.expire.assert_any_await("idem:t-fatal", ttl)


async def test_reclaimed_start_stream_is_refused_without_writes() -> None:
    servicer = _mock_servicer()
    servicer._idempotency.claim = AsyncMock(return_value=ClaimResult.RECLAIMED)
    request = MagicMock(task_id="t-reclaim", setup_id="setups:s", mission_id="missions:m")

    response = await servicer.StartStream(request, _mock_context())

    assert response.accepted is False
    servicer._redis_client.pipeline.assert_not_called()
