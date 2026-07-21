"""At-most-once task-execution guard backed by an atomic Redis claim.

A single ``StartStream`` per ``task_id`` should drive exactly one module
execution. Without a durable guard, a retried or duplicated ``StartStream``
(after the in-memory session was torn down, or from a second gateway replica)
would re-dial and re-run the module. The claim key ``idem:{task_id}`` survives
session teardown and is shared across replicas, so only the first caller gets
``CLAIMED``; everyone else gets ``RECLAIMED``/``TAKEN`` and must resume the
existing output via ``Stream`` + ``from_seq`` instead of re-executing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from digitalkin.models.core.redis import ClaimResult
from digitalkin.models.settings.redis import get_redis_settings

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient


class RedisIdempotency:
    """Atomic ``idem:{task_id}`` claim guarding at-most-once execution."""

    _redis_client: RedisClient

    def __init__(self, redis_client: RedisClient) -> None:
        """Initialize the guard.

        Args:
            redis_client: Redis used for the atomic claim.
        """
        self._redis_client = redis_client

    async def claim(self, task_id: str, instance_id: str) -> ClaimResult:
        """Atomically claim execution of ``task_id``.

        ``CLAIMED`` on the first claim; ``RECLAIMED`` if ``instance_id``
        already owns it (same replica retrying); ``TAKEN`` if another
        replica owns it. The GET/SET is a single Lua eval so concurrent
        callers can never both win. The script returns the ``ClaimResult``
        integer value (0/1/2) — a Redis integer reply.

        Args:
            task_id: Task whose execution is being claimed.
            instance_id: Stable per-process identifier of the claimer.

        Returns:
            The claim outcome.
        """
        script = (
            f"local current = redis.call('GET', KEYS[1])\n"
            f"if current == false then\n"
            f"    redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))\n"
            f"    return {ClaimResult.CLAIMED.value}\n"
            f"elseif current == ARGV[1] then\n"
            f"    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))\n"
            f"    return {ClaimResult.RECLAIMED.value}\n"
            f"else\n"
            f"    return {ClaimResult.TAKEN.value}\n"
            f"end"
        )
        raw = await self._redis_client.eval(
            script,
            [f"idem:{task_id}"],
            [instance_id, str(get_redis_settings().idem_ttl)],
        )
        value = raw.decode() if isinstance(raw, bytes) else raw
        # An unexpected nil reply is treated as TAKEN so we never double-execute.
        return ClaimResult(int(value)) if value is not None else ClaimResult.TAKEN

    async def release(self, task_id: str) -> None:
        """Drop the claim so the task can be retried immediately.

        Used when a claim was acquired but execution could not start (e.g.
        the session was rejected at capacity), so the TTL doesn't block a
        legitimate retry.

        Args:
            task_id: Task whose claim is released.
        """
        await self._redis_client.delete(f"idem:{task_id}")
