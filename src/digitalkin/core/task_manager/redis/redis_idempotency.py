"""Idempotency guards using Redis Lua atomic claims.

Prevents duplicate task execution after network partitions or worker
restarts. Uses a Lua script that atomically reads and conditionally
writes the claim key, eliminating the SET NX race.
"""

from __future__ import annotations

from digitalkin.core.task_manager.redis.redis_client import RedisClient  # noqa: TC001
from digitalkin.logger import logger
from digitalkin.models.core.redis import ClaimResult
from digitalkin.models.settings.redis import RedisSettings

# Lua script: atomic claim with reclaim support.
# KEYS[1] = idem:{task_id}, ARGV[1] = task_id, ARGV[2] = TTL
# Returns: 1 = claimed, 2 = already ours (reclaim), 0 = taken
_CLAIM_SCRIPT = """
local v = redis.call('GET', KEYS[1])
if v == false then
    redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
    return 1
elseif v == ARGV[1] then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
    return 2
else
    return 0
end
"""


class RedisIdempotencyGuard:
    """Atomic task claim using Redis Lua scripts.

    Each task_id can be claimed by exactly one worker. The claim key
    ``idem:{task_id}`` has a TTL so stale claims from crashed workers
    expire and allow reclaim via XAUTOCLAIM.
    """

    _redis_client: RedisClient
    _claim_ttl: int

    def __init__(
        self,
        redis_client: RedisClient,
        claim_ttl: int | None = None,
    ) -> None:
        """Initialize idempotency guard.

        Args:
            redis_client: Shared Redis connection.
            claim_ttl: TTL in seconds for claim keys.
                Defaults to RedisSettings.idem_ttl.
        """
        self._redis_client = redis_client
        self._claim_ttl = claim_ttl if claim_ttl is not None else RedisSettings().idem_ttl

    async def claim(self, task_id: str) -> ClaimResult:
        """Attempt to claim a task atomically.

        Args:
            task_id: Unique task identifier to claim.

        Returns:
            CLAIMED if this is a fresh claim, RECLAIMED if we already own it,
            TAKEN if another worker claimed it.
        """
        result = await self._redis_client.eval(
            _CLAIM_SCRIPT,
            keys=[f"idem:{task_id}"],
            args=[task_id, str(self._claim_ttl)],
        )
        claim_result = ClaimResult(int(result or 0))
        logger.debug("IdempotencyGuard.claim: task_id=%s result=%s", task_id, claim_result.name)
        return claim_result

    async def release(self, task_id: str) -> None:
        """Release a claim after task completion.

        Args:
            task_id: Unique task identifier to release.
        """
        await self._redis_client.delete(f"idem:{task_id}")
        logger.debug("IdempotencyGuard.release: task_id=%s", task_id)
