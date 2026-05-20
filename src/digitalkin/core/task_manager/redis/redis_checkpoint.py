"""Redis-backed checkpoint manager for crash recovery.

Serializes session state to Redis hashes, enabling seamless restart:
new process reads checkpoints and restores sessions without client
re-submission.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from digitalkin.core.task_manager.redis.redis_client import RedisClient  # noqa: TC001
from digitalkin.logger import logger
from digitalkin.models.settings.redis import RedisSettings


class RedisCheckpointManager:
    """Writes and restores session checkpoints in Redis.

    Checkpoint key: ``checkpoint:{session_id}`` with TTL (default 5min).

    Checkpointed fields:
    - session_id, task_id, mission_id, setup_id, setup_version_id
    - status, last_seq (stream resume point)
    - state (user-defined module state, must be JSON-serializable)
    - created_at (checkpoint timestamp)
    """

    _redis_client: RedisClient
    _checkpoint_ttl: int

    def __init__(
        self,
        redis_client: RedisClient,
        checkpoint_ttl: int | None = None,
    ) -> None:
        """Initialize checkpoint manager.

        Args:
            redis_client: Shared Redis connection.
            checkpoint_ttl: TTL in seconds for checkpoint keys.
                Defaults to RedisSettings.checkpoint_ttl.
        """
        self._redis_client = redis_client
        self._checkpoint_ttl = checkpoint_ttl if checkpoint_ttl is not None else RedisSettings().checkpoint_ttl

    async def checkpoint(
        self,
        session_id: str,
        task_id: str,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        status: str,
        last_seq: int,
        state: dict[str, Any] | None = None,
    ) -> None:
        """Write a checkpoint to Redis.

        Args:
            session_id: Session identifier.
            task_id: Task identifier.
            mission_id: Mission identifier.
            setup_id: Setup identifier.
            setup_version_id: Setup version identifier.
            status: Current session status.
            last_seq: Last output sequence number produced.
            state: User-defined module state (JSON-serializable).
        """
        key = f"checkpoint:{session_id}"
        mapping: dict[str, str] = {
            "session_id": session_id,
            "task_id": task_id,
            "mission_id": mission_id,
            "setup_id": setup_id,
            "setup_version_id": setup_version_id,
            "status": status,
            "last_seq": str(last_seq),
            "state": json.dumps(state or {}, default=str),
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        pipe = self._redis_client.pipeline()
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, self._checkpoint_ttl)
        # Track in secondary index for list_checkpoints() / startup restore
        pipe.sadd("checkpoints:active", session_id)
        pipe.expire("checkpoints:active", 86400)  # 24h safety net — stale entries cleaned on list
        await pipe.execute()
        logger.debug("Checkpoint written: session_id=%s status=%s last_seq=%d", session_id, status, last_seq)

    async def restore(self, session_id: str) -> dict[str, Any] | None:
        """Restore a checkpoint from Redis.

        Args:
            session_id: Session identifier.

        Returns:
            Checkpoint data dict, or None if no checkpoint exists.
        """
        raw = await self._redis_client.hgetall(f"checkpoint:{session_id}")
        if not raw:
            return None

        result: dict[str, Any] = {k.decode(): v.decode() for k, v in raw.items()}
        result["last_seq"] = int(result.get("last_seq", "0"))
        state_raw = result.get("state", "{}")
        result["state"] = json.loads(state_raw) if isinstance(state_raw, str) else {}
        logger.debug("Checkpoint restored: session_id=%s status=%s", session_id, result.get("status"))
        return result

    async def delete(self, session_id: str) -> None:
        """Delete a checkpoint after successful restore or completion.

        Args:
            session_id: Session identifier.
        """
        pipe = self._redis_client.pipeline()
        pipe.delete(f"checkpoint:{session_id}")
        pipe.srem("checkpoints:active", session_id)
        await pipe.execute()

    async def list_checkpoints(self) -> list[dict[str, Any]]:
        """List all active checkpoints via the secondary index.

        Reads the ``checkpoints:active`` set, then fetches each checkpoint.
        Stale entries (expired TTL) are cleaned from the index.

        Returns:
            List of checkpoint data dicts.
        """
        members = await self._redis_client.smembers("checkpoints:active")
        if not members:
            return []

        results: list[dict[str, Any]] = []
        stale: list[str] = []
        for raw_id in members:
            session_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            checkpoint = await self.restore(session_id)
            if checkpoint is not None:
                results.append(checkpoint)
            else:
                stale.append(session_id)

        # Clean stale entries from index (checkpoint TTL expired but set entry remains)
        for sid in stale:
            await self._redis_client.srem("checkpoints:active", sid)

        return results
