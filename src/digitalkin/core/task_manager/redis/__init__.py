"""Redis infrastructure for core task management.

Provides durable state persistence, lossless token streaming,
checkpoint/recovery, and idempotency guarantees. These are core
infrastructure concerns, not swappable service strategies.

The ``RedisClient`` singleton manages connection pooling.
All other classes depend on it for Redis access.
"""

from digitalkin.core.task_manager.redis.redis_checkpoint import RedisCheckpointManager
from digitalkin.core.task_manager.redis.redis_client import RedisClient
from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotencyGuard
from digitalkin.core.task_manager.redis.redis_signal import RedisSendBuffer, SharedRedisListener
from digitalkin.core.task_manager.redis.redis_state import RedisStateManager
from digitalkin.core.task_manager.redis.redis_streams import (
    RedisStreamBatchWriter,
    RedisStreamReader,
    RedisStreamWriter,
)
from digitalkin.models.core.redis import ClaimResult

__all__ = [
    "ClaimResult",
    "RedisCheckpointManager",
    "RedisClient",
    "RedisIdempotencyGuard",
    "RedisSendBuffer",
    "RedisStateManager",
    "RedisStreamBatchWriter",
    "RedisStreamReader",
    "RedisStreamWriter",
    "SharedRedisListener",
]
