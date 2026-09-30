"""Redis infrastructure for core task management.

Provides lossless token streaming, idempotency claims and signal delivery. These are
core infrastructure concerns, not swappable service strategies.

The ``RedisClient`` singleton manages connection pooling. All other classes
depend on it for Redis access.
"""

from digitalkin.core.task_manager.redis.redis_client import RedisClient
from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotency
from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.models.core.redis import ClaimResult

__all__ = [
    "ClaimResult",
    "RedisClient",
    "RedisIdempotency",
    "SharedRedisListener",
]
