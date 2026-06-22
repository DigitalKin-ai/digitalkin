"""Redis infrastructure for core task management.

Provides durable state persistence and lossless token streaming. These are
core infrastructure concerns, not swappable service strategies.

The ``RedisClient`` singleton manages connection pooling. All other classes
depend on it for Redis access.
"""

from digitalkin.core.task_manager.redis.redis_client import RedisClient
from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotency
from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.core.task_manager.redis.redis_state import RedisStateManager
from digitalkin.models.core.redis import ClaimResult

__all__ = [
    "ClaimResult",
    "RedisClient",
    "RedisIdempotency",
    "RedisStateManager",
    "SharedRedisListener",
]
