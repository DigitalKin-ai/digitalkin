"""Constants for the Gateway subsystem.

Centralizes Redis key patterns, default values, and magic numbers
used across gateway_servicer, stream_registry, proto_streams, and
auth_interceptor.
"""

from __future__ import annotations

import os
import re

# ══════════════════════════════════════════════════════════════════
# Redis Key Patterns
# ══════════════════════════════════════════════════════════════════

REDIS_KEY_SESSION = "gateway:session:{task_id}"
REDIS_KEY_SESSION_COUNT = "gateway:session_count"
REDIS_KEY_HEARTBEATS = "gateway:heartbeats"
REDIS_KEY_STREAM = "task:{task_id}:stream"
REDIS_KEY_CURSOR = "task:{task_id}:cursor"
REDIS_KEY_SIGNAL_CHANNEL = "signal_ch:{task_id}"


# ══════════════════════════════════════════════════════════════════
# Default Values (all overridable via env vars)
# ══════════════════════════════════════════════════════════════════

# -- Gateway --
MAX_STREAMS = int(os.environ.get("DIGITALKIN_GATEWAY_MAX_STREAMS", "20000"))
MAX_LOCAL_CACHE = int(os.environ.get("DIGITALKIN_GATEWAY_MAX_LOCAL_CACHE", "5000"))
HEARTBEAT_TTL_S = float(os.environ.get("DIGITALKIN_GATEWAY_HEARTBEAT_TTL", "45"))
REAPER_INTERVAL_S = float(os.environ.get("DIGITALKIN_GATEWAY_REAPER_INTERVAL", "30"))
SESSION_STATE_TTL_S = int(os.environ.get("DIGITALKIN_SESSION_STATE_TTL_S", "3600"))  # 1h — session metadata

# -- Streams --
STREAM_TTL_S = int(os.environ.get("DIGITALKIN_REDIS_STREAM_TTL", "60"))
STREAM_MAXLEN = int(os.environ.get("DIGITALKIN_REDIS_STREAM_MAXLEN", "1000"))
CURSOR_TTL_S = int(os.environ.get("DIGITALKIN_REDIS_CURSOR_TTL", "360"))
STREAM_READ_BLOCK_MS = int(os.environ.get("DIGITALKIN_STREAM_READ_BLOCK_MS", "100"))

# -- Backpressure --
BACKPRESSURE_THRESHOLD = float(os.environ.get("DIGITALKIN_STREAM_BACKPRESSURE_THRESHOLD", "0.8"))
BACKPRESSURE_DELAY_MS = int(os.environ.get("DIGITALKIN_STREAM_BACKPRESSURE_DELAY_MS", "50"))
BACKPRESSURE_CHECK_INTERVAL = int(os.environ.get("DIGITALKIN_STREAM_BACKPRESSURE_CHECK_INTERVAL", "100"))
BACKPRESSURE_TIMEOUT_S = float(os.environ.get("DIGITALKIN_STREAM_BACKPRESSURE_TIMEOUT_S", "30"))

# -- Redis Pool --
REDIS_POOL_SIZE = int(os.environ.get("DIGITALKIN_REDIS_POOL_SIZE", "2000"))
REDIS_POOL_SIZE_DEFAULT = int(os.environ.get("DIGITALKIN_REDIS_POOL_SIZE_DEFAULT", str(REDIS_POOL_SIZE // 2)))
REDIS_POOL_SIZE_BLOCKING = int(os.environ.get("DIGITALKIN_REDIS_POOL_SIZE_BLOCKING", str(REDIS_POOL_SIZE // 2)))

# -- Validation --
MAX_FROM_SEQ = STREAM_MAXLEN * 10  # upper bound for from_seq — 10x stream capacity

# -- Queue Sizes --
DEFAULT_OUTPUT_QUEUE_SIZE = int(os.environ.get("DIGITALKIN_OUTPUT_QUEUE_SIZE", "512"))
DEFAULT_INPUT_QUEUE_SIZE = int(os.environ.get("DIGITALKIN_INPUT_QUEUE_SIZE", "512"))
ENQUEUE_TIMEOUT_S = float(os.environ.get("DIGITALKIN_ENQUEUE_TIMEOUT_S", "5.0"))

# -- gRPC --
REDIS_HEALTH_CHECK_TIMEOUT_S = float(os.environ.get("DIGITALKIN_REDIS_HEALTH_TIMEOUT", "5.0"))

# -- Stream Batching --
STREAM_BATCH_SIZE = int(os.environ.get("DIGITALKIN_STREAM_BATCH_SIZE", "20"))
STREAM_FLUSH_MS = int(os.environ.get("DIGITALKIN_STREAM_FLUSH_MS", "50"))

# -- uvloop --
UVLOOP_ENABLED = os.environ.get("DIGITALKIN_UVLOOP", "false").lower() == "true"


# ══════════════════════════════════════════════════════════════════
# Input Validation
# ══════════════════════════════════════════════════════════════════

_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_:.-]{1,256}$")


def validate_id(value: str, field_name: str) -> str | None:
    """Validate a user-provided ID against a safe pattern.

    Allows alphanumeric, underscore, colon, dot, hyphen. Max 256 chars.
    Colons are needed for IDs like ``setups:my_setup`` and
    ``modules:01kjcsma75vee1m0rdny90tvqg``.

    Args:
        value: The ID value to validate.
        field_name: Name of the field (for error messages).

    Returns:
        None if valid, error message string if invalid.
    """
    if not isinstance(value, str) or not value:
        return f"{field_name} is required"
    if not _ID_PATTERN.match(value):
        return f"{field_name} contains invalid characters"
    return None


def mask_redis_url(url: str) -> str:
    """Mask password in a Redis URL for safe logging.

    Args:
        url: Redis connection URL.

    Returns:
        URL with password replaced by ``****``.
    """
    # redis://user:password@host:port/db → redis://user:****@host:port/db
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:****@", url)
