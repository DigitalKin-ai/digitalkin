"""Constants for the Gateway subsystem.

All values sourced from ``GatewaySettings`` (pydantic-settings).
Module-level constants kept for backward compatibility with existing imports.
"""

from __future__ import annotations

import re

from digitalkin.models.settings.gateway import GatewaySettings
from digitalkin.models.settings.profiling import ProfilingSettings

# Singleton settings — loaded once at import, env vars read here
_gw = GatewaySettings()
_prof = ProfilingSettings()

# ══════════════════════════════════════════════════════════════════
# Redis Key Patterns (not configurable — structural)
# ══════════════════════════════════════════════════════════════════

REDIS_KEY_SESSION = "gateway:session:{task_id}"
REDIS_KEY_SESSION_COUNT = "gateway:session_count"
REDIS_KEY_HEARTBEATS = "gateway:heartbeats"
REDIS_KEY_STREAM = "task:{task_id}:stream"
REDIS_KEY_CURSOR = "task:{task_id}:cursor"
REDIS_KEY_SIGNAL_CHANNEL = "signal_ch:{task_id}"

# ══════════════════════════════════════════════════════════════════
# Gateway
# ══════════════════════════════════════════════════════════════════

MAX_STREAMS = _gw.max_streams
MAX_LOCAL_CACHE = _gw.max_local_cache
HEARTBEAT_TTL_S = _gw.heartbeat_ttl
REAPER_INTERVAL_S = _gw.reaper_interval
SESSION_STATE_TTL_S = _gw.session_state_ttl
DIAL_BACK_BIDI_TIMEOUT_S = _gw.dial_back_bidi_timeout_s

# ══════════════════════════════════════════════════════════════════
# Streams
# ══════════════════════════════════════════════════════════════════

STREAM_TTL_S = _gw.stream.redis_stream_ttl
STREAM_MAXLEN = _gw.stream.redis_stream_maxlen
CURSOR_TTL_S = _gw.stream.redis_cursor_ttl
STREAM_READ_BLOCK_MS = _gw.stream.stream_read_block_ms
STREAM_BATCH_SIZE = _gw.stream.stream_batch_size
STREAM_FLUSH_MS = _gw.stream.stream_flush_ms

# ══════════════════════════════════════════════════════════════════
# Backpressure
# ══════════════════════════════════════════════════════════════════

BACKPRESSURE_THRESHOLD = _gw.backpressure.backpressure_threshold
BACKPRESSURE_DELAY_MS = _gw.backpressure.backpressure_delay_ms
BACKPRESSURE_CHECK_INTERVAL = _gw.backpressure.backpressure_check_interval
BACKPRESSURE_TIMEOUT_S = _gw.backpressure.backpressure_timeout_s

# ══════════════════════════════════════════════════════════════════
# Queue & Timeout
# ══════════════════════════════════════════════════════════════════

DEFAULT_OUTPUT_QUEUE_SIZE = _gw.queue.output_queue_size
DEFAULT_INPUT_QUEUE_SIZE = _gw.queue.input_queue_size
ENQUEUE_TIMEOUT_S = _gw.queue.enqueue_timeout_s
INPUT_WAIT_TIMEOUT_S = _gw.queue.dispatcher_input_wait_s  # retired in 2.B; see GatewayQueueSettings
TOOLKIT_CACHE_TTL_S = _gw.queue.toolkit_cache_ttl_s
REDIS_HEALTH_CHECK_TIMEOUT_S = _gw.redis_health_timeout

# ══════════════════════════════════════════════════════════════════
# Derived
# ══════════════════════════════════════════════════════════════════

MAX_FROM_SEQ = STREAM_MAXLEN * 10
UVLOOP_ENABLED = _prof.uvloop


# ══════════════════════════════════════════════════════════════════
# Input Validation
# ══════════════════════════════════════════════════════════════════

_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_:.-]{1,256}$")
_ADDRESS_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]{1,253}:\d{1,5}$")
# Wildcard bind addresses — invalid as dial-back targets even though
# servers commonly bind to them. (S104 flags the literal as a bind hint.)
_WILDCARD_HOSTS = frozenset({"[::]", "0.0.0.0", "::"})  # noqa: S104


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


def validate_address(value: str, field_name: str) -> str | None:
    """Validate a ``host:port`` address used for dial-back.

    Rejects empty, malformed, out-of-range, and wildcard bind addresses.
    Wildcards (``[::]``, ``0.0.0.0``, ``::``) are bind addresses, not
    routable destinations — accepting them as ``x-client-address`` is a
    debugging trap because the gateway cannot dial back to them.

    Args:
        value: The address to validate.
        field_name: Name of the field (for error messages).

    Returns:
        None if valid, error message string if invalid.
    """
    if not isinstance(value, str) or not value:
        return f"{field_name} is required"
    if not _ADDRESS_PATTERN.match(value):
        return f"{field_name} must be host:port"
    host, _, port_str = value.partition(":")
    port = int(port_str)
    if not (1 <= port <= 65535):  # noqa: PLR2004 — TCP port range
        return f"{field_name} port out of range"
    if host in _WILDCARD_HOSTS:
        return f"{field_name} cannot be a wildcard bind address"
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
