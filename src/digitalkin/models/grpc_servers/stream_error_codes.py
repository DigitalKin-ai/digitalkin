"""Stable codes for in-band ``stream.error`` sentinels.

Every code identifies a distinct failure point in the dial-back protocol.
Consumers can switch on the code without parsing the free-form ``message``
field; bench/observability tools aggregate by code.
"""

from __future__ import annotations

from enum import Enum


class StreamErrorCode(str, Enum):
    """Codes carried in ``stream.error.code`` for the dial-back path."""

    DIAL_BACK_UNREACHABLE = "DIAL_BACK_UNREACHABLE"
    DIAL_BACK_RPC_ERROR = "DIAL_BACK_RPC_ERROR"
    DIAL_BACK_INTERNAL = "DIAL_BACK_INTERNAL"
    DIAL_BACK_NO_QUERY = "DIAL_BACK_NO_QUERY"
    DIAL_BACK_IDLE_TIMEOUT = "DIAL_BACK_IDLE_TIMEOUT"
    STREAM_IDLE_TIMEOUT = "STREAM_IDLE_TIMEOUT"
    REDIS_UNAVAILABLE = "REDIS_UNAVAILABLE"
    MODULE_RUNTIME_ERROR = "MODULE_RUNTIME_ERROR"
    INPUT_VALIDATION_ERROR = "INPUT_VALIDATION_ERROR"
    BACKPRESSURE_TIMEOUT = "BACKPRESSURE_TIMEOUT"
