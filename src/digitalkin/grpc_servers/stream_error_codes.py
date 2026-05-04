"""Stable codes for in-band ``stream.error`` sentinels.

Every code identifies a distinct failure point in the dial-back protocol.
Consumers can switch on the code without parsing the free-form ``message``
field; bench/observability tools aggregate by code.
"""

from __future__ import annotations

from enum import Enum


class StreamErrorCode(str, Enum):
    """Codes carried in ``stream.error.code`` for the dial-back path."""

    DISPATCH_UNAVAILABLE = "DISPATCH_UNAVAILABLE"
    DIAL_BACK_UNREACHABLE = "DIAL_BACK_UNREACHABLE"
    DIAL_BACK_RPC_ERROR = "DIAL_BACK_RPC_ERROR"
    DIAL_BACK_INTERNAL = "DIAL_BACK_INTERNAL"
    DIAL_BACK_NO_QUERY = "DIAL_BACK_NO_QUERY"
    INPUT_WAIT_TIMEOUT = "INPUT_WAIT_TIMEOUT"
    MODULE_RUNTIME_ERROR = "MODULE_RUNTIME_ERROR"
