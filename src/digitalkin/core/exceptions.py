"""Exceptions for the DigitalKin core package."""


class BackpressureTimeoutError(Exception):
    """Producer's XADD throttled past the backpressure timeout.

    Throttled past :data:`GatewayBackpressureSettings.backpressure_timeout_s`.
    Caller (typically the module's ``_on_output`` callback) must surface
    this as ``stream.error(code=BACKPRESSURE_TIMEOUT)`` via the
    ``_emit_fatal_to_redis`` path so the consumer sees a typed sentinel
    instead of a silent stall.
    """


class BulkheadFullError(Exception):
    """Raised when a bulkhead semaphore cannot be acquired within timeout."""
