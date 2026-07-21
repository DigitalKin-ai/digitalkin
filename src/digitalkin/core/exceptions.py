"""Exceptions for the DigitalKin core package."""


class BackpressureTimeoutError(Exception):
    """Producer's XADD throttled past the backpressure timeout.

    Throttled past ``JobManagerSettings.backpressure_timeout``.
    Caller (typically the module's ``_on_output`` callback) must surface
    this as ``stream.error(code=BACKPRESSURE_TIMEOUT)`` via the
    ``_emit_fatal_to_redis`` path so the consumer sees a typed sentinel
    instead of a silent stall.
    """


class BulkheadFullError(Exception):
    """Raised when a bulkhead semaphore cannot be acquired within timeout."""


class RedisUnreachableError(Exception):
    """Raised at gateway boot when Redis ping fails.

    Redis is a required dependency for gateway operation (stream persistence,
    pub/sub signals). Failing fast at boot is preferable to lazy first-request
    failures that surface as opaque task errors.
    """

    def __init__(self, masked_url: str) -> None:
        """Initialize the error with a (masked) Redis URL for context.

        Args:
            masked_url: Redis connection URL with credentials masked.
        """
        super().__init__(f"Redis ping failed at gateway boot ({masked_url})")
