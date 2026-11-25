"""Stable codes carried in ``RUN_ERROR.code`` and ``SUBAGENT_ERROR.code``."""

from enum import Enum


class RunErrorCode(str, Enum):
    """Why a run, or a delegated run, failed.

    Values are UPPER_SNAKE_CASE, at most 63 characters (AIP-193 ``reason``), and equal to their
    name. The set is append-only: a value is never renamed or reused, since consumers switch on
    it. ``message`` stays free-form technical text.
    """

    MODEL_CONTENT_FILTERED = "MODEL_CONTENT_FILTERED"  # finish_reason content_filter, or a content-policy rejection
    MODEL_CONTEXT_LENGTH_EXCEEDED = "MODEL_CONTEXT_LENGTH_EXCEEDED"  # prompt still too long after compaction
    MODEL_BUDGET_EXCEEDED = "MODEL_BUDGET_EXCEEDED"  # 402, or a proxy budget
    MODEL_REQUEST_INVALID = "MODEL_REQUEST_INVALID"  # 400, 422
    MODEL_AUTHENTICATION_FAILED = "MODEL_AUTHENTICATION_FAILED"  # 401
    MODEL_PERMISSION_DENIED = "MODEL_PERMISSION_DENIED"  # 403
    MODEL_NOT_FOUND = "MODEL_NOT_FOUND"  # 404, unknown model
    MODEL_RATE_LIMITED = "MODEL_RATE_LIMITED"  # 429
    MODEL_OVERLOADED = "MODEL_OVERLOADED"  # 529, matched on "overloaded": litellm reports it as a 500
    MODEL_TIMEOUT = "MODEL_TIMEOUT"  # request timeout
    MODEL_STREAM_INTERRUPTED = "MODEL_STREAM_INTERRUPTED"  # stream dropped after it started
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"  # 5xx, connection failure
    INPUT_INVALID = "INPUT_INVALID"  # run input failed validation
    INPUT_REJECTED = "INPUT_REJECTED"  # agno input guardrail
    OUTPUT_REJECTED = "OUTPUT_REJECTED"  # agno output guardrail
    TOOL_RESULTS_PARTIAL = "TOOL_RESULTS_PARTIAL"  # HITL resume without every pending tool result
    AUTO_CONTINUE_LIMIT = "AUTO_CONTINUE_LIMIT"  # HITL in-process auto-continue cap reached
    RUN_CANCELLED = "RUN_CANCELLED"  # user stop: a subagent it cut short; the run itself ends on RUN_FINISHED cancelled
    PERMISSION_DENIED = "PERMISSION_DENIED"  # a platform service refused the module
    MODULE_FAILED = "MODULE_FAILED"  # module raised while running
    MODULE_INIT_FAILED = "MODULE_INIT_FAILED"  # module raised while initializing
    UNKNOWN = "UNKNOWN"  # anything not mapped above
    RUN_INTERRUPTED = "RUN_INTERRUPTED"  # cancelled by the platform, not the user: shutdown, timeout, service error

    @property
    def retryable(self) -> bool | None:
        """Report whether a new attempt, started later by the user or the platform, can succeed.

        The SDK's own retries (proxy, litellm client, stream resend) are already spent when a
        code is emitted; this is a hint about a later attempt, not about retrying now.

        Returns:
            True for transient failures, False when the same request fails again, None when the
            SDK cannot tell.
        """
        match self:
            case (
                RunErrorCode.MODEL_RATE_LIMITED
                | RunErrorCode.MODEL_OVERLOADED
                | RunErrorCode.MODEL_TIMEOUT
                | RunErrorCode.MODEL_STREAM_INTERRUPTED
                | RunErrorCode.MODEL_UNAVAILABLE
                | RunErrorCode.TOOL_RESULTS_PARTIAL
                | RunErrorCode.AUTO_CONTINUE_LIMIT
                | RunErrorCode.RUN_INTERRUPTED
            ):
                return True
            case (
                RunErrorCode.MODEL_CONTENT_FILTERED
                | RunErrorCode.MODEL_CONTEXT_LENGTH_EXCEEDED
                | RunErrorCode.MODEL_BUDGET_EXCEEDED
                | RunErrorCode.MODEL_REQUEST_INVALID
                | RunErrorCode.MODEL_AUTHENTICATION_FAILED
                | RunErrorCode.MODEL_PERMISSION_DENIED
                | RunErrorCode.MODEL_NOT_FOUND
                | RunErrorCode.INPUT_INVALID
                | RunErrorCode.INPUT_REJECTED
                | RunErrorCode.OUTPUT_REJECTED
                | RunErrorCode.RUN_CANCELLED
                | RunErrorCode.PERMISSION_DENIED
            ):
                return False
            case _:
                return None
