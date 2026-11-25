"""Failure a trigger raises with a message the calling agent can act on."""

import re


class ToolError(Exception):
    """A trigger failure whose message is written for the calling agent.

    The SDK answers it as ``MODULE_FAILED`` with this message (redacted) and the retry hint, and logs it
    without a traceback. Any other exception is answered the same way with ``<Type>: <text>``.
    """

    def __init__(self, message: str, *, retryable: bool | None = None) -> None:
        """Initialize the error.

        Args:
            message: What failed and what the agent can do about it.
            retryable: True when a later call can succeed, False when the same call fails again, None when
                unknown.
        """
        super().__init__(message)
        self.message = message
        self.retryable = retryable

    @staticmethod
    def redact(text: str) -> str:
        """Mask URL query strings and credential values in a failure text.

        Args:
            text: The failure text.

        Returns:
            The text with every URL query string and every ``key=value`` / ``key: value`` credential masked.
        """
        text = re.sub(r"(https?://[^\s?#'\"<>]+)\?[^\s#'\"<>]+", r"\1?<redacted>", text)
        return re.sub(
            r"(?i)\b(api[_-]?key|access[_-]?token|token|secret|password|authorization)(['\"]?\s*[:=]\s*['\"]?)"
            r"(?:bearer\s+)?[^\s'\",&}]+",
            r"\1\2<redacted>",
            text,
        )
