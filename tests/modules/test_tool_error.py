"""ToolError: the message a trigger raises for the calling agent, and its redaction."""

import pytest

from digitalkin.modules import ToolError


class TestRedact:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            (
                "Client error '429' for url 'https://serpapi.com/search?engine=baidu&api_key=K1'",
                "Client error '429' for url 'https://serpapi.com/search?<redacted>'",
            ),
            ("GET https://eutils.ncbi.nlm.nih.gov/efetch.fcgi?db=pubmed&api_key=K2 failed", (
                "GET https://eutils.ncbi.nlm.nih.gov/efetch.fcgi?<redacted> failed"
            )),
            ("bad credentials api_key=K3 for tenant", "bad credentials api_key=<redacted> for tenant"),
            ("{'Authorization': 'Bearer K4'}", "{'Authorization': '<redacted>'}"),
            ('{"access_token": "K5", "n": 1}', '{"access_token": "<redacted>", "n": 1}'),
            ("password: K6", "password: <redacted>"),
            ("timeout after 30 s on https://api.x/v1/items", "timeout after 30 s on https://api.x/v1/items"),
        ],
    )
    def test_masks_query_strings_and_credentials(self, text: str, expected: str) -> None:
        assert ToolError.redact(text) == expected


class TestToolError:
    def test_keeps_message_and_retry_hint(self) -> None:
        error = ToolError("rate limit", retryable=True)
        assert (str(error), error.message, error.retryable) == ("rate limit", "rate limit", True)

    def test_retry_hint_defaults_to_unknown(self) -> None:
        assert ToolError("boom").retryable is None
