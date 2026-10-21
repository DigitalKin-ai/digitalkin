"""The run's ``sources`` activity: one writer, one message per owner, deduplicated, enriched, capped."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from digitalkin.mixins.agui_mixin import AgUiRunState
from digitalkin.models.events import SourceCitation

pytestmark = pytest.mark.unit


def _ctx() -> MagicMock:
    ctx = MagicMock()
    ctx.callbacks.send_message = AsyncMock()
    ctx.session.current_ids = MagicMock(return_value={})
    return ctx


def _snapshots(ctx: MagicMock) -> list[dict[str, Any]]:
    return [
        call.args[0].model_dump(mode="json")["root"]["event"] for call in ctx.callbacks.send_message.await_args_list
    ]


def _cite(url: str, title: str | None = None, description: str | None = None) -> SourceCitation:
    return SourceCitation(url=url, title=title, description=description)


class TestCite:
    async def test_new_source_snapshots_the_full_list_with_explicit_replace(self) -> None:
        ctx, run = _ctx(), AgUiRunState()

        await run.cite(ctx, _cite("https://a.io/x", "A"), None)
        await run.cite(ctx, _cite("https://b.io/y"), None)

        first, second = _snapshots(ctx)
        assert first["replace"] is True
        assert first["activityType"] == "sources"
        assert first["messageId"] == second["messageId"] == f"sources-{run.sources_id}"
        assert second["content"] == {"sources": [{"url": "https://a.io/x", "title": "A"}, {"url": "https://b.io/y"}]}
        assert "subagentRunId" not in second

    async def test_dedup_on_normalised_url_without_fragment(self) -> None:
        ctx, run = _ctx(), AgUiRunState()

        await run.cite(ctx, _cite("HTTPS://A.io"), None)
        await run.cite(ctx, _cite("https://a.io/"), None)
        await run.cite(ctx, _cite("https://a.io/#section"), None)

        assert len(_snapshots(ctx)) == 1
        assert _snapshots(ctx)[0]["content"] == {"sources": [{"url": "https://a.io/"}]}

    async def test_later_citation_enriches_missing_fields_only(self) -> None:
        ctx, run = _ctx(), AgUiRunState()

        await run.cite(ctx, _cite("https://a.io/x"), None)
        await run.cite(ctx, _cite("https://a.io/x", "Title", "Why"), None)
        await run.cite(ctx, _cite("https://a.io/x", "Other title"), None)

        snapshots = _snapshots(ctx)
        assert len(snapshots) == 2
        assert snapshots[-1]["content"] == {
            "sources": [{"url": "https://a.io/x", "title": "Title", "description": "Why"}]
        }

    async def test_each_owner_has_its_own_message(self) -> None:
        """A subagent's list never replaces its parent's (``replace`` re-mints ownership)."""
        ctx, run = _ctx(), AgUiRunState()

        await run.cite(ctx, _cite("https://parent.io/"), None)
        await run.cite(ctx, _cite("https://child.io/"), "member-1")

        parent, child = _snapshots(ctx)
        assert parent["messageId"] == f"sources-{run.sources_id}"
        assert child["messageId"] == f"sources-{run.sources_id}-member-1"
        assert child["subagentRunId"] == "member-1"
        assert child["content"] == {"sources": [{"url": "https://child.io/"}]}

    async def test_cap_drops_and_logs_each_source(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_AGUI_MAX_SOURCES", "2")
        log = MagicMock()
        monkeypatch.setattr("digitalkin.mixins.agui_mixin.logger", log)
        ctx, run = _ctx(), AgUiRunState()

        for i in range(3):
            await run.cite(ctx, _cite(f"https://a.io/{i}", f"t{i}"), "member-1" if i else None)

        assert len(_snapshots(ctx)) == 2
        (message, url, title, _), _ = log.warning.call_args
        assert message.startswith("[VALIDATE AGUI-SOURCES] dropped")
        assert (url, title) == ("https://a.io/2", "t2")

    async def test_overlong_text_is_truncated(self) -> None:
        ctx, run = _ctx(), AgUiRunState()

        await run.cite(ctx, _cite("https://a.io/", "t" * 400, "d" * 1200), None)

        (source,) = _snapshots(ctx)[0]["content"]["sources"]
        assert (len(source["title"]), len(source["description"])) == (300, 1000)

    @pytest.mark.parametrize(
        "url", ["javascript:alert(1)", "data:text/html,<b>x</b>", "file:///etc/passwd", "/relative/path", "ftp://a.io/"]
    )
    def test_only_http_and_https_are_accepted(self, url: str) -> None:
        with pytest.raises(ValidationError):
            SourceCitation(url=url)

    async def test_snapshot_payload_is_not_mutated_by_later_enrichment(self) -> None:
        """Each snapshot owns its list: enriching an entry later must not rewrite an emitted event."""
        ctx, run = _ctx(), AgUiRunState()

        await run.cite(ctx, _cite("https://a.io/x"), None)
        first = ctx.callbacks.send_message.await_args_list[0].args[0]
        await run.cite(ctx, _cite("https://a.io/x", "T"), None)

        assert first.model_dump(mode="json")["root"]["event"]["content"] == {"sources": [{"url": "https://a.io/x"}]}


class TestAgUiSettings:
    def test_defaults_and_scoped_env_binding(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.models.settings.agui import get_agui_settings

        defaults = get_agui_settings()
        assert (defaults.max_sources, defaults.max_source_title_chars, defaults.max_source_description_chars) == (50, 300, 1000)
        assert defaults.max_inline_b64_chars == 13_981_016

        get_agui_settings.cache_clear()
        monkeypatch.setenv("DIGITALKIN_AGUI_MAX_SOURCES", "7")
        assert get_agui_settings().max_sources == 7

    @pytest.mark.parametrize("field", ["MAX_SOURCES", "MAX_INLINE_B64_CHARS"])
    def test_non_positive_limits_are_rejected(self, field: str, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.models.settings.agui import AgUiSettings

        monkeypatch.setenv(f"DIGITALKIN_AGUI_{field}", "0")
        with pytest.raises(ValidationError):
            AgUiSettings()
