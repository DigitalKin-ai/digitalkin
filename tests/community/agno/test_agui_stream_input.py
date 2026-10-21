"""AgUiStreamInput: legacy ``binary`` attachments are upgraded to 1.0 media parts, never rejected."""

from typing import Any
from unittest.mock import MagicMock

import pytest
from ag_ui.core import AssistantMessage, DataSource, DocumentPart, ImagePart, TextPart, UrlSource, UserMessage
from pydantic import ValidationError

from digitalkin.community.agno import AgUiStreamInput
from digitalkin.models.module.base_types import DataModel

pytestmark = pytest.mark.unit


class _Input(DataModel[AgUiStreamInput]):
    """A kin input shaped like the archetypes'."""


class TestAgUiStreamInput:
    @staticmethod
    def _input(*parts: dict[str, Any]) -> dict[str, Any]:
        return {
            "root": {
                "protocol": "agui_stream",
                "thread_id": "t1",
                "run_id": "r1",
                "messages": [{"id": "u1", "role": "user", "content": [{"type": "text", "text": "résume"}, *parts]}],
            }
        }

    def test_document_by_url_and_id_becomes_a_document_part(self) -> None:
        legacy = {
            "type": "binary",
            "mimeType": "application/pdf",
            "id": "f1",
            "url": "https://files.example/f1",
            "filename": "rapport.pdf",
        }

        (message,) = _Input.model_validate(self._input(legacy)).root.messages

        assert isinstance(message, UserMessage)
        text, document = message.content
        assert isinstance(text, TextPart)
        assert isinstance(document, DocumentPart)
        assert isinstance(document.source, UrlSource)
        assert (document.source.value, document.source.mime_type) == ("https://files.example/f1", "application/pdf")
        assert document.metadata == {"fileId": "f1", "filename": "rapport.pdf"}

    @pytest.mark.parametrize(("mime", "part_type"), [("image/png", ImagePart), ("text/csv", DocumentPart)])
    def test_part_type_follows_the_mime_type(self, mime: str, part_type: type) -> None:
        legacy = {"type": "binary", "mimeType": mime, "url": "https://files.example/x"}

        (message,) = _Input.model_validate(self._input(legacy)).root.messages

        assert isinstance(message.content[1], part_type)
        assert message.content[1].metadata is None

    def test_inline_data_becomes_a_data_source(self) -> None:
        legacy = {"type": "binary", "mime_type": "image/png", "data": "aGk=", "url": "https://ignored"}

        (message,) = _Input.model_validate(self._input(legacy)).root.messages

        assert isinstance(message.content[1].source, DataSource)
        assert message.content[1].source.value == "aGk="

    def test_id_only_part_is_dropped_and_logged_the_run_still_validates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        log = MagicMock()
        monkeypatch.setattr("digitalkin.community.agno.models.logger", log)

        (message,) = _Input.model_validate(
            self._input({"type": "binary", "mimeType": "application/pdf", "id": "f1", "filename": "a.pdf"})
        ).root.messages

        assert [type(part) for part in message.content] == [TextPart]
        (template, message_id, file_id, filename, mime), _ = log.warning.call_args
        assert template.startswith("[VALIDATE AGUI-BINARY-UPGRADE]")
        assert (message_id, file_id, filename, mime) == ("u1", "f1", "a.pdf", "application/pdf")

    def test_modern_parts_and_other_messages_are_untouched(self) -> None:
        modern = {
            "type": "document",
            "source": {"type": "url", "value": "https://files.example/f1", "mimeType": "application/pdf"},
            "metadata": {"fileId": "f1"},
        }
        raw = self._input(modern)
        raw["root"]["messages"] += [
            {"id": "a1", "role": "assistant", "content": "ok"},
            {"id": "u2", "role": "user", "content": "plain"},
        ]

        user, assistant, plain = _Input.model_validate(raw).root.messages

        assert user.content[1].metadata == {"fileId": "f1"}
        assert isinstance(assistant, AssistantMessage)
        assert plain.content == "plain"

    def test_an_invalid_input_is_still_rejected(self) -> None:
        with pytest.raises(ValidationError):
            _Input.model_validate({"root": {"protocol": "agui_stream", "thread_id": "t1"}})
