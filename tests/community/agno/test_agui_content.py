"""Tests for AgUiContent: AG-UI 1.0 user content to an Agno prompt, images and attachment markers."""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

from digitalkin.community.agno.agui_content import AgUiContent
from digitalkin.community.agno.models import AgUiStreamInput


def _content(*parts: dict[str, Any]) -> Any:
    """User content as Horizon sends it: pre-1.0 ``binary`` parts upgraded by the SDK trigger."""
    data = AgUiStreamInput.model_validate({
        "thread_id": "thread-1",
        "run_id": "run-1",
        "messages": [{"id": "u1", "role": "user", "content": [{"type": "text", "text": "Read this"}, *parts]}],
    })
    return data.messages[0].content


def _context(record: Any = None, error: Exception | None = None) -> Mock:
    context = Mock()
    context.filesystem.get_file = AsyncMock(return_value=record, side_effect=error)
    return context


async def test_plain_text_is_passed_as_is() -> None:
    assert await AgUiContent.extract("hello there", _context()) == ("hello there", [])


async def test_legacy_binary_image_becomes_an_image_and_a_marker() -> None:
    content = _content({
        "type": "binary",
        "mimeType": "image/png",
        "id": "files:img",
        "url": "https://s3/i.png",
        "filename": "i.png",
    })

    message, images = await AgUiContent.extract(content, _context())

    assert message == "[Image attached id=files:img filename=i.png]\nRead this"
    assert [(image.url, image.id) for image in images] == [("https://s3/i.png", "files:img")]


async def test_legacy_binary_document_becomes_a_marker() -> None:
    content = _content({
        "type": "binary",
        "mimeType": "application/pdf",
        "id": "files:doc",
        "url": "https://s3/x.pdf",
        "filename": "x.pdf",
    })

    message, images = await AgUiContent.extract(content, _context())

    assert message == "[Attached file id=files:doc (mime=application/pdf, filename=x.pdf)]\nRead this"
    assert images == []


async def test_inline_image_data_is_decoded() -> None:
    content = _content({"type": "image", "source": {"type": "data", "value": "aGVsbG8=", "mimeType": "image/png"}})

    _, images = await AgUiContent.extract(content, _context())

    assert (images[0].content, images[0].mime_type) == (b"hello", "image/png")


async def test_invalid_inline_image_is_dropped() -> None:
    content = _content({"type": "image", "source": {"type": "data", "value": "not base64!", "mimeType": "image/png"}})
    assert (await AgUiContent.extract(content, _context()))[1] == []


async def test_oversized_inline_image_is_dropped(monkeypatch: Any) -> None:
    monkeypatch.setattr(
        "digitalkin.community.agno.agui_content.get_agui_settings", lambda: SimpleNamespace(max_inline_b64_chars=4)
    )
    content = _content({"type": "image", "source": {"type": "data", "value": "aGVsbG8=", "mimeType": "image/png"}})
    assert (await AgUiContent.extract(content, _context()))[1] == []


async def test_file_handle_image_is_resolved_through_the_filesystem() -> None:
    context = _context(SimpleNamespace(file_url="https://s3/resolved.png"))
    content = _content({
        "type": "image",
        "source": {"type": "file", "value": "provider-handle", "mimeType": "image/png"},
        "metadata": {"fileId": "files:img"},
    })

    message, images = await AgUiContent.extract(content, context)

    context.filesystem.get_file.assert_awaited_once_with("files:img")
    assert images[0].url == "https://s3/resolved.png"
    assert message.startswith("[Image attached id=files:img")


async def test_unresolvable_file_image_keeps_its_marker() -> None:
    content = _content({
        "type": "image",
        "source": {"type": "file", "value": "provider-handle", "mimeType": "image/png"},
        "metadata": {"fileId": "files:img"},
    })

    message, images = await AgUiContent.extract(content, _context(error=RuntimeError("gone")))

    assert images == []
    assert message.startswith("[Image attached id=files:img")


async def test_a_url_only_document_still_reaches_the_agent() -> None:
    content = _content({
        "type": "document",
        "source": {"type": "url", "value": "https://s3/y.pdf", "mimeType": "application/pdf"},
    })

    message, _ = await AgUiContent.extract(content, _context())

    assert message.startswith("[Attached file url=https://s3/y.pdf (mime=application/pdf")


async def test_a_document_without_id_or_url_is_dropped() -> None:
    content = _content({
        "type": "document",
        "source": {"type": "data", "value": "aGVsbG8=", "mimeType": "application/pdf"},
    })
    assert await AgUiContent.extract(content, _context()) == ("Read this", [])


async def test_a_part_id_is_never_taken_for_a_file_id() -> None:
    """A 1.0 part's own id is not a platform file id: only ``metadata.fileId`` names a file."""
    content = _content({
        "type": "document",
        "id": "part-7",
        "source": {"type": "url", "value": "https://s3/z.pdf", "mimeType": "application/pdf"},
    })

    message, _ = await AgUiContent.extract(content, _context())

    assert "part-7" not in message
    assert message.startswith("[Attached file url=https://s3/z.pdf")
