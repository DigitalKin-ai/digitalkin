"""Tests for MediaViewerTools media typing and capability gating.

``FilesystemRecord.content_type`` is a full MIME type (``image/png``). Agno's ``format`` field
expects the bare subtype (``png``); a MIME type there makes Agno build
``data:image/image/png;base64,...``, which providers reject.
"""

from unittest.mock import AsyncMock

import pytest
from agno.utils.openai import process_image

from digitalkin.community.agno.toolkits import MediaViewerTools
from digitalkin.services.filesystem.filesystem_strategy import FilesystemRecord

FILE_ID = "files:media"


def _record(name: str, content_type: str, content: bytes | None) -> FilesystemRecord:
    return FilesystemRecord(
        id=FILE_ID,
        context="MISSIONS",
        name=name,
        content_type=content_type,
        size_bytes=len(content or b""),
        storage_uri="s3://bucket/x",
        file_url="https://example.com/x",
        content=content,
    )


def _toolkit(record: FilesystemRecord | None = None, *, error: Exception | None = None) -> MediaViewerTools:
    filesystem = AsyncMock()
    filesystem.get_file = AsyncMock(return_value=record, side_effect=error)
    return MediaViewerTools(filesystem=filesystem, image=True, audio=True, video=True)


async def test_view_image_sets_mime_type_and_bare_format() -> None:
    toolkit = _toolkit(_record("logo.png", "image/png", b"\x89PNG\r\n\x1a\n" + b"\0" * 16))

    result = await toolkit.view_image(FILE_ID)

    assert result.images is not None
    image = result.images[0]
    assert (image.mime_type, image.format) == ("image/png", "png")
    assert process_image(image)["image_url"]["url"].startswith("data:image/png;base64,")


async def test_listen_audio_sets_mime_type_and_bare_format() -> None:
    result = await _toolkit(_record("voice.mp3", "audio/mpeg", b"\xff\xfb\x90\x00")).listen_audio(FILE_ID)
    assert result.audios is not None
    assert (result.audios[0].mime_type, result.audios[0].format) == ("audio/mpeg", "mp3")


async def test_watch_video_sets_mime_type_and_bare_format() -> None:
    result = await _toolkit(_record("clip.mov", "video/quicktime", b"\0\0\0\x14ftypqt  ")).watch_video(FILE_ID)
    assert result.videos is not None
    assert (result.videos[0].mime_type, result.videos[0].format) == ("video/quicktime", "mov")


async def test_fetch_failure_is_reported_to_the_model() -> None:
    result = await _toolkit(error=RuntimeError("gone")).view_image(FILE_ID)
    assert result.content == f"Error: could not fetch media {FILE_ID}: gone"
    assert not result.images


async def test_empty_content_is_reported_to_the_model() -> None:
    result = await _toolkit(_record("empty.png", "image/png", None)).view_image(FILE_ID)
    assert result.content == "Media 'empty.png' has no content available."


def test_only_capable_tools_are_registered() -> None:
    toolkit = MediaViewerTools(filesystem=AsyncMock(), image=True, audio=False, video=False)
    assert [tool.__name__ for tool in toolkit.tools] == ["view_image"]
    assert "view_image" in (toolkit.instructions or "")
    assert "listen_audio" not in (toolkit.instructions or "")


def test_no_capability_means_no_tools() -> None:
    assert MediaViewerTools(filesystem=AsyncMock(), image=False, audio=False, video=False).tools == []


@pytest.mark.parametrize(
    ("mime", "expected"),
    [
        ("image/png", "png"),
        ("image/jpeg", "jpeg"),
        ("image/jpg", "jpeg"),
        ("IMAGE/WEBP", "webp"),
        ("image/svg+xml", "svg"),
        ("audio/mpeg", "mp3"),
        ("audio/x-wav", "wav"),
        ("audio/wav; codecs=1", "wav"),
        ("video/quicktime", "mov"),
        ("video/mp4", "mp4"),
        ("application/octet-stream", None),
        ("image/", None),
        ("", None),
        (None, None),
        ("png", None),
    ],
)
def test_media_format(mime: str | None, expected: str | None) -> None:
    assert MediaViewerTools.media_format(mime) == expected
