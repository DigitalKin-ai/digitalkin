"""Toolkit letting the agent perceive uploaded media (images, audio, video)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agno.media import Audio, Image, Video
from agno.tools import Toolkit
from agno.tools.function import ToolResult

from digitalkin.logger import logger
from digitalkin.models.services.services import Context

if TYPE_CHECKING:
    from digitalkin.services.filesystem.filesystem_strategy import FilesystemRecord, FilesystemStrategy


class MediaViewerTools(Toolkit):
    """Load uploaded media from the filesystem so the model perceives it.

    Only the tools the model can use are registered: pass the model's capabilities as the
    ``image`` / ``audio`` / ``video`` flags. With every flag off the toolkit has no tools.
    """

    def __init__(self, filesystem: FilesystemStrategy, *, image: bool, audio: bool, video: bool) -> None:
        """Register the tools matching the model's media capabilities.

        Args:
            filesystem: The filesystem strategy the media is fetched from.
            image: Register ``view_image`` (the model accepts image input).
            audio: Register ``listen_audio`` (the model accepts audio input).
            video: Register ``watch_video`` (the model accepts video input).
        """
        tools = []
        lines = []
        if image:
            tools.append(self.view_image)
            lines.append("- `view_image(file_id)` — see an image")
        if audio:
            tools.append(self.listen_audio)
            lines.append("- `listen_audio(file_id)` — hear an audio")
        if video:
            tools.append(self.watch_video)
            lines.append("- `watch_video(file_id)` — watch a video")
        super().__init__(
            name="media_viewer_tools",
            instructions="You can perceive uploaded media using these tools:\n"
            + "\n".join(lines)
            + '\n\nUse the file_id from the conversation context (shown as "ID: xxx").\n'
            "Only call these when you need to perceive the actual content.",
            add_instructions=True,
            tools=tools,
        )
        self._filesystem = filesystem

    @staticmethod
    def media_format(content_type: str | None) -> str | None:
        """Derive Agno's short media ``format`` (``png``, ``mp3``, ``mov``) from a MIME type.

        ``FilesystemRecord.content_type`` is a full MIME type; Agno's ``format`` expects the bare
        subtype, and a MIME type there yields ``data:image/image/png;...``, which providers reject.

        Args:
            content_type: A MIME type such as ``image/png`` or ``audio/mpeg``.

        Returns:
            The short format, or ``None`` for a missing, malformed or non-media MIME type (Agno
            then falls back to magic-byte detection).
        """
        if not content_type or "/" not in content_type:
            return None
        main_type, subtype = content_type.split("/", 1)
        if main_type.strip().lower() not in {"image", "audio", "video"}:
            return None
        subtype = subtype.split(";", 1)[0].strip().lower()
        if not subtype:
            return None
        return {
            "jpg": "jpeg",
            "mpeg": "mp3",
            "x-wav": "wav",
            "wave": "wav",
            "x-m4a": "m4a",
            "quicktime": "mov",
            "x-matroska": "mkv",
            "x-msvideo": "avi",
            "x-flv": "flv",
            "x-ms-wmv": "wmv",
            "3gpp": "3gp",
            "svg+xml": "svg",
        }.get(subtype, subtype)

    async def view_image(self, file_id: str) -> ToolResult:
        """Load an uploaded image so you can see it. Call this when you need to perceive the visual content of an image.

        Args:
            file_id: The unique identifier of the image.

        Returns:
            The image content for visual perception.
        """
        record = await self._fetch(file_id)
        if isinstance(record, ToolResult):
            return record
        logger.info("MediaViewerTools: returning image %s (%s)", record.name, record.content_type)
        return ToolResult(
            content=f"Image: {record.name} ({record.size_bytes} bytes)",
            images=[
                Image(
                    content=record.content,
                    mime_type=record.content_type,
                    format=self.media_format(record.content_type),
                )
            ],
        )

    async def listen_audio(self, file_id: str) -> ToolResult:
        """Load an uploaded audio so you can hear it. Call this when you need to perceive the content of an audio.

        Args:
            file_id: The unique identifier of the audio.

        Returns:
            The audio content for auditory perception.
        """
        record = await self._fetch(file_id)
        if isinstance(record, ToolResult):
            return record
        logger.info("MediaViewerTools: returning audio %s (%s)", record.name, record.content_type)
        return ToolResult(
            content=f"Audio: {record.name} ({record.size_bytes} bytes)",
            audios=[
                Audio(
                    content=record.content,
                    mime_type=record.content_type,
                    format=self.media_format(record.content_type),
                )
            ],
        )

    async def watch_video(self, file_id: str) -> ToolResult:
        """Load an uploaded video so you can watch it. Call this when you need to perceive the content of a video.

        Args:
            file_id: The unique identifier of the video.

        Returns:
            The video content for visual perception.
        """
        record = await self._fetch(file_id)
        if isinstance(record, ToolResult):
            return record
        logger.info("MediaViewerTools: returning video %s (%s)", record.name, record.content_type)
        return ToolResult(
            content=f"Video: {record.name} ({record.size_bytes} bytes)",
            videos=[
                Video(
                    content=record.content,
                    mime_type=record.content_type,
                    format=self.media_format(record.content_type),
                )
            ],
        )

    async def _fetch(self, file_id: str) -> ToolResult | FilesystemRecord:
        """Fetch a media file with its content.

        Args:
            file_id: The unique identifier of the media.

        Returns:
            The record on success, or a ToolResult carrying the error for the model.
        """
        try:
            record = await self._filesystem.get_file(file_id, context=Context.MISSIONS, include_content=True)
        except Exception as error:
            logger.warning("MediaViewerTools: failed to fetch %s: %s", file_id, error)
            return ToolResult(content=f"Error: could not fetch media {file_id}: {error}")
        if not record.content:
            return ToolResult(content=f"Media '{record.name}' has no content available.")
        return record
