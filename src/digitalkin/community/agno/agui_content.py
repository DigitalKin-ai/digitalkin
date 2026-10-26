"""AG-UI user message content to an Agno prompt and images, attachments included."""

from __future__ import annotations

import base64
import binascii
from typing import TYPE_CHECKING

from ag_ui.core.types import DataSource, FileSource, ImagePart, TextPart, UrlSource
from agno.media import Image

from digitalkin.logger import logger
from digitalkin.models.settings.agui import get_agui_settings

if TYPE_CHECKING:
    from ag_ui.core.types import ContentPart

    from digitalkin.models.module import ModuleContext


class AgUiContent:
    """Resolve an AG-UI 1.0 user message into the text and images an Agno run receives.

    Unlike :meth:`AgnoHitlRunner._content_text`, attachments are kept: image parts become Agno
    images (``url`` as is, inline ``data`` decoded, a platform file resolved through the
    filesystem) and every attachment is surfaced as a ``[... id=...]`` marker the agent can
    reference in later turns. Legacy ``binary`` parts arrive already upgraded by
    :class:`~digitalkin.community.agno.models.AgUiStreamInput`, the file id in ``metadata.fileId``.
    """

    @staticmethod
    async def extract(content: str | list[ContentPart], context: ModuleContext) -> tuple[str, list[Image]]:
        """Build the prompt and images of a user message.

        Args:
            content: The ``UserMessage.content``.
            context: Module context for filesystem lookups.

        Returns:
            ``(message, images)``: the attachment markers, one per line, followed by the text parts
            joined by a space; and the resolved images.
        """
        if isinstance(content, str):
            return content, []
        images: list[Image] = []
        lines: list[str] = []
        texts: list[str] = []
        for part in content:
            if isinstance(part, TextPart):
                texts.append(part.text)
                continue
            metadata = part.metadata if isinstance(part.metadata, dict) else {}
            # The platform file id; a content part's own id is never one.
            file_id = metadata.get("fileId")
            filename = metadata.get("filename") or "n/a"
            source = part.source
            if isinstance(part, ImagePart):
                image = await AgUiContent.resolve_image(source, file_id, context)
                if image is not None:
                    images.append(image)
                if file_id:
                    lines.append(f"[Image attached id={file_id} filename={filename}]")
            elif file_id:
                lines.append(
                    f"[Attached file id={file_id} (mime={source.mime_type or 'unknown'}, filename={filename})]"
                )
            elif isinstance(source, UrlSource):
                lines.append(
                    f"[Attached file url={source.value} (mime={source.mime_type or 'unknown'}, filename={filename})]"
                )
            else:
                # TODO(validate): AGUI-PART-DROPPED attachments with neither a file id nor a url are logged
                logger.warning(
                    "[VALIDATE AGUI-PART-DROPPED] %s part with neither a file id nor a url (%s source, %s, %d chars)",
                    part.type,
                    source.type,
                    source.mime_type,
                    len(source.value),
                )
        lines.append(" ".join(texts))
        return "\n".join(lines), images

    @staticmethod
    async def resolve_image(
        source: DataSource | UrlSource | FileSource, file_id: str | None, context: ModuleContext
    ) -> Image | None:
        """Turn an image part's source into an Agno image.

        Inline data is bounded by ``max_inline_b64_chars`` before decoding; a provider ``file``
        handle is never used, the platform file id is resolved to its URL instead.

        Args:
            source: The part's ``url``, inline ``data`` or provider ``file`` source.
            file_id: The platform file id, kept on ``Image.id``.
            context: Module context for filesystem lookups.

        Returns:
            The image, or ``None`` when it cannot be resolved (logged).
        """
        if isinstance(source, UrlSource):
            return Image(url=source.value, id=file_id)
        if isinstance(source, DataSource):
            if len(source.value) <= get_agui_settings().max_inline_b64_chars:
                try:
                    return Image(
                        content=base64.b64decode(source.value, validate=True), mime_type=source.mime_type, id=file_id
                    )
                except binascii.Error as error:
                    logger.info("Inline image %s is not valid base64: %s", file_id, error)
            # TODO(validate): AGUI-PART-DROPPED oversized or invalid inline images are logged, never repr'd
            logger.warning(
                "[VALIDATE AGUI-PART-DROPPED] inline image %s (%s, %d chars) not passed to the model",
                file_id,
                source.mime_type,
                len(source.value),
            )
            return None
        if not file_id:
            return None
        try:
            record = await context.filesystem.get_file(file_id)
        except Exception as error:
            logger.warning("Could not resolve image %s: %s", file_id, error)
            return None
        return Image(url=record.file_url, id=file_id) if record.file_url else None
