"""Knowledge-service data models.

The knowledge service searches the enterprise knowledge base a user has connected
(Google Drive, Notion, ...) through the Context Platform. It answers with citations:
one per document, carrying the passage that matched and the URL to the original.
"""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Modality(str, Enum):
    """What kind of content a citation's passage is.

    Member names mirror the proto ``Modality`` enum (minus the ``MODALITY_`` prefix):
    they are looked up by name from the wire value, so they must match.
    """

    UNSPECIFIED = "unspecified"
    TEXT = "text"
    IMAGE = "image"
    PAGE = "page"
    SLIDE = "slide"
    TABLE = "table"
    AUDIO = "audio"
    VIDEO = "video"


class Citation(BaseModel):
    """One document the search matched, with the passage that matched it.

    A document contributes a single citation however many of its passages matched,
    which is why ``limit`` counts documents and not passages.
    """

    document_id: str = ""
    title: str = ""
    snippet: str = ""
    score: float = 0.0
    """Fused rank, NOT a similarity: comparable only against the other citations of the
    same answer, and never to be shown as a confidence."""
    source: str = ""
    """The connector the document came from, e.g. ``google_drive`` or ``notion``."""
    source_url: str = ""
    modality: Modality = Modality.UNSPECIFIED
    locator: dict[str, Any] = Field(default_factory=dict)
    """Where the passage sits in the document: ``index``, ``start``, ``end``, ``heading_path``."""
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def heading_path(self) -> list[str]:
        """The document headings leading to the passage, outermost first.

        Returns:
            The heading path, or an empty list when the locator carries none.
        """
        path = self.locator.get("heading_path")
        return [str(item) for item in path] if isinstance(path, list) else []
