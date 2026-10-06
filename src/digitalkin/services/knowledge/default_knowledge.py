"""Default knowledge implementation."""

import re
from typing import Any

from digitalkin.models.services.knowledge import Citation
from digitalkin.services.knowledge.knowledge_strategy import KnowledgeStrategy

_WORDS = re.compile(r"\w+", re.UNICODE)


class DefaultKnowledge(KnowledgeStrategy):
    """Local knowledge strategy backed by an in-memory corpus.

    Keeps ``mode=LOCAL`` usable without the Context Platform: a sandbox or a test seeds
    documents through the strategy config and gets deterministic answers offline::

        services_config_params = {
            "knowledge": {
                "config": {
                    "documents": [
                        {
                            "document_id": "doc_1",
                            "title": "Pricing",
                            "snippet": "Apollo moved to per-seat.",
                            "source": "google_drive",
                            "source_url": "https://...",
                        },
                    ]
                }
            }
        }

    Matching is word overlap over title and snippet — enough to exercise a caller, never
    a substitute for the real hybrid search.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize with the corpus declared in the strategy config."""
        super().__init__(*args, **kwargs)
        documents = (self.config or {}).get("documents") or []
        self._documents: list[Citation] = [
            document if isinstance(document, Citation) else Citation.model_validate(document) for document in documents
        ]

    @staticmethod
    def _overlap(query: str, document: Citation) -> float:
        """Score a document by how much of the query its text carries.

        Args:
            query: The search query.
            document: The candidate document.

        Returns:
            The share of distinct query words found, between 0 and 1.
        """
        wanted = {word.casefold() for word in _WORDS.findall(query)}
        if not wanted:
            return 0.0
        haystack = {word.casefold() for word in _WORDS.findall(f"{document.title} {document.snippet}")}
        return len(wanted & haystack) / len(wanted)

    async def search(
        self,
        query: str,
        sources: list[str] | None = None,
        limit: int | None = None,
    ) -> list[Citation]:
        """Search the in-memory corpus.

        Args:
            query: What to look for.
            sources: Restrict to these connectors; ``None`` searches the whole corpus.
            limit: Maximum number of documents to return.

        Returns:
            The matching documents, best first.
        """
        text, wanted_sources, bounded = self.normalize(query, sources, limit)
        scored = [
            (score, document)
            for document in self._documents
            if (not wanted_sources or document.source in wanted_sources)
            and (score := self._overlap(text, document)) > 0
        ]
        # Title breaks ties, so a corpus always answers in the same order.
        scored.sort(key=lambda pair: (-pair[0], pair[1].title))
        return [document.model_copy(update={"score": score}) for score, document in scored[:bounded]]
