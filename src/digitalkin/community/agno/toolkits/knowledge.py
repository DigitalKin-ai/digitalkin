"""Toolkit exposing the user's connected knowledge base to the agent."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.logger import logger
from digitalkin.models.services.knowledge import Citation, Modality
from digitalkin.services.knowledge.exceptions import KnowledgeServiceError

if TYPE_CHECKING:
    from digitalkin.models.module import ModuleContext
    from digitalkin.services.knowledge.knowledge_strategy import KnowledgeStrategy


class KnowledgeTools(DkToolkit):
    """Toolkit that lets the agent search the enterprise knowledge base.

    What the search reaches is what the *current user* connected and may read: the
    permission filter is compiled into the query by the platform, so there is no identity
    to pass and no result to filter afterwards.

    An empty answer is a legitimate answer — nothing matched, or nothing is connected —
    and is reported as a success with zero results so the agent says so rather than retrying.
    """

    def __init__(self, knowledge: KnowledgeStrategy, context: ModuleContext | None = None) -> None:
        """Initialize the toolkit with the ``search_knowledge_base`` tool.

        Args:
            knowledge: The module's knowledge service strategy.
            context: Module context; enables AG-UI notifications via the base toolkit.
        """
        self._knowledge = knowledge
        super().__init__(
            name="knowledge_tools",
            tools=[self.search_knowledge_base],
            context=context,
        )

    @staticmethod
    def _row(citation: Citation) -> dict[str, Any]:
        """Trim a citation to what an agent can reason and cite from.

        ``score`` is dropped on purpose: it is a fused rank comparable only within one
        answer, and a model shown a number reads it as a confidence.

        Args:
            citation: The citation to render.

        Returns:
            The fields worth spending context on.
        """
        row: dict[str, Any] = {
            "document_id": citation.document_id,
            "title": citation.title,
            "passage": citation.snippet,
            "source": citation.source,
            "source_url": citation.source_url,
        }
        if headings := citation.heading_path:
            row["section"] = " > ".join(headings)
        if citation.modality not in {Modality.TEXT, Modality.UNSPECIFIED}:
            row["modality"] = citation.modality.value
        return row

    async def search_knowledge_base(
        self,
        query: str,
        sources: list[str] | None = None,
        limit: int = 10,
    ) -> str:
        """Search the user's own documents — their connected Google Drive, Notion, and the like.

        Use this whenever the answer could live in the user's company documents rather than
        in your general knowledge: internal decisions, meeting notes, specifications,
        contracts, processes, numbers. Prefer one precise question in natural language over
        keywords; the search is semantic as well as lexical.

        Each result is a whole passage, not an excerpt, plus the URL of the document it came
        from — quote the passage and cite the ``source_url`` rather than paraphrasing blindly.
        Only documents this user is allowed to read are ever returned, so anything you get
        back is safe to show them.

        An empty result list means nothing matched or nothing is connected: say so, and do
        not call the tool again with the same question.

        Args:
            query: The question to answer, in natural language (1-4000 characters).
            sources: Restrict the search to these connectors, e.g. ``["google_drive"]`` or
                ``["notion"]``. Omit it to search everything the user connected, which is
                almost always what you want.
            limit: How many documents to return, 1-50 (default 10). Several passages of one
                document collapse into a single result, so this counts documents.

        Returns:
            JSON string: ``{"output": {"results": [{"document_id", "title", "passage",
            "source", "source_url", "section"?}], "returned": N}}``, or ``{"error": ...}``
            when the knowledge base could not be reached.
        """
        try:
            citations = await self._knowledge.search(query, sources=sources, limit=limit)
        except ValueError as error:
            return self._fail(str(error), tool="search_knowledge_base")
        except KnowledgeServiceError as error:
            logger.warning("KnowledgeTools: search failed: %s", error)
            return self._fail("the knowledge base could not be reached", tool="search_knowledge_base")

        rows = [self._row(citation) for citation in citations]
        await self._notify("knowledge_searched", {"query": query, "sources": sources or [], "results": len(rows)})
        return self._ok({"results": rows, "returned": len(rows)}, tool="search_knowledge_base")
