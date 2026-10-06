"""Tests for KnowledgeTools — what the agent is handed, and what it is never handed."""

import json

import pytest

from digitalkin.community.agno.toolkits import KnowledgeTools
from digitalkin.models.services.knowledge import Citation, Modality
from digitalkin.services.knowledge import DefaultKnowledge, KnowledgeServiceError, KnowledgeStrategy

CORPUS = [
    {
        "document_id": "doc_pricing",
        "title": "Apollo pricing",
        "snippet": "Apollo moved to a per-seat model billed yearly.",
        "score": 0.032,
        "source": "google_drive",
        "source_url": "https://docs.google.com/document/d/pricing",
        "locator": {"index": 1, "heading_path": ["Pricing", "2026"]},
    },
    {
        "document_id": "doc_deck",
        "title": "Board deck",
        "snippet": "Apollo revenue per seat.",
        "source": "google_drive",
        "source_url": "https://docs.google.com/presentation/d/deck",
        "modality": Modality.SLIDE,
    },
]


class _FailingKnowledge(KnowledgeStrategy):
    """Strategy whose search always fails."""

    def __init__(self) -> None:
        super().__init__("missions:m1", "", "")

    async def search(
        self,
        query: str,
        sources: list[str] | None = None,
        limit: int | None = None,
    ) -> list[Citation]:
        msg = "index unreachable"
        raise KnowledgeServiceError(msg)


class _RecordingKnowledge(KnowledgeStrategy):
    """Strategy that records what it was asked and answers nothing."""

    def __init__(self) -> None:
        super().__init__("missions:m1", "", "")
        self.calls: list[tuple[str, list[str] | None, int | None]] = []

    async def search(
        self,
        query: str,
        sources: list[str] | None = None,
        limit: int | None = None,
    ) -> list[Citation]:
        self.calls.append((query, sources, limit))
        return []


@pytest.fixture
def tools() -> KnowledgeTools:
    return KnowledgeTools(DefaultKnowledge("missions:m1", "", "", config={"documents": CORPUS}))


async def test_a_match_is_returned_as_a_citable_passage(tools: KnowledgeTools) -> None:
    result = json.loads(await tools.search_knowledge_base("billed yearly"))

    assert result["metadata"]["success"] is True
    assert result["output"]["returned"] == 1
    assert result["output"]["results"][0] == {
        "document_id": "doc_pricing",
        "title": "Apollo pricing",
        "passage": "Apollo moved to a per-seat model billed yearly.",
        "source": "google_drive",
        "source_url": "https://docs.google.com/document/d/pricing",
        "section": "Pricing > 2026",
    }


async def test_the_score_is_never_shown_to_the_model(tools: KnowledgeTools) -> None:
    result = json.loads(await tools.search_knowledge_base("apollo"))

    # A fused rank read as a confidence is worse than no number at all.
    assert all("score" not in row for row in result["output"]["results"])


async def test_a_non_text_modality_is_named(tools: KnowledgeTools) -> None:
    result = json.loads(await tools.search_knowledge_base("board deck revenue"))

    assert result["output"]["results"][0]["modality"] == "slide"


async def test_a_document_without_headings_carries_no_section(tools: KnowledgeTools) -> None:
    result = json.loads(await tools.search_knowledge_base("board deck revenue"))

    assert "section" not in result["output"]["results"][0]


async def test_nothing_found_is_a_success_with_zero_results(tools: KnowledgeTools) -> None:
    result = json.loads(await tools.search_knowledge_base("warehouse logistics in Brazil"))

    # Reported as success on purpose: the agent must say "nothing found", not retry.
    assert result["metadata"]["success"] is True
    assert result["output"] == {"results": [], "returned": 0}


async def test_sources_is_forwarded_untouched() -> None:
    recording = _RecordingKnowledge()
    tools = KnowledgeTools(recording)

    await tools.search_knowledge_base("apollo", sources=["notion"], limit=3)

    assert recording.calls == [("apollo", ["notion"], 3)]


async def test_an_empty_query_is_reported_not_raised(tools: KnowledgeTools) -> None:
    result = json.loads(await tools.search_knowledge_base("   "))

    assert result["error"] == "query must not be empty"
    assert result["metadata"]["success"] is False


async def test_an_unreachable_backend_is_reported_not_raised() -> None:
    tools = KnowledgeTools(_FailingKnowledge())

    result = json.loads(await tools.search_knowledge_base("apollo"))

    # Never raises into the agent loop, and never leaks the backend's own message.
    assert result["error"] == "the knowledge base could not be reached"
    assert "index unreachable" not in result["error"]


def test_the_toolkit_exposes_exactly_one_tool(tools: KnowledgeTools) -> None:
    assert [tool.__name__ for tool in tools.tools] == ["search_knowledge_base"]
