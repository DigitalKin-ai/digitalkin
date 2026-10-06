"""Tests for DefaultKnowledge, the local in-memory knowledge strategy."""

import pytest

from digitalkin.models.services.knowledge import Citation, Modality
from digitalkin.services.knowledge.default_knowledge import DefaultKnowledge
from digitalkin.services.knowledge.knowledge_strategy import MAX_LIMIT

pytestmark = pytest.mark.unit

CORPUS = [
    {
        "document_id": "doc_pricing",
        "title": "Apollo pricing",
        "snippet": "Apollo moved to a per-seat model billed yearly.",
        "source": "google_drive",
        "source_url": "https://docs.google.com/document/d/pricing",
        "locator": {"index": 1, "heading_path": ["Pricing", "2026"]},
    },
    {
        "document_id": "doc_onboarding",
        "title": "Onboarding",
        "snippet": "New joiners get a laptop on day one.",
        "source": "notion",
        "source_url": "https://notion.so/onboarding",
    },
]


@pytest.fixture
def knowledge() -> DefaultKnowledge:
    return DefaultKnowledge("missions:m1", "setups:s1", "setup_versions:v1", config={"documents": CORPUS})


class TestSearch:
    async def test_it_finds_a_document_by_its_words(self, knowledge: DefaultKnowledge) -> None:
        results = await knowledge.search("apollo pricing")

        assert [citation.document_id for citation in results] == ["doc_pricing"]

    async def test_a_document_nothing_matches_is_absent(self, knowledge: DefaultKnowledge) -> None:
        results = await knowledge.search("quarterly revenue in Brazil")

        assert results == []

    async def test_sources_narrows_the_corpus(self, knowledge: DefaultKnowledge) -> None:
        results = await knowledge.search("laptop day one", sources=["google_drive"])

        assert results == []

    async def test_the_best_match_comes_first(self, knowledge: DefaultKnowledge) -> None:
        results = await knowledge.search("apollo laptop")

        # Both carry one of the two words; the tie breaks on title, deterministically.
        assert [citation.title for citation in results] == ["Apollo pricing", "Onboarding"]

    async def test_limit_caps_the_answer(self, knowledge: DefaultKnowledge) -> None:
        results = await knowledge.search("apollo laptop", limit=1)

        assert len(results) == 1

    async def test_the_locator_survives(self, knowledge: DefaultKnowledge) -> None:
        results = await knowledge.search("apollo")

        assert results[0].heading_path == ["Pricing", "2026"]
        assert results[0].modality is Modality.UNSPECIFIED

    async def test_an_empty_corpus_answers_nothing(self) -> None:
        empty = DefaultKnowledge("missions:m1", "", "")

        assert await empty.search("anything") == []

    async def test_it_accepts_citations_as_well_as_dicts(self) -> None:
        seeded = DefaultKnowledge(
            "missions:m1",
            "",
            "",
            config={"documents": [Citation(document_id="doc_1", title="Apollo", snippet="per-seat")]},
        )

        assert [citation.document_id for citation in await seeded.search("apollo")] == ["doc_1"]


class TestWhatItRefuses:
    async def test_an_empty_query_is_refused(self, knowledge: DefaultKnowledge) -> None:
        with pytest.raises(ValueError, match="query must not be empty"):
            await knowledge.search("   ")


class TestNormalization:
    def test_a_limit_over_the_protocol_bound_is_clamped(self) -> None:
        _, _, limit = DefaultKnowledge.normalize("apollo", None, 500)

        assert limit == MAX_LIMIT

    def test_a_limit_of_zero_still_asks_for_one_document(self) -> None:
        _, _, limit = DefaultKnowledge.normalize("apollo", None, 0)

        assert limit == 1

    def test_more_than_sixteen_sources_are_dropped(self) -> None:
        _, sources, _ = DefaultKnowledge.normalize("apollo", [f"src_{i}" for i in range(20)], None)

        assert len(sources) == 16

    def test_an_over_long_query_is_trimmed(self) -> None:
        text, _, _ = DefaultKnowledge.normalize("x" * 5000, None, None)

        assert len(text) == 4000

    def test_no_limit_falls_back_to_the_setting(self) -> None:
        _, _, limit = DefaultKnowledge.normalize("apollo", None, None)

        assert limit == 10


async def test_the_local_strategy_is_always_ready() -> None:
    assert await DefaultKnowledge("missions:m1", "", "").wait_for_ready() is True
