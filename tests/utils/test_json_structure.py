"""Tests for reading a document at the paths a structure map names.

The SDK no longer builds or validates the map — the agent writes it and the backend stores
it. What remains is the reading half of the grammar, which ``DefaultSetup`` needs to
project content the way the backend does. These tests are that contract.
"""

from typing import Any
from unittest.mock import Mock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from digitalkin.utils import json_structure
from digitalkin.utils.json_structure import JsonStructure

pytestmark = pytest.mark.unit


class TestClip:
    """The one thing done to an authored map: bound each description's length."""

    def test_a_short_description_is_untouched(self) -> None:
        authored = {"llm.model": "which model answers", "region": "where it runs"}

        assert JsonStructure.clip(authored) == authored

    def test_a_description_at_the_budget_is_untouched(self) -> None:
        text = "x" * JsonStructure._DESCRIPTION_CHARS

        assert JsonStructure.clip({"a": text})["a"] == text

    def test_a_longer_description_is_cut_to_the_budget(self) -> None:
        clipped = JsonStructure.clip({"a": "x" * 5000})["a"]

        assert len(clipped) == JsonStructure._DESCRIPTION_CHARS
        assert clipped.endswith("…")

    def test_clipping_is_logged_with_the_key_and_the_original_length(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorded = Mock()
        monkeypatch.setattr(json_structure.logger, "debug", recorded)

        JsonStructure.clip({"llm.model": "x" * 900})

        assert "llm.model" in recorded.call_args.args
        assert 900 in recorded.call_args.args

    def test_no_entry_is_ever_dropped(self) -> None:
        """Only lengths are bounded — which keys the map names is the agent's business."""
        authored = {f"k{index}": "x" * 5000 for index in range(1000)}

        assert set(JsonStructure.clip(authored)) == set(authored)

    def test_an_entry_naming_an_absent_key_survives(self) -> None:
        """Nothing here reads the content, so nothing can be judged against it."""
        assert JsonStructure.clip({"nowhere.at.all": "kept"}) == {"nowhere.at.all": "kept"}

    def test_an_empty_map_stays_empty(self) -> None:
        assert JsonStructure.clip({}) == {}


class TestDecode:
    """Every form the grammar allows must split into the right segments."""

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("llm", ["llm"]),
            ("llm.provider", ["llm", "provider"]),
            ("a.b.c", ["a", "b", "c"]),
            ('limits["max.tokens"]', ["limits", "max.tokens"]),
            ('["a[0]"]', ["a[0]"]),
            ('["say\\"hi\\""]', ['say"hi"']),
            ('["back\\\\slash"]', ["back\\slash"]),
            ('[""]', [""]),
            ("résumé", ["résumé"]),
            ("tools[0]", ["tools", 0]),
            ("tools[12].name", ["tools", 12, "name"]),
            ("t[-1]", ["t", -1]),
            ('a["b.c"][0].d', ["a", "b.c", 0, "d"]),
        ],
    )
    def test_reads_every_grammar_form(self, path: str, expected: list[str | int]) -> None:
        assert JsonStructure._decode(path) == expected

    @pytest.mark.parametrize("path", ["a[", "a[x]", 'a["b', "a[0", 'a["b"x'])
    def test_malformed_paths_raise(self, path: str) -> None:
        """A path the backend could not parse either is refused, not guessed at."""
        with pytest.raises(ValueError, match="malformed path"):
            JsonStructure._decode(path)


class TestResolve:
    """Reading values back at those paths."""

    @staticmethod
    def _document() -> dict[str, Any]:
        """A document exercising every grammar form at once."""
        return {
            "llm": {"provider": "litellm", "temperature": 0.2},
            "limits": {"max.tokens": 8000, "": "blank"},
            "tools": [{"name": "search"}, {"name": "fetch"}],
            "flags": [],
            "off": False,
            "missing": None,
        }

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("llm", {"provider": "litellm", "temperature": 0.2}),
            ("llm.provider", "litellm"),
            ('limits["max.tokens"]', 8000),
            ('limits[""]', "blank"),
            ("tools", [{"name": "search"}, {"name": "fetch"}]),
            ("tools[1].name", "fetch"),
            ("tools[-1].name", "fetch"),
            ("flags", []),
            ("off", False),
            ("missing", None),
        ],
    )
    def test_reads_the_value_at_a_path(self, path: str, expected: Any) -> None:
        assert JsonStructure.resolve(self._document(), [path]) == {path: expected}

    def test_a_stored_null_is_a_hit_not_a_miss(self) -> None:
        """``None`` is a value; only an absent path is a miss."""
        assert "missing" in JsonStructure.resolve(self._document(), ["missing"])

    @pytest.mark.parametrize("path", ["absent", "llm.absent", "llm[0]", "llm.provider.deeper", "tools[9]"])
    def test_unresolvable_paths_are_dropped_and_logged(self, path: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """Dropped input is logged with its content, never counted (CLAUDE.md).

        Asserted on the logger rather than caplog: the project logger does not propagate,
        so a root-level capture sees nothing and the assertion would pass vacuously.
        """
        recorded = Mock()
        monkeypatch.setattr(json_structure.logger, "debug", recorded)

        assert JsonStructure.resolve(self._document(), [path]) == {}
        assert path in recorded.call_args.args

    def test_reads_several_paths_at_once(self) -> None:
        resolved = JsonStructure.resolve(self._document(), ["llm.provider", "off", "nope"])

        assert resolved == {"llm.provider": "litellm", "off": False}

    def test_an_empty_path_list_reads_nothing(self) -> None:
        assert JsonStructure.resolve(self._document(), []) == {}

    def test_a_malformed_path_raises_rather_than_being_dropped(self) -> None:
        """Distinct from unresolvable: this one cannot be parsed at all."""
        with pytest.raises(ValueError, match="malformed path"):
            JsonStructure.resolve(self._document(), ['a["'])


@pytest.mark.property
class TestGrammarProperties:
    """Properties that must hold for any document, not just the examples."""

    @given(
        document=st.dictionaries(
            st.text(min_size=1, max_size=8).filter(lambda k: not set(k) & set('.["]\\')),
            st.one_of(st.integers(), st.text(max_size=8), st.booleans(), st.none()),
            max_size=6,
        )
    )
    @settings(max_examples=300)
    def test_every_plain_key_resolves_at_its_own_name(self, document: dict[str, Any]) -> None:
        """A plain key is its own path — the first grammar rule, over generated documents."""
        assert JsonStructure.resolve(document, list(document)) == document

    @given(key=st.text(min_size=1, max_size=12))
    @settings(max_examples=300)
    def test_a_bracket_quoted_key_resolves_whatever_it_contains(self, key: str) -> None:
        """The second rule is the interop risk: a quoted key must survive any content."""
        escaped = key.replace("\\", "\\\\").replace('"', '\\"')
        path = f'["{escaped}"]'

        assert JsonStructure.resolve({key: "value"}, [path]) == {path: "value"}

    @given(index=st.integers(min_value=0, max_value=4))
    @settings(max_examples=100)
    def test_a_list_index_resolves_to_its_element(self, index: int) -> None:
        assert JsonStructure.resolve({"t": list(range(5))}, [f"t[{index}]"]) == {f"t[{index}]": index}

    @given(depth=st.integers(min_value=1, max_value=25))
    @settings(max_examples=50)
    def test_a_deeply_nested_path_does_not_recurse(self, depth: int) -> None:
        """The walk is iterative, so depth is bounded by the document, not the stack."""
        document: dict[str, Any] = {"leaf": "found"}
        for _ in range(depth):
            document = {"n": document}
        path = ".".join(["n"] * depth + ["leaf"])

        assert JsonStructure.resolve(document, [path]) == {path: "found"}
