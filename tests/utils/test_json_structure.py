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


class TestPatch:
    """Replacing the value at a path that already exists."""

    @staticmethod
    def _document() -> dict[str, Any]:
        return {
            "llm": {"model": "small", "temperature": 0.2},
            "limits": {"max.tokens": 8000},
            "tools": [{"name": "search"}, {"name": "fetch"}],
            "region": "eu-west",
        }

    @pytest.mark.parametrize(
        ("path", "value", "read_back"),
        [
            ("llm.model", "large", ("llm", {"model": "large", "temperature": 0.2})),
            ("llm", "inline", ("llm", "inline")),
            ('limits["max.tokens"]', 16000, ("limits", {"max.tokens": 16000})),
            ("tools[1].name", "browse", ("tools", [{"name": "search"}, {"name": "browse"}])),
            ("tools[1]", {"name": "last"}, ("tools", [{"name": "search"}, {"name": "last"}])),
            ("region", None, ("region", None)),
        ],
    )
    def test_replaces_the_value_at_an_existing_path(self, path: str, value: Any, read_back: tuple[str, Any]) -> None:
        document = self._document()

        JsonStructure.patch(document, path, value)

        assert document[read_back[0]] == read_back[1]

    def test_leaves_every_other_key_alone(self) -> None:
        document = self._document()

        JsonStructure.patch(document, "llm.model", "large")

        expected = self._document()
        expected["llm"]["model"] = "large"
        assert document == expected

    @pytest.mark.parametrize("path", ["absent", "llm.absent", "tools[2]"])
    def test_an_absent_path_is_refused_and_points_at_append(self, path: str) -> None:
        """Patch never creates: adding is append's job, so a typo cannot add a stray key."""
        document = self._document()

        with pytest.raises(ValueError, match="append"):
            JsonStructure.patch(document, path, "x")
        assert document == self._document()

    @pytest.mark.parametrize("path", ["region.zone", "llm.model[0]", "tools.name", "llm[0]"])
    def test_a_path_through_a_value_of_the_wrong_kind_is_refused(self, path: str) -> None:
        with pytest.raises(ValueError, match="not found"):
            JsonStructure.patch(self._document(), path, "x")

    def test_an_empty_path_is_refused(self) -> None:
        """Replacing the whole document is ``update``'s job."""
        with pytest.raises(ValueError, match="empty"):
            JsonStructure.patch(self._document(), "", "x")

    def test_a_malformed_path_is_refused(self) -> None:
        with pytest.raises(ValueError, match="malformed path"):
            JsonStructure.patch(self._document(), 'llm["', "x")

    def test_a_path_through_a_stored_null_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not found"):
            JsonStructure.patch({"llm": None}, "llm.model", "x")

    @pytest.mark.parametrize("path", ["tools[-1]", "tools[-1].name"])
    def test_a_negative_index_is_refused(self, path: str) -> None:
        """An edited path keys the structure map, where ``tools[-1]`` would never match ``tools[1]``."""
        with pytest.raises(ValueError, match="negative"):
            JsonStructure.patch(self._document(), path, "x")


class TestAppend:
    """Adding a new element: a key that does not exist yet, or one more item of a list."""

    @staticmethod
    def _document() -> dict[str, Any]:
        return {
            "llm": {"model": "small"},
            "tools": [{"name": "search", "tags": ["web"]}],
            "region": "eu-west",
        }

    def test_adds_a_new_key_and_returns_its_path(self) -> None:
        document = self._document()

        added = JsonStructure.append(document, "llm.temperature", 0.2)

        assert added == "llm.temperature"
        assert document["llm"] == {"model": "small", "temperature": 0.2}

    def test_creates_the_missing_parent_objects(self) -> None:
        document = self._document()

        JsonStructure.append(document, "llm.fallback.model", "tiny")

        assert document["llm"]["fallback"] == {"model": "tiny"}

    def test_a_quoted_key_is_added_verbatim(self) -> None:
        document = self._document()

        added = JsonStructure.append(document, 'limits["max.tokens"]', 8000)

        assert added == 'limits["max.tokens"]'
        assert document["limits"] == {"max.tokens": 8000}

    def test_pushes_onto_a_list_and_returns_the_new_index(self) -> None:
        document = self._document()

        added = JsonStructure.append(document, "tools", {"name": "fetch"})

        assert added == "tools[1]"
        assert document["tools"][1] == {"name": "fetch"}

    def test_pushes_onto_a_list_nested_in_a_list(self) -> None:
        document = self._document()

        added = JsonStructure.append(document, "tools[0].tags", "search")

        assert added == "tools[0].tags[1]"
        assert document["tools"][0]["tags"] == ["web", "search"]

    def test_the_returned_path_resolves_to_the_added_value(self) -> None:
        document = self._document()

        added = JsonStructure.append(document, "tools", {"name": "fetch"})

        assert JsonStructure.resolve(document, [added]) == {added: {"name": "fetch"}}

    @pytest.mark.parametrize("path", ["region", "llm", "llm.model", "tools[0]"])
    def test_an_existing_non_list_value_is_refused_and_points_at_patch(self, path: str) -> None:
        """Append never overwrites: changing what is there is patch's job."""
        document = self._document()

        with pytest.raises(ValueError, match="patch"):
            JsonStructure.append(document, path, "x")
        assert document == self._document()

    @pytest.mark.parametrize("path", ["tools[5].name", "region.zone", "llm[0]", "tools.name"])
    def test_a_path_that_cannot_be_built_is_refused(self, path: str) -> None:
        """Only missing object keys are created; a list index or a scalar parent cannot be."""
        document = self._document()

        with pytest.raises(ValueError, match="cannot add"):
            JsonStructure.append(document, path, "x")
        assert document == self._document()

    def test_an_empty_path_is_refused(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            JsonStructure.append(self._document(), "", "x")

    def test_a_malformed_path_is_refused(self) -> None:
        with pytest.raises(ValueError, match="malformed path"):
            JsonStructure.append(self._document(), "tools[", "x")

    def test_a_negative_index_is_refused(self) -> None:
        document = self._document()

        with pytest.raises(ValueError, match="negative"):
            JsonStructure.append(document, "tools[-1].tags", "x")
        assert document == self._document()

    def test_a_stored_null_is_a_value_not_a_gap(self) -> None:
        """``null`` is there: appending at it would overwrite, which is patch's job."""
        document = {"llm": None}

        with pytest.raises(ValueError, match="patch"):
            JsonStructure.append(document, "llm", "x")
        with pytest.raises(ValueError, match="cannot add"):
            JsonStructure.append(document, "llm.model", "x")
        assert document == {"llm": None}


class TestMisplaced:
    """An edit's map entries must describe what it wrote, nothing else."""

    @staticmethod
    def _document() -> dict[str, Any]:
        return {"llm": {"model": "small"}, "tools": [{"name": "search"}, {"name": "fetch"}]}

    def test_entries_at_and_under_the_written_element_are_in_place(self) -> None:
        entries = {"tools[1]": "the fetcher", "tools[1].name": "its name", '["tools"][1]["name"]': "same, quoted"}

        assert JsonStructure.misplaced(entries, self._document(), "tools[1]") == []

    def test_an_entry_for_another_element_is_misplaced(self) -> None:
        """The pushed item landed at tools[1], so an entry guessed at tools[0] describes the wrong one."""
        entries = {"tools[0].name": "guessed index", "tools[1]": "right one"}

        assert JsonStructure.misplaced(entries, self._document(), "tools[1]") == ["tools[0].name"]

    def test_a_parent_entry_is_misplaced(self) -> None:
        assert JsonStructure.misplaced({"llm": "the section"}, self._document(), "llm.model") == ["llm"]

    def test_an_entry_naming_nothing_in_the_content_is_misplaced(self) -> None:
        """A typo under the right element would otherwise point later readers at a missing key."""
        assert JsonStructure.misplaced({"llm.modle": "typo"}, self._document(), "llm") == ["llm.modle"]

    def test_no_entries_is_never_misplaced(self) -> None:
        assert JsonStructure.misplaced({}, self._document(), "llm") == []

    def test_a_malformed_entry_is_refused(self) -> None:
        with pytest.raises(ValueError, match="malformed path"):
            JsonStructure.misplaced({'llm["': "broken"}, self._document(), "llm")


class TestRemove:
    """Removing one existing element: an object key or a list item."""

    @staticmethod
    def _document() -> dict[str, Any]:
        return {
            "llm": {"model": "small", "temperature": 0.2},
            "limits": {"max.tokens": 8000},
            "tools": ["search", "fetch", "browse"],
            "off": None,
        }

    @pytest.mark.parametrize(
        ("path", "read_back"),
        [
            ("llm.model", ("llm", {"temperature": 0.2})),
            ("llm", ("llm", None)),
            ('limits["max.tokens"]', ("limits", {})),
            ("tools[1]", ("tools", ["search", "browse"])),
        ],
    )
    def test_removes_the_element(self, path: str, read_back: tuple[str, Any]) -> None:
        document = self._document()

        JsonStructure.remove(document, path)

        assert document.get(read_back[0]) == read_back[1]

    def test_a_stored_null_is_removed_as_a_key(self) -> None:
        document = self._document()

        JsonStructure.remove(document, "off")

        assert "off" not in document

    @pytest.mark.parametrize("path", ["absent", "llm.absent", "tools[3]", "off.x", "tools.name"])
    def test_an_absent_path_is_refused(self, path: str) -> None:
        document = self._document()

        with pytest.raises(ValueError, match="not found"):
            JsonStructure.remove(document, path)
        assert document == self._document()

    @pytest.mark.parametrize(("path", "match"), [("", "empty"), ("tools[-1]", "negative"), ("tools[", "malformed")])
    def test_an_unusable_path_is_refused(self, path: str, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            JsonStructure.remove(self._document(), path)


class TestForget:
    """The map after a removal: the element's entries go, later list items move down one."""

    @staticmethod
    def _map() -> dict[str, str]:
        return {
            "tools": "what it may call",
            "tools[0]": "search",
            "tools[1]": "fetch",
            "tools[1].name": "fetch's name",
            "tools[2]": "browse",
            "tools[2].name": "browse's name",
            "tools[3]": "crawl",
            "llm.model": "which model answers",
        }

    def test_removing_a_list_item_moves_the_later_items_down(self) -> None:
        assert JsonStructure.forget(self._map(), "tools[1]") == {
            "tools": "what it may call",
            "tools[0]": "search",
            "tools[1]": "browse",
            "tools[1].name": "browse's name",
            "tools[2]": "crawl",
            "llm.model": "which model answers",
        }

    def test_removing_the_last_item_renames_nothing(self) -> None:
        forgotten = JsonStructure.forget(self._map(), "tools[3]")

        assert forgotten == {key: text for key, text in self._map().items() if key != "tools[3]"}

    def test_removing_a_key_only_drops_its_entries(self) -> None:
        forgotten = JsonStructure.forget(self._map(), "tools")

        assert forgotten == {"llm.model": "which model answers"}

    def test_a_nested_list_moves_only_its_own_items(self) -> None:
        stored = {"a[0].b[1]": "inner", "a[1]": "outer", "a[0].b[2]": "inner next"}

        assert JsonStructure.forget(stored, "a[0].b[0]") == {
            "a[0].b[0]": "inner",
            "a[1]": "outer",
            "a[0].b[1]": "inner next",
        }

    def test_a_renamed_entry_keeps_quoted_keys_readable(self) -> None:
        """A renamed entry is re-encoded, so its keys must survive the grammar's quoting rule."""
        forgotten = JsonStructure.forget({'cfg["max.tokens"][2]': "third"}, 'cfg["max.tokens"][0]')

        assert forgotten == {'cfg["max.tokens"][1]': "third"}
        assert JsonStructure.resolve({"cfg": {"max.tokens": ["b", "third"]}}, list(forgotten)) == {
            'cfg["max.tokens"][1]': "third"
        }

    def test_a_malformed_stored_entry_is_kept(self) -> None:
        assert JsonStructure.forget({'tools["': "broken", "tools[2]": "x"}, "tools[0]") == {
            'tools["': "broken",
            "tools[1]": "x",
        }

    def test_every_renamed_entry_is_logged(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorded = Mock()
        monkeypatch.setattr(json_structure.logger, "info", recorded)

        JsonStructure.forget({"tools[2]": "browse"}, "tools[0]")

        assert any("tools[2]" in call.args and "tools[1]" in call.args for call in recorded.call_args_list)

    def test_the_input_map_is_left_untouched(self) -> None:
        stored = self._map()

        JsonStructure.forget(stored, "tools[1]")

        assert stored == self._map()


class TestPrune:
    """Removing the map entries that describe one element, before its new ones go in."""

    @staticmethod
    def _map() -> dict[str, str]:
        return {
            "llm": "the model section",
            "llm.model": "which model answers",
            "llm.fallback.model": "the backup model",
            "llmx": "a sibling whose name starts the same",
            "tools": "what it may call",
            "tools[0]": "the first tool",
            "tools[1]": "the second tool",
            "tools[1].name": "the second tool's name",
        }

    def test_drops_the_entry_at_the_path_and_every_entry_under_it(self) -> None:
        kept = JsonStructure.prune(self._map(), "llm")

        assert set(kept) == {"llmx", "tools", "tools[0]", "tools[1]", "tools[1].name"}

    def test_keeps_the_parents_of_the_path(self) -> None:
        kept = JsonStructure.prune(self._map(), "llm.model")

        assert "llm" in kept
        assert "llm.model" not in kept
        assert "llm.fallback.model" in kept

    def test_matches_list_items_by_index(self) -> None:
        kept = JsonStructure.prune(self._map(), "tools[1]")

        assert set(kept) == {"llm", "llm.model", "llm.fallback.model", "llmx", "tools", "tools[0]"}

    def test_compares_segments_not_spellings(self) -> None:
        """``["llm"].model`` and ``llm.model`` name the same element."""
        kept = JsonStructure.prune({'["llm"].model': "which model answers", "tools": "x"}, "llm")

        assert kept == {"tools": "x"}

    def test_a_malformed_stored_entry_is_kept(self) -> None:
        """The stored map is never judged, so an entry that cannot be parsed is not this call's to drop."""
        assert JsonStructure.prune({'llm["': "broken"}, "llm") == {'llm["': "broken"}

    def test_the_input_map_is_left_untouched(self) -> None:
        stored = self._map()

        JsonStructure.prune(stored, "llm")

        assert stored == self._map()

    def test_every_dropped_entry_is_logged_with_its_description(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Dropped input is logged with its content, never counted (CLAUDE.md)."""
        recorded = Mock()
        monkeypatch.setattr(json_structure.logger, "info", recorded)

        JsonStructure.prune(self._map(), "tools[1]")

        logged = [call.args for call in recorded.call_args_list]
        assert any("tools[1]" in args and "the second tool" in args for args in logged)
        assert any("tools[1].name" in args and "the second tool's name" in args for args in logged)
        assert len(logged) == 2


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

    @given(
        segments=st.lists(
            st.one_of(st.text(max_size=8), st.integers(min_value=0, max_value=50)),
            min_size=1,
            max_size=6,
        )
    )
    @settings(max_examples=300)
    def test_an_encoded_path_decodes_back_to_its_segments(self, segments: list[str | int]) -> None:
        """A map entry renamed after a removal must still be read the way it was meant."""
        assert JsonStructure._decode(JsonStructure._encode(segments)) == segments
