"""Tests for JsonStructure: the leaf key map and path read/write over the same grammar."""

import copy
import logging
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from digitalkin.utils.json_structure import JsonStructure

pytestmark = pytest.mark.unit


class TestEncode:
    """Tests for the path emitter — the cross-repo grammar."""

    def test_plain_key_at_root(self) -> None:
        """A root key is emitted bare, with no leading separator."""
        assert JsonStructure._encode("", "llm") == "llm"

    def test_plain_key_joins_with_dot(self) -> None:
        """A plain nested key joins to its parent with a dot."""
        assert JsonStructure._encode("llm", "provider") == "llm.provider"

    @pytest.mark.parametrize("key", ["max.tokens", "a[0]", 'say"hi"', "back\\slash", "]close"])
    def test_awkward_key_is_bracket_quoted(self, key: str) -> None:
        """A key carrying a grammar character is bracket-quoted, never dot-joined."""
        encoded = JsonStructure._encode("limits", key)

        assert encoded.startswith('limits["')
        assert encoded.endswith('"]')

    def test_empty_key_is_bracket_quoted(self) -> None:
        """An empty key is quoted, otherwise the path would end in a bare dot."""
        assert JsonStructure._encode("root", "") == 'root[""]'

    def test_quote_and_backslash_are_escaped(self) -> None:
        """Quotes and backslashes inside a quoted key are escaped, not emitted raw."""
        assert JsonStructure._encode("", 'a"b') == '["a\\"b"]'
        assert JsonStructure._encode("", "a\\b") == '["a\\\\b"]'

    def test_unicode_key_stays_plain(self) -> None:
        """A non-ASCII key carries no grammar character, so it is not quoted."""
        assert JsonStructure._encode("prompts", "résumé") == "prompts.résumé"


class TestDecode:
    """Tests for the path parser."""

    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("llm", ["llm"]),
            ("max.tokens", ["max.tokens"]),
            ("a[0]", ["a[0]"]),
            ('say"hi"', ['say"hi"']),
            ("back\\slash", ["back\\slash"]),
            ("", [""]),
            ("résumé", ["résumé"]),
        ],
    )
    def test_round_trips_every_encode_form(self, key: str, expected: list[str]) -> None:
        """Every key _encode can emit decodes back to itself."""
        assert JsonStructure._decode(JsonStructure._encode("", key)) == expected

    def test_round_trips_nested_mixed_path(self) -> None:
        """A path mixing plain keys, quoted keys and indices decodes segment by segment."""
        path = JsonStructure._encode(JsonStructure._encode("limits", "max.tokens"), "hard")

        assert JsonStructure._decode(path) == ["limits", "max.tokens", "hard"]

    def test_decodes_list_index_as_int(self) -> None:
        """A bracketed number is an index, not a key."""
        assert JsonStructure._decode("tools[0].name") == ["tools", 0, "name"]

    def test_decodes_negative_index(self) -> None:
        """A negative index parses as a negative int."""
        assert JsonStructure._decode("tools[-1]") == ["tools", -1]

    def test_decodes_empty_path_as_root(self) -> None:
        """The empty path is the document root: no segments."""
        assert JsonStructure._decode("") == []

    @pytest.mark.parametrize("path", ["a[", "a[x]", 'a["b', "a[0", 'a["b"x'])
    def test_malformed_path_raises_value_error(self, path: str) -> None:
        """A malformed path fails loudly; _guard maps ValueError to a fail envelope."""
        with pytest.raises(ValueError, match="malformed path"):
            JsonStructure._decode(path)


class TestSummarise:
    """Tests for the one-line leaf description — content, never type or size."""

    @pytest.mark.parametrize(("value", "expected"), [(True, "true"), (False, "false")])
    def test_bool_renders_as_word_not_number(self, value: bool, expected: str) -> None:
        """Bool subclasses int, so it must be tested before the numeric fallback."""
        assert JsonStructure._summarise(value) == expected

    def test_none_renders_as_null(self) -> None:
        """A null leaf is described as null."""
        assert JsonStructure._summarise(None) == "null"

    def test_number_renders_as_its_value(self) -> None:
        """Numbers carry their own value as the description."""
        assert JsonStructure._summarise(0.2) == "0.2"
        assert JsonStructure._summarise(4096) == "4096"

    def test_short_string_is_verbatim(self) -> None:
        """A string inside the budget is described by itself, unquoted and unclipped."""
        assert JsonStructure._summarise("eu-west") == "eu-west"

    def test_string_at_the_budget_is_not_clipped(self) -> None:
        """A string of exactly the budget length keeps every character."""
        text = "x" * JsonStructure._DESCRIPTION_CHARS

        assert JsonStructure._summarise(text) == text

    def test_long_string_is_clipped_with_ellipsis(self) -> None:
        """A string over the budget is cut to the budget plus a marker."""
        summary = JsonStructure._summarise("y" * 500)

        assert summary == "y" * JsonStructure._DESCRIPTION_CHARS + "…"

    def test_empty_object_and_array_read_as_empty(self) -> None:
        """Empty containers are leaves, so they need a description of their own."""
        assert JsonStructure._summarise({}) == "empty"
        assert JsonStructure._summarise([]) == "empty"

    def test_array_of_objects_lists_element_keys(self) -> None:
        """An array of objects is described by its element shape, not its contents."""
        items = [{"name": "search_web", "description": "…", "params": {}}]

        assert JsonStructure._summarise(items) == "name, description, params"

    def test_array_of_scalars_lists_values(self) -> None:
        """An array of scalars is described by the values themselves."""
        assert JsonStructure._summarise(["a", "b", "c"]) == "a, b, c"

    def test_array_element_keys_are_capped(self) -> None:
        """More element keys than the cap are not all listed."""
        wide = [{f"k{index}": index for index in range(30)}]

        assert JsonStructure._summarise(wide).count(", ") == JsonStructure._NAMED_KEYS - 1

    def test_long_scalar_array_is_capped_before_joining(self) -> None:
        """A long scalar array is sliced before the join, not merely clipped after."""
        summary = JsonStructure._summarise(list(range(1000)))

        assert summary.startswith("0, 1, 2")
        assert "999" not in summary


class TestDescribe:
    """Tests for the leaves-only key map."""

    def _config(self) -> dict[str, Any]:
        """Build a representative service configuration.

        Returns:
            A config with nesting, a long string, an array of objects and a scalar.
        """
        return {
            "llm": {"provider": "litellm", "model": "gpt-4o", "temperature": 0.2},
            "prompts": {"system": "You are a helpful assistant. " * 20, "retry": "Try again."},
            "tools": [{"name": "search_web", "description": "…", "params": {}}] * 43,
            "region": "eu-west",
        }

    def test_emits_no_entry_for_a_populated_container(self) -> None:
        """Containers are implied by the dotted paths; only leaves get entries."""
        entries = JsonStructure.describe(self._config())

        assert "llm" not in entries
        assert "prompts" not in entries
        assert "llm.provider" in entries

    def test_array_is_one_entry_never_expanded(self) -> None:
        """A 43-element array contributes exactly one entry."""
        entries = JsonStructure.describe(self._config())

        assert entries["tools"] == "name, description, params"
        assert not [path for path in entries if path.startswith("tools[")]

    def test_preserves_document_order(self) -> None:
        """The map is insertion-ordered so the model reads it like the document."""
        entries = JsonStructure.describe(self._config())

        assert list(entries) == [
            "llm.provider",
            "llm.model",
            "llm.temperature",
            "prompts.system",
            "prompts.retry",
            "tools",
            "region",
        ]

    def test_long_leaf_is_clipped(self) -> None:
        """A long string leaf carries a clipped preview, not the whole value."""
        entries = JsonStructure.describe(self._config())

        assert entries["prompts.system"].endswith("…")
        assert len(entries["prompts.system"]) == JsonStructure._DESCRIPTION_CHARS + 1

    def test_empty_document_describes_its_root(self) -> None:
        """An empty document is itself a leaf, so the root gets the only entry."""
        assert JsonStructure.describe({}) == {"": "empty"}

    def test_nested_empty_object_keeps_its_key(self) -> None:
        """An empty nested object still appears, or its key would vanish entirely."""
        entries = JsonStructure.describe({"limits": {}, "region": "eu"})

        assert entries == {"limits": "empty", "region": "eu"}

    def test_awkward_keys_are_quoted_in_the_map(self) -> None:
        """A key carrying a dot is emitted in the resolvable bracket form."""
        entries = JsonStructure.describe({"limits": {"max.tokens": 8000}})

        assert entries == {'limits["max.tokens"]': "8000"}

    def test_caps_entries_and_logs_each_dropped_path(self, caplog: pytest.LogCaptureFixture) -> None:
        """Over the cap, every dropped path is logged with its description, not counted."""
        overflow = 5
        content = {f"k{index}": f"v{index}" for index in range(JsonStructure._MAX_ENTRIES + overflow)}

        with caplog.at_level(logging.DEBUG, logger="digitalkin"):
            entries = JsonStructure.describe(content)

        dropped = [record for record in caplog.records if "dropped" in record.getMessage()]
        assert len(entries) == JsonStructure._MAX_ENTRIES
        assert len(dropped) == overflow
        assert f"v{JsonStructure._MAX_ENTRIES}" in dropped[0].getMessage()

    def test_deeply_nested_document_does_not_recurse(self) -> None:
        """The walk is an explicit stack, so depth cannot blow the interpreter stack."""
        depth = 1000
        content: dict[str, Any] = {"leaf": "bottom"}
        for _ in range(depth):
            content = {"next": content}

        entries = JsonStructure.describe(content)

        assert list(entries.values()) == ["bottom"]
        assert next(iter(entries)).count(".") == depth


class TestResolve:
    """Tests for reading values back at the emitted paths."""

    def test_every_described_path_resolves_to_what_it_described(self) -> None:
        """The property that protects the cross-repo grammar: describe and resolve agree."""
        content = {
            "llm": {"provider": "litellm", "temperature": 0.2},
            "limits": {"max.tokens": 8000, "": "blank"},
            "tools": [{"name": "a"}],
            "flags": [],
            "off": False,
            "missing": None,
        }

        entries = JsonStructure.describe(content)
        resolved = JsonStructure.resolve(content, list(entries))

        assert set(resolved) == set(entries)
        assert resolved["llm.provider"] == "litellm"
        assert resolved['limits["max.tokens"]'] == 8000
        assert resolved["tools"] == [{"name": "a"}]
        assert resolved["off"] is False
        assert resolved["missing"] is None

    def test_reads_a_list_element_and_its_field(self) -> None:
        """Indices are resolvable even though describe never emits them."""
        content = {"tools": [{"name": "a"}, {"name": "b"}]}

        assert JsonStructure.resolve(content, ["tools[1].name"]) == {"tools[1].name": "b"}

    def test_negative_index_reads_from_the_end(self) -> None:
        """Negative indices follow Python list semantics."""
        assert JsonStructure.resolve({"a": [1, 2, 3]}, ["a[-1]"]) == {"a[-1]": 3}

    @pytest.mark.parametrize("path", ["absent", "llm.absent", "llm[0]", "llm.provider.deeper", "tools[9]"])
    def test_unresolvable_path_is_omitted_not_raised(self, path: str) -> None:
        """A path that does not resolve is dropped from the result, never an exception."""
        content = {"llm": {"provider": "litellm"}, "tools": [{"name": "a"}]}

        assert JsonStructure.resolve(content, [path]) == {}

    def test_partial_result_keeps_the_paths_that_worked(self) -> None:
        """One bad path does not discard the good ones."""
        content = {"llm": {"provider": "litellm"}}

        assert JsonStructure.resolve(content, ["llm.provider", "nope"]) == {"llm.provider": "litellm"}


class TestMerge:
    """Tests for the patch path used by a scoped update."""

    def test_sets_an_existing_leaf(self) -> None:
        """A patch replaces the value at an existing path."""
        merged = JsonStructure.merge({"llm": {"model": "gpt-4o"}}, {"llm.model": "opus"})

        assert merged == {"llm": {"model": "opus"}}

    def test_does_not_mutate_the_original(self) -> None:
        """The caller's document is untouched; nested containers are copied."""
        content = {"llm": {"model": "gpt-4o"}, "tools": [{"name": "a"}]}

        merged = JsonStructure.merge(content, {"llm.model": "opus"})
        merged["tools"][0]["name"] = "changed"

        assert content == {"llm": {"model": "gpt-4o"}, "tools": [{"name": "a"}]}

    def test_creates_missing_intermediate_objects(self) -> None:
        """A genuinely new key can be added, including under a new parent."""
        merged = JsonStructure.merge({"llm": {}}, {"llm.retry.attempts": 3})

        assert merged == {"llm": {"retry": {"attempts": 3}}}

    def test_writes_null_rather_than_deleting(self) -> None:
        """There is no key deletion; a null patch stores null."""
        merged = JsonStructure.merge({"a": 1}, {"a": None})

        assert merged == {"a": None}

    def test_patches_an_existing_list_element(self) -> None:
        """An index that exists is writable."""
        merged = JsonStructure.merge({"tools": [{"name": "a"}]}, {"tools[0].name": "b"})

        assert merged == {"tools": [{"name": "b"}]}

    def test_applies_every_patch_entry(self) -> None:
        """Multiple paths in one patch all land."""
        merged = JsonStructure.merge({"a": 1, "b": 2}, {"a": 10, "b": 20})

        assert merged == {"a": 10, "b": 20}

    def test_awkward_key_round_trips_through_a_patch(self) -> None:
        """A bracket-quoted path writes the key it names, not a literal quoted key."""
        content = {"limits": {"max.tokens": 8000}}

        merged = JsonStructure.merge(content, {'limits["max.tokens"]': 4000})

        assert merged == {"limits": {"max.tokens": 4000}}

    @pytest.mark.parametrize("path", ["tools[5].name", "tools[5]", "llm.new[0]"])
    def test_refuses_to_create_a_list_index(self, path: str) -> None:
        """List positions are ambiguous, so merge raises rather than guessing."""
        content = {"tools": [{"name": "a"}], "llm": {}}

        with pytest.raises(ValueError, match="explicitly"):
            JsonStructure.merge(content, {path: "x"})

    def test_refuses_to_set_a_key_on_a_scalar(self) -> None:
        """A scalar parent is a caller error, not a silent overwrite of the scalar."""
        with pytest.raises(ValueError, match="cannot set"):
            JsonStructure.merge({"llm": "litellm"}, {"llm.model": "opus"})

    def test_refuses_to_descend_through_a_scalar(self) -> None:
        """A scalar mid-path fails on the way down, before anything is written."""
        with pytest.raises(ValueError, match="cannot descend"):
            JsonStructure.merge({"llm": "litellm"}, {"llm.model.name": "opus"})

    def test_refuses_to_patch_the_root(self) -> None:
        """The empty path addresses the whole document, which a patch cannot replace."""
        with pytest.raises(ValueError, match="document root"):
            JsonStructure.merge({"a": 1}, {"": {"b": 2}})

    def test_malformed_patch_path_raises(self) -> None:
        """A malformed path fails before anything is written."""
        with pytest.raises(ValueError, match="malformed path"):
            JsonStructure.merge({"a": 1}, {"a[": "x"})

    def test_map_reflects_the_patch(self) -> None:
        """Describe of a merged document shows the new value — no drift after an update."""
        content = {"llm": {"model": "gpt-4o"}}

        merged = JsonStructure.merge(content, {"llm.model": "opus"})

        assert JsonStructure.describe(merged)["llm.model"] == "opus"


class TestCheck:
    """Tests for filtering an authored key map against the content it describes."""

    def test_keeps_entries_whose_paths_resolve(self) -> None:
        content = {"llm": {"provider": "litellm", "temperature": 0.2}, "tools": [{"name": "a"}]}
        authored = {
            "llm.provider": "which backend routes the completion",
            "llm.temperature": "sampling temperature; lower is more deterministic",
            "tools": "the tools this service exposes",
        }

        assert JsonStructure.check(content, authored) == authored

    def test_accepts_a_container_path(self) -> None:
        """An author may summarise a whole section rather than each leaf under it."""
        content = {"llm": {"provider": "litellm", "temperature": 0.2}}

        assert JsonStructure.check(content, {"llm": "model routing"}) == {"llm": "model routing"}

    def test_accepts_a_list_index_path(self) -> None:
        content = {"tools": [{"name": "search"}, {"name": "fetch"}]}

        assert JsonStructure.check(content, {"tools[1]": "the fetch tool"}) == {"tools[1]": "the fetch tool"}

    def test_drops_a_path_absent_from_the_content(self, caplog: pytest.LogCaptureFixture) -> None:
        """A hallucinated key would advertise a scope no read can satisfy."""
        content = {"llm": {"provider": "litellm"}}
        authored = {"llm.provider": "the backend", "llm.hallucinated": "not a real key"}

        with caplog.at_level(logging.WARNING):
            kept = JsonStructure.check(content, authored)

        assert kept == {"llm.provider": "the backend"}
        assert "llm.hallucinated" in caplog.text
        assert "not a real key" in caplog.text

    def test_drops_a_malformed_path(self, caplog: pytest.LogCaptureFixture) -> None:
        """A path the backend could not parse either is dropped rather than raising."""
        with caplog.at_level(logging.WARNING):
            kept = JsonStructure.check({"a": 1}, {"a[": "unterminated", "a": "fine"})

        assert kept == {"a": "fine"}
        assert "unterminated" in caplog.text

    def test_clips_a_long_summary(self) -> None:
        summary = "x" * 400
        kept = JsonStructure.check({"a": 1}, {"a": summary})

        assert kept["a"] == "x" * 120 + "…"

    def test_drops_entries_over_the_cap(self, caplog: pytest.LogCaptureFixture) -> None:
        content: dict[str, Any] = {f"k{index}": index for index in range(600)}
        authored = {f"k{index}": f"summary {index}" for index in range(600)}

        with caplog.at_level(logging.WARNING):
            kept = JsonStructure.check(content, authored)

        assert len(kept) == 512
        assert "k599" in caplog.text
        assert "summary 599" in caplog.text

    def test_preserves_the_authored_order(self) -> None:
        content = {"b": 1, "a": 2}
        kept = JsonStructure.check(content, {"a": "second key", "b": "first key"})

        assert list(kept) == ["a", "b"]

    def test_an_empty_map_stays_empty(self) -> None:
        assert JsonStructure.check({"a": 1}, {}) == {}


_KEYS = st.text(min_size=0, max_size=12)
_SCALARS = st.one_of(st.none(), st.booleans(), st.integers(), st.floats(allow_nan=False, allow_infinity=False), _KEYS)
_JSON = st.recursive(
    _SCALARS,
    lambda children: st.one_of(st.lists(children, max_size=4), st.dictionaries(_KEYS, children, max_size=4)),
    max_leaves=25,
)


@pytest.mark.property
class TestGrammarProperties:
    """The grammar is a cross-repo contract; these hold for any document, not just the examples."""

    @given(keys=st.lists(_KEYS, min_size=1, max_size=5))
    @settings(max_examples=300)
    def test_encode_decode_round_trips_any_key(self, keys: list[str]) -> None:
        """Whatever _encode emits, _decode reads back as the same key sequence."""
        path = ""
        for key in keys:
            path = JsonStructure._encode(path, key)

        assert JsonStructure._decode(path) == keys

    @given(content=st.dictionaries(_KEYS, _JSON, max_size=6))
    @settings(max_examples=300)
    def test_every_described_path_resolves(self, content: dict[str, Any]) -> None:
        """The property that protects the contract: describe never emits an unreadable path."""
        entries = JsonStructure.describe(content)

        assert set(JsonStructure.resolve(content, list(entries))) == set(entries)

    @given(content=st.dictionaries(_KEYS, _JSON, max_size=6))
    @settings(max_examples=200)
    def test_a_described_path_is_never_a_container_with_children(self, content: dict[str, Any]) -> None:
        """Leaves only: a populated object is implied by the dotted paths, never listed itself."""
        resolved = JsonStructure.resolve(content, list(JsonStructure.describe(content)))

        assert not [value for value in resolved.values() if isinstance(value, dict) and value]

    @given(content=st.dictionaries(_KEYS, _JSON, max_size=6))
    @settings(max_examples=200)
    def test_check_only_keeps_resolvable_paths(self, content: dict[str, Any]) -> None:
        """Whatever an author supplies, what survives is exactly what a reader can fetch."""
        authored = dict.fromkeys(JsonStructure.describe(content), "summary")
        authored["definitely::absent::path"] = "hallucinated"

        kept = JsonStructure.check(content, authored)

        assert "definitely::absent::path" not in kept
        assert set(JsonStructure.resolve(content, list(kept))) == set(kept)

    @given(content=st.dictionaries(_KEYS, _JSON, min_size=1, max_size=6))
    @settings(max_examples=200)
    def test_merge_then_resolve_returns_what_was_written(self, content: dict[str, Any]) -> None:
        """A patch at any described path is readable back at that same path."""
        path = next(iter(JsonStructure.describe(content)))

        merged = JsonStructure.merge(content, {path: "written"})

        assert JsonStructure.resolve(merged, [path]) == {path: "written"}

    @given(content=st.dictionaries(_KEYS, _JSON, min_size=1, max_size=6))
    @settings(max_examples=200)
    def test_merge_does_not_mutate_the_original(self, content: dict[str, Any]) -> None:
        before = copy.deepcopy(content)
        JsonStructure.merge(content, {next(iter(JsonStructure.describe(content))): "written"})

        assert content == before

    @given(summary=st.text(max_size=400))
    @settings(max_examples=200)
    def test_a_kept_summary_never_exceeds_the_budget(self, summary: str) -> None:
        kept = JsonStructure.check({"a": 1}, {"a": summary})

        assert len(kept["a"]) <= JsonStructure._DESCRIPTION_CHARS + 1


@pytest.mark.edge_case
class TestCheckBoundaries:
    """Boundaries of the authored map that the happy-path tests do not reach."""

    def test_drops_a_list_index_past_the_end(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            kept = JsonStructure.check({"tools": [{"name": "a"}]}, {"tools[3]": "the fourth tool"})

        assert kept == {}
        assert "the fourth tool" in caplog.text

    def test_keeps_a_negative_index_python_can_reach(self) -> None:
        """The resolver follows Python indexing, so a backend splitting on it must too."""
        assert JsonStructure.check({"t": [1, 2]}, {"t[-1]": "the last one"}) == {"t[-1]": "the last one"}

    def test_does_not_top_up_a_partial_map(self) -> None:
        """An author covering one key of three gets exactly that — no derived entries appended."""
        content = {"a": 1, "b": 2, "c": 3}

        assert JsonStructure.check(content, {"b": "the b knob"}) == {"b": "the b knob"}

    def test_keeps_an_empty_summary(self) -> None:
        """An author may name a key without describing it; that is still a real entry."""
        assert JsonStructure.check({"a": 1}, {"a": ""}) == {"a": ""}

    def test_a_summary_at_the_budget_is_not_clipped(self) -> None:
        summary = "x" * JsonStructure._DESCRIPTION_CHARS

        assert JsonStructure.check({"a": 1}, {"a": summary})["a"] == summary

    def test_keeps_a_path_whose_key_needs_quoting(self) -> None:
        content = {"limits": {"max.tokens": 8000}}

        assert JsonStructure.check(content, {'limits["max.tokens"]': "ceiling"}) == {'limits["max.tokens"]': "ceiling"}

    def test_an_empty_document_keeps_nothing(self) -> None:
        assert JsonStructure.check({}, {"a": "gone"}) == {}
