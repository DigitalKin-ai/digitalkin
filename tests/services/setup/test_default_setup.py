"""Tests for the concrete create_service_setup on the strategy ABC."""

import pytest

from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.exceptions import SetupServiceError


class TestCreateServiceSetup:
    """create_service_setup delegates to create_setup with name + content only."""

    async def test_creates_service_setup(self) -> None:
        setup = await DefaultSetup().create_service_setup("Nikita", {"branding": True})
        assert setup.name == "Nikita"
        assert setup.current_setup_version.content == {"branding": True}
        assert setup.current_setup_version.documentation == ""

    async def test_forwards_documentation(self) -> None:
        setup = await DefaultSetup().create_service_setup("Nikita", {"branding": True}, "brand voice service")
        assert setup.current_setup_version.documentation == "brand voice service"


class TestDocumentation:
    """documentation rides on the version it was cut with."""

    async def test_create_stores_documentation(self) -> None:
        setup = await DefaultSetup().create_setup({"name": "n", "content": {}, "documentation": "what it does"})
        assert setup.current_setup_version.documentation == "what it does"

    async def test_update_replaces_documentation(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {}, "documentation": "old"})

        await strategy.update_setup(
            {"setup_id": setup.id, "name": "n", "content": {}, "documentation": "new"}
        )
        assert setup.current_setup_version.documentation == "new"

    async def test_get_reads_the_documentation_back(self) -> None:
        strategy = DefaultSetup()
        created = await strategy.create_setup({"name": "n", "content": {}, "documentation": "what it does"})

        fetched = await strategy.get_setup({"setup_id": created.id})
        assert fetched.current_setup_version.documentation == "what it does"

    async def test_get_reads_back_the_documentation_of_the_active_version(self) -> None:
        """After an update the read follows the newly activated revision, not the first one."""
        strategy = DefaultSetup()
        created = await strategy.create_setup({"name": "n", "content": {}, "documentation": "first"})
        await strategy.update_setup(
            {"setup_id": created.id, "name": "n", "content": {}, "documentation": "second"}
        )

        fetched = await strategy.get_setup({"setup_id": created.id})
        assert fetched.current_setup_version.documentation == "second"

    async def test_a_staged_version_does_not_change_what_get_returns(self) -> None:
        """set_as_current=False stages the text with the version; the active one is unchanged."""
        strategy = DefaultSetup()
        created = await strategy.create_setup({"name": "n", "content": {}, "documentation": "live"})
        await strategy.update_setup({
            "setup_id": created.id,
            "name": "n",
            "content": {},
            "documentation": "staged",
            "set_as_current": False,
        })

        fetched = await strategy.get_setup({"setup_id": created.id})
        assert fetched.current_setup_version.documentation == "live"
        page = await strategy.list_setup_versions({"setup_id": created.id})
        assert [v.documentation for v in page.setup_versions] == ["staged", "live"]

    async def test_update_without_documentation_leaves_the_new_version_empty(self) -> None:
        """A new revision carries only what the call supplied; it does not inherit."""
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {}, "documentation": "old"})

        await strategy.update_setup({"setup_id": setup.id, "name": "n", "content": {}})
        assert setup.current_setup_version.documentation == ""


class TestOutputFormatSpecGuard:
    """An oversized output_format_spec is refused on both write paths, not just update."""

    async def test_create_refuses_an_oversized_spec(self) -> None:
        strategy = DefaultSetup()
        with pytest.raises(ValueError, match="must stay under 4096"):
            await strategy.create_setup({"name": "n", "content": {"output_format_spec": "x" * 4096}})
        assert strategy.setups == {}, "nothing may be stored when the guard trips"

    async def test_update_refuses_an_oversized_spec(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"output_format_spec": "ok"}})

        with pytest.raises(ValueError, match="must stay under 4096"):
            await strategy.update_setup(
                {"setup_id": setup.id, "name": "n", "content": {"output_format_spec": "x" * 4096}}
            )
        # The guard runs before the revision is cut, so no half-written version survives.
        assert setup.current_setup_version.content == {"output_format_spec": "ok"}
        assert (await strategy.list_setup_versions({"setup_id": setup.id})).total_count == 1


class TestVersionHistory:
    """The local strategy keeps a version history so the two version RPCs are meaningful."""

    async def test_update_cuts_a_new_version_and_activates_it(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"v": 1}})
        first = setup.current_setup_version.id

        await strategy.update_setup({"setup_id": setup.id, "name": "n", "content": {"v": 2}})

        assert setup.current_setup_version.id != first
        assert setup.current_setup_version.content == {"v": 2}
        assert (await strategy.list_setup_versions({"setup_id": setup.id})).total_count == 2

    async def test_update_can_leave_the_new_version_inactive(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"v": 1}})
        first = setup.current_setup_version.id

        await strategy.update_setup({"setup_id": setup.id, "name": "n", "content": {"v": 2}, "set_as_current": False})

        assert setup.current_setup_version.id == first
        assert (await strategy.list_setup_versions({"setup_id": setup.id})).total_count == 2

    async def test_list_is_most_recent_first_and_paginates(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"v": 0}})
        for i in (1, 2):
            await strategy.update_setup({"setup_id": setup.id, "name": "n", "content": {"v": i}})

        page = await strategy.list_setup_versions({"setup_id": setup.id})
        assert [v.content for v in page.setup_versions] == [{"v": 2}, {"v": 1}, {"v": 0}]
        assert page.current_setup_version_id == setup.current_setup_version.id

        window = await strategy.list_setup_versions({"setup_id": setup.id, "limit": 1, "offset": 2})
        assert [v.content for v in window.setup_versions] == [{"v": 0}]
        assert window.total_count == 3

    async def test_set_current_rolls_back_to_an_earlier_version(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"v": 0}})
        first = setup.current_setup_version.id
        await strategy.update_setup({"setup_id": setup.id, "name": "n", "content": {"v": 1}})

        rolled = await strategy.set_current_setup_version({"setup_id": setup.id, "setup_version_id": first})

        assert rolled.current_setup_version.id == first
        assert rolled.current_setup_version.content == {"v": 0}

    async def test_set_current_rejects_a_version_from_another_setup(self) -> None:
        strategy = DefaultSetup()
        mine = await strategy.create_setup({"name": "mine", "content": {}})
        theirs = await strategy.create_setup({"name": "theirs", "content": {}})

        with pytest.raises(SetupServiceError, match="not found on setup"):
            await strategy.set_current_setup_version({
                "setup_id": mine.id,
                "setup_version_id": theirs.current_setup_version.id,
            })

    async def test_delete_drops_the_history_too(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {}})

        assert await strategy.delete_setup({"setup_id": setup.id}) is True
        assert setup.id not in strategy.versions


class TestAuthoredStructure:
    """A supplied key map is stored, filtered to the paths that resolve in the content."""

    async def test_create_stores_the_authored_map(self) -> None:
        setup = await DefaultSetup().create_setup({
            "name": "n",
            "content": {"llm": {"provider": "litellm"}, "region": "eu-west"},
            "structure": {"llm.provider": "which backend routes the call", "region": "where it runs"},
        })

        assert setup.current_setup_version.structure == {
            "llm.provider": "which backend routes the call",
            "region": "where it runs",
        }

    async def test_create_drops_an_entry_the_content_does_not_have(self) -> None:
        setup = await DefaultSetup().create_setup({
            "name": "n",
            "content": {"region": "eu-west"},
            "structure": {"region": "where it runs", "llm.provider": "not in this document"},
        })

        assert setup.current_setup_version.structure == {"region": "where it runs"}

    async def test_update_replaces_the_map_with_the_one_supplied(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"a": "one"}, "structure": {"a": "the a knob"}})

        updated = await strategy.update_setup({
            "setup_id": setup.id,
            "name": "n",
            "content": {"a": "two", "b": "new"},
            "structure": {"a": "the a knob", "b": "the b knob"},
        })

        assert updated.current_setup_version.structure == {"a": "the a knob", "b": "the b knob"}

    async def test_update_without_a_map_falls_back_to_derived(self) -> None:
        """Omitting it discards the authored summaries — the documented downgrade."""
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"a": "one"}, "structure": {"a": "the a knob"}})

        updated = await strategy.update_setup({"setup_id": setup.id, "name": "n", "content": {"a": "two"}})

        assert updated.current_setup_version.structure == {"a": "two"}

    async def test_create_service_setup_forwards_the_map(self) -> None:
        setup = await DefaultSetup().create_service_setup("n", {"a": 1}, structure={"a": "the a knob"})

        assert setup.current_setup_version.structure == {"a": "the a knob"}


class TestStructure:
    """With no map supplied, one is derived from the values; reads project content down to keys."""

    async def test_create_stores_the_key_map(self) -> None:
        setup = await DefaultSetup().create_setup({
            "name": "n",
            "content": {"llm": {"provider": "litellm"}, "region": "eu-west"},
        })

        assert setup.current_setup_version.structure == {
            "llm.provider": "litellm",
            "region": "eu-west",
        }

    async def test_update_recomputes_the_map_from_the_new_content(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"a": "one"}})

        updated = await strategy.update_setup({
            "setup_id": setup.id,
            "name": "n",
            "content": {"a": "two", "b": "new"},
        })

        assert updated.current_setup_version.structure == {"a": "two", "b": "new"}

    async def test_each_version_keeps_its_own_map(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"a": "one"}})
        first = setup.current_setup_version

        await strategy.update_setup({"setup_id": setup.id, "name": "n", "content": {"a": "two"}})

        assert first.structure == {"a": "one"}

    async def test_get_without_keys_returns_the_whole_document(self) -> None:
        strategy = DefaultSetup()
        content = {"llm": {"provider": "litellm", "model": "gpt-4o"}, "region": "eu"}
        setup = await strategy.create_setup({"name": "n", "content": content})

        fetched = await strategy.get_setup({"setup_id": setup.id})

        assert fetched.current_setup_version.content == content

    async def test_get_with_a_key_projects_content_to_that_path(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({
            "name": "n",
            "content": {"llm": {"provider": "litellm", "model": "gpt-4o"}, "region": "eu"},
        })

        fetched = await strategy.get_setup({"setup_id": setup.id, "structure_key": "llm.provider"})

        assert fetched.current_setup_version.content == {"llm.provider": "litellm"}

    async def test_projection_does_not_mutate_the_stored_setup(self) -> None:
        strategy = DefaultSetup()
        content = {"llm": {"provider": "litellm"}, "region": "eu"}
        setup = await strategy.create_setup({"name": "n", "content": content})

        await strategy.get_setup({"setup_id": setup.id, "structure_key": "region"})
        fetched = await strategy.get_setup({"setup_id": setup.id})

        assert fetched.current_setup_version.content == content

    async def test_unresolvable_key_is_dropped_not_raised(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"a": 1}})

        fetched = await strategy.get_setup({"setup_id": setup.id, "structure_key": "nope"})

        assert fetched.current_setup_version.content == {}

    async def test_empty_key_returns_the_whole_document(self) -> None:
        """structure_key has no proto3 presence, so "" and unset are one request."""
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {"a": 1}})

        fetched = await strategy.get_setup({"setup_id": setup.id, "structure_key": ""})

        assert fetched.current_setup_version.content == {"a": 1}

    async def test_every_stored_key_is_resolvable(self) -> None:
        strategy = DefaultSetup()
        content = {"llm": {"provider": "litellm"}, "limits": {"max.tokens": 8000}, "tools": [{"n": 1}]}
        setup = await strategy.create_setup({"name": "n", "content": content})
        keys = list(setup.current_setup_version.structure)

        for key in keys:
            fetched = await strategy.get_setup({"setup_id": setup.id, "structure_key": key})
            assert list(fetched.current_setup_version.content) == [key]
