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

        await strategy.update_setup(
            {"setup_id": setup.id, "name": "n", "content": {"v": 2}, "set_as_current": False}
        )

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

        rolled = await strategy.set_current_setup_version(
            {"setup_id": setup.id, "setup_version_id": first}
        )

        assert rolled.current_setup_version.id == first
        assert rolled.current_setup_version.content == {"v": 0}

    async def test_set_current_rejects_a_version_from_another_setup(self) -> None:
        strategy = DefaultSetup()
        mine = await strategy.create_setup({"name": "mine", "content": {}})
        theirs = await strategy.create_setup({"name": "theirs", "content": {}})

        with pytest.raises(SetupServiceError, match="not found on setup"):
            await strategy.set_current_setup_version(
                {"setup_id": mine.id, "setup_version_id": theirs.current_setup_version.id}
            )

    async def test_delete_drops_the_history_too(self) -> None:
        strategy = DefaultSetup()
        setup = await strategy.create_setup({"name": "n", "content": {}})

        assert await strategy.delete_setup({"setup_id": setup.id}) is True
        assert setup.id not in strategy.versions
