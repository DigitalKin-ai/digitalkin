"""Tests for SetupTools — setup CRUD + visibility over the module's shared setup service."""

import datetime
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from digitalkin.community.agno.toolkits import SetupTools
from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.setup.setup_strategy import SetupData, SetupStrategy, SetupVersionData

_WHEN = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)


def _setup(setup_id: str = "s1", name: str = "my setup") -> SetupData:
    return SetupData(
        id=setup_id,
        name=name,
        organisation_id="org1",
        owner_id="owner1",
        module_id="mod1",
        status="READY",
        visibility="VISIBILITY_PRIVATE",
        current_setup_version=SetupVersionData(
            id="v1", setup_id=setup_id, version="1.0.0", content={"k": "v"}, creation_date=_WHEN
        ),
    )


class _RecordingSetup(SetupStrategy):
    """Records the dict each op receives and returns a canned, backend-shaped value."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        self.calls.append(("get_setup", setup_dict))
        return _setup(setup_dict["setup_id"])

    async def create_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        self.calls.append(("create_setup", setup_dict))
        return _setup(name=setup_dict["name"])

    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        self.calls.append(("update_setup", setup_dict))
        return _setup(setup_dict["setup_id"], name=setup_dict["name"])

    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        self.calls.append(("delete_setup", setup_dict))
        return True

    async def change_visibility(self, setup_dict: dict[str, Any]) -> SetupData:
        self.calls.append(("change_visibility", setup_dict))
        setup = _setup(setup_dict["setup_id"])
        setup.visibility = f"VISIBILITY_{setup_dict['visibility'].upper()}"
        return setup


class _RaisingSetup(SetupStrategy):
    """Every op raises the configured exception (for degradation tests)."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def get_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        raise self._exc

    async def create_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        raise self._exc

    async def update_setup(self, setup_dict: dict[str, Any]) -> SetupData:
        raise self._exc

    async def delete_setup(self, setup_dict: dict[str, Any]) -> bool:
        raise self._exc

    async def change_visibility(self, setup_dict: dict[str, Any]) -> SetupData:
        raise self._exc


def _envelope(raw: str) -> dict[str, Any]:
    return json.loads(raw)


class TestSetupToolsHappyPath:
    """Each tool builds the expected dict and returns the success envelope."""

    def test_exposed_surface(self) -> None:
        """The agent sees exactly the 6 setup-level tools — no version RPCs, no list."""
        toolkit = SetupTools(_RecordingSetup())
        assert {fn.__name__ for fn in toolkit.tools} == {
            "get_setup",
            "create_setup",
            "create_service",
            "update_setup",
            "delete_setup",
            "change_visibility",
        }

    @pytest.mark.asyncio
    async def test_get_setup(self) -> None:
        backend = _RecordingSetup()
        env = _envelope(await SetupTools(backend).get_setup("s1", version="1.0.0"))
        assert env["metadata"]["success"] is True
        assert env["output"]["id"] == "s1"
        assert env["output"]["status"] == "READY"
        assert env["output"]["visibility"] == "VISIBILITY_PRIVATE"
        assert backend.calls == [("get_setup", {"setup_id": "s1", "version": "1.0.0"})]

    @pytest.mark.asyncio
    async def test_create_setup_sends_only_name_and_content(self) -> None:
        backend = _RecordingSetup()
        env = _envelope(await SetupTools(backend).create_setup("n", {"a": 1}))
        assert env["metadata"]["success"] is True
        assert env["output"]["name"] == "n"
        # Owner/organisation/module derive server-side — the tool sends nothing else.
        assert backend.calls == [("create_setup", {"name": "n", "content": {"a": 1}})]

    @pytest.mark.asyncio
    async def test_update_setup(self) -> None:
        backend = _RecordingSetup()
        env = _envelope(await SetupTools(backend).update_setup("s1", "renamed", {"a": 2}))
        assert env["output"]["name"] == "renamed"
        assert backend.calls == [("update_setup", {"setup_id": "s1", "name": "renamed", "content": {"a": 2}})]

    @pytest.mark.asyncio
    async def test_delete_setup(self) -> None:
        backend = _RecordingSetup()
        env = _envelope(await SetupTools(backend).delete_setup("s1"))
        assert env["output"] is True
        assert backend.calls == [("delete_setup", {"setup_id": "s1"})]

    @pytest.mark.asyncio
    async def test_change_visibility(self) -> None:
        backend = _RecordingSetup()
        env = _envelope(await SetupTools(backend).change_visibility("s1", "public"))
        assert env["output"]["visibility"] == "VISIBILITY_PUBLIC"
        assert backend.calls == [("change_visibility", {"setup_id": "s1", "visibility": "public"})]


class TestSetupToolsDegradation:
    """Every failure mode returns a structured error envelope — never raises."""

    @pytest.mark.asyncio
    async def test_permission_denied_is_distinct(self) -> None:
        env = _envelope(await SetupTools(_RaisingSetup(PermissionDeniedError("x"))).get_setup("s1"))
        assert env["metadata"]["success"] is False
        assert env["error"] == "permission denied: get_setup"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exc", [SetupServiceError("boom"), ServerError("down"), ValueError("bad")])
    async def test_service_errors_are_caught(self, exc: Exception) -> None:
        env = _envelope(await SetupTools(_RaisingSetup(exc)).create_setup("n", {"a": 1}))
        assert env["metadata"]["success"] is False
        assert env["metadata"]["tool"] == "create_setup"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exc", [KeyError("data"), TypeError("nope"), RuntimeError("boom")])
    async def test_unexpected_errors_never_raise(self, exc: Exception) -> None:
        """Backend contract surprises (KeyError, ...) still land in a fail envelope."""
        env = _envelope(await SetupTools(_RaisingSetup(exc)).create_setup("n", {"a": 1}))
        assert env["metadata"]["success"] is False
        assert type(exc).__name__ in env["error"]

    @pytest.mark.asyncio
    async def test_invalid_visibility_fails_cleanly(self) -> None:
        env = _envelope(await SetupTools(DefaultSetup()).change_visibility("s1", "everyone"))  # type: ignore[arg-type]
        assert env["metadata"]["success"] is False


class TestSetupToolsInvalidation:
    """Successful writes invalidate the servicer's setup cache via the context callback."""

    @staticmethod
    def _tools(backend: SetupStrategy) -> tuple[SetupTools, Mock]:
        invalidate = Mock()
        context = SimpleNamespace(callbacks=SimpleNamespace(invalidate_setup=invalidate))
        return SetupTools(backend, context=context), invalidate  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_writes_invalidate(self) -> None:
        tools, invalidate = self._tools(_RecordingSetup())
        await tools.create_setup("n", {"a": 1})
        await tools.update_setup("s1", "n", {"a": 1})
        await tools.delete_setup("s1")
        await tools.change_visibility("s1", "internal")
        assert invalidate.call_count == 4

    @pytest.mark.asyncio
    async def test_reads_do_not_invalidate(self) -> None:
        tools, invalidate = self._tools(_RecordingSetup())
        await tools.get_setup("s1")
        invalidate.assert_not_called()

    @pytest.mark.asyncio
    async def test_failed_write_does_not_invalidate(self) -> None:
        tools, invalidate = self._tools(_RaisingSetup(SetupServiceError("boom")))
        await tools.update_setup("s1", "n", {"a": 1})
        invalidate.assert_not_called()

    @pytest.mark.asyncio
    async def test_absent_callback_is_noop(self) -> None:
        tools = SetupTools(_RecordingSetup(), context=SimpleNamespace(callbacks=SimpleNamespace()))  # type: ignore[arg-type]
        env = _envelope(await tools.create_setup("n", {"a": 1}))
        assert env["metadata"]["success"] is True


class TestSetupToolsWithDefaultSetup:
    """LOCAL-mode integration: the toolkit round-trips against the real DefaultSetup."""

    @pytest.mark.asyncio
    async def test_full_round_trip(self) -> None:
        tools = SetupTools(DefaultSetup())

        created = _envelope(await tools.create_setup("my setup", {"a": 1}))
        assert created["metadata"]["success"] is True
        setup_id = created["output"]["id"]
        assert created["output"]["visibility"] == "VISIBILITY_PRIVATE"

        got = _envelope(await tools.get_setup(setup_id))
        assert got["output"]["name"] == "my setup"
        assert got["output"]["current_setup_version"]["content"] == {"a": 1}

        updated = _envelope(await tools.update_setup(setup_id, "renamed", {"a": 2}))
        assert updated["output"]["name"] == "renamed"
        assert updated["output"]["current_setup_version"]["content"] == {"a": 2}

        shared = _envelope(await tools.change_visibility(setup_id, "internal"))
        assert shared["output"]["visibility"] == "VISIBILITY_INTERNAL"

        assert _envelope(await tools.delete_setup(setup_id))["output"] is True

    @pytest.mark.asyncio
    async def test_missing_ids_fail_cleanly(self) -> None:
        tools = SetupTools(DefaultSetup())
        env = _envelope(await tools.get_setup("nope"))
        assert env["metadata"]["success"] is False


class TestCreateService:
    """create_service tool: name + content only, always registered."""

    @pytest.mark.asyncio
    async def test_creates_service(self) -> None:
        tools = SetupTools(DefaultSetup())
        assert any(tool.__name__ == "create_service" for tool in tools.tools)
        env = _envelope(await tools.create_service("Nikita", {"branding": True}))
        assert env["output"]["name"] == "Nikita"
        assert env["output"]["current_setup_version"]["content"] == {"branding": True}
