"""Coverage for custom exception classes and B904 cause-chaining (P4.4).

Every custom exception class gets at least one ``pytest.raises``; the
re-wrap sites fixed in Tier B assert both the raised type and ``__cause__``.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from digitalkin.core.exceptions import BackpressureTimeoutError, BulkheadFullError
from digitalkin.exceptions import DigitalKinError
from digitalkin.grpc_servers.exceptions import ReflectionError, ServerError
from digitalkin.services.registry.exceptions import (
    InvalidStatusError,
    ModuleAlreadyExistsError,
    RegistryModuleNotFoundError,
    RegistryServiceError,
)
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.task_manager.exceptions import TaskManagerServiceError
from digitalkin.services.user_profile.exceptions import UserProfileServiceError
from digitalkin.utils.package_discover import ModuleDiscoverer


class TestSimpleExceptions:
    """Plain ``Exception`` subclasses raise, carry their message, and isinstance correctly."""

    @pytest.mark.parametrize(
        "exc_cls",
        [
            DigitalKinError,
            BackpressureTimeoutError,
            BulkheadFullError,
            TaskManagerServiceError,
            SetupServiceError,
            UserProfileServiceError,
            RegistryServiceError,
        ],
    )
    def test_raise_and_message(self, exc_cls: type[Exception]) -> None:
        with pytest.raises(exc_cls, match="boom"):
            raise exc_cls("boom")

    def test_digitalkin_error_is_base_of_server_error(self) -> None:
        assert issubclass(ServerError, DigitalKinError)
        with pytest.raises(DigitalKinError):
            raise ReflectionError("reflection down")

    def test_reflection_error_hierarchy(self) -> None:
        err = ReflectionError("x")
        assert isinstance(err, ServerError)
        assert isinstance(err, DigitalKinError)


class TestRegistryExceptions:
    """Registry exceptions store their id/status and format a message."""

    def test_module_not_found_carries_module_id(self) -> None:
        with pytest.raises(RegistryModuleNotFoundError, match="mod-1") as ei:
            raise RegistryModuleNotFoundError("mod-1")
        assert ei.value.module_id == "mod-1"
        assert isinstance(ei.value, RegistryServiceError)

    def test_module_already_exists_carries_module_id(self) -> None:
        with pytest.raises(ModuleAlreadyExistsError, match="already registered") as ei:
            raise ModuleAlreadyExistsError("mod-2")
        assert ei.value.module_id == "mod-2"

    def test_invalid_status_carries_status(self) -> None:
        with pytest.raises(InvalidStatusError, match="Invalid module status: 99") as ei:
            raise InvalidStatusError(99)
        assert ei.value.status == 99


class TestCauseChaining:
    """B904 re-wrap sites preserve ``__cause__`` (Tier B fixes)."""

    async def test_default_setup_wraps_validation_error(self) -> None:
        setup = DefaultSetup()
        with pytest.raises(ValueError, match="Validation failed for SetupData") as ei:
            await setup.create_setup({"name": "n", "content": "not-a-dict"})
        assert isinstance(ei.value.__cause__, ValidationError)

    def test_get_trigger_wraps_stop_iteration(self) -> None:
        class _Handler:
            input_format = int

        handlers = {"p": (_Handler(),)}

        class _Input:
            protocol = "p"

        with pytest.raises(ValueError, match="No handler for input format") as ei:
            ModuleDiscoverer.get_trigger(handlers, "p", _Input())  # type: ignore[arg-type]
        assert isinstance(ei.value.__cause__, StopIteration)

    def test_get_trigger_unknown_protocol_has_no_cause(self) -> None:
        class _Input:
            protocol = "missing"

        with pytest.raises(ValueError, match="No handler for protocol") as ei:
            ModuleDiscoverer.get_trigger({}, "missing", _Input())  # type: ignore[arg-type]
        assert ei.value.__cause__ is None
