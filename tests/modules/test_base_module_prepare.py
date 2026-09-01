"""Phase 3.A — `BaseModule.prepare()` is idempotent and decoupled from input.

The dial-back orchestrator (`ModuleRunner`) calls `prepare(setup_data,
callback)` to pay LiteLLM/agno init costs in parallel with the wait for
the consumer's first reply. The eventual `start(input, setup, callback)`
call short-circuits past prepare via the `_prepared` guard.

These tests assert the contract:
- `prepare()` runs `set_callback`, `build_tool_cache`, `initialize`, and
  `init_handlers` exactly once.
- A second call is a no-op.
- `start()` after `prepare()` skips the prepare phase.
- Failures inside `prepare()` propagate so the caller can convert to
  `stream.error(MODULE_RUNTIME_ERROR)`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytestmark = [pytest.mark.timeout(15)]


class _MinimalModule:
    """Concrete BaseModule-shaped object with just the methods prepare()/
    start() touch. Avoids abstract-class instantiation overhead."""


def _make_module_skeleton(
    setup_data: Any, *, initialize_side_effect: Any = None, builds_tool_cache: bool = True
) -> Any:
    """Build a minimal BaseModule-like instance for prepare()/start() tests.

    Bypasses the full ModuleFactory + ModuleContext wiring; we only
    care about the prepare/start lifecycle gating here. We import the
    real `prepare` and `start` methods from BaseModule and bind them to
    a plain instance.
    """
    from digitalkin.models.module.module import ModuleStatus
    from digitalkin.modules._base_module import BaseModule

    inst = _MinimalModule()
    inst._status = ModuleStatus.CREATED
    inst._prebuilt_tool_cache = None
    inst.trigger_handlers = {}
    inst._prepared = False
    inst._builds_tool_cache = builds_tool_cache

    ctx = MagicMock()
    ctx.callbacks = MagicMock()
    ctx.session.current_ids.return_value = {"task_id": "task_test"}
    ctx.registry = MagicMock()
    ctx.communication = MagicMock()
    # prepare() restores this mission's runtime-loaded tools right after the cache build.
    ctx.rehydrate_loaded_tools = AsyncMock(return_value=0)
    inst.context = ctx

    setup_data.build_tool_cache = AsyncMock(return_value=MagicMock(entries=[]))

    inst.initialize = AsyncMock(side_effect=initialize_side_effect) if initialize_side_effect else AsyncMock()
    inst.triggers_discoverer = MagicMock()
    inst.triggers_discoverer.init_handlers = MagicMock(return_value={})

    # Bind the real prepare() and start() methods onto the skeleton.
    inst.prepare = BaseModule.prepare.__get__(inst, _MinimalModule)
    inst.start = BaseModule.start.__get__(inst, _MinimalModule)
    inst.stop = AsyncMock()
    return inst


class TestPrepareIdempotent:
    async def test_first_call_runs_full_init_chain(self) -> None:
        setup = MagicMock()
        m = _make_module_skeleton(setup)
        cb = AsyncMock()

        await m.prepare(setup, cb)

        assert m._prepared is True  # noqa: SLF001
        m.initialize.assert_awaited_once()
        m.triggers_discoverer.init_handlers.assert_called_once()
        setup.build_tool_cache.assert_awaited_once()
        assert m.context.callbacks.send_message is cb

    async def test_tool_module_skips_tool_cache(self) -> None:
        """A leaf module (`_builds_tool_cache=False`, e.g. ToolModule) skips build_tool_cache."""
        setup = MagicMock()
        m = _make_module_skeleton(setup, builds_tool_cache=False)
        cb = AsyncMock()

        await m.prepare(setup, cb)

        assert m._prepared is True
        m.initialize.assert_awaited_once()
        m.triggers_discoverer.init_handlers.assert_called_once()
        setup.build_tool_cache.assert_not_called()

    async def test_second_call_is_noop(self) -> None:
        setup = MagicMock()
        m = _make_module_skeleton(setup)
        cb = AsyncMock()

        await m.prepare(setup, cb)
        await m.prepare(setup, cb)

        m.initialize.assert_awaited_once()
        m.triggers_discoverer.init_handlers.assert_called_once()
        setup.build_tool_cache.assert_awaited_once()

    async def test_prepare_failure_propagates(self) -> None:
        setup = MagicMock()
        m = _make_module_skeleton(setup, initialize_side_effect=RuntimeError("init kaboom"))
        cb = AsyncMock()

        with pytest.raises(RuntimeError, match="init kaboom"):
            await m.prepare(setup, cb)

        assert m._prepared is False  # noqa: SLF001


class TestStartSkipsPrepareWhenPrepared:
    async def test_start_after_prepare_skips_init(self) -> None:
        setup = MagicMock()
        m = _make_module_skeleton(setup)
        cb = AsyncMock()

        # Stub _run_lifecycle + stop so start() runs end-to-end.
        m._run_lifecycle = AsyncMock()  # noqa: SLF001
        m.stop = AsyncMock()

        await m.prepare(setup, cb)
        # Reset call counts after the warm pass.
        m.initialize.reset_mock()
        m.triggers_discoverer.init_handlers.reset_mock()
        setup.build_tool_cache.reset_mock()

        await m.start(input_data=MagicMock(), setup_data=setup, callback=cb)

        # start() should call prepare() but prepare short-circuits.
        m.initialize.assert_not_called()
        m.triggers_discoverer.init_handlers.assert_not_called()
        setup.build_tool_cache.assert_not_called()
        m._run_lifecycle.assert_awaited_once()  # noqa: SLF001

    async def test_start_without_prior_prepare_does_init(self) -> None:
        setup = MagicMock()
        m = _make_module_skeleton(setup)
        cb = AsyncMock()
        m._run_lifecycle = AsyncMock()  # noqa: SLF001
        m.stop = AsyncMock()

        await m.start(input_data=MagicMock(), setup_data=setup, callback=cb)

        m.initialize.assert_awaited_once()
        m._run_lifecycle.assert_awaited_once()  # noqa: SLF001


class TestStartHandlesPrepareFailure:
    async def test_start_emits_error_callback_on_init_failure(self) -> None:
        setup = MagicMock()
        m = _make_module_skeleton(setup, initialize_side_effect=ValueError("bad config"))
        m._run_lifecycle = AsyncMock()  # noqa: SLF001
        m.stop = AsyncMock()
        cb = AsyncMock()

        await m.start(input_data=MagicMock(), setup_data=setup, callback=cb)

        # ModuleCodeModel error should have been sent through the callback.
        cb.assert_called_once()
        sent = cb.call_args.args[0]
        assert sent.code == "Error"
        assert "ValueError" in sent.message
        # _run_lifecycle must not have been entered.
        m._run_lifecycle.assert_not_called()  # noqa: SLF001
        m.stop.assert_awaited()
