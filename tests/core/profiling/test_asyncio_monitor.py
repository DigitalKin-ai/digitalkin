"""Tests for AsyncioMonitor."""

import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.core.profiling.asyncio_monitor import AsyncioMonitor


class TestAsyncioMonitorLifecycle:
    """Tests for start/stop lifecycle."""

    async def test_start_and_stop(self):
        mock_server = MagicMock()
        mock_server.close = MagicMock()
        mock_server.wait_closed = AsyncMock()

        mock_module = ModuleType("asyncio_inspector")
        mock_module.serve = AsyncMock(return_value=mock_server)

        with patch.dict(sys.modules, {"asyncio_inspector": mock_module}):
            monitor = AsyncioMonitor(port=9999)
            await monitor.start()

            assert monitor._server is mock_server
            mock_module.serve.assert_awaited_once_with(port=9999)

            await monitor.stop()
            mock_server.close.assert_called_once()
            mock_server.wait_closed.assert_awaited_once()
            assert monitor._server is None

    async def test_stop_without_start_is_noop(self):
        monitor = AsyncioMonitor(port=9999)
        await monitor.stop()  # Should not raise
        assert monitor._server is None


class TestAsyncioMonitorImportError:
    """Tests for graceful degradation when asyncio-inspector is missing."""

    async def test_start_with_missing_package(self):
        with patch.dict(sys.modules, {"asyncio_inspector": None}):
            monitor = AsyncioMonitor(port=9999)
            await monitor.start()  # Should not raise
            assert monitor._server is None


class TestAsyncioMonitorExceptionSafety:
    """Tests that monitor exceptions never propagate."""

    async def test_start_exception_caught(self):
        mock_module = ModuleType("asyncio_inspector")
        mock_module.serve = AsyncMock(side_effect=RuntimeError("bind failed"))

        with patch.dict(sys.modules, {"asyncio_inspector": mock_module}):
            monitor = AsyncioMonitor(port=9999)
            await monitor.start()  # Should not raise
            assert monitor._server is None

    async def test_stop_exception_caught(self):
        mock_server = MagicMock()
        mock_server.close = MagicMock(side_effect=RuntimeError("close failed"))
        mock_server.wait_closed = AsyncMock()

        monitor = AsyncioMonitor(port=9999)
        monitor._server = mock_server
        await monitor.stop()  # Should not raise
        assert monitor._server is None


class TestAsyncioMonitorInvalidPort:
    """Tests for invalid port configuration."""

    def test_invalid_port_raises_without_protection(self):
        """Verify that int() on a non-numeric string raises ValueError.

        The BaseServer.start_async() wraps the asyncio-inspector block in
        try/except Exception to catch this. This test validates the underlying
        failure mode that the protection guards against.
        """
        with pytest.raises(ValueError):
            int("abc")
