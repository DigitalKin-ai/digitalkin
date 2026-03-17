"""Server-level asyncio task monitor via asyncio-inspector."""

from typing import Any

from digitalkin.logger import logger


class AsyncioMonitor:
    """Server-level asyncio task monitor with HTTP stats endpoint.

    Wraps asyncio-inspector to expose real-time asyncio task statistics
    on an HTTP endpoint. Gracefully degrades if the package is not installed.
    """

    def __init__(self, port: int) -> None:
        """Initialize the asyncio monitor.

        Args:
            port: HTTP port for the stats endpoint.
        """
        self._port = port
        self._server: Any = None

    async def start(self) -> None:
        """Start the asyncio-inspector HTTP server."""
        try:
            from asyncio_inspector import serve

            self._server = await serve(port=self._port)
            logger.info("asyncio-inspector started on port %d", self._port)
        except ImportError:
            logger.warning("asyncio-inspector requested but package not installed, skipping")
        except Exception:
            logger.exception("Failed to start asyncio-inspector on port %d", self._port)

    async def stop(self) -> None:
        """Stop the asyncio-inspector HTTP server."""
        if self._server is None:
            return

        try:
            self._server.close()
            await self._server.wait_closed()
            logger.info("asyncio-inspector stopped")
        except Exception:
            logger.exception("Failed to stop asyncio-inspector")
        finally:
            self._server = None
