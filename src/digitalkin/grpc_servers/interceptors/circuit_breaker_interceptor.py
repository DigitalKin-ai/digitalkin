"""Circuit breaker gRPC interceptor — fast rejection when unhealthy.

Rejects all incoming RPCs with ``UNAVAILABLE`` when the circuit is open
(N consecutive failures). Zero Redis overhead — state is in-memory.

Wire into ``ModuleServer`` via the ``interceptors`` parameter::

    cb = CircuitBreaker("gateway", fail_max=5, reset_timeout=10)
    interceptor = CircuitBreakerInterceptor(cb)
    server = ModuleServer(MyModule, config, interceptors=[interceptor])
"""

from __future__ import annotations

from typing import Any

import grpc
import grpc.aio

from digitalkin.grpc_servers.exceptions import CircuitOpenError
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.logger import logger


class CircuitBreakerInterceptor(grpc.aio.ServerInterceptor):
    """Rejects RPCs immediately when the circuit breaker is open.

    The circuit breaker tracks consecutive failures recorded by the handler
    (via ``record_failure()``). When failures exceed ``fail_max``, the
    interceptor short-circuits all incoming RPCs without entering the handler.
    """

    _cb: CircuitBreaker

    def __init__(self, circuit_breaker: CircuitBreaker) -> None:
        """Initialize with a circuit breaker instance.

        Args:
            circuit_breaker: The circuit breaker to check on each RPC.
        """
        self._cb = circuit_breaker

    async def intercept_service(
        self,
        continuation: Any,
        handler_call_details: grpc.HandlerCallDetails,
    ) -> Any:
        """Check circuit state before forwarding to handler.

        Args:
            continuation: Next handler in the chain.
            handler_call_details: RPC metadata and method info.

        Returns:
            Handler from continuation, or abort handler if circuit is open.
        """
        try:
            self._cb.check()
        except CircuitOpenError:
            logger.warning(
                "Circuit open, rejecting RPC: method=%s service=%s",
                handler_call_details.method,
                self._cb.service_id,
            )

            async def _unavailable(_request: Any, context: grpc.aio.ServicerContext) -> None:
                await context.abort(grpc.StatusCode.UNAVAILABLE, "Service temporarily unavailable")

            return grpc.unary_unary_rpc_method_handler(_unavailable)

        return await continuation(handler_call_details)
