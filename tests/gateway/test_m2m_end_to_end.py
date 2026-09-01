"""Full end-to-end M2M tool call, mimicking real usage across every layer.

Flow under test (the real production path):
    caller ``call_module`` → BACKEND ``AssociateTask`` (mints + registers child)
    → ``StartStream(child)`` on the TARGET → target ``GatewayServicer`` dial-back
    → real ``ModuleRunner.run`` → ``resolve_setup`` → ``_check_setup_access``
    → real ``GrpcUserProfile.CheckResourceAccess`` (backend authenticates the
    child via its ``x-task-id`` metadata) → real module trigger emits output
    → Redis stream → dial-back → caller receives outputs.

The stateful backend authenticates exactly like prod: a task id it did not
register is rejected ``UNAUTHENTICATED "Invalid or inactive task"`` — the
regression test reproduces the prod bug that motivated the backend mint.

Assertions are tied to the prod validation markers: ``[VALIDATE AT2]`` (caller
mint) and ``[VALIDATE AC1]`` (setup access verdict).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, ClassVar, Literal
from unittest.mock import AsyncMock, MagicMock, Mock

import grpc
import grpc.aio
import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from agentic_mesh_protocol.user_profile.v1 import user_profile_pb2, user_profile_service_pb2_grpc
from google.protobuf import json_format

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.module_runner import ModuleRunner
from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.grpc_servers.module_servicer import ModuleServicer
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.module.module_types import DataModel, DataTrigger
from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.services.services import ServicesMode
from digitalkin.models.settings.utils.channel import SecurityMode
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.communication.grpc_communication import GrpcCommunication
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.user_profile.grpc_user_profile import GrpcUserProfile
from digitalkin.utils.package_discover import ModuleDiscoverer
from tests.gateway.test_dial_consumer import _FakeRedisClient
from tests.mocks.models import MockSecretModel

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Generator

    from digitalkin.models.module.module_context import ModuleContext

pytestmark = [pytest.mark.timeout(30)]

SETUP_ID = "setups:e2e"
MISSION_ID = "missions:e2e"
PARENT_TASK_ID = "task:parent-e2e"


def _client(host: str, port: int) -> ClientConfig:
    return ClientConfig(host=host, port=port, security=SecurityMode.INSECURE)


class _E2ESetupModel(SetupModel):
    config: str = "default"


class _E2EInputTrigger(DataTrigger):
    protocol: Literal["e2e"] = "e2e"


class _E2EInputModel(DataModel[_E2EInputTrigger]):
    pass


class _E2EOutputTrigger(DataTrigger):
    protocol: Literal["e2e"] = "e2e"


class _E2EOutputModel(DataModel[_E2EOutputTrigger]):
    pass


class _E2EModule(BaseModule[_E2EInputModel, _E2EOutputModel, _E2ESetupModel, MockSecretModel]):
    """Minimal real module: LOCAL default services, built-in healthcheck trigger emits output."""

    name = "E2EModule"
    description = "End-to-end test module"
    input_format = _E2EInputModel
    output_format = _E2EOutputModel
    setup_format = _E2ESetupModel
    secret_format = MockSecretModel
    services_config_strategies: ClassVar[dict[str, Any]] = {}
    services_config_params: ClassVar[dict[str, Any]] = {}
    services_config: Any = ServicesConfig(mode=ServicesMode.LOCAL)
    # A real importable package with no TriggerHandler classes: only the builtin
    # triggers (healthcheck_ping, ...) end up registered — enough for the e2e call.
    triggers_discoverer = ModuleDiscoverer(packages=["tests.mocks"])

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict:
        """Skip per-service init (established mock pattern); the e2e focus is the M2M path."""
        return dict.fromkeys(self.services_config.valid_strategy_names())

    async def initialize(self, context: ModuleContext, setup_data: _E2ESetupModel) -> None:
        """No-op init."""

    async def cleanup(self) -> None:
        """No-op cleanup."""


@pytest.fixture(autouse=True)
def _clear_singletons() -> Generator[None]:
    """Isolate class-level singletons (breakers, redis listener) between tests.

    Yields:
        None: while the test body runs.
    """
    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()
    yield
    CircuitBreaker._instances.clear()
    SharedRedisListener._instances.clear()


@pytest.fixture
def digitalkin_records() -> Generator[list[logging.LogRecord]]:
    """Capture 'digitalkin' logger records (the [VALIDATE ...] markers) at INFO.

    Yields:
        The captured records list, live-updated while the test runs.
    """
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.setLevel(logging.INFO)
    handler.emit = records.append  # type: ignore[method-assign]
    lg = logging.getLogger("digitalkin")
    lg.addHandler(handler)
    yield records
    lg.removeHandler(handler)


def _marker_lines(records: list[logging.LogRecord], marker: str) -> list[str]:
    return [r.getMessage() for r in records if marker in r.getMessage()]


# ---------------------------------------------------------------------------
# Stateful backend: GatewayService (AssociateTask) + UserProfileService
# (CheckResourceAccess) sharing one task registry, exactly like prod.
# ---------------------------------------------------------------------------


class _BackendState:
    """Task registry shared by the backend's two services."""

    def __init__(self) -> None:
        self.registered: set[str] = set()
        self.mint_count = 0
        self.mint_parents: list[str] = []
        self.mint_idempotency_keys: list[str] = []
        self.mint_time_remaining: list[float] = []
        self.access_task_ids: list[str] = []
        self.access_setup_ids: list[str] = []


class _BackendGateway(gateway_service_pb2_grpc.GatewayServiceServicer):
    """Backend GatewayService: mints + registers the child task."""

    def __init__(
        self,
        state: _BackendState,
        *,
        register_on_mint: bool = True,
        fail_first_n: int = 0,
    ) -> None:
        self._state = state
        self._register_on_mint = register_on_mint
        self._fail_first_n = fail_first_n

    async def AssociateTask(self, request: Any, context: grpc.aio.ServicerContext) -> Any:
        self._state.mint_count += 1
        self._state.mint_parents.append(request.parent_task_id)
        md = dict(context.invocation_metadata() or ())
        self._state.mint_idempotency_keys.append(str(md.get("x-idempotency-key", "")))
        self._state.mint_time_remaining.append(context.time_remaining())
        if self._state.mint_count <= self._fail_first_n:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "backend transient blip")
        child = f"child-{self._state.mint_count}"
        if self._register_on_mint:
            self._state.registered.add(child)
        return gateway_pb2.AssociateTaskResponse(task_id=child, parent_task_id=request.parent_task_id)


class _BackendUserProfile(user_profile_service_pb2_grpc.UserProfileServiceServicer):
    """Backend UserProfileService: authenticates the caller task like prod."""

    def __init__(self, state: _BackendState, *, deny: bool = False) -> None:
        self._state = state
        self._deny = deny

    async def CheckResourceAccess(self, request: Any, context: grpc.aio.ServicerContext) -> Any:
        md = dict(context.invocation_metadata() or ())
        task_id = str(md.get("x-task-id", ""))
        self._state.access_task_ids.append(task_id)
        self._state.access_setup_ids.append(request.resource_id)
        if task_id not in self._state.registered:
            # Exactly the prod backend behavior that exposed the bug.
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid or inactive task")
        return user_profile_pb2.CheckResourceAccessResponse(allowed=not self._deny)


@pytest.fixture
async def start_backend() -> AsyncIterator[Any]:
    """Factory starting a backend server (Gateway + UserProfile services); auto-stopped.

    Yields:
        Async factory ``(gateway, user_profile) -> port``.
    """
    servers: list[grpc.aio.Server] = []

    async def _start(gateway: _BackendGateway, user_profile: _BackendUserProfile) -> int:
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(gateway, server)
        user_profile_service_pb2_grpc.add_UserProfileServiceServicer_to_server(user_profile, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers.append(server)
        return port

    yield _start
    for s in servers:
        await s.stop(grace=0.1)


# ---------------------------------------------------------------------------
# Real TARGET stack: GatewayServicer + ModuleServicer (real GrpcUserProfile)
# + real SingleJobManager + real ModuleRunner + real module trigger.
# ---------------------------------------------------------------------------


class _E2ERedis(_FakeRedisClient):
    """Functional fakeredis with the boot-time surface GatewayServicer.start needs."""

    url = "redis://fake-e2e"

    async def verify(self) -> bool:
        return True


class _TargetStack:
    """A real target module server: gateway + servicer + runner + module."""

    def __init__(self, backend_port: int, redis: Any | None = None) -> None:
        self.redis = redis if redis is not None else _E2ERedis()

        servicer = ModuleServicer.__new__(ModuleServicer)
        _E2EModule.discover()  # register builtin triggers (healthcheck_ping) like the real servicer
        servicer.module_class = _E2EModule
        servicer.job_manager = SingleJobManager(_E2EModule, ServicesMode.LOCAL, self.redis)
        servicer.user_profile = GrpcUserProfile("", "", "", _client("127.0.0.1", backend_port))
        setup_strategy = Mock()
        setup_data = Mock()
        setup_data.current_setup_version = Mock()
        setup_data.current_setup_version.id = "setup_versions:e2e"
        setup_data.current_setup_version.setup_id = SETUP_ID
        setup_data.current_setup_version.content = {}
        setup_strategy.get_setup = AsyncMock(return_value=setup_data)
        servicer.setup = setup_strategy
        servicer._setup_cache = {}
        servicer._setup_inflight = {}
        servicer._registry_cache = None
        servicer._tool_cache_by_setup = {}
        servicer._communication_cache = None
        self.servicer = servicer

        self.runner = ModuleRunner(redis_client=self.redis, servicer=servicer)
        self.gateway = GatewayServicer(
            redis_client=self.redis,
            client_config=_client("127.0.0.1", 1),
            module_runner=self.runner,
        )
        self._server: grpc.aio.Server | None = None
        self.port: int = 0

    async def start(self) -> None:
        self._server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(self.gateway, self._server)
        self.port = self._server.add_insecure_port("127.0.0.1:0")
        await self._server.start()
        await self.gateway.start()

    async def stop(self) -> None:
        await self.gateway.stop()
        await self.servicer.user_profile.close_channel()
        if self._server is not None:
            await self._server.stop(grace=0.1)


@pytest.fixture
async def start_target() -> AsyncIterator[Any]:
    """Factory building + starting a real target stack for a backend port; auto-stopped.

    Yields:
        Async factory ``(backend_port) -> _TargetStack``.
    """
    stacks: list[_TargetStack] = []

    async def _start(backend_port: int) -> _TargetStack:
        stack = _TargetStack(backend_port)
        await stack.start()
        stacks.append(stack)
        return stack

    yield _start
    for stack in stacks:
        await stack.stop()


@pytest.fixture
async def caller() -> AsyncIterator[tuple[GatewayServicer, int]]:
    """Real caller GatewayServicer handling the dial-back (in-memory output queue).

    Yields:
        ``(gateway_servicer, port)`` of the live caller.
    """
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock()
    fake_redis.xlen = AsyncMock(return_value=0)
    fake_redis.verify = AsyncMock(return_value=True)
    fake_redis.close = AsyncMock()
    gw = GatewayServicer(
        redis_client=fake_redis,
        client_config=_client("127.0.0.1", 1),
        module_runner=MagicMock(run=AsyncMock()),
    )
    server = grpc.aio.server()
    gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(gw, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    await gw.start()
    gw._m2m.effective_advertise_address = lambda: f"127.0.0.1:{port}"  # type: ignore[method-assign]
    try:
        yield gw, port
    finally:
        await gw.stop()
        await server.stop(grace=0.1)


async def _call_tool(
    caller_gw: GatewayServicer,
    target: _TargetStack,
    backend_port: int,
) -> list[Any]:
    """Run one real tool call end-to-end and return the yielded output Structs."""
    comm = GrpcCommunication(
        mission_id=MISSION_ID,
        setup_id=SETUP_ID,
        setup_version_id="setup_versions:e2e",
        client_config=_client("127.0.0.1", target.port),
        m2m_calls=caller_gw._m2m,
        gateway_backend_config=_client("127.0.0.1", backend_port),
    )
    outputs: list[Any] = []
    token = RequestContext.bind(task_id=PARENT_TASK_ID, setup_id=SETUP_ID, mission_id=MISSION_ID)
    try:
        outputs.extend([
            out
            async for out in comm.call_module(
                module_address="127.0.0.1",
                module_port=target.port,
                input_data={"root": {"protocol": "healthcheck_ping"}},
                setup_id=SETUP_ID,
                mission_id=MISSION_ID,
            )
        ])
    finally:
        RequestContext.reset(token)
        await comm.close()
    return outputs


def _protocols(outputs: list[Any]) -> list[str]:
    """Protocol per output — sentinels live under ``root``, utility outputs at top level."""
    result = []
    for o in outputs:
        root = o.fields.get("root")
        proto = root.struct_value.fields.get("protocol") if root is not None else o.fields.get("protocol")
        result.append(proto.string_value if proto is not None else "")
    return result


def _stream_errors(outputs: list[Any]) -> list[tuple[str, str]]:
    errors = []
    for o in outputs:
        root = o.fields.get("root")
        if root is None:
            continue
        fields = root.struct_value.fields
        proto = fields.get("protocol")
        if proto is not None and proto.string_value == "stream.error":
            code = fields.get("code")
            message = fields.get("message")
            errors.append((
                code.string_value if code is not None else "",
                message.string_value if message is not None else "",
            ))
    return errors


class TestM2MEndToEnd:
    """Real usage, every layer live: backend mint → target module run → output back."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    async def test_full_tool_call_child_authenticated_and_output_streamed(
        self,
        start_backend: Any,
        start_target: Any,
        caller: tuple[GatewayServicer, int],
        digitalkin_records: list[logging.LogRecord],
    ) -> None:
        """Happy path: backend-minted child passes CheckResourceAccess; module output returns."""
        state = _BackendState()
        backend_port = await start_backend(_BackendGateway(state), _BackendUserProfile(state))
        target = await start_target(backend_port)
        caller_gw, _ = caller

        outputs = await _call_tool(caller_gw, target, backend_port)

        # 1. The backend minted the child from the running parent, once, with an
        #    idempotency key and the tight ~5s deadline (clock skew tolerance;
        #    the point is distinguishing from the 30s default).
        assert state.mint_count == 1
        assert state.mint_parents == [PARENT_TASK_ID]
        assert state.mint_idempotency_keys[0]
        assert 0 < state.mint_time_remaining[0] <= 5.5

        # 2. The target authenticated to the backend AS THE CHILD (ambient x-task-id
        #    bound by module_runner) for the tool's setup — and was granted.
        assert state.access_task_ids == ["child-1"]
        assert state.access_setup_ids == [SETUP_ID]

        # 3. The real module ran: its healthcheck trigger output streamed back and
        #    the stream terminated cleanly (no errors). Dump structs on failure.
        protocols = _protocols(outputs)
        assert "healthcheck_ping" in protocols, [json_format.MessageToDict(o) for o in outputs]
        assert _stream_errors(outputs) == []

        # 4. The prod validation markers traced the whole chain.
        at2 = _marker_lines(digitalkin_records, "[VALIDATE AT2]")
        assert len(at2) == 1
        assert f"parent={PARENT_TASK_ID}" in at2[0]
        assert "child=child-1" in at2[0]
        ac1 = _marker_lines(digitalkin_records, "[VALIDATE AC1]")
        assert any("setup access granted" in line and SETUP_ID in line for line in ac1)

        # 5. Nothing leaked on the caller.
        assert not caller_gw._m2m.entries

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.regression
    async def test_unregistered_child_is_unauthenticated_prod_bug(
        self,
        start_backend: Any,
        start_target: Any,
        caller: tuple[GatewayServicer, int],
    ) -> None:
        """Regression: a child the backend does NOT know is rejected exactly like prod.

        This reproduces the original bug (SDK-minted ids unknown to the backend):
        CheckResourceAccess aborts UNAUTHENTICATED and the caller receives a fatal
        ``stream.error`` instead of tool output.
        """
        state = _BackendState()
        backend_port = await start_backend(_BackendGateway(state, register_on_mint=False), _BackendUserProfile(state))
        target = await start_target(backend_port)
        caller_gw, _ = caller

        outputs = await _call_tool(caller_gw, target, backend_port)

        assert state.access_task_ids == ["child-1"]  # target authenticated as the child…
        errors = _stream_errors(outputs)
        assert len(errors) == 1  # …and the backend rejected it, failing the task
        code, message = errors[0]
        assert code == "MODULE_RUNTIME_ERROR"
        assert "UNAUTHENTICATED" in message
        assert "Invalid or inactive task" in message
        assert "healthcheck_ping" not in _protocols(outputs)  # the module never produced output
        assert not caller_gw._m2m.entries

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    async def test_access_denied_child_stops_task_with_setup_access_denied(
        self,
        start_backend: Any,
        start_target: Any,
        caller: tuple[GatewayServicer, int],
        digitalkin_records: list[logging.LogRecord],
    ) -> None:
        """A registered child whose user lacks setup access → fatal SETUP_ACCESS_DENIED."""
        state = _BackendState()
        backend_port = await start_backend(_BackendGateway(state), _BackendUserProfile(state, deny=True))
        target = await start_target(backend_port)
        caller_gw, _ = caller

        outputs = await _call_tool(caller_gw, target, backend_port)

        errors = _stream_errors(outputs)
        assert len(errors) == 1
        code, message = errors[0]
        assert code == "SETUP_ACCESS_DENIED"
        assert SETUP_ID in message
        ac1 = _marker_lines(digitalkin_records, "[VALIDATE AC1]")
        assert any("setup access DENIED" in line and SETUP_ID in line for line in ac1)
        assert not caller_gw._m2m.entries

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.chaos
    async def test_transient_mint_failure_retries_with_same_idempotency_key(
        self,
        start_backend: Any,
        start_target: Any,
        caller: tuple[GatewayServicer, int],
    ) -> None:
        """A transient UNAVAILABLE on the mint is retried (same idempotency key) and the call completes."""
        state = _BackendState()
        backend_port = await start_backend(_BackendGateway(state, fail_first_n=1), _BackendUserProfile(state))
        target = await start_target(backend_port)
        caller_gw, _ = caller

        outputs = await _call_tool(caller_gw, target, backend_port)

        assert state.mint_count == 2  # first attempt UNAVAILABLE, retry succeeded
        assert state.mint_idempotency_keys[0] == state.mint_idempotency_keys[1]
        assert "healthcheck_ping" in _protocols(outputs)
        assert _stream_errors(outputs) == []
        assert not caller_gw._m2m.entries

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.chaos
    async def test_backend_outage_opens_breaker_and_fast_fails(
        self,
        start_target: Any,
        caller: tuple[GatewayServicer, int],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A dead backend exhausts retries, opens the mint breaker, then fast-fails without I/O."""
        from digitalkin.grpc_servers.exceptions import ServerError
        from digitalkin.models.settings.grpc_client import get_circuit_breaker_settings

        monkeypatch.setenv("DIGITALKIN_CB_FAIL_MAX", "1")
        get_circuit_breaker_settings.cache_clear()
        try:
            # A port nothing listens on → UNAVAILABLE after retries.
            dead_backend_port = 1
            target = await start_target(dead_backend_port)
            caller_gw, _ = caller

            with pytest.raises(ServerError):
                await _call_tool(caller_gw, target, dead_backend_port)

            breaker = CircuitBreaker.get_or_create("GatewayBackendService")
            assert breaker.state.name == "OPEN"

            t0 = time.perf_counter()
            with pytest.raises(ServerError, match=r"[Cc]ircuit"):
                await _call_tool(caller_gw, target, dead_backend_port)
            assert time.perf_counter() - t0 < 1.0  # fast-fail, no network wait

            assert not caller_gw._m2m.entries
        finally:
            get_circuit_breaker_settings.cache_clear()

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.concurrency
    async def test_two_concurrent_tool_calls_get_distinct_children(
        self,
        start_backend: Any,
        start_target: Any,
        caller: tuple[GatewayServicer, int],
    ) -> None:
        """Two concurrent calls each get their own backend-minted child and both complete."""
        state = _BackendState()
        backend_port = await start_backend(_BackendGateway(state), _BackendUserProfile(state))
        target = await start_target(backend_port)
        caller_gw, _ = caller

        results = await asyncio.gather(
            _call_tool(caller_gw, target, backend_port),
            _call_tool(caller_gw, target, backend_port),
        )

        assert state.mint_count == 2
        assert len(set(state.access_task_ids)) == 2  # distinct children authenticated
        for outputs in results:
            assert "healthcheck_ping" in _protocols(outputs)
            assert _stream_errors(outputs) == []
        assert not caller_gw._m2m.entries
