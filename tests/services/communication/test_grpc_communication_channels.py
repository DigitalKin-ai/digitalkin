"""GrpcCommunication channel refs: one per pooled target, one per dial."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.communication.grpc_communication import GrpcCommunication


def _comm() -> GrpcCommunication:
    config = ClientConfig(host="localhost", port=1, mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE)
    return GrpcCommunication("missions:m", "setups:s", "setup_versions:v", config)


@pytest.fixture
def channels():
    with patch("digitalkin.grpc_servers.utils.grpc_client_wrapper.grpc.aio.insecure_channel") as factory:
        factory.side_effect = lambda *_a, **_k: MagicMock(close=AsyncMock())
        yield factory


def _key(comm: GrpcCommunication, host: str, port: int) -> str:
    return GrpcClientWrapper.channel_cache_key(comm._channel_config(host, port))


async def test_pooled_target_is_acquired_once(channels: MagicMock) -> None:
    comm = _comm()
    key = _key(comm, "mod", 50051)
    for _ in range(3):
        comm._create_stub("mod", 50051)

    assert GrpcClientWrapper._ref_counts[key] == 1
    await comm.close_all_channels()
    assert key not in GrpcClientWrapper._ref_counts


async def test_each_dial_pairs_its_own_ref(channels: MagicMock) -> None:
    comm = _comm()
    key = _key(comm, "gw", 50052)
    comm._create_stub("gw", 50052)
    _, release_a = comm.dial_consumer_stream("gw:50052")
    _, release_b = comm.dial_consumer_stream("gw:50052")
    assert GrpcClientWrapper._ref_counts[key] == 3

    await release_a()
    await release_b()
    assert GrpcClientWrapper._ref_counts[key] == 1
    await comm.close_all_channels()
    assert key not in GrpcClientWrapper._ref_counts


async def test_evict_mid_dial_keeps_the_dial_channel_open(channels: MagicMock) -> None:
    comm = _comm()
    key = _key(comm, "gw", 50052)
    _, release_old = comm.dial_consumer_stream("gw:50052")
    old_channel = GrpcClientWrapper._channel_cache[key]

    await comm.evict_consumer_channel("gw:50052")
    _, release_new = comm.dial_consumer_stream("gw:50052")
    new_channel = GrpcClientWrapper._channel_cache[key]
    assert new_channel is not old_channel
    old_channel.close.assert_not_awaited()

    await release_old()
    old_channel.close.assert_awaited_once()
    assert GrpcClientWrapper._ref_counts[key] == 1
    await release_new()
    new_channel.close.assert_awaited_once()


async def test_pooled_channel_evicted_is_reacquired_and_both_released(channels: MagicMock) -> None:
    comm = _comm()
    key = _key(comm, "mod", 50051)
    comm._create_stub("mod", 50051)
    old_channel = GrpcClientWrapper._channel_cache[key]
    await GrpcClientWrapper.evict_cached_channel(key)

    comm._create_stub("mod", 50051)
    new_channel = GrpcClientWrapper._channel_cache[key]
    await comm.close_all_channels()

    old_channel.close.assert_awaited_once()
    new_channel.close.assert_awaited_once()
    assert not GrpcClientWrapper._evicted_refs
