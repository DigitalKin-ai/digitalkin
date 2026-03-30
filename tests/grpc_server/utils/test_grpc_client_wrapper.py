"""Tests for GrpcClientWrapper channel caching and ref counting."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.models.grpc_servers.models import ClientConfig, GrpcCompression
from digitalkin.models.settings.utils.channel import CommunicationMode, SecurityMode


@pytest.fixture(autouse=True)
def _clear_channel_cache():
    """Ensure channel cache is clean before and after each test."""
    GrpcClientWrapper._channel_cache.clear()
    GrpcClientWrapper._ref_counts.clear()
    yield
    GrpcClientWrapper._channel_cache.clear()
    GrpcClientWrapper._ref_counts.clear()


def _make_config(host: str = "localhost", port: int = 50051) -> ClientConfig:
    """Create a minimal insecure ClientConfig for testing."""
    return ClientConfig(
        host=host,
        port=port,
        mode=CommunicationMode.ASYNC,
        security=SecurityMode.INSECURE,
        compression=GrpcCompression.GZIP,
    )


@pytest.mark.grpc
class TestChannelCache:
    """Tests for class-level channel caching."""

    @patch("digitalkin.grpc_servers.utils.grpc_client_wrapper.grpc.aio.insecure_channel")
    def test_same_config_reuses_channel(self, mock_insecure_channel: MagicMock) -> None:
        """Two wrappers with the same config share one channel."""
        fake_channel = MagicMock()
        mock_insecure_channel.return_value = fake_channel

        config = _make_config()
        wrapper_a = GrpcClientWrapper()
        wrapper_b = GrpcClientWrapper()

        ch_a = wrapper_a._init_channel(config)
        ch_b = wrapper_b._init_channel(config)

        if ch_a is not ch_b:
            pytest.fail("Expected same channel object for identical configs")

        mock_insecure_channel.assert_called_once()

        cache_key = f"{config.address}:{config.security.value}:{config.compression.value}"
        if GrpcClientWrapper._ref_counts[cache_key] != 2:
            pytest.fail(f"Expected ref_count=2, got {GrpcClientWrapper._ref_counts[cache_key]}")

    @patch("digitalkin.grpc_servers.utils.grpc_client_wrapper.grpc.aio.insecure_channel")
    def test_different_addresses_get_different_channels(self, mock_insecure_channel: MagicMock) -> None:
        """Different addresses should create separate channels."""
        channel_a = MagicMock()
        channel_b = MagicMock()
        mock_insecure_channel.side_effect = [channel_a, channel_b]

        config_a = _make_config(host="host-a", port=50051)
        config_b = _make_config(host="host-b", port=50052)

        wrapper_a = GrpcClientWrapper()
        wrapper_b = GrpcClientWrapper()

        ch_a = wrapper_a._init_channel(config_a)
        ch_b = wrapper_b._init_channel(config_b)

        if ch_a is ch_b:
            pytest.fail("Expected different channel objects for different addresses")

        if mock_insecure_channel.call_count != 2:
            pytest.fail(f"Expected 2 channel creations, got {mock_insecure_channel.call_count}")


@pytest.mark.grpc
class TestRefCounting:
    """Tests for ref-counted channel lifecycle."""

    @patch("digitalkin.grpc_servers.utils.grpc_client_wrapper.grpc.aio.insecure_channel")
    @pytest.mark.asyncio
    async def test_close_one_user_keeps_channel_alive(self, mock_insecure_channel: MagicMock) -> None:
        """Closing one wrapper doesn't close the shared channel if another holds a ref."""
        fake_channel = AsyncMock()
        mock_insecure_channel.return_value = fake_channel

        config = _make_config()
        wrapper_a = GrpcClientWrapper()
        wrapper_b = GrpcClientWrapper()
        wrapper_a._init_channel(config)
        wrapper_b._init_channel(config)

        await wrapper_a.close_channel()

        fake_channel.close.assert_not_called()

        cache_key = f"{config.address}:{config.security.value}:{config.compression.value}"
        if cache_key not in GrpcClientWrapper._channel_cache:
            pytest.fail("Channel should still be in cache with one remaining ref")
        if GrpcClientWrapper._ref_counts[cache_key] != 1:
            pytest.fail(f"Expected ref_count=1, got {GrpcClientWrapper._ref_counts[cache_key]}")

    @patch("digitalkin.grpc_servers.utils.grpc_client_wrapper.grpc.aio.insecure_channel")
    @pytest.mark.asyncio
    async def test_close_last_user_closes_channel(self, mock_insecure_channel: MagicMock) -> None:
        """Closing the last wrapper ref closes and removes the channel."""
        fake_channel = AsyncMock()
        mock_insecure_channel.return_value = fake_channel

        config = _make_config()
        wrapper_a = GrpcClientWrapper()
        wrapper_b = GrpcClientWrapper()
        wrapper_a._init_channel(config)
        wrapper_b._init_channel(config)

        await wrapper_a.close_channel()
        await wrapper_b.close_channel()

        fake_channel.close.assert_awaited_once()

        cache_key = f"{config.address}:{config.security.value}:{config.compression.value}"
        if cache_key in GrpcClientWrapper._channel_cache:
            pytest.fail("Channel should be removed from cache after last ref closed")
        if cache_key in GrpcClientWrapper._ref_counts:
            pytest.fail("Ref count entry should be removed after last ref closed")

    @patch("digitalkin.grpc_servers.utils.grpc_client_wrapper.grpc.aio.insecure_channel")
    @pytest.mark.asyncio
    async def test_close_channel_idempotent(self, mock_insecure_channel: MagicMock) -> None:
        """Calling close_channel twice on the same wrapper is safe."""
        fake_channel = AsyncMock()
        mock_insecure_channel.return_value = fake_channel

        config = _make_config()
        wrapper = GrpcClientWrapper()
        wrapper._init_channel(config)

        await wrapper.close_channel()
        await wrapper.close_channel()  # Should not raise

        fake_channel.close.assert_awaited_once()


@pytest.mark.grpc
class TestCloseAllCachedChannels:
    """Tests for the close_all_cached_channels classmethod."""

    @patch("digitalkin.grpc_servers.utils.grpc_client_wrapper.grpc.aio.insecure_channel")
    @pytest.mark.asyncio
    async def test_close_all_clears_everything(self, mock_insecure_channel: MagicMock) -> None:
        """close_all_cached_channels closes all channels and resets state."""
        channel_a = AsyncMock()
        channel_b = AsyncMock()
        mock_insecure_channel.side_effect = [channel_a, channel_b]

        wrapper_a = GrpcClientWrapper()
        wrapper_b = GrpcClientWrapper()
        wrapper_a._init_channel(_make_config(host="host-a"))
        wrapper_b._init_channel(_make_config(host="host-b"))

        await GrpcClientWrapper.close_all_cached_channels()

        channel_a.close.assert_awaited_once()
        channel_b.close.assert_awaited_once()

        if GrpcClientWrapper._channel_cache:
            pytest.fail("Channel cache should be empty after close_all")
        if GrpcClientWrapper._ref_counts:
            pytest.fail("Ref counts should be empty after close_all")
