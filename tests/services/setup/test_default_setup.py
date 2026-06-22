"""Tests for the concrete create_service_setup on the strategy ABC."""

from digitalkin.services.setup.default_setup import DefaultSetup


class TestCreateServiceSetup:
    """create_service_setup delegates to create_setup with name + content only."""

    async def test_creates_service_setup(self) -> None:
        setup = await DefaultSetup().create_service_setup("Nikita", {"branding": True})
        assert setup.name == "Nikita"
        assert setup.current_setup_version.content == {"branding": True}
