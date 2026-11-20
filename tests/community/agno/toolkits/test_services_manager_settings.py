"""``services_manager`` limits: the ``DIGITALKIN_SERVICES_MANAGER_*`` settings."""

import pytest
from pydantic import ValidationError

from digitalkin.models.settings.services_manager import ServicesManagerSettings, get_services_manager_settings


class TestServicesManagerSettings:
    """Defaults, the prefixed names, and the positive bound."""

    def test_defaults(self) -> None:
        settings = ServicesManagerSettings()

        assert settings.structure_max_chars == 20000
        assert settings.load_page_chars == 20000
        assert settings.writer_concurrency == 4

    def test_reads_the_prefixed_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_STRUCTURE_MAX_CHARS", "300")
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "500")
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_WRITER_CONCURRENCY", "2")
        get_services_manager_settings.cache_clear()

        settings = get_services_manager_settings()

        assert (settings.structure_max_chars, settings.load_page_chars, settings.writer_concurrency) == (300, 500, 2)

    def test_refuses_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_LOAD_PAGE_CHARS", "0")

        with pytest.raises(ValidationError):
            ServicesManagerSettings()
