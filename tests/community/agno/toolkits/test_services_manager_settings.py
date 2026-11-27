"""``services_manager`` limits: the ``DIGITALKIN_SERVICES_MANAGER_*`` settings."""

from collections.abc import Iterator

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

    def test_the_text_overlap_defaults_to_500_and_reads_its_variable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        assert ServicesManagerSettings().text_overlap_chars == 500
        monkeypatch.setenv("DIGITALKIN_SERVICES_MANAGER_TEXT_OVERLAP_CHARS", "0")

        assert ServicesManagerSettings().text_overlap_chars == 0, "no overlap is a valid choice"

    @pytest.mark.parametrize(
        ("name", "value"),
        [
            ("STRUCTURE_MAX_CHARS", "0"),
            ("STRUCTURE_MAX_CHARS", "-1"),
            ("LOAD_PAGE_CHARS", "-20000"),
            ("WRITER_CONCURRENCY", "0"),
            ("WRITER_CONCURRENCY", "-4"),
            ("TEXT_OVERLAP_CHARS", "-1"),
            ("LOAD_PAGE_CHARS", "twenty"),
            ("WRITER_CONCURRENCY", "2.5"),
        ],
    )
    def test_refuses_a_value_that_would_break_the_reads_or_writes(
        self, monkeypatch: pytest.MonkeyPatch, name: str, value: str
    ) -> None:
        """A zero page loops forever, a zero writer limit deadlocks generate: refused at startup."""
        monkeypatch.setenv(f"DIGITALKIN_SERVICES_MANAGER_{name}", value)

        with pytest.raises(ValidationError):
            ServicesManagerSettings()


@pytest.fixture(autouse=True)
def _fresh_settings() -> Iterator[None]:
    """Keep a value a test set out of the cached settings the other tests read."""
    get_services_manager_settings.cache_clear()
    yield
    get_services_manager_settings.cache_clear()
