"""Tests for the shared knowledge-files setup field."""

import pytest
from pydantic import Field, ValidationError

from digitalkin.models.module import SetupModel, knowledge_files_input
from digitalkin.models.services.filesystem import FileType


class _JsonOnlySetup(SetupModel):
    """A module that ingests JSON only."""

    knowledge_files: knowledge_files_input(extensions=[".json", "jsonld"]) = Field(  # type: ignore[valid-type]
        default_factory=list,
        title="Knowledge Files",
    )


class _ImagesOnlySetup(SetupModel):
    """A module that ingests images only."""

    knowledge_files: knowledge_files_input(file_types=[FileType.IMAGE]) = Field(  # type: ignore[valid-type]
        default_factory=list,
        title="Knowledge Files",
    )


class _AnySetup(SetupModel):
    """A module that accepts anything."""

    knowledge_files: knowledge_files_input() = Field(default_factory=list)  # type: ignore[valid-type]


class TestSchema:
    """The declared formats reach the front-end widget."""

    def test_widget_options_carry_the_allowed_extensions(self) -> None:
        """`accept` is what the file widget filters on."""
        prop = _JsonOnlySetup.model_json_schema()["properties"]["knowledge_files"]
        assert prop["ui:widget"] == "file"
        assert prop["ui:options"] == {"multiple": True, "accept": [".json", ".jsonld"]}
        assert prop["config"] is True

    def test_no_accept_key_when_nothing_is_declared(self) -> None:
        """A module accepting anything must not advertise a filter."""
        prop = _AnySetup.model_json_schema()["properties"]["knowledge_files"]
        assert "accept" not in prop["ui:options"]

    def test_the_field_holds_the_canonical_format(self) -> None:
        """Entries are `FileMetadata`, not bare ids."""
        setup = _JsonOnlySetup(knowledge_files=[{"id": "files:1", "name": "a.json"}])
        assert setup.model_dump(mode="json")["knowledge_files"] == [
            {
                "id": "files:1",
                "name": "a.json",
                "type": "FILE_TYPE_UNSPECIFIED",
                "content_type": "",
                "size_bytes": 0,
                "file_url": "",
            }
        ]


class TestEnforcement:
    """The widget's filter is a hint; the model is the gate."""

    def test_accepted_extension_passes(self) -> None:
        """A declared extension is allowed, however it was spelled in the declaration."""
        assert len(_JsonOnlySetup(knowledge_files=[{"id": "files:1", "name": "a.jsonld"}]).knowledge_files) == 1

    def test_unaccepted_extension_is_refused(self) -> None:
        """An undeclared extension names both the file and what was expected."""
        with pytest.raises(ValidationError, match=r"'a\.pdf' is not an accepted format"):
            _JsonOnlySetup(knowledge_files=[{"id": "files:1", "name": "a.pdf"}])

    def test_extension_matching_ignores_case(self) -> None:
        """Uploads arrive with whatever casing the user's filesystem had."""
        assert len(_JsonOnlySetup(knowledge_files=[{"id": "files:1", "name": "A.JSON"}]).knowledge_files) == 1

    def test_unaccepted_category_is_refused(self) -> None:
        """Categories gate formats an extension list cannot enumerate."""
        with pytest.raises(ValidationError, match="is a DOCUMENT file"):
            _ImagesOnlySetup(knowledge_files=[{"id": "files:1", "name": "a.pdf", "type": "FILE_TYPE_DOCUMENT"}])

    def test_unknown_category_is_not_second_guessed(self) -> None:
        """A file whose type was never resolved is not rejected on that basis."""
        assert len(_ImagesOnlySetup(knowledge_files=[{"id": "files:1", "name": "a.pdf"}]).knowledge_files) == 1

    def test_anything_passes_when_nothing_is_declared(self) -> None:
        """Modules that accept everything keep working unchanged."""
        assert len(_AnySetup(knowledge_files=[{"id": "files:1", "name": "a.exe"}]).knowledge_files) == 1


class TestPartialSubmissions:
    """A setup form may submit little more than an id."""

    def test_a_nameless_entry_is_not_rejected(self) -> None:
        """The format cannot be judged before the service resolves the name."""
        assert len(_JsonOnlySetup(knowledge_files=[{"id": "files:1"}]).knowledge_files) == 1

    def test_a_name_without_an_extension_is_not_rejected(self) -> None:
        """An extensionless name says nothing about the format either."""
        assert len(_JsonOnlySetup(knowledge_files=[{"id": "files:1", "name": "README"}]).knowledge_files) == 1
