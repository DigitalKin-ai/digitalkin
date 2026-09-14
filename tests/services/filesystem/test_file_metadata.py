"""Tests locking the canonical file format shared by every producer of a file."""

from pathlib import Path

import pytest
from agentic_mesh_protocol.filesystem.v1 import filesystem_pb2
from pydantic import BaseModel, ConfigDict

from digitalkin.models.services.filesystem import FileMetadata, FileType, FileUploadMetadata
from digitalkin.services.filesystem import DefaultFilesystem, GrpcFilesystem, UploadFileData
from digitalkin.services.filesystem.exceptions import FilesystemServiceError


class TestFileTypeCoercion:
    """`FileType` accepts every spelling in circulation, and rejects the rest."""

    @pytest.mark.parametrize("value", ["IMAGE", "image", "Image", "FILE_TYPE_IMAGE", "file_type_image", " image "])
    def test_every_spelling_lands_on_the_same_member(self, value: str) -> None:
        """Bare, prefixed and mixed-case names all resolve to one member."""
        assert FileType(value) is FileType.IMAGE

    def test_empty_is_unspecified(self) -> None:
        """An empty string means the type was never set."""
        assert FileType("") is FileType.UNSPECIFIED

    def test_unknown_raises(self) -> None:
        """An unrecognised type is an error, not a silent UNSPECIFIED."""
        with pytest.raises(ValueError, match="NOT_A_TYPE"):
            FileType("NOT_A_TYPE")

    def test_every_member_matches_the_proto_enum(self) -> None:
        """The SDK enum and the wire enum cannot drift apart."""
        assert {t.value for t in FileType} == set(filesystem_pb2.FileType.keys())


class TestFileTypeFromContentType:
    """MIME classification, previously duplicated in module code."""

    @pytest.mark.parametrize(
        ("content_type", "expected"),
        [
            ("application/pdf", FileType.DOCUMENT),
            ("application/pdf; charset=utf-8", FileType.DOCUMENT),
            ("text/plain", FileType.DOCUMENT),
            ("application/json", FileType.DOCUMENT),
            ("image/png", FileType.IMAGE),
            ("image/jpeg; charset=utf-8", FileType.IMAGE),
            ("text/html", FileType.DOCUMENT),
            ("application/gzip", FileType.ARCHIVE),
            ("application/weird", FileType.OTHER),
            ("audio/mpeg", FileType.AUDIO),
            ("video/mp4", FileType.VIDEO),
            ("application/zip", FileType.ARCHIVE),
            ("text/x-python", FileType.CODE),
            ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", FileType.DOCUMENT),
            ("application/octet-stream", FileType.OTHER),
        ],
    )
    def test_classification(self, content_type: str, expected: FileType) -> None:
        """A MIME type maps to exactly one category."""
        assert FileType.from_content_type(content_type) is expected


class TestCanonicalShape:
    """The serialised format is fixed, and records carry it unchanged."""

    def test_dumps_exactly_the_agreed_keys(self) -> None:
        """The JSON form is the contract the front-end file widget round-trips."""
        dumped = FileMetadata(
            id="files:01m1",
            name="rapport.pdf",
            type=FileType.DOCUMENT,
            content_type="application/pdf",
            size_bytes=21923,
            file_url="https://example.test/v1/files/files:01m1",
        ).model_dump(mode="json")
        assert dumped == {
            "id": "files:01m1",
            "name": "rapport.pdf",
            "type": "FILE_TYPE_DOCUMENT",
            "content_type": "application/pdf",
            "size_bytes": 21923,
            "file_url": "https://example.test/v1/files/files:01m1",
        }

    def test_accepts_the_pre_unification_field_names(self) -> None:
        """File-history rows written before the rename still validate."""
        parsed = FileMetadata.model_validate({"file_id": "files:9", "name": "a.txt", "file_type": "FILE_TYPE_IMAGE"})
        assert parsed.id == "files:9"
        assert parsed.type is FileType.IMAGE


class TestUploadMetadata:
    """The imposed metadata schema validates without inflating the payload."""

    def test_validating_adds_nothing_the_caller_did_not_set(self) -> None:
        """Unset defaults must not leak onto the wire."""
        assert FileUploadMetadata.model_validate({"key": "value"}).model_dump(exclude_unset=True) == {"key": "value"}

    def test_declared_fields_survive(self) -> None:
        """Reserved keys are typed, not dropped."""
        meta = FileUploadMetadata(sdk_created=True, filename="a.pdf")
        assert meta.model_dump(exclude_unset=True) == {"sdk_created": True, "filename": "a.pdf"}

    def test_a_model_may_be_passed_instead_of_a_dict(self) -> None:
        """`UploadFileData.metadata` accepts the model form too."""
        upload = UploadFileData(
            content=b"x",
            name="a.pdf",
            type=FileType.DOCUMENT,
            metadata=FileUploadMetadata(sdk_created=True),
        )
        assert upload.metadata == {"sdk_created": True, "source_url": None, "filename": None}


class TestRoundTrip:
    """A record read back must be usable as an upload, on both backends."""

    @pytest.mark.asyncio
    async def test_local_record_feeds_straight_back_into_an_upload(self) -> None:
        """The defect this replaces: a read record could not be re-uploaded."""
        filesystem = DefaultFilesystem("missions:m1", "setup:1", "setup_version:1")
        records, _uploaded, _failed = await filesystem.upload_files([
            UploadFileData(content=b"hello", name="a.txt", type=FileType.DOCUMENT, content_type="text/plain")
        ])
        fetched = await filesystem.get_file(records[0].id)

        again, uploaded, failed = await filesystem.upload_files([
            UploadFileData(
                content=b"hello again",
                name=fetched.name,
                type=fetched.type,
                content_type=fetched.content_type,
                replace_if_exists=True,
            )
        ])
        assert (uploaded, failed) == (1, 0)
        assert again[0].type is FileType.DOCUMENT

    def test_the_wire_conversion_is_symmetric(self) -> None:
        """Encoding then decoding a type returns the same member, for every member."""
        for file_type in FileType:
            proto = filesystem_pb2.File(
                file_id="files:1",
                context="missions:m1",
                name="a.txt",
                file_type=filesystem_pb2.FileType.Value(file_type.value),
                storage_uri="gs://b/a.txt",
                file_url="https://example.test/a.txt",
            )
            assert GrpcFilesystem._file_proto_to_data(proto).type is file_type

    @pytest.mark.asyncio
    async def test_local_and_remote_agree_on_the_value(self) -> None:
        """Local and gRPC backends used to return different strings for one type."""
        filesystem = DefaultFilesystem("missions:m1", "setup:1", "setup_version:1")
        records, _uploaded, _failed = await filesystem.upload_files([
            UploadFileData(content=b"x", name="a.png", type=FileType.IMAGE, content_type="image/png")
        ])
        remote = GrpcFilesystem._file_proto_to_data(
            filesystem_pb2.File(
                file_id="files:1",
                context="missions:m1",
                name="a.png",
                file_type=filesystem_pb2.FileType.FILE_TYPE_IMAGE,
                storage_uri="gs://b/a.png",
                file_url="https://example.test/a.png",
            )
        )
        assert records[0].type is remote.type


class TestRegisteredMetadataModel:
    """A module may impose its own metadata schema, as storage collections do."""

    @pytest.mark.asyncio
    async def test_a_registered_model_is_enforced(self) -> None:
        """Metadata that does not match the registered schema is refused."""

        class ReportMetadata(BaseModel):
            """Metadata every report upload must carry."""

            model_config = ConfigDict(extra="forbid")

            author: str

        filesystem = DefaultFilesystem("missions:m1", "setup:1", "setup_version:1")
        filesystem.config = {"metadata_model": ReportMetadata}

        records, uploaded, _failed = await filesystem.upload_files([
            UploadFileData(content=b"x", name="ok.txt", type=FileType.DOCUMENT, metadata={"author": "gmx"})
        ])
        assert uploaded == 1
        assert records[0].metadata == {"author": "gmx"}

        with pytest.raises(FilesystemServiceError, match="Invalid metadata for file 'bad.txt'"):
            await filesystem.upload_files([
                UploadFileData(content=b"x", name="bad.txt", type=FileType.DOCUMENT, metadata={"nope": 1})
            ])
        assert not (Path(filesystem.temp_root) / "missions:m1" / "bad.txt").exists()

    @pytest.mark.asyncio
    async def test_without_registration_the_sdk_schema_applies(self) -> None:
        """`FileUploadMetadata` allows extras, so unregistered modules keep working."""
        filesystem = DefaultFilesystem("missions:m1", "setup:1", "setup_version:1")
        records, uploaded, _failed = await filesystem.upload_files([
            UploadFileData(content=b"x", name="a.txt", type=FileType.DOCUMENT, metadata={"anything": True})
        ])
        assert uploaded == 1
        assert records[0].metadata == {"anything": True}
