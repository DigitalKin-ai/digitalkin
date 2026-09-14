"""Tests for FilesystemMixin: delegation, type inference, and degrade-on-failure paths.

Every method here catches broadly and returns a fallback rather than raising, so the
failure branch is as much the contract as the success one and is covered per method.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.mixins.filesystem_mixin import FilesystemMixin
from digitalkin.models.services.filesystem import FileMetadata, FileType
from digitalkin.models.services.services import Context
from digitalkin.models.services.storage import Visibility
from digitalkin.services.filesystem.filesystem_strategy import FileFilter, FilesystemRecord, UploadFileData


def _make_context() -> MagicMock:
    """Build a mock ModuleContext whose filesystem strategy is an AsyncMock."""
    ctx = MagicMock()
    ctx.session.current_ids.return_value = {"mission_id": "missions:m1"}
    ctx.filesystem = AsyncMock()
    return ctx


def _record(file_id: str = "files:1", **overrides: object) -> FilesystemRecord:
    """Build a realistic record as the service would return it."""
    fields = {
        "id": file_id,
        "context": "missions:m1",
        "name": "a.pdf",
        "type": FileType.DOCUMENT,
        "content_type": "application/pdf",
        "size_bytes": 12,
        "file_url": "https://example.test/a.pdf",
        "storage_uri": "gs://b/a.pdf",
        "status": "ACTIVE",
    }
    fields.update(overrides)
    return FilesystemRecord(**fields)  # type: ignore[arg-type]


class TestUploadFile:
    """`upload_file` wraps UploadFileData, infers the type, and degrades to None."""

    @pytest.mark.asyncio
    async def test_infers_type_from_content_type(self) -> None:
        """An omitted type is derived from the MIME type, not left UNSPECIFIED."""
        ctx = _make_context()
        ctx.filesystem.upload_files = AsyncMock(return_value=([_record()], 1, 0))

        await FilesystemMixin.upload_file(ctx, "a.pdf", b"x", "application/pdf")

        sent: UploadFileData = ctx.filesystem.upload_files.await_args.args[0][0]
        assert sent.type is FileType.DOCUMENT

    @pytest.mark.asyncio
    async def test_explicit_type_wins_over_inference(self) -> None:
        """A caller-supplied category is not second-guessed."""
        ctx = _make_context()
        ctx.filesystem.upload_files = AsyncMock(return_value=([_record()], 1, 0))

        await FilesystemMixin.upload_file(ctx, "a.bin", b"x", "application/pdf", type=FileType.ARCHIVE)

        assert ctx.filesystem.upload_files.await_args.args[0][0].type is FileType.ARCHIVE

    @pytest.mark.asyncio
    async def test_stamps_metadata_and_visibility(self) -> None:
        """sdk_created and the filename reach the imposed metadata schema."""
        ctx = _make_context()
        ctx.filesystem.upload_files = AsyncMock(return_value=([_record()], 1, 0))

        await FilesystemMixin.upload_file(
            ctx, "a.pdf", b"x", "application/pdf", visibility=Visibility.PUBLIC, sdk_created=True
        )

        sent: UploadFileData = ctx.filesystem.upload_files.await_args.args[0][0]
        assert sent.metadata == {"sdk_created": True, "source_url": None, "filename": "a.pdf"}
        assert sent.visibility is Visibility.PUBLIC
        assert sent.replace_if_exists is True

    @pytest.mark.asyncio
    async def test_returns_none_when_the_strategy_raises(self) -> None:
        """A raised error degrades to None rather than propagating."""
        ctx = _make_context()
        ctx.filesystem.upload_files = AsyncMock(side_effect=RuntimeError("boom"))

        assert await FilesystemMixin.upload_file(ctx, "a.pdf", b"x", "application/pdf") is None

    @pytest.mark.asyncio
    async def test_returns_none_when_the_service_reports_a_failure(self) -> None:
        """A refused upload is a distinct branch from a raised error."""
        ctx = _make_context()
        ctx.filesystem.upload_files = AsyncMock(return_value=([], 0, 1))

        assert await FilesystemMixin.upload_file(ctx, "a.pdf", b"x", "application/pdf") is None


class TestResolveFiles:
    """`resolve_files` backfills a bare-id submission from the stored record."""

    @pytest.mark.asyncio
    async def test_backfills_only_the_empty_fields(self) -> None:
        """A bare id gains every field; nothing already set is overwritten."""
        ctx = _make_context()
        ctx.filesystem.get_file = AsyncMock(return_value=_record(name="stored.pdf", size_bytes=99))

        resolved = await FilesystemMixin.resolve_files(
            ctx, [FileMetadata(id="files:1"), FileMetadata(id="files:2", name="kept.pdf")]
        )

        assert resolved[0].name == "stored.pdf"
        assert resolved[0].size_bytes == 99
        assert resolved[0].type is FileType.DOCUMENT
        assert resolved[1].name == "kept.pdf"

    @pytest.mark.asyncio
    async def test_forwards_the_file_context(self) -> None:
        """Setup-time uploads live in the setup scope, not the mission."""
        ctx = _make_context()
        ctx.filesystem.get_file = AsyncMock(return_value=_record())

        await FilesystemMixin.resolve_files(ctx, [FileMetadata(id="files:1")], Context.SETUP)

        assert ctx.filesystem.get_file.await_args.kwargs["context"] is Context.SETUP

    @pytest.mark.asyncio
    async def test_unresolvable_file_is_kept_as_submitted(self) -> None:
        """A file the service cannot return must not be dropped from the setup."""
        ctx = _make_context()
        ctx.filesystem.get_file = AsyncMock(side_effect=RuntimeError("NOT_FOUND"))

        resolved = await FilesystemMixin.resolve_files(ctx, [FileMetadata(id="files:1", name="as-sent.pdf")])

        assert len(resolved) == 1
        assert resolved[0].id == "files:1"
        assert resolved[0].name == "as-sent.pdf"


class TestDegradingReads:
    """`get_files` and `delete_file` swallow errors and return a neutral value."""

    @pytest.mark.asyncio
    async def test_get_files_returns_the_records(self) -> None:
        """The total count is dropped; callers get the list."""
        ctx = _make_context()
        ctx.filesystem.get_files = AsyncMock(return_value=([_record()], 1))

        assert len(await FilesystemMixin.get_files(ctx, FileFilter())) == 1

    @pytest.mark.asyncio
    async def test_get_files_returns_empty_on_failure(self) -> None:
        """A failed listing is an empty list, not an exception."""
        ctx = _make_context()
        ctx.filesystem.get_files = AsyncMock(side_effect=RuntimeError("boom"))

        assert await FilesystemMixin.get_files(ctx, FileFilter()) == []

    @pytest.mark.asyncio
    async def test_delete_file_reports_the_per_file_result(self) -> None:
        """The map is keyed by file id; a missing key is a failed delete."""
        ctx = _make_context()
        ctx.filesystem.delete_files = AsyncMock(return_value=({"files:1": True}, 1, 0))

        assert await FilesystemMixin.delete_file(ctx, "files:1") is True
        assert await FilesystemMixin.delete_file(ctx, "files:absent") is False

    @pytest.mark.asyncio
    async def test_delete_file_returns_false_on_failure(self) -> None:
        """A raised error degrades to False."""
        ctx = _make_context()
        ctx.filesystem.delete_files = AsyncMock(side_effect=RuntimeError("boom"))

        assert await FilesystemMixin.delete_file(ctx, "files:1") is False


class TestDelegates:
    """The thin wrappers must reach the strategy with the right arguments."""

    @pytest.mark.asyncio
    async def test_upload_files_passes_the_list_through(self) -> None:
        """No repackaging between the caller and the strategy."""
        ctx = _make_context()
        ctx.filesystem.upload_files = AsyncMock(return_value=([_record()], 1, 0))
        files = [UploadFileData(content=b"x", name="a.pdf", type=FileType.DOCUMENT)]

        await FilesystemMixin.upload_files(ctx, files)

        assert ctx.filesystem.upload_files.await_args.args[0] is files

    @pytest.mark.asyncio
    async def test_get_file_asks_for_content_by_default(self) -> None:
        """The mixin's default differs from the strategy's — pin it."""
        ctx = _make_context()
        ctx.filesystem.get_file = AsyncMock(return_value=_record())

        await FilesystemMixin.get_file(ctx, "files:1")

        assert ctx.filesystem.get_file.await_args.kwargs["include_content"] is True

    @pytest.mark.asyncio
    async def test_set_visibility_updates_only_visibility(self) -> None:
        """Visibility is write-once at upload; this is the only way to change it."""
        ctx = _make_context()
        ctx.filesystem.update_file = AsyncMock(return_value=_record())

        await FilesystemMixin.set_visibility(ctx, "files:1", Visibility.PRIVATE)

        assert ctx.filesystem.update_file.await_args.args[0] == "files:1"
        assert ctx.filesystem.update_file.await_args.kwargs == {"visibility": Visibility.PRIVATE}
