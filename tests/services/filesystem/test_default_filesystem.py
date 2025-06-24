"""Test the default filesystem implementation."""

from pathlib import Path

import pytest

from digitalkin.services.filesystem import DefaultFilesystem
from digitalkin.services.filesystem.filesystem_strategy import (
    FileFilter,
    FilesystemRecord,
    FilesystemServiceError,
    UploadFileData,
)


@pytest.fixture
def filesystem() -> DefaultFilesystem:
    """Create a DefaultFilesystem instance for testing with isolated temp_root."""
    return DefaultFilesystem("test_mission", "test_setup")


@pytest.fixture
def sample_file_data() -> bytes:
    """Generate sample file data for testing.

    Returns:
        bytes: Sample file data
    """
    return b"This is sample file content for testing."


@pytest.fixture
def file_metadata() -> dict:
    """Generate file metadata for testing.

    Returns:
        dict: File metadata with context, name, file_type, and url
    """
    return {
        "context": "test_mission",
        "name": "test_file.txt",
        "file_type": "DOCUMENT",
        "content_type": "text/plain",
        "metadata": {"key": "value"},
        "status": "ACTIVE",
    }


class TestDefaultFilesystem:
    """Test the DefaultFilesystem class."""

    def test_init(self) -> None:
        """Test initialization of DefaultFilesystem."""
        filesystem = DefaultFilesystem("test_mission", "test_setup")
        assert filesystem.temp_root
        assert filesystem.mission_id == "test_mission"

    def test_upload_files_success(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test successful file upload.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # Create upload file data
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            status=file_metadata["status"],
            replace_if_exists=False,
        )

        # Upload the file
        files, total_uploaded, total_failed = filesystem.upload_files([upload_file])
        assert len(files) == 1
        assert total_uploaded == 1
        assert total_failed == 0

        # Verify the file data
        file_data = files[0]
        assert isinstance(file_data, FilesystemRecord)
        assert file_data.context == file_metadata["context"]
        assert file_data.name == file_metadata["name"]
        assert file_data.file_type == file_metadata["file_type"]
        assert file_data.content_type == file_metadata["content_type"]
        assert file_data.metadata == file_metadata["metadata"]
        assert file_data.status == file_metadata["status"]
        assert file_data.storage_url is not None

        # Verify the file exists on disk
        file_path = Path(filesystem._get_context_temp_dir(file_metadata["context"]), file_metadata["name"])
        assert file_path.exists()
        assert file_path.read_bytes() == sample_file_data

    def test_get_file_success(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test successful file retrieval.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # First upload a file
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            status=file_metadata["status"],
            replace_if_exists=False,
        )
        files, _, _ = filesystem.upload_files([upload_file])
        file_id = files[0].id

        # Get the file
        file_data = filesystem.get_file(file_id)
        assert isinstance(file_data, FilesystemRecord)
        assert file_data.id == file_id
        assert file_data.context == file_metadata["context"]
        assert file_data.name == file_metadata["name"]
        assert file_data.file_type == file_metadata["file_type"]
        assert file_data.content_type == file_metadata["content_type"]
        assert file_data.metadata == file_metadata["metadata"]
        assert file_data.status == file_metadata["status"]
        assert file_data.storage_url is not None

    def test_get_files_success(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test successful retrieval of multiple files.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # Upload multiple files
        file_names = [f"{file_metadata['name']}_{i}" for i in range(3)]
        upload_files = [
            UploadFileData(
                content=sample_file_data,
                name=name,
                file_type=file_metadata["file_type"],
                content_type=file_metadata["content_type"],
                metadata=file_metadata["metadata"],
                status=file_metadata["status"],
                replace_if_exists=False,
            )
            for name in file_names
        ]

        _files, _, _ = filesystem.upload_files(upload_files)

        # Create filter criteria
        filters = FileFilter(
            context=file_metadata["context"],
            file_types=[file_metadata["file_type"]],
            status=file_metadata["status"],
        )

        # Get the files
        result_files, total_count = filesystem.get_files(
            filters,
            list_size=10,
            offset=0,
            order="created_at:desc",
            include_content=False,
        )

        assert len(result_files) == 3
        assert total_count == 3

        for file_data in result_files:
            assert isinstance(file_data, FilesystemRecord)
            assert file_data.context == file_metadata["context"]
            assert file_data.name in file_names
            assert file_data.file_type == file_metadata["file_type"]
            assert file_data.content_type == file_metadata["content_type"]
            assert file_data.metadata == file_metadata["metadata"]
            assert file_data.status == file_metadata["status"]
            assert file_data.storage_url is not None

    def test_update_file_success(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test successful file update.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # First upload a file
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            status=file_metadata["status"],
            replace_if_exists=False,
        )
        files, _, _ = filesystem.upload_files([upload_file])
        file_id = files[0].id

        # Update the file
        updated_content = b"Updated content"
        updated_file = filesystem.update_file(
            file_id,
            content=updated_content,
            file_type="DOCUMENT",
            content_type="text/plain",
            metadata={"new_key": "new_value"},
            new_name="updated_file.txt",
            status="ACTIVE",
        )

        assert isinstance(updated_file, FilesystemRecord)
        assert updated_file.id == file_id
        assert updated_file.context == file_metadata["context"]
        assert updated_file.name == "updated_file.txt"
        assert updated_file.file_type == "DOCUMENT"
        assert updated_file.content_type == "text/plain"
        assert updated_file.metadata == {"new_key": "new_value"}
        assert updated_file.status == "ACTIVE"
        assert updated_file.storage_url is not None

        # Verify the file content was updated
        file_path = Path(filesystem._get_context_temp_dir(file_metadata["context"]), "updated_file.txt")
        assert file_path.exists()
        assert file_path.read_bytes() == updated_content

    def test_delete_files_success(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test successful file deletion.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # Upload multiple files
        file_names = [f"{file_metadata['name']}_{i}" for i in range(3)]
        upload_files = [
            UploadFileData(
                content=sample_file_data,
                name=name,
                file_type=file_metadata["file_type"],
                content_type=file_metadata["content_type"],
                metadata=file_metadata["metadata"],
                status=file_metadata["status"],
                replace_if_exists=False,
            )
            for name in file_names
        ]

        files, _, _ = filesystem.upload_files(upload_files)
        file_ids = [file_data.id for file_data in files]

        # Create filter criteria
        filters = FileFilter(
            context=file_metadata["context"],
            file_types=[file_metadata["file_type"]],
            status=file_metadata["status"],
        )

        # Delete the files
        results, total_deleted, total_failed = filesystem.delete_files(
            filters,
            permanent=True,
            force=False,
        )

        assert len(results) == 3
        assert total_deleted == 3
        assert total_failed == 0

        for file_id in file_ids:
            assert results[file_id] is True

        # Verify the files are deleted
        for name in file_names:
            file_path = Path(filesystem._get_context_temp_dir(file_metadata["context"]), name)
            assert not file_path.exists()

    def test_get_file_nonexistent(self, filesystem: DefaultFilesystem) -> None:
        """Test getting a non-existent file.

        Args:
            filesystem: DefaultFilesystem instance
        """
        with pytest.raises(FilesystemServiceError):
            filesystem.get_file("nonexistent_file_id")

    def test_update_file_nonexistent(self, filesystem: DefaultFilesystem, sample_file_data: bytes) -> None:
        """Test updating a non-existent file.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
        """
        with pytest.raises(FilesystemServiceError):
            filesystem.update_file(
                "nonexistent_file_id",
                content=sample_file_data,
                file_type="DOCUMENT",
                content_type="text/plain",
                metadata={"key": "value"},
                new_name="updated_file.txt",
                status="ACTIVE",
            )

    def test_delete_files_nonexistent(self, filesystem: DefaultFilesystem) -> None:
        """Test deleting non-existent files.

        Args:
            filesystem: DefaultFilesystem instance
        """
        # Create filter criteria for non-existent files
        filters = FileFilter(
            context="nonexistent_context",
            file_types=["DOCUMENT"],
            status="ACTIVE",
        )

        # Attempt to delete the files
        results, total_deleted, total_failed = filesystem.delete_files(
            filters,
            permanent=True,
            force=False,
        )

        assert len(results) == 0
        assert total_deleted == 0
        assert total_failed == 0

    def test_upload_files_duplicate_error(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test that uploading a duplicate file raises an error when replace_if_exists is False.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # First upload a file
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            replace_if_exists=False,
        )
        filesystem.upload_files([upload_file])

        # Try to upload the same file again
        with pytest.raises(FilesystemServiceError):
            filesystem.upload_files([upload_file])

    def test_upload_files_replace_existing(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test that uploading a duplicate file succeeds when replace_if_exists is True.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # First upload a file
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            replace_if_exists=False,
        )
        filesystem.upload_files([upload_file])

        # Upload the same file with replace_if_exists=True
        new_content = b"New content"
        upload_file_replace = UploadFileData(
            content=new_content,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            replace_if_exists=True,
        )
        files, total_uploaded, total_failed = filesystem.upload_files([upload_file_replace])
        assert len(files) == 1
        assert total_uploaded == 1
        assert total_failed == 0

        # Verify the file content was updated
        file_path = Path(filesystem._get_context_temp_dir(file_metadata["context"]), file_metadata["name"])
        assert file_path.exists()
        assert file_path.read_bytes() == new_content

    def test_get_files_with_filters(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test getting files with various filter combinations.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # Upload multiple files with different types
        files_to_upload = [
            UploadFileData(
                content=sample_file_data,
                name="file1.txt",
                file_type="DOCUMENT",
                content_type="text/plain",
                metadata={"key": "value1"},
                replace_if_exists=False,
            ),
            UploadFileData(
                content=sample_file_data,
                name="file2.txt",
                file_type="IMAGE",
                content_type="image/png",
                metadata={"key": "value2"},
                replace_if_exists=False,
            ),
            UploadFileData(
                content=sample_file_data,
                name="file3.txt",
                file_type="DOCUMENT",
                content_type="text/plain",
                metadata={"key": "value3"},
                replace_if_exists=False,
            ),
        ]
        files, _, _ = filesystem.upload_files(files_to_upload)

        # Update one file to ARCHIVED status
        filesystem.update_file(files[1].id, status="ARCHIVED")

        # Test filtering by type
        filters = FileFilter(
            context=file_metadata["context"],
            file_types=["DOCUMENT"],
        )
        result_files, total_count = filesystem.get_files(filters)
        assert len(result_files) == 2
        assert total_count == 2
        assert all(f.file_type == "DOCUMENT" for f in result_files)

        # Test filtering by status
        filters = FileFilter(
            context=file_metadata["context"],
            status="ARCHIVED",
        )
        result_files, total_count = filesystem.get_files(filters)
        assert len(result_files) == 1
        assert total_count == 1
        assert result_files[0].status == "ARCHIVED"

        # Test filtering by content type
        filters = FileFilter(
            context=file_metadata["context"],
            content_type="image/png",
        )
        result_files, total_count = filesystem.get_files(filters)
        assert len(result_files) == 1
        assert total_count == 1
        assert result_files[0].content_type == "image/png"

        # Test filtering by name prefix
        filters = FileFilter(
            context=file_metadata["context"],
            prefix="file1",
        )
        result_files, total_count = filesystem.get_files(filters)
        assert len(result_files) == 1
        assert total_count == 1
        assert result_files[0].name == "file1.txt"

    def test_get_files_pagination(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test getting files with pagination.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # Upload multiple files
        files_to_upload = [
            UploadFileData(
                content=sample_file_data,
                name=f"file{i}.txt",
                file_type=file_metadata["file_type"],
                content_type=file_metadata["content_type"],
                metadata=file_metadata["metadata"],
                status=file_metadata["status"],
                replace_if_exists=False,
            )
            for i in range(5)
        ]
        filesystem.upload_files(files_to_upload)

        # Test pagination with list_size=2
        filters = FileFilter(context=file_metadata["context"])

        # First page
        result_files, total_count = filesystem.get_files(filters, list_size=2, offset=0)
        assert len(result_files) == 2
        assert total_count == 5

        # Second page
        result_files, total_count = filesystem.get_files(filters, list_size=2, offset=2)
        assert len(result_files) == 2
        assert total_count == 5

        # Last page
        result_files, total_count = filesystem.get_files(filters, list_size=2, offset=4)
        assert len(result_files) == 1
        assert total_count == 5

    def test_delete_files_soft_delete(
        self, filesystem: DefaultFilesystem, sample_file_data: bytes, file_metadata: dict
    ) -> None:
        """Test soft deletion of files.

        Args:
            filesystem: DefaultFilesystem instance
            sample_file_data: Sample file data
            file_metadata: File metadata
        """
        # Upload a file
        upload_file = UploadFileData(
            content=sample_file_data,
            name=file_metadata["name"],
            file_type=file_metadata["file_type"],
            content_type=file_metadata["content_type"],
            metadata=file_metadata["metadata"],
            status=file_metadata["status"],
            replace_if_exists=False,
        )
        files, _, _ = filesystem.upload_files([upload_file])
        file_id = files[0].id

        # Soft delete the file
        filters = FileFilter(
            context=file_metadata["context"],
            file_ids=[file_id],
        )
        results, total_deleted, total_failed = filesystem.delete_files(filters, permanent=False)
        assert len(results) == 1
        assert total_deleted == 1
        assert total_failed == 0
        assert results[file_id] is True

        # Verify the file still exists but is marked as deleted
        file_data = filesystem.get_file(file_id)
        assert file_data.status == "DELETED"
        file_path = Path(filesystem._get_context_temp_dir(file_metadata["context"]), file_metadata["name"])
        assert file_path.exists()
