import contextlib
import os
from collections import UserDict
from unittest.mock import MagicMock, patch

import pytest

from digitalkin.services.filesystem import DefaultFilesystem
from digitalkin.services.filesystem.filesystem_strategy import (
    FilesystemData,
    FilesystemServiceError,
    FileType,
)


@pytest.fixture
def test_dir(tmp_path):
    """Create a temporary directory for testing."""
    test_dir = tmp_path / "test_filesystem"
    test_dir.mkdir()
    return str(test_dir)


@pytest.fixture
def default_fs(test_dir):
    """Create a DefaultFilesystem instance for testing."""
    config = {"temp_root": test_dir}
    mission_id = "test_mission:123"
    setup_version_id = "setup_version:1"
    fs = DefaultFilesystem(mission_id, setup_version_id, config)
    yield fs
    # Clean up any test files
    for file_data in fs.get_all():
        with contextlib.suppress(FileNotFoundError, OSError):
            os.remove(file_data.url)


@pytest.fixture
def sample_file() -> bytes:
    """Create a sample file content for testing."""
    return b"Sample file content for testing"


@pytest.fixture
def uploaded_file(default_fs, sample_file):
    """Create and upload a sample file."""
    file_name = "test_file.txt"
    file_data = default_fs.upload(content=sample_file, name=file_name, file_type=FileType.DOCUMENT)
    default_fs.db[file_name] = file_data
    return file_data


class TestDefaultFilesystem:
    """Test suite for DefaultFilesystem class."""

    def test_init(self, test_dir) -> None:
        """Test initialization of DefaultFilesystem."""
        config = {"temp_root": test_dir}
        mission_id = "test_mission:123"
        setup_version_id = "setup_version:1"
        fs = DefaultFilesystem(mission_id, setup_version_id, config)

        assert fs.mission_id == mission_id
        assert fs.temp_root == test_dir
        assert fs.db == {}
        assert os.path.isdir(test_dir)

    def test_init_with_default_temp_dir(self) -> None:
        """Test initialization with default temp directory."""
        import tempfile

        mission_id = "test_mission:456"
        setup_version_id = "setup_version:1"
        fs = DefaultFilesystem(mission_id, setup_version_id, {})

        assert fs.mission_id == mission_id
        assert fs.temp_root == tempfile.gettempdir()
        assert fs.db == {}

    def test_get_kin_context_temp_dir(self, default_fs, test_dir) -> None:
        """Test _get_kin_context_temp_dir method."""
        kin_context = "test:context"
        expected_dir = os.path.join(test_dir, "test_context")

        result = default_fs._get_kin_context_temp_dir(kin_context)

        assert result == expected_dir
        assert os.path.isdir(expected_dir)

    def test_upload_success(self, default_fs, sample_file) -> None:
        """Test successful file upload."""
        file_name = "new_file.txt"
        file_type = FileType.DOCUMENT

        result = default_fs.upload(sample_file, file_name, file_type)

        assert isinstance(result, FilesystemData)
        assert result.name == file_name
        assert result.file_type == file_type
        assert result.kin_context == default_fs.mission_id
        assert os.path.exists(result.url)

        # Verify file content
        with open(result.url, "rb") as f:
            assert f.read() == sample_file

    def test_upload_file_exists(self, default_fs, uploaded_file) -> None:
        """Test upload when file already exists."""
        with pytest.raises(FileExistsError):
            default_fs.upload(b"New content", uploaded_file.name, FileType.DOCUMENT)

    @patch("pathlib.Path.write_bytes")
    def test_upload_error(self, mock_write, default_fs, sample_file) -> None:
        """Test upload with error."""
        mock_write.side_effect = Exception("Test error")

        with pytest.raises(FilesystemServiceError):
            default_fs.upload(sample_file, "error_file.txt", FileType.DOCUMENT)

    def test_get_success(self, default_fs, uploaded_file) -> None:
        """Test successful file retrieval."""
        result = default_fs.get(uploaded_file.name)

        assert result == uploaded_file

    def test_get_not_found(self, default_fs) -> None:
        """Test get with non-existent file."""
        with pytest.raises(FileNotFoundError):
            default_fs.get("nonexistent_file.txt")

    def test_get_error(self, default_fs) -> None:
        """Test get with unexpected error."""
        with patch.object(default_fs, "db", new=MagicMock()) as mock_db:
            mock_db.__getitem__.side_effect = Exception("Test error")

            with pytest.raises(FilesystemServiceError):
                default_fs.get("error_file.txt")

    def test_update_success(self, default_fs, uploaded_file) -> None:
        """Test successful file update."""
        new_content = b"Updated content"

        result = default_fs.update(uploaded_file.name, new_content, FileType.DOCUMENT)

        assert result.name == uploaded_file.name
        assert result.file_type == FileType.DOCUMENT

        # Verify file content
        with open(result.url, "rb") as f:
            assert f.read() == new_content

    def test_update_not_found(self, default_fs) -> None:
        """Test update with non-existent file."""
        with pytest.raises(FileNotFoundError):
            default_fs.update("nonexistent_file.txt", b"Content", FileType.DOCUMENT)

    @patch("pathlib.Path.write_bytes")
    def test_update_error(self, mock_write, default_fs, uploaded_file) -> None:
        """Test update with error."""
        mock_write.side_effect = Exception("Test error")

        with pytest.raises(FilesystemServiceError):
            default_fs.update(uploaded_file.name, b"New content", FileType.DOCUMENT)

    def test_delete_success(self, default_fs, uploaded_file) -> None:
        """Test successful file deletion."""
        result = default_fs.delete(uploaded_file.name)

        assert result == 1
        assert uploaded_file.name not in default_fs.db
        assert not os.path.exists(uploaded_file.url)

    def test_delete_not_found_in_db(self, default_fs) -> None:
        """Test delete with file not in database."""
        with pytest.raises(FileNotFoundError):
            default_fs.delete("nonexistent_file.txt")

    def test_delete_not_found_in_filesystem(self, default_fs, uploaded_file) -> None:
        """Test delete with file in db but not in filesystem."""
        # Remove the actual file but keep the db entry
        os.remove(uploaded_file.url)

        with pytest.raises(FilesystemServiceError) as excinfo:
            default_fs.delete(uploaded_file.name)

        assert "exists in database but not in filesystem" in str(excinfo.value)

    def test_delete_os_error(self, default_fs, uploaded_file) -> None:
        """Test delete with OSError during file removal."""
        with patch("os.remove") as mock_remove:
            mock_remove.side_effect = OSError("Permission denied")

            with pytest.raises(FilesystemServiceError) as excinfo:
                default_fs.delete(uploaded_file.name)

            assert "Error deleting file" in str(excinfo.value)

    def test_delete_unexpected_error(self, default_fs, uploaded_file) -> None:
        """Test delete with unexpected error."""

        # Create a custom dict-like object that will raise an exception when __delitem__ is called
        class ExceptionDict(UserDict):
            def __delitem__(self, key) -> None:
                msg = "Unexpected error"
                raise Exception(msg)

        # Replace the db with our custom dict containing the same items
        original_db = default_fs.db
        custom_db = ExceptionDict(original_db)
        default_fs.db = custom_db

        try:
            with pytest.raises(FilesystemServiceError) as excinfo:
                default_fs.delete(uploaded_file.name)

            assert "Unexpected error deleting file" in str(excinfo.value)
        finally:
            # Restore the original db
            default_fs.db = original_db

    def test_get_all(self, default_fs, uploaded_file) -> None:
        """Test get_all method."""
        # Upload another file
        another_file = default_fs.upload(b"Another file content", "another_file.txt", FileType.DOCUMENT)
        default_fs.db["another_file.txt"] = another_file

        result = default_fs.get_all()

        assert len(result) == 2
        assert uploaded_file in result
        assert another_file in result

    def test_get_batch(self, default_fs, uploaded_file) -> None:
        """Test get_batch method."""
        # Upload another file
        another_file = default_fs.upload(b"Another file content", "another_file.txt", FileType.DOCUMENT)
        default_fs.db["another_file.txt"] = another_file

        # Request both files
        result = default_fs.get_batch([uploaded_file.name, another_file.name])
        assert len(result) == 2
        assert uploaded_file.name in result
        assert another_file.name in result
        assert result[uploaded_file.name] == uploaded_file
        assert result[another_file.name] == another_file

        # Request one existing and one non-existent file
        result = default_fs.get_batch([uploaded_file.name, "nonexistent.txt"])
        assert len(result) == 2
        assert uploaded_file.name in result
        assert result[uploaded_file.name] == uploaded_file
        assert "nonexistent.txt" in result
        assert result["nonexistent.txt"] is None

        # Request all non-existent files
        result = default_fs.get_batch(["nonexistent1.txt", "nonexistent2.txt"])
        assert len(result) == 2
        assert "nonexistent1.txt" in result
        assert "nonexistent2.txt" in result
        assert result["nonexistent1.txt"] is None
        assert result["nonexistent2.txt"] is None
