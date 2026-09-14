"""Canonical file models shared by every producer and consumer of a file."""

from enum import Enum

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class FileType(Enum):
    """File category, mirroring the filesystem proto ``FileType`` by value."""

    UNSPECIFIED = "FILE_TYPE_UNSPECIFIED"
    DOCUMENT = "FILE_TYPE_DOCUMENT"
    IMAGE = "FILE_TYPE_IMAGE"
    VIDEO = "FILE_TYPE_VIDEO"
    AUDIO = "FILE_TYPE_AUDIO"
    ARCHIVE = "FILE_TYPE_ARCHIVE"
    CODE = "FILE_TYPE_CODE"
    OTHER = "FILE_TYPE_OTHER"

    @classmethod
    def _missing_(cls, value: object) -> "FileType | None":
        """Coerce a bare name (``IMAGE``), any casing, or a wire name; empty -> UNSPECIFIED.

        Returns:
            The matching member, or ``None`` for an unrecognised non-empty value.
        """
        if isinstance(value, str):
            key = value.strip().upper().removeprefix("FILE_TYPE_")
            if not key:
                return cls.UNSPECIFIED
            return cls.__members__.get(key)
        return None

    @classmethod
    def from_content_type(cls, content_type: str) -> "FileType":
        """Classify a MIME type.

        Args:
            content_type: A MIME type, with or without parameters.

        Returns:
            The matching member, ``OTHER`` when nothing matches.
        """
        base = content_type.split(";", 1)[0].strip().lower()
        match base:
            case (
                "application/javascript"
                | "application/x-httpd-php"
                | "application/x-python-code"
                | "application/x-sh"
                | "text/javascript"
                | "text/x-c"
                | "text/x-java-source"
                | "text/x-python"
                | "text/x-script.python"
            ):
                file_type = cls.CODE
            case (
                "application/gzip"
                | "application/vnd.rar"
                | "application/x-7z-compressed"
                | "application/x-bzip2"
                | "application/x-tar"
                | "application/zip"
            ):
                file_type = cls.ARCHIVE
            case (
                "application/json"
                | "application/ld+json"
                | "application/msword"
                | "application/pdf"
                | "application/rtf"
                | "application/xml"
            ):
                file_type = cls.DOCUMENT
            case _ if base.startswith("image/"):
                file_type = cls.IMAGE
            case _ if base.startswith("audio/"):
                file_type = cls.AUDIO
            case _ if base.startswith("video/"):
                file_type = cls.VIDEO
            case _ if base.startswith(("text/", "application/vnd.openxmlformats-", "application/vnd.oasis.")):
                file_type = cls.DOCUMENT
            case _:
                file_type = cls.OTHER
        return file_type


class FileMetadata(BaseModel):
    """A filesystem-service file, as stored in setup forms and streamed to modules.

    Mirrors the payload the front-end file widget keeps in the form data (the upload
    response), so the field round-trips: what is submitted is what is persisted in the
    setup version, and the widget can repopulate itself.
    """

    id: str = Field(
        title="File ID",
        description="Filesystem-service file identifier.",
        validation_alias=AliasChoices("id", "file_id"),
    )
    name: str = Field(default="", title="File Name", description="Original file name.")
    type: FileType = Field(
        default=FileType.UNSPECIFIED,
        title="File Type",
        description="Category of the file.",
        validation_alias=AliasChoices("type", "file_type"),
    )
    content_type: str = Field(default="", title="Content Type", description="MIME type of the file.")
    size_bytes: int = Field(default=0, title="Size (bytes)", description="Size of the file in bytes.")
    file_url: str = Field(default="", title="File URL", description="Public URL for accessing the file content.")

    model_config = ConfigDict(populate_by_name=True)


class FileUploadMetadata(BaseModel):
    """Default schema for the ``metadata`` carried on an upload.

    Modules needing a stricter contract register their own model under
    ``services_config_params["filesystem"]["config"]["metadata_model"]``.
    """

    model_config = ConfigDict(extra="allow")

    sdk_created: bool = Field(default=False, description="Set by the SDK on files it produced")
    source_url: str | None = Field(default=None, description="Origin the content was fetched from")
    filename: str | None = Field(default=None, description="Original filename when it differs from `name`")
