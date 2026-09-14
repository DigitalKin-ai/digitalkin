"""Setup field for files a module ingests at config-setup time.

Declares the pair every file-consuming module needs — the selected files and the
formats it accepts — so the field, its widget options and its validation stay
identical across modules::

    class MySetup(SetupModel):
        knowledge_files: knowledge_files_input(extensions=[".json"]) = Field(
            default_factory=list,
            title="Knowledge Files",
            description="Files imported into setup-scoped storage at config-setup time.",
        )

The allowed formats reach the front-end file widget as ``ui:options.accept`` and are
enforced again on submit, since the widget's filter is only a hint.
"""

from collections.abc import Sequence
from typing import Annotated, Any

from pydantic import AfterValidator, Field

from digitalkin.models.services.filesystem import FileMetadata, FileType


class KnowledgeFileFormatError(ValueError):
    """A submitted file is not in a format the module accepts."""


def knowledge_files_input(
    extensions: Sequence[str] = (),
    file_types: Sequence[FileType] = (),
    *,
    multiple: bool = True,
    config: bool = True,
) -> Any:
    """Build the annotated type for a setup field holding uploaded files.

    Args:
        extensions: Accepted file extensions, leading dot included (e.g. ``.pdf``).
            Empty accepts any extension.
        file_types: Accepted categories. Empty accepts any category.
        multiple: Whether the widget allows selecting more than one file.
        config: Whether the field belongs to the config-setup schema rather than
            the runtime setup schema.

    Returns:
        Annotated type for use in a `SetupModel` field.
    """
    allowed_extensions = frozenset(e.lower() if e.startswith(".") else f".{e.lower()}" for e in extensions)
    allowed_types = frozenset(file_types)

    def validate_formats(files: list[FileMetadata]) -> list[FileMetadata]:
        """Reject any file whose extension or category the module does not accept.

        Returns:
            The files, unchanged.

        Raises:
            KnowledgeFileFormatError: If a file is in an unaccepted format.
        """
        for file in files:
            # A bare-id submission carries no name yet; the format is checked once the
            # filesystem service has resolved it, not guessed at here.
            suffix = f".{file.name.rsplit('.', 1)[-1].lower()}" if "." in file.name else ""
            if suffix and allowed_extensions and suffix not in allowed_extensions:
                msg = f"'{file.name}' is not an accepted format; expected one of {sorted(allowed_extensions)}"
                raise KnowledgeFileFormatError(msg)
            if allowed_types and file.type is not FileType.UNSPECIFIED and file.type not in allowed_types:
                msg = (
                    f"'{file.name}' is a {file.type.name} file; expected one of {sorted(t.name for t in allowed_types)}"
                )
                raise KnowledgeFileFormatError(msg)
        return files

    ui_options: dict[str, Any] = {"multiple": multiple}
    if allowed_extensions:
        ui_options["accept"] = sorted(allowed_extensions)
    schema_extra: dict[str, Any] = {"ui:widget": "file", "ui:options": ui_options}
    if config:
        schema_extra["config"] = True

    return Annotated[
        list[FileMetadata],
        Field(json_schema_extra=schema_extra),
        AfterValidator(validate_formats),
    ]
