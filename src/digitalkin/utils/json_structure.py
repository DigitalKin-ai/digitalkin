r"""Read a JSON document at the key paths a setup's structure map names.

The *structure* is ``{leaf key path: description}``, written by the agent that creates or
updates a service setup and stored by the backend alongside the content. The SDK does not
build it and does not judge it: ``clip`` bounds each description's length and nothing else,
and ``resolve`` reads the paths it names so ``DefaultSetup`` can project content the way the
backend does for ``GetSetupRequest.structure_key``.

The path grammar is a cross-repo contract: the agent writes it, the backend resolves it
against stored JSON, and this reads it the same way. Three rules, normative:

- a plain object key joins with ``.`` — ``prompts.system``;
- a key that is empty or carries ``.`` ``[`` ``]`` ``"`` ``\\`` is bracket-quoted with
  ``"``, with ``"`` and ``\\`` backslash-escaped — ``limits["max.tokens"]``;
- a list element is indexed — ``tools[0]``.

A resolver that splits naively on ``.`` gets the second rule wrong silently, returning the
wrong key or nothing. That is the one real interop risk in the design.
"""

from typing import Any, ClassVar

from digitalkin.logger import logger


class JsonStructure:
    """Length-bounding and path access over the structure map."""

    _DESCRIPTION_CHARS: ClassVar[int] = 512

    @classmethod
    def clip(cls, structure: dict[str, str]) -> dict[str, str]:
        """Bound each description's length, leaving every entry in place.

        The only thing done to an authored map. Which keys it names and whether they are
        good descriptions is the agent's business; an unbounded description is not, since
        the map rides on every search result.

        Args:
            structure: The map as the agent wrote it.

        Returns:
            The same entries, each description at most ``_DESCRIPTION_CHARS`` long with a
            trailing ellipsis where it was cut.
        """
        clipped: dict[str, str] = {}
        for path, text in structure.items():
            if len(text) <= cls._DESCRIPTION_CHARS:
                clipped[path] = text
                continue
            logger.debug("json structure: clipped %s from %d chars", path, len(text))
            clipped[path] = text[: cls._DESCRIPTION_CHARS - 1] + "…"
        return clipped

    @classmethod
    def resolve(cls, content: dict[str, Any], paths: list[str]) -> dict[str, Any]:
        """Read the values at exactly ``paths``.

        Args:
            content: The JSON document to read.
            paths: Key paths from a setup's structure map.

        Returns:
            Path to value for every path that resolves; unresolvable paths are omitted
            and logged.
        """
        values: dict[str, Any] = {}
        for path in paths:
            found, node = cls._walk(content, path)
            if found:
                values[path] = node
            else:
                logger.debug("json structure: unresolved path %s", path)
        return values

    @classmethod
    def _walk(cls, content: dict[str, Any], path: str) -> tuple[bool, Any]:
        """Follow one path through a document.

        Args:
            content: The document to walk.
            path: A key path in the grammar above.

        Returns:
            ``(True, value)`` when the path resolves, ``(False, None)`` when it does not.
        """
        node: Any = content
        for segment in cls._decode(path):
            if isinstance(segment, int):
                if not isinstance(node, list) or not -len(node) <= segment < len(node):
                    return False, None
                node = node[segment]
            elif isinstance(node, dict) and segment in node:
                node = node[segment]
            else:
                return False, None
        return True, node

    @classmethod
    def _decode(cls, path: str) -> list[str | int]:
        """Split a path into its object keys and list indices.

        Args:
            path: A key path in the grammar above.

        Returns:
            The segments; an integer for a list index, a string for an object key.

        Raises:
            ValueError: The path is malformed — an unterminated bracket or quote, or a
                non-integer index.
        """
        try:
            return cls._scan(path)
        except (IndexError, ValueError) as error:
            msg = f"malformed path: {path!r}"
            raise ValueError(msg) from error

    @classmethod
    def _scan(cls, path: str) -> list[str | int]:
        """Walk a path character by character, emitting its segments.

        Split out of :meth:`_decode` so the error translation wraps a single call rather
        than the whole scan.

        Args:
            path: A key path in the grammar above.

        Returns:
            The segments, in order.

        Raises:
            ValueError: A quoted key is not closed by ``"]"``. An unterminated bracket or
                a non-integer index surfaces as IndexError or ValueError from the scan
                itself; :meth:`_decode` translates both.
        """
        segments: list[str | int] = []
        cursor = 0
        while cursor < len(path):
            if path[cursor] == ".":
                cursor += 1
                continue
            if path[cursor] != "[":
                end = min(
                    position if position != -1 else len(path)
                    for position in (path.find(".", cursor), path.find("[", cursor))
                )
                segments.append(path[cursor:end])
                cursor = end
                continue
            cursor += 1
            if path[cursor] != '"':
                end = path.index("]", cursor)
                segments.append(int(path[cursor:end]))
                cursor = end + 1
                continue
            cursor += 1
            buffer = ""
            while path[cursor] != '"':
                if path[cursor] == "\\":
                    cursor += 1
                buffer += path[cursor]
                cursor += 1
            if path[cursor + 1] != "]":
                msg = f"unterminated quoted key in {path!r}"
                raise ValueError(msg)
            segments.append(buffer)
            cursor += 2
        return segments
