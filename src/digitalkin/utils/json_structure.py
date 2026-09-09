"""Flat key map of a JSON document, plus path read/write against it.

The *structure* is ``{key path: short summary}``. It is authored by whoever writes the
content — an agent, on the toolkit path — and ``check`` filters that map down to the
entries whose paths actually resolve. ``describe`` derives one mechanically instead, and
is the fallback for writers with no author to ask (``create_service_setup``, config
modules, direct strategy calls).

``resolve`` and ``merge`` read and write those same paths. The path grammar is a
cross-repo contract: the SDK emits it, the backend resolves it against stored JSON.
See ``_encode`` for the three rules.
"""

from typing import Any, ClassVar

from digitalkin.logger import logger


class JsonStructure:
    """Leaf key map of a JSON document, and path access over the same grammar."""

    _DESCRIPTION_CHARS: ClassVar[int] = 120
    _NAMED_KEYS: ClassVar[int] = 8
    _MAX_ENTRIES: ClassVar[int] = 512

    @classmethod
    def describe(cls, content: dict[str, Any]) -> dict[str, str]:
        """Build the flat {leaf key path: short description} map of a document.

        Non-empty objects recurse without emitting an entry. Everything else — scalars,
        strings, arrays, empty objects — is a leaf and terminates the walk, so an array
        never expands per element.

        Args:
            content: The JSON document to describe.

        Returns:
            Leaf paths in document order, each mapped to its description.
        """
        entries: dict[str, str] = {}
        stack: list[tuple[str, Any]] = [("", content)]
        while stack:
            path, value = stack.pop()
            if isinstance(value, dict) and value:
                stack.extend((cls._encode(path, key), value[key]) for key in reversed(list(value)))
                continue
            if len(entries) >= cls._MAX_ENTRIES:
                logger.debug("json structure: dropped %s = %s", path, cls._summarise(value))
                continue
            entries[path] = cls._summarise(value)
        return entries

    @classmethod
    def check(cls, content: dict[str, Any], structure: dict[str, str]) -> dict[str, str]:
        """Keep the supplied entries whose paths resolve against ``content``.

        The map is authored by whoever wrote the content — an agent, in the toolkit path —
        so nothing guarantees its paths exist. An entry naming a key that is not there
        advertises a scope no read can satisfy, and one that is malformed cannot be
        resolved by the backend either; both are dropped, each logged with its summary.
        Summaries are clipped to ``_DESCRIPTION_CHARS``.

        Args:
            content: The document the map describes.
            structure: Key path to summary, as authored.

        Returns:
            The entries that survived, in the order supplied.
        """
        kept: dict[str, str] = {}
        for path, summary in structure.items():
            try:
                found, _ = cls._walk(content, path)
            except ValueError:
                found = False
            if not found:
                logger.warning("json structure: dropped unresolvable entry %s = %s", path, summary)
            elif len(kept) >= cls._MAX_ENTRIES:
                logger.warning("json structure: dropped over-cap entry %s = %s", path, summary)
            else:
                kept[path] = cls._clip(summary)
        return kept

    @classmethod
    def resolve(cls, content: dict[str, Any], paths: list[str]) -> dict[str, Any]:
        """Read the values at exactly ``paths``.

        Args:
            content: The JSON document to read.
            paths: Key paths as emitted by :meth:`describe`.

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
            path: A key path as emitted by :meth:`_encode`.

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
    def merge(cls, content: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
        """Return a copy of ``content`` with each patch path set to its value.

        Missing intermediate objects are created so a new key can be added. List indices
        are never created or extended — the position is ambiguous, so it raises instead.

        Args:
            content: The document to patch.
            patch: Key path to new value.

        Returns:
            A new document; ``content`` is not mutated.

        Raises:
            ValueError: A path is malformed, or it traverses a list index that does not
                exist or a scalar that cannot hold children.
        """
        merged = cls._clone(content)
        for path, value in patch.items():
            segments = cls._decode(path)
            if not segments:
                msg = f"cannot patch the document root: {path!r}"
                raise ValueError(msg)
            node: Any = merged
            for index, segment in enumerate(segments[:-1]):
                node = cls._descend(node, segment, segments[index + 1], path)
            cls._assign(node, segments[-1], value, path)
        return merged

    @classmethod
    def _descend(cls, node: Any, segment: str | int, following: str | int, path: str) -> Any:
        """Step one segment into ``node``, creating a missing object on the way.

        Args:
            node: The container to step into.
            segment: The segment to follow.
            following: The next segment, which decides the shape of a created container.
            path: The full path, for error messages.

        Returns:
            The child container.

        Raises:
            ValueError: The step is impossible — a list index that does not exist, or a
                scalar in the middle of the path.
        """
        if isinstance(segment, int):
            if not isinstance(node, list) or not -len(node) <= segment < len(node):
                msg = f"{path!r}: list index [{segment}] does not exist; create it explicitly"
                raise ValueError(msg)
            return node[segment]
        if isinstance(node, dict):
            if segment not in node:
                if isinstance(following, int):
                    msg = f"{path!r}: cannot create a list at {segment!r}; create it explicitly"
                    raise ValueError(msg)
                node[segment] = {}
            return node[segment]
        msg = f"{path!r}: cannot descend into a {type(node).__name__} at {segment!r}"
        raise ValueError(msg)

    @classmethod
    def _assign(cls, node: Any, segment: str | int, value: Any, path: str) -> None:
        """Set the final segment of a patch path.

        Args:
            node: The container holding the target.
            segment: The final segment.
            value: The value to write.
            path: The full path, for error messages.

        Raises:
            ValueError: The target cannot be written — a missing list index, or a
                non-container parent.
        """
        if isinstance(segment, int):
            if not isinstance(node, list) or not -len(node) <= segment < len(node):
                msg = f"{path!r}: list index [{segment}] does not exist; create it explicitly"
                raise ValueError(msg)
            node[segment] = value
            return
        if isinstance(node, dict):
            node[segment] = value
            return
        msg = f"{path!r}: cannot set {segment!r} on a {type(node).__name__}"
        raise ValueError(msg)

    @classmethod
    def _clone(cls, value: Any) -> Any:
        """Deep-copy the containers of a JSON value, sharing its immutable leaves.

        Args:
            value: The JSON value to copy.

        Returns:
            A structure that can be patched without mutating the original.
        """
        if isinstance(value, dict):
            return {key: cls._clone(item) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._clone(item) for item in value]
        return value

    @classmethod
    def _summarise(cls, value: Any) -> str:
        """Describe one leaf value in a single short line.

        Carries no type name and no size — the description is the content, abbreviated.

        Args:
            value: The leaf value.

        Returns:
            The description, clipped to ``_DESCRIPTION_CHARS``.
        """
        if isinstance(value, dict) or (isinstance(value, list) and not value):
            return "empty"
        if isinstance(value, list):
            first = value[0]
            parts = (
                list(first)[: cls._NAMED_KEYS]
                if isinstance(first, dict)
                else [str(item) for item in value[: cls._NAMED_KEYS]]
            )
            return cls._clip(", ".join(parts))
        if isinstance(value, str):
            return cls._clip(value)
        # bool before the numeric fallback: bool subclasses int, so the reverse renders True as "1".
        if isinstance(value, bool):
            return str(value).lower()
        return "null" if value is None else str(value)

    @classmethod
    def _clip(cls, text: str) -> str:
        """Trim a description to the length budget.

        Args:
            text: The untrimmed description.

        Returns:
            The text, with a trailing ellipsis when it was too long.
        """
        return text if len(text) <= cls._DESCRIPTION_CHARS else text[: cls._DESCRIPTION_CHARS] + "…"

    @classmethod
    def _encode(cls, parent: str, key: str) -> str:
        r"""Append an object key to a parent path.

        Three rules, normative for any resolver: a plain key joins with ``.``; a key that
        is empty or carries ``.`` ``[`` ``]`` ``"`` ``\\`` is bracket-quoted with ``"``
        and ``\\`` backslash-escaped; a list element is ``parent[n]``.

        Args:
            parent: The path so far, empty at the root.
            key: The object key to append.

        Returns:
            The child path.
        """
        if not key or any(char in key for char in '.["]\\'):
            escaped = key.replace("\\", "\\\\").replace('"', '\\"')
            return f'{parent}["{escaped}"]'
        return f"{parent}.{key}" if parent else key

    @classmethod
    def _decode(cls, path: str) -> list[str | int]:
        """Split a path into its object keys and list indices.

        Args:
            path: A path as emitted by :meth:`_encode`.

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
            path: A path as emitted by :meth:`_encode`.

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
