r"""Read and edit a JSON document at the key paths a setup's structure map names.

The *structure* is ``{leaf key path: description}``, written by the agent that creates or
updates a service setup and stored by the backend alongside the content. The SDK does not
build it and does not judge it: ``clip`` bounds each description's length and nothing else,
and ``resolve`` reads the paths it names so ``DefaultSetup`` can project content the way the
backend does for ``GetSetupRequest.structure_key``. ``patch``, ``append`` and ``remove`` edit one
element in the same grammar, for the services manager actions of the same names; ``prune``,
``misplaced`` and ``forget`` keep the map in step with those edits.

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
    def patch(cls, content: dict[str, Any], path: str, value: Any) -> None:
        """Replace, in place, the value at a path that already exists.

        Args:
            content: The document to edit.
            path: A key path in the grammar above, naming an existing element.
            value: The JSON value to put there.

        Raises:
            ValueError: The path is empty, malformed, or names nothing in ``content``.
        """
        segments = cls._target(path)
        found, parent = cls._follow(content, segments[:-1])
        if not found or not cls._follow(parent, segments[-1:])[0]:
            msg = f"{path!r} not found: patch only changes an existing element; use append to add one"
            raise ValueError(msg)
        parent[segments[-1]] = value

    @classmethod
    def append(cls, content: dict[str, Any], path: str, value: Any) -> str:
        """Add a new element in place: a missing object key, or one more item of a list.

        A path naming a list pushes ``value`` at its end. A path whose tail is missing creates
        it, along with any missing parent objects. ``content`` is left untouched on refusal.

        Args:
            content: The document to edit.
            path: A key path in the grammar above.
            value: The JSON value to add.

        Returns:
            The path of the added element, which ``resolve`` reads back.

        Raises:
            ValueError: The path is empty or malformed, names an existing value that is not a
                list, or crosses a list index or a non-object that does not exist.
        """
        segments = cls._target(path)
        node: Any = content
        for position, segment in enumerate(segments):
            if isinstance(segment, str) and isinstance(node, dict) and segment not in node:
                rest = segments[position + 1 :]
                blocking = next((index for index in rest if isinstance(index, int)), None)
                if blocking is not None:
                    msg = (
                        f"cannot add at {path!r}: list index [{blocking}] does not exist "
                        "and only object keys are created"
                    )
                    raise ValueError(msg)
                for key in reversed(rest):
                    value = {key: value}
                node[segment] = value
                return path
            found, node = cls._follow(node, [segment])
            if not found:
                msg = f"cannot add at {path!r}: {segment!r} does not exist and only object keys are created"
                raise ValueError(msg)
        if isinstance(node, list):
            node.append(value)
            return f"{path}[{len(node) - 1}]"
        msg = f"{path!r} already holds a value: append only adds; use patch to change it"
        raise ValueError(msg)

    @classmethod
    def remove(cls, content: dict[str, Any], path: str) -> None:
        """Remove, in place, the element at a path that exists: an object key or a list item.

        Removing a list item moves every later item down one index.

        Args:
            content: The document to edit.
            path: A key path in the grammar above, naming an existing element.

        Raises:
            ValueError: The path is empty, malformed, carries a negative index, or names nothing.
        """
        segments = cls._target(path)
        found, parent = cls._follow(content, segments[:-1])
        if not found or not cls._follow(parent, segments[-1:])[0]:
            msg = f"{path!r} not found: nothing to remove"
            raise ValueError(msg)
        del parent[segments[-1]]

    @classmethod
    def forget(cls, structure: dict[str, str], path: str) -> dict[str, str]:
        """Update a map for the removal of the element at ``path``.

        The element's entries are dropped (:meth:`prune`). When it was a list item, the entries
        of the later items of that list move down one index, as the items did; each renamed
        entry is logged. A stored entry that cannot be parsed is kept as is.

        Args:
            structure: The stored map; left untouched.
            path: A key path in the grammar above, naming the removed element.

        Returns:
            The map describing the content after the removal.

        Raises:
            ValueError: ``path`` is empty, malformed, or carries a negative list index.
        """
        branch = cls._target(path)
        kept = cls.prune(structure, path)
        removed = branch[-1]
        if not isinstance(removed, int):
            return kept
        depth = len(branch) - 1
        forgotten: dict[str, str] = {}
        for entry, text in kept.items():
            try:
                segments = cls._decode(entry)
            except ValueError:
                forgotten[entry] = text
                continue
            index = segments[depth] if len(segments) > depth else None
            if segments[:depth] != branch[:depth] or not isinstance(index, int) or index <= removed:
                forgotten[entry] = text
                continue
            segments[depth] = index - 1
            renamed = cls._encode(segments)
            logger.info("json structure: renamed %s to %s after removing %s", entry, renamed, path)
            forgotten[renamed] = text
        return forgotten

    @classmethod
    def _encode(cls, segments: list[str | int]) -> str:
        """Write segments back as a path, quoting the keys the grammar requires quoted.

        Args:
            segments: Object keys and list indices, in order.

        Returns:
            The path, which :meth:`_decode` reads back into ``segments``.
        """
        path = ""
        for segment in segments:
            if isinstance(segment, int):
                path += f"[{segment}]"
            elif segment and not set(segment) & set('.[]"\\'):
                path += f".{segment}" if path else segment
            else:
                path += '["' + segment.replace("\\", "\\\\").replace('"', '\\"') + '"]'
        return path

    @classmethod
    def prune(cls, structure: dict[str, str], path: str) -> dict[str, str]:
        """Drop the map entries describing the element at ``path``: itself and everything under it.

        Called before an edit's own entries go in, so the map never keeps describing what the
        edit replaced. Entries are compared segment by segment, so ``["llm"].model`` is under
        ``llm``. A stored entry that cannot be parsed is kept: the map is never judged.

        Args:
            structure: The stored map; left untouched.
            path: A key path in the grammar above, naming the edited element.

        Returns:
            The entries outside that element.

        Raises:
            ValueError: ``path`` is empty, malformed, or carries a negative list index.
        """
        branch = cls._target(path)
        kept: dict[str, str] = {}
        for entry, text in structure.items():
            try:
                under = cls._decode(entry)[: len(branch)] == branch
            except ValueError:
                under = False
            if under:
                logger.info("json structure: dropped %s (%s), described again by the edit of %s", entry, text, path)
            else:
                kept[entry] = text
        return kept

    @classmethod
    def misplaced(cls, entries: dict[str, str], content: dict[str, Any], path: str) -> list[str]:
        """List the entries that do not describe the element written at ``path``.

        An edit's entries must name that element or something inside it, and resolve in the
        edited content. Anything else would describe another element — an item pushed onto a
        list lands at an index the caller may have guessed wrong — or a key that is not there.

        Args:
            entries: The map entries the edit carries.
            content: The edited document.
            path: The path of the written element.

        Returns:
            The misplaced entry paths, in order; empty when every entry is in place.

        Raises:
            ValueError: ``path`` or an entry is empty, malformed, or carries a negative list index.
        """
        branch = cls._target(path)
        return [
            entry
            for entry in entries
            if cls._decode(entry)[: len(branch)] != branch or not cls._walk(content, entry)[0]
        ]

    @classmethod
    def _walk(cls, content: dict[str, Any], path: str) -> tuple[bool, Any]:
        """Follow one path through a document.

        Args:
            content: The document to walk.
            path: A key path in the grammar above.

        Returns:
            ``(True, value)`` when the path resolves, ``(False, None)`` when it does not.
        """
        return cls._follow(content, cls._decode(path))

    @classmethod
    def _follow(cls, node: Any, segments: list[str | int]) -> tuple[bool, Any]:
        """Follow decoded segments from ``node``.

        Args:
            node: Where to start.
            segments: Object keys and list indices, in order.

        Returns:
            ``(True, value)`` when every segment resolves, ``(False, None)`` otherwise.
        """
        for segment in segments:
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
    def _target(cls, path: str) -> list[str | int]:
        """Decode the path of an element to edit, refusing the empty one and negative indices.

        An edited path also keys the structure map, where ``tools[-1]`` would never match the
        ``tools[1]`` entry describing the same item, so only positions counted from 0 are taken.

        Args:
            path: A key path in the grammar above.

        Returns:
            The segments, at least one.

        Raises:
            ValueError: The path is empty, malformed, or carries a negative list index.
        """
        segments = cls._decode(path)
        if not segments:
            msg = "empty path: name the element to edit (replacing the whole document is update's job)"
            raise ValueError(msg)
        if any(isinstance(segment, int) and segment < 0 for segment in segments):
            msg = f"negative list index in {path!r}: give the item's position counted from 0"
            raise ValueError(msg)
        return segments

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
