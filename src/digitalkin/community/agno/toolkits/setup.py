"""Toolkit exposing the DigitalKin setup service to the agent (setup CRUD).

Wraps the same ``SetupStrategy`` instance the module servicer already uses for the
base StartStream/Stream flow (shared gRPC channel, borrowed on ``context.setup``),
so the agent can create/read/update/delete setups and change their visibility.
Owner/organisation/module of a created setup are resolved server-side from the
request context; version lifecycle is platform-owned (content flows through the
setup's ``current_setup_version``). Every tool returns the canonical envelope and
never raises into the agent loop; permission denials are surfaced distinctly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from google.protobuf.message import Message as ProtoMessage
from pydantic import BaseModel

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.logger import logger
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.utils.proto_utils import ProtoUtils

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from digitalkin.models.module import ModuleContext
    from digitalkin.services.setup.setup_strategy import SetupStrategy


class SetupTools(DkToolkit):
    """CRUD + visibility access to the DigitalKin setup service over the module's shared channel.

    A setup is a configured instance of a module; its content lives in the embedded
    ``current_setup_version``. The tools build the flat dicts the ``SetupStrategy``
    expects and return a JSON envelope; results are normalised to plain JSON
    regardless of whether the backend returns a Pydantic model, a proto, or a scalar.
    """

    def __init__(self, setup: SetupStrategy, context: ModuleContext | None = None) -> None:
        """Initialize the toolkit with the module's setup service.

        Args:
            setup: The setup service strategy (shared with the servicer's base flow).
            context: Module context; enables AG-UI notifications via the base toolkit.
        """
        self._setup = setup
        super().__init__(
            name="setup_tools",
            tools=[
                self.get_setup,
                self.create_setup,
                self.update_setup,
                self.delete_setup,
                self.change_visibility,
            ],
            context=context,
        )

    async def _guard(self, op: str, coro: Awaitable[Any]) -> tuple[bool, Any]:
        """Await a setup-service call, converting failures into a fail envelope.

        Args:
            op: Tool name, used in the error message and metadata.
            coro: The setup-service coroutine to await.

        Returns:
            ``(True, result)`` on success; ``(False, fail_envelope)`` on any
            error — never raises into the agent loop.
        """
        try:
            return True, await coro
        except PermissionDeniedError:
            return False, self._fail(f"permission denied: {op}", tool=op)
        except (SetupServiceError, ServerError, ValueError) as error:
            logger.warning("SetupTools: %s failed: %s", op, error)
            return False, self._fail(str(error), tool=op)
        except Exception as error:
            # Backend contract surprises (KeyError, TypeError, ...) must not
            # raise into the agent loop either.
            logger.exception("SetupTools: %s failed unexpectedly", op)
            return False, self._fail(f"{op} failed: {type(error).__name__}: {error}", tool=op)

    async def _invalidate(self) -> None:
        """Invalidate the servicer's setup cache after a successful write (best-effort).

        No-op when the callback is not installed (e.g. outside the M4 flow).
        """
        if self._ctx is None:
            return
        invalidate = vars(self._ctx.callbacks).get("invalidate_setup")
        if invalidate is None:
            return
        try:
            invalidate()
        except Exception:
            logger.exception("SetupTools: setup-cache invalidation failed")

    @staticmethod
    def _jsonable(value: Any) -> Any:
        """Normalise a backend return value to a JSON-serializable form.

        Args:
            value: A Pydantic model, proto message, or plain scalar/collection.

        Returns:
            A dict for models/protos, otherwise the value unchanged.
        """
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if isinstance(value, ProtoMessage):
            return ProtoUtils.proto_to_dict(value)
        return value

    async def get_setup(self, setup_id: str, version: str = "") -> str:
        """Fetch a setup by id (optionally a specific version).

        Args:
            setup_id: The setup id to read.
            version: Optional version to pin; omit for the current version.

        Returns:
            The canonical envelope; ``output`` = the setup with its current version,
            status and visibility.
        """
        ok, result = await self._guard("get_setup", self._setup.get_setup({"setup_id": setup_id, "version": version}))
        return result if not ok else self._ok(self._jsonable(result), tool="get_setup")

    async def create_setup(self, name: str, content: dict[str, Any]) -> str:
        """Create a new setup with an initial version.

        The owner, organisation and target module are derived server-side from
        this request's context — only a name and the configuration content are
        needed. New setups start private; use ``change_visibility`` to share them.

        Args:
            name: Human-readable setup name.
            content: The initial version's configuration payload.

        Returns:
            The canonical envelope; ``output`` = the created setup (id, status,
            visibility, current version).
        """
        ok, result = await self._guard("create_setup", self._setup.create_setup({"name": name, "content": content}))
        if not ok:
            return result
        await self._invalidate()
        return self._ok(self._jsonable(result), tool="create_setup")

    async def update_setup(self, setup_id: str, name: str, content: dict[str, Any]) -> str:
        """Update an existing setup's name and current version content.

        Args:
            setup_id: The setup to update.
            name: New setup name.
            content: The current version's new configuration payload.

        Returns:
            The canonical envelope; ``output`` = the updated setup.
        """
        ok, result = await self._guard(
            "update_setup",
            self._setup.update_setup({"setup_id": setup_id, "name": name, "content": content}),
        )
        if not ok:
            return result
        await self._invalidate()
        return self._ok(self._jsonable(result), tool="update_setup")

    async def delete_setup(self, setup_id: str) -> str:
        """Delete a setup by id.

        Args:
            setup_id: The setup to delete.

        Returns:
            The canonical envelope; ``output`` = the deletion result.
        """
        ok, result = await self._guard("delete_setup", self._setup.delete_setup({"setup_id": setup_id}))
        if not ok:
            return result
        await self._invalidate()
        return self._ok(self._jsonable(result), tool="delete_setup")

    async def change_visibility(self, setup_id: str, visibility: Literal["public", "private", "internal"]) -> str:
        """Change who can see and use a setup.

        Args:
            setup_id: The setup whose visibility to change.
            visibility: "public" (everyone), "private" (owner only) or
                "internal" (whole organisation).

        Returns:
            The canonical envelope; ``output`` = the setup with its updated visibility.
        """
        ok, result = await self._guard(
            "change_visibility",
            self._setup.change_visibility({"setup_id": setup_id, "visibility": visibility}),
        )
        if not ok:
            return result
        await self._invalidate()
        return self._ok(self._jsonable(result), tool="change_visibility")
