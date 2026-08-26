"""Shared foundation for the Registry Toolkit managers (Tools / Services / Kins).

All three managers operate on **setups** of a given ``module_type`` (Tool =
TOOL_MODULE, Service = SERVICE, Kin = ARCHETYPE). They share the same CRUD actions
and the same guard/invalidate/normalise plumbing, so this module holds:

- :class:`RegistryActionCtx` — what an action needs to run (setup + registry services
  and the manager's object type);
- :class:`RegistryAction` — the abstract, discriminated action base;
- :class:`RegistryObjectToolKit` — the base toolkit carrying the shared plumbing.

Each manager is then a thin subclass declaring its ``module_type`` and a single
``manage_*`` dispatcher over its own action union.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from agno.tools.function import Function
from google.protobuf.message import Message as ProtoMessage
from pydantic import BaseModel, TypeAdapter, ValidationError, create_model, field_validator

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.logger import logger
from digitalkin.services.registry.exceptions import RegistryServiceError
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.utils.proto_utils import ProtoUtils
from digitalkin.utils.setup_content_validator import SetupContentValidator

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from digitalkin.models.module import ModuleContext
    from digitalkin.models.services.registry import RegistryModuleType
    from digitalkin.services.registry.registry_strategy import RegistryStrategy
    from digitalkin.services.setup.setup_strategy import SetupData, SetupStrategy


class BaseActionCtx:
    """Base for the per-manager action contexts passed to :meth:`BaseAction.execute`.

    A marker base carrying no shared state — each manager family defines its own context
    (services + object type for the CRUD managers, live tool list + notifier for the loader).
    """

    __slots__ = ()


CtxT = TypeVar("CtxT", bound=BaseActionCtx)


class BaseAction(BaseModel, ABC, Generic[CtxT]):
    """Base for every manager's discriminated actions.

    Each concrete action declares an ``action`` ``Literal`` discriminator, carries its
    parameters as fields, and implements :meth:`execute`, which runs it against its manager's
    context ``CtxT`` and returns the raw result. Abstract, so it is never a valid discriminator
    target and is never instantiated.
    """

    @abstractmethod
    async def execute(self, ctx: CtxT) -> Any:
        """Run this action against its manager's context and return the raw result.

        Args:
            ctx: The manager-specific action context.

        Returns:
            The raw result the manager's dispatcher normalises and wraps in the envelope.
        """


@dataclass(frozen=True, slots=True)
class RegistryActionCtx(BaseActionCtx):
    """What a registry action needs to run.

    Attributes:
        setup: The setup service strategy (CRUD writes + create).
        registry: The registry service strategy (search + service load).
        module_type: The manager's object type, used to filter searches and tag creates.
        context: Module context; enables pre-write ``content`` validation against the module's
            config schema. ``None`` outside a running job — validation is then skipped.
    """

    setup: SetupStrategy
    registry: RegistryStrategy
    module_type: RegistryModuleType
    context: ModuleContext | None = None

    async def validate_content(self, module_id: str, content: dict[str, Any]) -> None:
        """Validate ``content`` against the module's config schema before a write (best-effort).

        No-op when no context is wired or the schema can't be fetched (the module's own
        ``ConfigSetupModule`` stays the authoritative backstop). When the schema IS available and
        the content doesn't match, raises so the dispatcher returns a correctable fail envelope.

        Args:
            module_id: The backing module whose config schema to validate against.
            content: The setup ``content`` about to be written.

        Raises:
            ValueError: The content is missing a required field or has a wrong-typed one.
        """
        if self.context is None:
            return
        try:
            schema = await self.context.get_module_config_schema(module_id)
        except Exception as error:
            logger.warning("content validation skipped (schema fetch failed for %s): %s", module_id, error)
            return
        SetupContentValidator.validate(content, schema)

    async def ensure_kind(self, setup_id: str) -> SetupData:
        """Resolve a setup by id and assert its backing module is this manager's type.

        The three managers share one ``SetupService`` backend, so an id resolves whatever
        its kind — a raw ``get``/``update``/``delete`` would let ``kins_manager`` read a
        tool or ``tools_manager`` delete a service. This gate reads the setup
        (immediately consistent, and the backend excludes deleted ids — so it doubles as
        the guard that refuses writes on a deleted resource), then resolves the
        backing module's type and refuses on mismatch. Id-targeting actions call it before
        acting; read actions reuse the returned setup instead of fetching twice.

        Args:
            setup_id: The setup id the action targets.

        Returns:
            The resolved setup.

        Raises:
            ValueError: The setup's backing module is not of this manager's type.
        """
        setup = await self.setup.get_setup({"setup_id": setup_id})
        module = await self.registry.discover_by_id(setup.module_id)
        if module.module_type != self.module_type:
            msg = f"{setup_id} is not a {self.module_type.value} setup (kind mismatch); refused"
            raise ValueError(msg)
        return setup


class RegistryAction(BaseAction[RegistryActionCtx], ABC):
    """Base for the discriminated registry actions shared across the three CRUD managers.

    Inherits the abstract :meth:`~BaseAction.execute` and binds it to
    :class:`RegistryActionCtx` (setup + registry services and the manager's object type).
    ``writes`` marks state-mutating operations so the dispatcher invalidates the
    servicer's setup cache after a successful call. Still abstract, so it is never a valid
    discriminator target and is never instantiated.
    """

    writes: ClassVar[bool] = False

    @field_validator("name", check_fields=False)
    @classmethod
    def _name_has_no_control_chars(cls, value: str) -> str:
        """Reject control characters in a user-facing ``name`` so it fails loudly, not silently.

        The action's ``name`` bypasses the content validator, so without this a NUL byte or ANSI
        escape would reach persistence and be stripped there, altering the value without telling
        the caller. Applies to any action declaring ``name`` (update, service create).

        Returns:
            The name unchanged when clean.

        Raises:
            ValueError: The name carries a control character.
        """
        return SetupContentValidator.reject_control_chars(value)

    @field_validator("content", check_fields=False)
    @classmethod
    def _content_keys_are_safe(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Reject ``content`` keys carrying characters persistence would silently drop.

        The content validator checks values against the module's config schema, but object KEYS
        bypass it and reach storage verbatim, where a non-BMP (emoji) or control character is
        stripped — altering the written configuration with ``success:true`` and no diagnostic.
        Runs on any action declaring ``content`` (service create, update), including create which
        otherwise skips schema validation.

        Returns:
            The content unchanged when every key is safe.

        Raises:
            ValueError: A key (at any depth) carries a control or non-BMP character.
        """
        return SetupContentValidator.reject_unsafe_keys(value)


class RegistryObjectToolKit(DkToolkit):
    """Base toolkit for one Registry object type (Tool / Service / Kin).

    Subclasses set :attr:`module_type` and pass their action union + a single ``manage_*``
    entrypoint. This base owns the action context, the fail-safe guard, the best-effort cache
    invalidation and the JSON normalisation.

    The one tool is registered with ``skip_entrypoint_processing`` and an explicit schema, so
    Agno does **not** wrap it in ``validate_call``. A malformed LLM argument therefore reaches
    :meth:`_run` (which validates it into the union and returns a clean fail envelope the model
    self-corrects from) instead of raising a ``ValidationError`` Agno logs as an error traceback —
    a bad tool call is the model's mistake, not an SDK error.
    """

    module_type: ClassVar[RegistryModuleType]

    def __init__(
        self,
        setup: SetupStrategy,
        registry: RegistryStrategy,
        context: ModuleContext | None = None,
        *,
        name: str,
        actions: Any,
        description: str,
        entrypoint: Any,
    ) -> None:
        """Initialize the toolkit with the module's setup and registry services.

        Args:
            setup: The setup service strategy (shared with the servicer's base flow).
            registry: The registry service strategy.
            context: Module context; enables AG-UI notifications via the base toolkit.
            name: The agno tool name exposed to the model.
            actions: The discriminated action union this manager dispatches.
            description: The LLM-facing tool description.
            entrypoint: The bound ``manage_*`` method Agno calls (delegates to :meth:`_run`).
        """
        self._name = name
        self._ctx_data = RegistryActionCtx(
            setup=setup, registry=registry, module_type=self.module_type, context=context
        )
        self._adapter = TypeAdapter(actions)
        # A wrapper model yields the exact ``{properties: {action: <union>}, $defs, required}`` schema
        # Agno would build for ``action: <union>`` — but paired with skip_entrypoint_processing it is
        # the *only* validation, done by us in _run rather than Agno's validate_call.
        args_schema = create_model(f"{name}_args", action=(actions, ...)).model_json_schema()
        tool = Function(
            name=name,
            description=description,
            parameters=args_schema,
            entrypoint=entrypoint,
            skip_entrypoint_processing=True,
        )
        super().__init__(name=name, tools=[tool], context=context)

    async def _run(self, action: Any) -> str:
        """Validate a raw discriminated action then dispatch it.

        The single entrypoint shared by every manager. Agno passes the model's raw ``action``
        payload (a dict); tests pass an already-built action instance — both go through
        :meth:`TypeAdapter.validate_python`. A validation failure (an out-of-range ``limit``, an
        empty ``content``, a missing field) becomes a clean fail envelope naming the offending
        field, never a raised ``ValidationError``.

        Args:
            action: The raw action payload, or a built action instance.

        Returns:
            The dispatch envelope, or a fail envelope naming the invalid field(s).
        """
        try:
            # Some models serialise the nested ``action`` object as a JSON string instead of an
            # object (the discriminated-union schema triggers it) — parse that with validate_json;
            # a dict (agno's parsed args) or an already-built instance (tests) go through
            # validate_python.
            parsed = (
                self._adapter.validate_json(action)
                if isinstance(action, str)
                else self._adapter.validate_python(action)
            )
        except ValidationError as error:
            detail = "; ".join(f"{'.'.join(str(p) for p in item['loc'])}: {item['msg']}" for item in error.errors())
            return self._fail(f"invalid action: {detail}", tool=self._name)
        return await self._dispatch(parsed)

    async def _guard(self, op: str, coro: Awaitable[Any]) -> tuple[bool, Any]:
        """Await a setup/registry call, converting failures into a fail envelope.

        Args:
            op: Action name, used in the error message and metadata.
            coro: The service coroutine to await.

        Returns:
            ``(True, result)`` on success; ``(False, fail_envelope)`` on any
            error — never raises into the agent loop.
        """
        try:
            return True, await coro
        except PermissionDeniedError:
            return False, self._fail(f"permission denied: {op}", tool=op)
        except (SetupServiceError, RegistryServiceError, ServerError, ValueError) as error:
            logger.warning("%s: %s failed: %s", type(self).__name__, op, error)
            return False, self._fail(str(error), tool=op)
        except Exception as error:
            # Backend contract surprises (KeyError, TypeError, ...) must not
            # raise into the agent loop either.
            logger.exception("%s: %s failed unexpectedly", type(self).__name__, op)
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
            logger.exception("%s: setup-cache invalidation failed", type(self).__name__)

    @staticmethod
    def _jsonable(value: Any) -> Any:
        """Normalise a backend return value to a JSON-serializable form.

        Also echoes ``visibility`` back in the caller's vocabulary — the input enum is
        ``public``/``private``/``internal`` but the backend returns the proto name
        ``VISIBILITY_INTERNAL``, so a naive round-trip fails. Strip the prefix and
        lower-case it so the field read back matches the field written.

        Args:
            value: A Pydantic model, proto message, or plain scalar/collection.

        Returns:
            A dict for models/protos, otherwise the value unchanged.
        """
        if isinstance(value, BaseModel):
            data = value.model_dump(mode="json")
            visibility = data.get("visibility")
            if isinstance(visibility, str) and visibility.startswith("VISIBILITY_"):
                data["visibility"] = visibility.removeprefix("VISIBILITY_").lower()
            return data
        if isinstance(value, ProtoMessage):
            return ProtoUtils.proto_to_dict(value)
        return value

    async def _dispatch(self, action: Any) -> str:
        """Run one discriminated action end-to-end behind a fail-safe guard.

        Shared by every manager: it awaits the action's service call through
        :meth:`_guard`, invalidates the setup cache after a successful write, and
        wraps the result in the canonical envelope. The whole body is guarded, so any
        error — a backend failure, a bad payload the agent sent, or an unexpected
        surprise — becomes a clean fail envelope instead of a raw traceback in the
        logs; it never raises into the agent loop.

        Args:
            action: The concrete :class:`RegistryAction` the agent selected.

        Returns:
            The canonical success envelope, or a fail envelope on any error.
        """
        try:
            ok, result = await self._guard(action.action, action.execute(self._ctx_data))
            if not ok:
                return result
            if action.writes:
                await self._invalidate()
            return self._ok(self._jsonable(result), tool=action.action)
        except Exception as error:
            logger.exception("%s: action dispatch failed unexpectedly", type(self).__name__)
            return self._fail(f"action failed: {type(error).__name__}: {error}", tool="dispatch")
