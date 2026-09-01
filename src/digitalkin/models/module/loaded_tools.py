"""Mission-scoped persistence for tools the agent loaded at runtime.

A dynamic tool load (``load_manager`` → :meth:`ModuleContext.resolve_tool`) has to
outlive the turn that made it: every user message builds a fresh module instance,
so the in-memory toolkit list and the ``dynamic`` layer of the
:class:`~digitalkin.models.module.tool_cache.ToolCache` are both gone by the next
message. This module owns the only thing that survives — the list of loaded
``setup_id``s, written to the mission-scoped ``loaded_tools`` storage collection.

Only ids are stored, never resolved schemas: rehydration re-runs ``resolve_tool``,
which re-checks authorization and picks up any schema change, so a tool revoked or
altered between two turns is handled correctly instead of being replayed from a
stale snapshot.

Modules that want runtime tool loading must register the collection::

    services_config_params = {"storage": {"config": {**LOADED_TOOLS_STORAGE_CONFIG}}}
"""

from typing import ClassVar

from pydantic import BaseModel

from digitalkin.logger import logger
from digitalkin.services.storage.storage_strategy import StorageStrategy

_LOADED_TOOLS_COLLECTION = "loaded_tools"

# The storage service caps a list() at 100 records; ask for the cap rather than the
# default 20, so a heavily-loaded mission rehydrates completely.
_LIST_LIMIT = 100


class LoadedToolRecord(BaseModel):
    """A tool the agent loaded at runtime during this mission."""

    setup_id: str


LOADED_TOOLS_STORAGE_CONFIG: dict[str, type[BaseModel]] = {_LOADED_TOOLS_COLLECTION: LoadedToolRecord}
"""Storage config fragment for the ``loaded_tools`` collection."""


class LoadedToolStore:
    """Storage wrapper for the mission-scoped ``loaded_tools`` collection.

    Every method is fail-soft and returns instead of raising: a module that never
    registered the collection (see :data:`LOADED_TOOLS_STORAGE_CONFIG`) must degrade
    to the old turn-scoped behaviour, not crash the run through the HITL runner.
    Registration is checked before each call so a module that does not opt into runtime
    tool loading pays no storage round-trip per turn.
    """

    COLLECTION: ClassVar[str] = _LOADED_TOOLS_COLLECTION

    def __init__(self, storage: StorageStrategy) -> None:
        """Initialize the store.

        Args:
            storage: The module's storage strategy. Records are written under the
                default mission context, which is what scopes a load to one
                conversation.
        """
        self._storage = storage

    async def save(self, setup_id: str) -> bool:
        """Record ``setup_id`` as loaded for this mission (idempotent).

        Args:
            setup_id: The loaded tool's registry setup id.

        Returns:
            ``True`` if the id is now persisted, ``False`` if persistence is
            unavailable — the caller keeps the tool for the current turn either way.
        """
        try:
            if self.COLLECTION not in self._storage.config:
                logger.warning(
                    "LoadedToolStore: collection '%s' is not registered on this module "
                    "(add LOADED_TOOLS_STORAGE_CONFIG to services_config_params); "
                    "tool '%s' will not survive the current turn",
                    self.COLLECTION,
                    setup_id,
                )
                return False
            await self._storage.upsert(
                collection=self.COLLECTION,
                record_id=setup_id,
                data=LoadedToolRecord(setup_id=setup_id).model_dump(),
            )
        except Exception:
            logger.warning(
                "LoadedToolStore: could not persist loaded tool '%s'; it will not survive this turn",
                setup_id,
                exc_info=True,
            )
            return False
        logger.info("LoadedToolStore: persisted loaded tool '%s'", setup_id)
        return True

    async def list_setup_ids(self) -> list[str]:
        """List the tool setup ids loaded so far in this mission.

        Returns:
            The persisted setup ids, or an empty list if none exist or the
            collection is not registered.
        """
        try:
            # Checked first so a module that never opted into runtime tool loading pays no
            # storage round-trip on every turn.
            if self.COLLECTION not in self._storage.config:
                return []
            records = await self._storage.list(collection=self.COLLECTION, limit=_LIST_LIMIT)
        except Exception:
            logger.debug("LoadedToolStore: could not list loaded tools", exc_info=True)
            return []
        # ``StorageRecord.data`` holds the instance the strategy validated against the
        # registered model, so this is a narrowing, not a parse. Anything else in the
        # collection is someone else's record and is skipped rather than guessed at.
        setup_ids = [
            record.data.setup_id
            for record in records
            if isinstance(record.data, LoadedToolRecord) and record.data.setup_id
        ]
        if setup_ids:
            logger.info("LoadedToolStore: %d loaded tool(s) to rehydrate: %s", len(setup_ids), setup_ids)
        return setup_ids

    async def forget(self, setup_id: str) -> None:
        """Drop a persisted id that no longer resolves, so it is not retried every turn.

        Args:
            setup_id: The setup id to remove from the mission's loaded set.
        """
        try:
            if self.COLLECTION not in self._storage.config:
                return
            await self._storage.remove(collection=self.COLLECTION, record_id=setup_id)
        except Exception:
            logger.debug("LoadedToolStore: could not forget '%s'", setup_id, exc_info=True)
