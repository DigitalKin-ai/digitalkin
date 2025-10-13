"""SurrealDB connection management."""

import datetime
import os
from collections.abc import AsyncGenerator
from typing import Any, Generic, TypeVar
from uuid import UUID

from surrealdb import AsyncHttpSurrealConnection, AsyncSurreal, AsyncWsSurrealConnection, RecordID

from digitalkin.logger import logger

TSurreal = TypeVar("TSurreal", bound=AsyncHttpSurrealConnection | AsyncWsSurrealConnection)


class SurrealDBSetupBadIDError(Exception):
    """Exception raised when an invalid ID is encountered during the setup process in the SurrealDB repository.

    This error is used to indicate that the provided ID does not meet the
    expected format or criteria.
    """


class SurrealDBSetupVersionBadIDError(Exception):
    """Exception raised when an invalid ID is encountered during the setup of a SurrealDB version.

    This error is intended to signal that the provided ID does not meet
    the expected format or criteria for a valid SurrealDB setup version ID.
    """


class SurrealDBConnection(Generic[TSurreal]):
    """Base repository for database operations.

    This class provides common database operations that can be used by
    specific table repositories.
    """

    db: TSurreal
    timeout: datetime.timedelta

    @staticmethod
    def _valid_id(raw_id: str, table_name: str) -> RecordID:
        """Validate and parse a raw ID string into a RecordID.

        Args:
            raw_id: The raw ID string to validate
            table_name: table name to enforce

        Raises:
            SurrealDBSetupBadIDError: If the raw ID string is not valid

        Returns:
            RecordID: Parsed RecordID object if valid, None otherwise
        """
        try:
            split_id = raw_id.split(":")
            if split_id[0] != table_name:
                msg = f"Invalid table name for ID: {raw_id}"
                raise SurrealDBSetupBadIDError(msg)
            return RecordID(split_id[0], split_id[1])
        except IndexError:
            raise SurrealDBSetupBadIDError

    def __init__(
        self,
        database: str | None = None,
        timeout: datetime.timedelta = datetime.timedelta(seconds=5),
    ) -> None:
        """Initialize the repository.

        Args:
            database: AsyncSurrealDB connection to a specific database
            table_name: Name of the table to interact with
            timeout: Timeout for database operations
        """
        self.timeout = timeout
        base_url = os.getenv("SURREALDB_URL", "ws://localhost").strip()
        port = (os.getenv("SURREALDB_PORT") or "").strip()
        self.url = f"{base_url}{f':{port}' if port else ''}/rpc"

        self.username = os.getenv("SURREALDB_USERNAME", "root")
        self.password = os.getenv("SURREALDB_PASSWORD", "root")
        self.namespace = os.getenv("SURREALDB_NAMESPACE", "test")
        self.database = database or os.getenv("SURREALDB_DATABASE", "task_manager")

    async def init_surreal_instance(self) -> None:
        """Init a SurrealDB connection instance."""
        logger.debug("Connecting to SurrealDB at %s", self.url)
        self.db = AsyncSurreal(self.url)  # type: ignore
        await self.db.signin({"username": self.username, "password": self.password})
        await self.db.use(self.namespace, self.database)
        logger.debug("Successfully connected to SurrealDB")

    async def close(self) -> None:
        """Close the SurrealDB connection if it exists."""
        logger.debug("Closing SurrealDB connection")
        await self.db.close()

    async def create(
        self,
        table_name: str,
        data: dict[str, Any],
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Create a new record.

        Args:
            table_name: Name of the table to insert into
            data: Data to insert

        Returns:
            Dict[str, Any]: The created record as returned by the database
        """
        logger.debug("Creating record in %s with data: %s", table_name, data)
        result = await self.db.create(table_name, data)
        logger.debug("create result: %s", result)
        return result

    async def merge(
        self,
        table_name: str,
        record_id: str | RecordID,
        data: dict[str, Any],
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Update an existing record.

        Args:
            table_name: Name of the table to insert into
            record_id: record ID to update
            data: Data to insert

        Returns:
            Dict[str, Any]: The created record as returned by the database
        """
        if isinstance(record_id, str):
            # validate surrealDB id if raw str
            record_id = self._valid_id(record_id, table_name)
        logger.debug("Updating record in %s with data: %s", record_id, data)
        result = await self.db.merge(record_id, data)
        logger.debug("update result: %s", result)
        return result

    async def update(
        self,
        table_name: str,
        record_id: str | RecordID,
        data: dict[str, Any],
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Update an existing record.

        Args:
            table_name: Name of the table to insert into
            record_id: record ID to update
            data: Data to insert

        Returns:
            Dict[str, Any]: The created record as returned by the database
        """
        if isinstance(record_id, str):
            # validate surrealDB id if raw str
            record_id = self._valid_id(record_id, table_name)
        logger.debug("Updating record in %s with data: %s", record_id, data)
        result = await self.db.update(record_id, data)
        logger.debug("update result: %s", result)
        return result

    async def execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a custom SurrealQL query.

        Args:
            query: SurrealQL query
            params: Query parameters

        Returns:
            List[Dict[str, Any]]: Query results
        """
        logger.debug("execute_query: %s with params: %s", query, params)
        result = await self.db.query(query, params or {})
        logger.debug("execute_query result: %s", result)
        return [result] if isinstance(result, dict) else result

    async def select_by_task_id(self, table: str, value: str) -> dict[str, Any]:
        """Fetch a record from a table by a unique field.

        Args:
            table: Table name
            value: Field value to match

        Raises:
            ValueError: If no records are found

        Returns:
            Dict with record data if found, else None
        """
        query = "SELECT * FROM type::table($table) WHERE task_id = $value;"
        params = {"table": table, "value": value}

        result = await self.execute_query(query, params)
        if not result:
            msg = f"No records found in table '{table}' with task_id '{value}'"
            logger.error(msg)
            raise ValueError(msg)

        return result[0]

    async def start_live(
        self,
        table_name: str,
    ) -> tuple[UUID, AsyncGenerator[dict[str, Any], None]]:
        """Create and subscribe to a live SurrealQL query.

        Args:
            table_name: Name of the table to insert into

        Returns:
            List[Dict[str, Any]]: Query results
        """
        live_id = await self.db.live(table_name, diff=False)
        return live_id, await self.db.subscribe_live(live_id)

    async def stop_live(self, live_id: UUID) -> None:
        """Kill a live SurrealQL query.

        Args:
            live_id: record ID to watch for
        """
        logger.debug("KILL Subscribe live for: %s", live_id)
        await self.db.kill(live_id)
