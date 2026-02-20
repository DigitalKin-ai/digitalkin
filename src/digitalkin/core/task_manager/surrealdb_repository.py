"""SurrealDB connection management."""

import asyncio
import datetime
import os
from collections.abc import AsyncGenerator
from typing import Any, Generic, TypeVar, cast
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
    _live_queries: set[UUID]  # Track active live queries for cleanup
    _closed: bool  # Flag to prevent operations on closed connection

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
        self._live_queries = set()  # Initialize live queries tracker
        self._closed = False

    async def init_surreal_instance(self, max_retries: int = 3, retry_delay: float = 1.0) -> None:
        """Init a SurrealDB connection instance with retry logic and exponential backoff.

        Args:
            max_retries: Maximum number of connection attempts before giving up.
            retry_delay: Initial delay between retries in seconds (doubles each attempt).

        Raises:
            ConnectionError: If all retry attempts fail, with detailed error context.
        """
        last_exception: Exception | None = None

        for attempt in range(max_retries):
            try:
                logger.debug("SurrealDB connecting (attempt %d/%d)", attempt + 1, max_retries)
                self.db = AsyncSurreal(self.url)  # type: ignore[assignment]  # surrealdb typing not fully resolved

                # Wrap signin with timeout to catch handshake timeouts
                await asyncio.wait_for(
                    self.db.signin({"username": self.username, "password": self.password}),
                    timeout=self.timeout.total_seconds(),
                )
                await self.db.use(
                    self.namespace,
                    self.database,  # type: ignore[arg-type]  # surrealdb.use() accepts str but typed differently
                )

            except TimeoutError as e:
                last_exception = e
                error_msg = str(e) or "operation timed out"

                if "timed out during opening handshake" in error_msg:
                    logger.warning("SurrealDB handshake timeout (attempt %d/%d)", attempt + 1, max_retries)
                else:
                    logger.warning("SurrealDB timeout (attempt %d/%d): %s", attempt + 1, max_retries, error_msg)

            except ConnectionError as e:
                last_exception = e
                logger.warning("SurrealDB connection refused (attempt %d/%d): %s", attempt + 1, max_retries, e)

            except OSError as e:
                last_exception = e
                logger.warning("SurrealDB OS error (attempt %d/%d): %s", attempt + 1, max_retries, e)

            except Exception as e:
                last_exception = e
                error_msg = str(e)

                if "keepalive ping timeout" in error_msg:
                    logger.warning("SurrealDB keepalive timeout (attempt %d/%d)", attempt + 1, max_retries)
                else:
                    logger.warning(
                        "SurrealDB error (attempt %d/%d): %s: %s",
                        attempt + 1,
                        max_retries,
                        type(e).__name__,
                        error_msg,
                        exc_info=True,
                    )

            else:
                logger.info("SurrealDB connected (attempt %d/%d)", attempt + 1, max_retries)
                return

            # Retry with exponential backoff (but not after the last attempt)
            if attempt < max_retries - 1:
                delay = retry_delay * (2**attempt)  # Exponential backoff: 1s, 2s, 4s, ...
                logger.debug("SurrealDB retry in %.1fs (next attempt %d)", delay, attempt + 2)
                await asyncio.sleep(delay)

        # All retries exhausted
        error_type = type(last_exception).__name__ if last_exception else "Unknown"
        error_msg = str(last_exception) if last_exception else "No exception captured"

        final_error = (
            f"Failed to connect to SurrealDB after {max_retries} attempts. "
            f"URL: {self.url}, namespace: {self.namespace}, database: {self.database}. "
            f"Last error: {error_type}: {error_msg}"
        )
        logger.error("SurrealDB connection failed after %d attempts: %s: %s", max_retries, error_type, error_msg)
        raise ConnectionError(final_error) from last_exception

    async def close(self) -> None:
        """Close the SurrealDB connection if it exists.

        This will also kill all active live queries to prevent memory leaks.
        """
        self._closed = True
        # Kill all tracked live queries before closing connection
        if self._live_queries:
            logger.debug("Killing %d live queries before close", len(self._live_queries))
            live_query_ids = list(self._live_queries)

            # Kill all queries concurrently, capturing any exceptions
            results = await asyncio.gather(
                *[self.db.kill(live_id) for live_id in live_query_ids], return_exceptions=True
            )

            # Process results and track failures
            failed_queries = []
            for live_id, result in zip(live_query_ids, results):
                if isinstance(result, ConnectionError | TimeoutError | Exception):
                    failed_queries.append((live_id, str(result)))
                else:
                    self._live_queries.discard(live_id)

            # Log aggregated failures once instead of per-query
            if failed_queries:
                logger.warning("Failed to kill %d live queries", len(failed_queries))

        logger.debug("Closing SurrealDB connection")
        await self.db.close()

    async def create(
        self,
        table_name: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """Create a new record.

        Args:
            table_name: Name of the table to insert into
            data: Data to insert

        Returns:
            The created record as a single dict.

        Raises:
            RuntimeError: If the database returns an error or empty list.
        """
        result = await self.db.create(table_name, data)

        # Normalize list return (some driver versions return [dict] instead of dict)
        if isinstance(result, list):
            if not result:
                msg = f"SurrealDB create returned empty list for '{table_name}'"
                raise RuntimeError(msg)
            result = result[0]

        # Check for error response from SurrealDB
        if isinstance(result, dict) and "code" in result:
            error_msg = result.get("message", result.get("information", "Unknown error"))
            logger.error("SurrealDB create failed [%s]: %s", result.get("code"), error_msg)
            msg = f"SurrealDB create failed in '{table_name}': {error_msg}"  # type: ignore[str-bytes-safe]
            raise RuntimeError(msg)

        return cast("dict[str, Any]", result)

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
        result = await self.db.merge(record_id, data)
        return cast("list[dict[str, Any]] | dict[str, Any]", result)

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
        result = await self.db.update(record_id, data)
        return cast("list[dict[str, Any]] | dict[str, Any]", result)

    async def execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a custom SurrealQL query.

        Args:
            query: SurrealQL query
            params: Query parameters

        Returns:
            List[Dict[str, Any]]: Query results
        """
        result = await self.db.query(query, params or {})
        return cast("list[dict[str, Any]]", [result] if isinstance(result, dict) else result)

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
            logger.error("No records found in %s for task_id %s", table, value)
            msg = f"No records found in table '{table}' with task_id '{value}'"
            raise ValueError(msg)

        return result[0]

    async def start_live(
        self,
        table_name: str,
    ) -> tuple[UUID, AsyncGenerator[dict[str, Any], None]]:
        """Create and subscribe to a live SurrealQL query.

        The live query ID is tracked to ensure proper cleanup on connection close.

        Args:
            table_name: Name of the table to insert into

        Returns:
            tuple[UUID, AsyncGenerator]: Live query ID and subscription generator
        """
        live_id = await self.db.live(table_name, diff=False)
        self._live_queries.add(live_id)  # Track for cleanup
        logger.debug("Live query %s started on %s (total: %d)", live_id, table_name, len(self._live_queries))
        return live_id, await self.db.subscribe_live(live_id)

    async def stop_live(self, live_id: UUID) -> None:
        """Kill a live SurrealQL query.

        Args:
            live_id: Live query ID to kill
        """
        if self._closed:
            self._live_queries.discard(live_id)
            return
        await self.db.kill(live_id)
        self._live_queries.discard(live_id)  # Remove from tracker
        logger.debug("Live query %s stopped (remaining: %d)", live_id, len(self._live_queries))
