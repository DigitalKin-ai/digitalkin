"""Database fixtures for integration testing with real SurrealDB.

This module provides pytest fixtures for testing with real SurrealDB connections
instead of mocks, enabling proper integration testing of database interactions,
live queries, connection management, and cleanup.
"""

import datetime
import os
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection

# Test database configuration
TEST_DB_CONFIG = {
    "SURREALDB_URL": os.getenv("TEST_SURREALDB_URL", "ws://localhost"),
    "SURREALDB_PORT": os.getenv("TEST_SURREALDB_PORT", "8008"),
    "SURREALDB_USERNAME": os.getenv("TEST_SURREALDB_USERNAME", "root"),
    "SURREALDB_PASSWORD": os.getenv("TEST_SURREALDB_PASSWORD", "root"),
    "SURREALDB_NAMESPACE": "test_surreal",
    "SURREALDB_DATABASE": "test_surreal",
}


@pytest_asyncio.fixture
async def real_db_connection() -> AsyncGenerator[SurrealDBConnection, None]:
    """Provide a real SurrealDB connection for integration tests.

    This fixture:
    - Sets up test environment variables
    - Initializes a real SurrealDB connection
    - Yields connection for test usage
    - Properly closes connection after test (killing live queries, closing websocket)

    Yields:
        SurrealDBConnection: An initialized connection to the test database

    Example:
        async def test_with_real_db(real_db_connection):
            result = await real_db_connection.create("tasks", {"status": "pending"})
            assert result is not None
    """
    # Set test environment
    for key, value in TEST_DB_CONFIG.items():
        os.environ[key] = value

    connection = SurrealDBConnection(
        database=TEST_DB_CONFIG["SURREALDB_DATABASE"],
        timeout=datetime.timedelta(seconds=10),
    )

    try:
        await connection.init_surreal_instance()
    except (ConnectionError, OSError):
        pytest.skip("SurrealDB not available")

    try:
        yield connection
    finally:
        # Ensure all live queries are killed and connection closed
        await connection.close()


@pytest_asyncio.fixture
def clean_db_table(real_db_connection: SurrealDBConnection) -> Callable[[str], Awaitable[None]]:
    """Provide a function to clean database tables between tests.

    This fixture returns an async function that can be called to delete
    all records from a specified table, ensuring test isolation.

    Args:
        real_db_connection: The real database connection fixture

    Returns:
        Callable: An async function that takes a table name and cleans it

    Example:
        async def test_with_clean_tables(clean_db_table):
            await clean_db_table("tasks")
            await clean_db_table("heartbeats")
            # Test with clean tables
    """

    async def _clean(table_name: str) -> None:
        """Delete all records from specified table.

        Args:
            table_name: Name of the table to clean

        Note:
            Ignores errors if table doesn't exist yet
        """
        try:
            await real_db_connection.execute_query(f"DELETE {table_name};")
        except Exception:
            # Table might not exist yet, ignore errors
            pass

    return _clean


@pytest_asyncio.fixture
async def isolated_db_connection() -> AsyncGenerator[SurrealDBConnection, None]:
    """Provide an isolated DB connection with unique database name.

    Each test gets its own database to prevent interference.
    This is particularly useful for concurrent test execution and
    tests that modify global database state.

    Yields:
        SurrealDBConnection: An initialized connection to a unique test database

    Example:
        async def test_with_isolation(isolated_db_connection):
            # This test has its own database, won't conflict with others
            result = await isolated_db_connection.create("tasks", {"data": "test"})
    """
    # Set test environment
    for key, value in TEST_DB_CONFIG.items():
        os.environ[key] = value

    # Use unique database name based on UUID
    unique_db = f"test_db_{uuid.uuid4().hex[:8]}"

    connection = SurrealDBConnection(
        database=unique_db,
        timeout=datetime.timedelta(seconds=10),
    )

    try:
        await connection.init_surreal_instance()
    except (ConnectionError, OSError):
        pytest.skip("SurrealDB not available")

    try:
        yield connection
    finally:
        await connection.close()
