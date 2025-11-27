"""SurrealDB connection mocks for testing.

Provides two types of mocks:
1. create_mock_surreal_connection(): Simple Mock object with AsyncMock methods
2. StatefulMockSurrealConnection: Class-based mock with operation tracking

Usage:
    # Simple mock (most tests)
    conn = create_mock_surreal_connection()

    # Custom overrides
    conn = create_mock_surreal_connection(
        create=AsyncMock(return_value={"id": "custom_id"})
    )

    # Stateful mock (integration tests)
    conn = StatefulMockSurrealConnection()
    await conn.create("tasks", {"task_id": "test"})
    assert len(conn.created) == 1
"""

from typing import Any
from unittest.mock import AsyncMock, Mock

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection


def create_mock_surreal_connection(**overrides: Any) -> Mock:
    """Factory for creating mock SurrealDB connections.

    Creates a Mock object with spec=SurrealDBConnection and pre-configured
    AsyncMock methods for all common operations.

    Args:
        **overrides: Override specific methods or attributes.
            Example: create=AsyncMock(return_value={"id": "custom"})

    Returns:
        Mock SurrealDBConnection with sensible defaults

    Example:
        # Basic usage
        conn = create_mock_surreal_connection()
        assert await conn.create("tasks", {}) == {"id": "mock_record_id"}

        # Custom behavior
        conn = create_mock_surreal_connection(
            create=AsyncMock(side_effect=Exception("DB error"))
        )
    """
    conn = Mock(spec=SurrealDBConnection)

    # Async methods with sensible defaults
    conn.init_surreal_instance = AsyncMock()
    conn.create = AsyncMock(return_value={"id": "mock_record_id"})
    conn.update = AsyncMock(return_value={"status": "updated"})
    conn.select_by_task_id = AsyncMock(return_value=None)
    conn.close = AsyncMock()
    conn.kill = AsyncMock()

    # Connection configuration attributes
    conn.url = "ws://localhost:8000/rpc"
    conn.username = "test"
    conn.password = "test"
    conn.namespace = "test"
    conn.database = "test"

    # Database client (for close operations)
    conn.db = Mock()

    # Apply any overrides
    for key, value in overrides.items():
        setattr(conn, key, value)

    return conn


class StatefulMockSurrealConnection:
    """Stateful mock SurrealDB connection with operation tracking.

    This mock maintains state and tracks all operations for assertions.
    Use this for integration tests that need to verify operation sequences.

    Attributes:
        created: List of all created records
        updated: List of all update operations
        queries: List of all query operations
        closed: Whether close() was called
        initialized: Whether init_surreal_instance() was called

    Example:
        conn = StatefulMockSurrealConnection()

        # Track operations
        await conn.create("tasks", {"task_id": "test"})
        await conn.update("tasks", "task_123", {"status": "completed"})

        # Assertions
        assert len(conn.created) == 1
        assert conn.created[0]["task_id"] == "test"
        assert len(conn.updated) == 1
    """

    def __init__(self) -> None:
        """Initialize stateful mock connection with empty state."""
        # Operation tracking
        self.created: list[dict[str, Any]] = []
        self.updated: list[dict[str, Any]] = []
        self.queries: list[dict[str, Any]] = []
        self.killed: list[str] = []

        # State flags
        self.closed = False
        self.initialized = False
        self.init_count = 0

        # Connection configuration attributes
        self.url = "ws://localhost:8000/rpc"
        self.username = "test"
        self.password = "test"
        self.namespace = "test"
        self.database = "test"

        # Live queries tracking
        self._live_queries: set[str] = set()

        # Database client mock
        self.db = Mock()

    async def init_surreal_instance(self) -> bool:
        """Mock initialization of SurrealDB connection.

        Returns:
            True indicating successful initialization
        """
        self.initialized = True
        self.init_count += 1
        return True

    async def create(self, table: str, record: dict[str, Any]) -> dict[str, Any]:
        """Mock creating a record in SurrealDB.

        Args:
            table: Table name
            record: Record data

        Returns:
            Record with generated ID
        """
        record_id = f"{table}_{len(self.created)}"
        record_with_id = {"id": record_id, **record}
        self.created.append(record_with_id)
        return record_with_id

    async def update(self, table: str, record_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Mock updating a record in SurrealDB.

        Args:
            table: Table name
            record_id: Record identifier
            payload: Update data

        Returns:
            Updated payload
        """
        update_op = {
            "table": table,
            "record_id": record_id,
            "payload": payload,
        }
        self.updated.append(update_op)
        return payload

    async def select_by_task_id(self, table: str, task_id: str) -> dict[str, Any] | None:
        """Mock selecting a record by task_id.

        Args:
            table: Table name
            task_id: Task identifier

        Returns:
            First matching record or None
        """
        self.queries.append({"table": table, "task_id": task_id})

        # Search through created records
        for record in self.created:
            if record.get("task_id") == task_id:
                return record

        return None

    async def kill(self, live_id: str) -> None:
        """Mock killing a live query.

        Args:
            live_id: Live query identifier
        """
        self.killed.append(live_id)
        self._live_queries.discard(live_id)

    async def close(self) -> None:
        """Mock closing the connection."""
        self.closed = True

        # Kill any remaining live queries
        for live_id in list(self._live_queries):
            await self.kill(live_id)

    def reset(self) -> None:
        """Reset all tracked state for test isolation."""
        self.created.clear()
        self.updated.clear()
        self.queries.clear()
        self.killed.clear()
        self.closed = False
        self.initialized = False
        self.init_count = 0
        self._live_queries.clear()
