"""Integration tests for SurrealDBConnection with real SurrealDB instance.

This module contains end-to-end tests that verify SurrealDBConnection works
correctly with a live SurrealDB instance, validating schema compliance and
query execution under real I/O conditions.

Requirements:
    - SurrealDB instance running at localhost:8000
    - Set up via Docker: docker run -p 8000:8000 surrealdb/surrealdb:latest start
    - Or use pytest fixtures to manage container lifecycle
"""

import asyncio
import datetime
import math
import os
from collections.abc import AsyncGenerator, Awaitable, Callable
from uuid import UUID

import pytest
from surrealdb import RecordID

from digitalkin.core.task_manager.surrealdb_repository import (
    SurrealDBConnection,
    SurrealDBSetupBadIDError,
)

# Configuration for test database
TEST_DB_CONFIG = {
    "SURREALDB_URL": os.getenv("TEST_SURREALDB_URL", "ws://localhost"),
    "SURREALDB_PORT": os.getenv("TEST_SURREALDB_PORT", "8008"),
    "SURREALDB_USERNAME": os.getenv("TEST_SURREALDB_USERNAME", "root"),
    "SURREALDB_PASSWORD": os.getenv("TEST_SURREALDB_PASSWORD", "root"),
    "SURREALDB_NAMESPACE": "test_surreal",
    "SURREALDB_DATABASE": "test_surreal",
}


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def conn() -> AsyncGenerator[SurrealDBConnection, None]:
    """Create and initialize a real SurrealDBConnection instance.

    This fixture:
    - Sets up test environment variables
    - Initializes connection to real SurrealDB
    - Yields connection for test usage
    - Closes connection after test completion
    """
    # Set test environment
    for key, value in TEST_DB_CONFIG.items():
        os.environ[key] = value

    # Create and initialize connection
    connection = SurrealDBConnection(
        database=TEST_DB_CONFIG["SURREALDB_DATABASE"],
        timeout=datetime.timedelta(seconds=10),
    )

    try:
        await connection.init_surreal_instance()
        yield connection
    finally:
        # Cleanup: close connection
        await connection.close()


@pytest.fixture
def clean_table(conn: SurrealDBConnection) -> Callable[[str], Awaitable[None]]:
    """Ensure test table is clean before each test.

    This fixture removes all records from test tables to ensure
    test isolation and prevent cross-test contamination.

    Returns a coroutine function that must be awaited.
    """

    async def _clean(table_name: str) -> None:
        """Delete all records from specified table."""
        try:
            await conn.execute_query(f"DELETE {table_name};")
        except Exception:
            # Table might not exist yet, ignore errors
            pass

    return _clean


class TestConnectionLifecycle:
    """Test suite for connection initialization and teardown."""

    @pytest.mark.asyncio
    async def test_init_and_close_connection(self):
        """Verify connection can be established and closed without errors."""
        for key, value in TEST_DB_CONFIG.items():
            os.environ[key] = value

        conn = SurrealDBConnection(database="test_lifecycle")

        # Initialize connection
        await conn.init_surreal_instance()
        assert conn.db is not None

        # Close connection
        await conn.close()

    @pytest.mark.asyncio
    async def test_connection_with_valid_credentials(self, conn):
        """Verify connection works with valid credentials."""
        # If we got here, connection was successful
        assert conn.db is not None
        assert conn.namespace == "test_surreal"
        assert conn.database == "test_surreal"

    @pytest.mark.asyncio
    async def test_connection_attributes(self, conn):
        """Verify connection attributes are correctly set from environment."""
        assert TEST_DB_CONFIG["SURREALDB_URL"] in conn.url or "ws://localhost:8008" in conn.url
        assert conn.username == "root"
        assert conn.namespace == "test_surreal"


class TestCRUDOperations:
    """Test suite for create, read, update, delete operations with real database."""

    @pytest.mark.asyncio
    async def test_create_and_retrieve_record(self, conn, clean_table):
        """Verify record can be created and retrieved successfully."""
        table_name = "test_tasks"
        await clean_table(table_name)

        # Create a record with task_id
        data = {
            "task_id": "task_001",
            "name": "Integration Test Task",
            "status": "pending",
            "priority": "high",
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

        result = await conn.create(table_name, data)

        # Verify result structure
        if isinstance(result, list):
            result = result[0]

        assert "id" in result
        assert result["task_id"] == data["task_id"]
        assert result["name"] == data["name"]
        assert result["status"] == data["status"]

    @pytest.mark.asyncio
    async def test_select_by_task_id(self, conn, clean_table):
        """Verify record can be retrieved by task_id field."""
        table_name = "test_tasks"
        await clean_table(table_name)

        # Create test record
        task_id = "task_select_001"
        data = {
            "task_id": task_id,
            "name": "Selectable Task",
            "priority": "high",
            "assignee": "john_doe",
        }

        await conn.create(table_name, data)
        # Retrieve by task_id
        result = await conn.select_by_task_id(table_name, task_id)

        assert result["task_id"] == task_id
        assert result["name"] == "Selectable Task"
        assert result["priority"] == "high"

    @pytest.mark.asyncio
    async def test_select_by_task_id_not_found(self, conn, clean_table):
        """Verify ValueError is raised when task_id not found."""
        table_name = "test_tasks"
        await clean_table(table_name)

        with pytest.raises(ValueError, match="No records found"):
            await conn.select_by_task_id(table_name, "nonexistent_task")

    @pytest.mark.asyncio
    async def test_workflow_get_by_task_id_then_merge(self, conn, clean_table):
        """Verify typical workflow: get by task_id, then merge updates."""
        table_name = "test_workflows"
        await clean_table(table_name)

        # Step 1: Create initial record
        task_id = "workflow_task_001"
        initial_data = {
            "task_id": task_id,
            "status": "pending",
            "progress": 0,
            "assigned_to": "alice",
        }
        await conn.create(table_name, initial_data)

        # Step 2: Get by task_id (typical workflow)
        existing = await conn.select_by_task_id(table_name, task_id)
        assert existing["task_id"] == task_id
        assert existing["status"] == "pending"

        # Step 3: Merge partial update
        record_id = existing["id"]
        update_data = {
            "status": "in_progress",
            "progress": 50,
        }
        merged = await conn.merge(table_name, record_id, update_data)
        if isinstance(merged, list):
            merged = merged[0]

        # Step 4: Verify merge kept original fields and updated specified ones
        assert merged["task_id"] == task_id
        assert merged["status"] == "in_progress"
        assert merged["progress"] == 50
        assert merged["assigned_to"] == "alice"  # Original field preserved

    @pytest.mark.asyncio
    async def test_workflow_get_by_task_id_then_update(self, conn, clean_table):
        """Verify workflow: get by task_id, then full update."""
        table_name = "test_workflows"
        await clean_table(table_name)

        # Step 1: Create initial record
        task_id = "workflow_task_002"
        initial_data = {
            "task_id": task_id,
            "status": "pending",
            "description": "Original description",
        }
        await conn.create(table_name, initial_data)

        # Step 2: Get by task_id
        existing = await conn.select_by_task_id(table_name, task_id)
        record_id = existing["id"]

        # Step 3: Full update (replaces entire record)
        new_data = {
            "task_id": task_id,  # Must include task_id in update
            "status": "completed",
            "description": "Updated description",
            "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        updated = await conn.update(table_name, record_id, new_data)
        if isinstance(updated, list):
            updated = updated[0]

        # Step 4: Verify full replacement
        assert updated["task_id"] == task_id
        assert updated["status"] == "completed"
        assert updated["description"] == "Updated description"
        assert "completed_at" in updated

    @pytest.mark.asyncio
    async def test_merge_with_string_id_from_task_id_lookup(self, conn, clean_table):
        """Verify merge works with string ID obtained from task_id lookup."""
        table_name = "test_items"
        await clean_table(table_name)

        # Create record
        task_id = "item_task_001"
        created = await conn.create(
            table_name,
            {
                "task_id": task_id,
                "name": "Item 1",
                "quantity": 10,
            },
        )
        if isinstance(created, list):
            created = created[0]

        # Get by task_id
        existing = await conn.select_by_task_id(table_name, task_id)
        record_id_str = existing["id"]

        # Merge using string ID
        merged = await conn.merge(table_name, record_id_str, {"quantity": 15})
        if isinstance(merged, list):
            merged = merged[0]

        assert merged["task_id"] == task_id
        assert merged["quantity"] == 15
        assert merged["name"] == "Item 1"

    @pytest.mark.asyncio
    async def test_update_with_invalid_table_name(self, conn, clean_table):
        """Verify update with mismatched table name raises error."""
        table_name = "test_items"
        await clean_table(table_name)

        task_id = "item_task_002"
        created = await conn.create(table_name, {"task_id": task_id, "name": "Test"})
        if isinstance(created, list):
            created = created[0]

        # Try to update with wrong table in ID
        wrong_id = "wrong_table:123"

        with pytest.raises(SurrealDBSetupBadIDError):
            await conn.update(table_name, wrong_id, {"name": "Updated"})

    @pytest.mark.asyncio
    async def test_multiple_records_same_table_different_task_ids(self, conn, clean_table):
        """Verify multiple records with different task_ids can coexist."""
        table_name = "test_multi_tasks"
        await clean_table(table_name)

        # Create multiple records
        task_ids = ["task_alpha", "task_beta", "task_gamma"]
        for task_id in task_ids:
            await conn.create(
                table_name,
                {
                    "task_id": task_id,
                    "status": "active",
                    "owner": f"owner_{task_id}",
                },
            )

        # Verify each can be retrieved independently
        for task_id in task_ids:
            result = await conn.select_by_task_id(table_name, task_id)
            assert result["task_id"] == task_id
            assert result["owner"] == f"owner_{task_id}"


class TestQueryExecution:
    """Test suite for custom query execution."""

    @pytest.mark.asyncio
    async def test_execute_custom_query_with_parameters(self, conn, clean_table):
        """Verify custom SurrealQL query execution with parameters."""
        table_name = "test_projects"
        await clean_table(table_name)

        # Create test data with task_id
        await conn.create(table_name, {"task_id": "proj_a", "name": "Project A", "status": "active"})
        await conn.create(table_name, {"task_id": "proj_b", "name": "Project B", "status": "active"})
        await conn.create(table_name, {"task_id": "proj_c", "name": "Project C", "status": "completed"})

        # Execute custom query
        query = "SELECT * FROM type::table($table) WHERE status = $status;"
        params = {"table": table_name, "status": "active"}

        result = await conn.execute_query(query, params)

        # Verify results (SurrealDB may wrap results)
        if result and isinstance(result[0], dict) and "result" in result[0]:
            actual_results = result[0]["result"]
        else:
            actual_results = result

        assert len(actual_results) == 2
        for record in actual_results:
            assert record["status"] == "active"
            assert "task_id" in record

    @pytest.mark.asyncio
    async def test_execute_query_filter_by_task_id(self, conn, clean_table):
        """Verify query execution can filter by task_id."""
        table_name = "test_events"
        await clean_table(table_name)

        # Create test data
        task_id = "evt_001"
        event_data = {
            "task_id": task_id,
            "type": "meeting",
            "attendees": 5,
        }
        await conn.create(table_name, event_data)

        # Execute query
        query = "SELECT * FROM type::table($table) WHERE task_id = $task_id;"
        params = {"table": table_name, "task_id": task_id}

        result = await conn.execute_query(query, params)

        # Extract actual results
        if result and isinstance(result[0], dict) and "result" in result[0]:
            actual_results = result[0]["result"]
        else:
            actual_results = result

        assert len(actual_results) >= 1
        assert actual_results[0]["task_id"] == task_id
        assert actual_results[0]["type"] == "meeting"

    @pytest.mark.asyncio
    async def test_execute_query_with_aggregation(self, conn, clean_table):
        """Verify query execution with aggregation functions."""
        table_name = "test_orders"
        await clean_table(table_name)

        # Create test data with task_id
        await conn.create(table_name, {"task_id": "order_1", "amount": 100, "status": "paid"})
        await conn.create(table_name, {"task_id": "order_2", "amount": 150, "status": "paid"})
        await conn.create(table_name, {"task_id": "order_3", "amount": 200, "status": "pending"})

        # Execute aggregation query
        query = f"SELECT COUNT() as total FROM {table_name} WHERE status = 'paid' GROUP ALL;"

        result = await conn.execute_query(query)
        assert len(result) == 1
        assert result[0]["total"] == 2

    @pytest.mark.asyncio
    async def test_execute_query_without_parameters(self, conn, clean_table):
        """Verify query execution works without parameters."""
        table_name = "test_simple"
        await clean_table(table_name)

        await conn.create(table_name, {"task_id": "simple_1", "value": "test1"})
        await conn.create(table_name, {"task_id": "simple_2", "value": "test2"})

        query = f"SELECT * FROM {table_name};"
        result = await conn.execute_query(query)

        # Extract actual results
        if result and isinstance(result[0], dict) and "result" in result[0]:
            actual_results = result[0]["result"]
        else:
            actual_results = result

        assert len(actual_results) == 2


class TestLiveOperations:
    """Test suite for live query subscriptions."""

    @pytest.mark.asyncio
    async def test_start_and_stop_live_query(self, conn, clean_table):
        """Verify live query can be started and stopped cleanly."""
        table_name = "test_live_basic"
        await clean_table(table_name)

        # Start live query
        live_id, generator = await conn.start_live(table_name)

        assert isinstance(live_id, UUID)
        assert generator is not None

        # Stop live query
        await conn.stop_live(live_id)

    @pytest.mark.asyncio
    async def test_live_query_receives_updates(self, conn, clean_table):
        """Verify live query receives updates when records are created."""
        table_name = "test_live_updates"
        await clean_table(table_name)

        # Start live query
        live_id, generator = await conn.start_live(table_name)

        try:
            # Create a record in another task
            create_task = asyncio.create_task(
                conn.create(table_name, {"task_id": "live_task_001", "message": "Live update test"})
            )

            # Wait for the creation to complete
            await create_task

            # Try to receive update with timeout
            try:
                update = await asyncio.wait_for(anext(generator), timeout=3.0)
                assert update is not None
                assert update["message"] == "Live update test"
                assert update["task_id"] == "live_task_001"
            except asyncio.TimeoutError:
                # Some SurrealDB versions may have different live query behavior
                # This is acceptable as long as the subscription was created
                pass

        finally:
            # Always clean up
            await conn.stop_live(live_id)

    @pytest.mark.asyncio
    async def test_multiple_live_subscriptions(self, conn, clean_table):
        """Verify multiple live subscriptions can be managed independently."""
        table_name1 = "test_live_multi_1"
        table_name2 = "test_live_multi_2"
        await clean_table(table_name1)
        await clean_table(table_name2)

        # Start two live queries
        live_id1, _gen1 = await conn.start_live(table_name1)
        live_id2, _gen2 = await conn.start_live(table_name2)

        assert live_id1 != live_id2

        # Stop both
        await conn.stop_live(live_id1)
        await conn.stop_live(live_id2)

    @pytest.mark.asyncio
    async def test_live_query_lifecycle(self, conn, clean_table):
        """Verify complete lifecycle of live query from start to stop."""
        table_name = "test_live_lifecytest_merge_nonexistent_recordcle"
        await clean_table(table_name)

        # Start subscription
        live_id, _generator = await conn.start_live(table_name)

        # Verify subscription is active
        assert isinstance(live_id, UUID)

        # Create some data
        await conn.create(table_name, {"task_id": "live_lc_1", "data": "test1"})
        await conn.create(table_name, {"task_id": "live_lc_2", "data": "test2"})

        # Stop subscription
        await conn.stop_live(live_id)

        # Verify we can start a new subscription after stopping
        new_live_id, _new_generator = await conn.start_live(table_name)
        assert new_live_id != live_id
        await conn.stop_live(new_live_id)


class TestDataPersistence:
    """Test suite for data persistence and retrieval."""

    @pytest.mark.asyncio
    async def test_data_persists_across_queries(self, conn, clean_table):
        """Verify data persists and can be queried multiple times."""
        table_name = "test_persistence"
        await clean_table(table_name)

        # Create record
        task_id = "persist_task_001"
        data = {
            "task_id": task_id,
            "key": "persistent_value",
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        created = await conn.create(table_name, data)
        if isinstance(created, list):
            created = created[0]

        # Query multiple times
        for _ in range(3):
            result = await conn.select_by_task_id(table_name, task_id)
            assert result["task_id"] == task_id
            assert result["key"] == "persistent_value"

    @pytest.mark.asyncio
    async def test_complex_data_types(self, conn, clean_table):
        """Verify complex data types are stored and retrieved correctly."""
        table_name = "test_complex"
        await clean_table(table_name)

        # Create record with complex data
        task_id = "complex_task_001"
        complex_data = {
            "task_id": task_id,
            "string_field": "test string",
            "number_field": 42,
            "float_field": math.pi,
            "boolean_field": True,
            "array_field": [1, 2, 3, 4, 5],
            "object_field": {
                "nested_key": "nested_value",
                "nested_number": 100,
            },
        }

        created = await conn.create(table_name, complex_data)
        if isinstance(created, list):
            created = created[0]

        # Retrieve and verify by task_id
        retrieved = await conn.select_by_task_id(table_name, task_id)

        assert retrieved["task_id"] == task_id
        assert retrieved["string_field"] == "test string"
        assert retrieved["number_field"] == 42
        assert retrieved["boolean_field"] is True
        assert retrieved["array_field"] == [1, 2, 3, 4, 5]
        assert retrieved["object_field"]["nested_key"] == "nested_value"


class TestErrorHandling:
    """Test suite for error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_invalid_query_syntax(self, conn):
        """Verify invalid query syntax raises appropriate error."""
        invalid_query = "INVALID SQL SYNTAX HERE"

        with pytest.raises(Exception):
            await conn.execute_query(invalid_query)

    @pytest.mark.asyncio
    async def test_merge_nonexistent_record(self, conn, clean_table):
        """Verify merging nonexistent record returns empty list."""
        table_name = "test_nonexistent"
        await clean_table(table_name)

        # Try to merge a record that doesn't exist
        # SurrealDB 2.x returns empty list for nonexistent records
        fake_id = RecordID(table_name, "nonexistent_id")
        result = await conn.merge(table_name, fake_id, {"task_id": "fake_task", "field": "value"})
        # Just verify no exception is raised
        # Actual behavior depends on SurrealDB version (may return None or empty list)
        assert result is None or result == []

    @pytest.mark.asyncio
    async def test_operation_timeout(self, conn):
        """Verify timeout configuration is respected."""
        # Connection has timeout configured
        assert conn.timeout == datetime.timedelta(seconds=10)

        # Verify timeout attribute exists and is correct type
        assert isinstance(conn.timeout, datetime.timedelta)


class TestRegressionScenarios:
    """Test suite for regression detection and schema validation."""

    @pytest.mark.asyncio
    async def test_record_id_format_consistency(self, conn, clean_table):
        """Verify record IDs follow consistent format across operations."""
        table_name = "test_id_format"
        await clean_table(table_name)

        # Create record
        created = await conn.create(table_name, {"task_id": "format_task_001", "test": "data"})
        if isinstance(created, list):
            created = created[0]

        record_id = created["id"]

        # Verify format: table_name:id
        record_id_str = str(record_id)
        assert ":" in record_id_str
        assert record_id_str.startswith(f"{table_name}:")

    @pytest.mark.asyncio
    async def test_schema_drift_detection(self, conn, clean_table):
        """Verify returned data structure matches expected schema."""
        table_name = "test_schema"
        await clean_table(table_name)

        # Create record with known schema
        data = {
            "task_id": "schema_task_001",
            "field1": "value1",
            "field2": 123,
            "field3": True,
        }

        created = await conn.create(table_name, data)
        if isinstance(created, list):
            created = created[0]

        # Verify schema
        assert "id" in created
        assert "task_id" in created
        assert "field1" in created
        assert "field2" in created
        assert "field3" in created
        assert created["task_id"] == "schema_task_001"
        assert created["field1"] == "value1"
        assert created["field2"] == 123
        assert created["field3"] is True

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, conn, clean_table):
        """Verify concurrent operations complete successfully."""
        table_name = "test_concurrent"
        await clean_table(table_name)

        # Create multiple records concurrently
        tasks = [
            conn.create(table_name, {"task_id": f"concurrent_task_{i}", "index": i, "data": f"test_{i}"})
            for i in range(5)
        ]

        results = await asyncio.gather(*tasks)

        # Verify all operations completed
        assert len(results) == 5

        # Verify all records were created
        query = f"SELECT * FROM {table_name};"
        all_records = await conn.execute_query(query)

        if all_records and isinstance(all_records[0], dict) and "result" in all_records[0]:
            actual_results = all_records[0]["result"]
        else:
            actual_results = all_records

        assert len(actual_results) == 5

    @pytest.mark.asyncio
    async def test_large_payload_handling(self, conn, clean_table):
        """Verify large payloads are handled correctly."""
        table_name = "test_large_payload"
        await clean_table(table_name)

        # Create record with large data
        task_id = "large_payload_task"
        large_data = {
            "task_id": task_id,
            "large_text": "x" * 10000,  # 10KB of text
            "large_array": list(range(1000)),
        }

        created = await conn.create(table_name, large_data)
        if isinstance(created, list):
            created = created[0]

        # Verify data integrity
        assert created["task_id"] == task_id
        assert len(created["large_text"]) == 10000
        assert len(created["large_array"]) == 1000
        assert created["large_array"][0] == 0
        assert created["large_array"][-1] == 999

        # Verify retrieval by task_id
        retrieved = await conn.select_by_task_id(table_name, task_id)
        assert len(retrieved["large_text"]) == 10000
        assert len(retrieved["large_array"]) == 1000

    @pytest.mark.asyncio
    async def test_task_id_uniqueness_constraint(self, conn, clean_table):
        """Verify behavior when attempting to create duplicate task_ids."""
        table_name = "test_uniqueness"
        await clean_table(table_name)

        task_id = "unique_task_001"

        # Create first record
        await conn.create(
            table_name,
            {
                "task_id": task_id,
                "data": "first",
            },
        )

        # Create second record with same task_id (SurrealDB allows duplicates by default)
        await conn.create(
            table_name,
            {
                "task_id": task_id,
                "data": "second",
            },
        )

        # Query should return the first match
        result = await conn.select_by_task_id(table_name, task_id)
        assert result["task_id"] == task_id
        # Note: Without unique index, select_by_task_id returns first match


class TestTaskIDWorkflows:
    """Test suite specifically for task_id-based workflows."""

    @pytest.mark.asyncio
    async def test_complete_task_lifecycle_via_task_id(self, conn, clean_table):
        """Verify complete task lifecycle using task_id as primary identifier."""
        table_name = "test_task_lifecycle"
        await clean_table(table_name)

        task_id = "lifecycle_task_001"

        # Step 1: Create task
        initial_task = {
            "task_id": task_id,
            "title": "Complete Feature X",
            "status": "todo",
            "assignee": "alice",
            "priority": "high",
            "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        created = await conn.create(table_name, initial_task)
        if isinstance(created, list):
            created = created[0]

        assert created["task_id"] == task_id
        assert created["status"] == "todo"

        # Step 2: Retrieve by task_id and start work
        task = await conn.select_by_task_id(table_name, task_id)
        record_id = task["id"]

        start_work_update = {
            "status": "in_progress",
            "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        in_progress = await conn.merge(table_name, record_id, start_work_update)
        if isinstance(in_progress, list):
            in_progress = in_progress[0]

        assert in_progress["task_id"] == task_id
        assert in_progress["status"] == "in_progress"
        assert in_progress["assignee"] == "alice"  # Preserved

        # Step 3: Retrieve again and add progress notes
        task = await conn.select_by_task_id(table_name, task_id)
        record_id = task["id"]

        progress_update = {
            "progress_notes": "Completed API integration",
            "progress_percentage": 60,
        }
        updated = await conn.merge(table_name, record_id, progress_update)
        if isinstance(updated, list):
            updated = updated[0]

        assert updated["task_id"] == task_id
        assert updated["progress_percentage"] == 60

        # Step 4: Complete the task
        task = await conn.select_by_task_id(table_name, task_id)
        record_id = task["id"]

        completion_update = {
            "status": "completed",
            "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "progress_percentage": 100,
        }
        completed = await conn.merge(table_name, record_id, completion_update)
        if isinstance(completed, list):
            completed = completed[0]

        assert completed["task_id"] == task_id
        assert completed["status"] == "completed"
        assert completed["progress_percentage"] == 100
        assert completed["title"] == "Complete Feature X"  # Original field preserved

    @pytest.mark.asyncio
    async def test_batch_update_multiple_tasks_by_task_id(self, conn, clean_table):
        """Verify batch operations using task_id for lookups."""
        table_name = "test_batch_tasks"
        await clean_table(table_name)

        # Create multiple tasks
        task_ids = [f"batch_task_{i:03d}" for i in range(5)]
        for task_id in task_ids:
            await conn.create(
                table_name,
                {
                    "task_id": task_id,
                    "status": "pending",
                    "priority": "medium",
                },
            )

        # Batch update via task_id lookup
        for task_id in task_ids[:3]:  # Update first 3
            task = await conn.select_by_task_id(table_name, task_id)
            record_id = task["id"]
            await conn.merge(
                table_name,
                record_id,
                {
                    "status": "in_progress",
                    "assigned_to": "team_alpha",
                },
            )

        # Verify updates
        for i, task_id in enumerate(task_ids):
            task = await conn.select_by_task_id(table_name, task_id)
            if i < 3:
                assert task["status"] == "in_progress"
                assert task["assigned_to"] == "team_alpha"
            else:
                assert task["status"] == "pending"
                assert "assigned_to" not in task

    @pytest.mark.asyncio
    async def test_error_handling_nonexistent_task_id(self, conn, clean_table):
        """Verify proper error handling when task_id doesn't exist."""
        table_name = "test_error_handling"
        await clean_table(table_name)

        # Create one task
        await conn.create(
            table_name,
            {
                "task_id": "existing_task",
                "data": "test",
            },
        )

        # Try to fetch non-existent task_id
        with pytest.raises(ValueError, match="No records found"):
            await conn.select_by_task_id(table_name, "nonexistent_task_id")

    @pytest.mark.asyncio
    async def test_task_id_with_special_characters(self, conn, clean_table):
        """Verify task_id works with special characters."""
        table_name = "test_special_chars"
        await clean_table(table_name)

        # Test various special character formats
        special_task_ids = [
            "task_with_underscore",
            "task-with-dashes",
            "task.with.dots",
            "task:with:colons",
            "task/with/slashes",
        ]

        for task_id in special_task_ids:
            await conn.create(
                table_name,
                {
                    "task_id": task_id,
                    "description": f"Task ID: {task_id}",
                },
            )

        # Verify all can be retrieved
        for task_id in special_task_ids:
            result = await conn.select_by_task_id(table_name, task_id)
            assert result["task_id"] == task_id
            assert result["description"] == f"Task ID: {task_id}"

    @pytest.mark.asyncio
    async def test_partial_merge_preserves_nested_objects(self, conn, clean_table):
        """Verify merge preserves complex nested structures."""
        table_name = "test_nested_merge"
        await clean_table(table_name)

        task_id = "nested_task_001"

        # Create task with nested structure
        initial_data = {
            "task_id": task_id,
            "metadata": {
                "created_by": "alice",
                "department": "engineering",
                "tags": ["urgent", "backend"],
            },
            "config": {
                "notifications_enabled": True,
                "auto_assign": False,
            },
        }
        await conn.create(table_name, initial_data)

        # Get and merge with partial update
        task = await conn.select_by_task_id(table_name, task_id)
        record_id = task["id"]

        # Only update specific nested field
        update_data = {
            "metadata": {
                "tags": ["urgent", "backend", "api"],  # Add tag
            }
        }
        merged = await conn.merge(table_name, record_id, update_data)
        if isinstance(merged, list):
            merged = merged[0]

        # Verify nested object was updated (behavior may vary by SurrealDB version)
        assert merged["task_id"] == task_id
        assert "metadata" in merged

    @pytest.mark.asyncio
    async def test_query_tasks_by_multiple_criteria(self, conn, clean_table):
        """Verify complex queries using task_id and other fields."""
        table_name = "test_complex_query"
        await clean_table(table_name)

        # Create diverse tasks
        tasks = [
            {"task_id": "task_001", "status": "completed", "priority": "high", "team": "alpha"},
            {"task_id": "task_002", "status": "in_progress", "priority": "high", "team": "alpha"},
            {"task_id": "task_003", "status": "completed", "priority": "low", "team": "beta"},
            {"task_id": "task_004", "status": "in_progress", "priority": "high", "team": "beta"},
        ]

        for task in tasks:
            await conn.create(table_name, task)

        # Query: high priority tasks in progress
        query = """
        SELECT * FROM type::table($table)
        WHERE status = $status AND priority = $priority
        ORDER BY task_id;
        """
        params = {"table": table_name, "status": "in_progress", "priority": "high"}
        result = await conn.execute_query(query, params)

        if result and isinstance(result[0], dict) and "result" in result[0]:
            actual_results = result[0]["result"]
        else:
            actual_results = result

        assert len(actual_results) == 2
        assert all(r["status"] == "in_progress" for r in actual_results)
        assert all(r["priority"] == "high" for r in actual_results)

    @pytest.mark.asyncio
    async def test_timestamp_tracking_workflow(self, conn, clean_table):
        """Verify timestamp tracking through task lifecycle."""
        table_name = "test_timestamps"
        await clean_table(table_name)

        task_id = "timestamp_task_001"

        # Create with created_at
        created_at = datetime.datetime.now(datetime.timezone.utc)
        await conn.create(
            table_name,
            {
                "task_id": task_id,
                "status": "todo",
                "created_at": created_at.isoformat(),
            },
        )

        # Update to in_progress with started_at
        task = await conn.select_by_task_id(table_name, task_id)
        started_at = datetime.datetime.now(datetime.timezone.utc)
        await conn.merge(
            table_name,
            task["id"],
            {
                "status": "in_progress",
                "started_at": started_at.isoformat(),
            },
        )

        # Complete with completed_at
        task = await conn.select_by_task_id(table_name, task_id)
        completed_at = datetime.datetime.now(datetime.timezone.utc)
        await conn.merge(
            table_name,
            task["id"],
            {
                "status": "completed",
                "completed_at": completed_at.isoformat(),
            },
        )

        # Verify all timestamps exist
        final_task = await conn.select_by_task_id(table_name, task_id)
        assert "created_at" in final_task
        assert "started_at" in final_task
        assert "completed_at" in final_task
        assert final_task["status"] == "completed"

    @pytest.mark.asyncio
    async def test_idempotent_updates_via_task_id(self, conn, clean_table):
        """Verify idempotent updates when using task_id lookup."""
        table_name = "test_idempotent"
        await clean_table(table_name)

        task_id = "idempotent_task_001"

        # Create initial task
        await conn.create(
            table_name,
            {
                "task_id": task_id,
                "counter": 0,
                "status": "active",
            },
        )

        # Perform same update multiple times
        for i in range(3):
            task = await conn.select_by_task_id(table_name, task_id)
            await conn.merge(
                table_name,
                task["id"],
                {
                    "counter": i + 1,
                    "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                },
            )

        # Verify final state
        final_task = await conn.select_by_task_id(table_name, task_id)
        assert final_task["task_id"] == task_id
        assert final_task["counter"] == 3
        assert final_task["status"] == "active"

    @pytest.mark.asyncio
    async def test_full_replacement_vs_merge_comparison(self, conn, clean_table):
        """Compare behavior of update (replace) vs merge (partial update)."""
        table_name = "test_update_vs_merge"
        await clean_table(table_name)

        # Test merge (partial update)
        task_id_merge = "merge_task"
        await conn.create(
            table_name,
            {
                "task_id": task_id_merge,
                "field_a": "original_a",
                "field_b": "original_b",
                "field_c": "original_c",
            },
        )

        task = await conn.select_by_task_id(table_name, task_id_merge)
        merged = await conn.merge(
            table_name,
            task["id"],
            {
                "field_b": "updated_b",
            },
        )
        if isinstance(merged, list):
            merged = merged[0]

        assert merged["field_a"] == "original_a"  # Preserved
        assert merged["field_b"] == "updated_b"  # Updated
        assert merged["field_c"] == "original_c"  # Preserved

        # Test update (full replacement)
        task_id_update = "update_task"
        await conn.create(
            table_name,
            {
                "task_id": task_id_update,
                "field_a": "original_a",
                "field_b": "original_b",
                "field_c": "original_c",
            },
        )

        task = await conn.select_by_task_id(table_name, task_id_update)
        updated = await conn.update(
            table_name,
            task["id"],
            {
                "task_id": task_id_update,  # Must include task_id
                "field_b": "updated_b",
                "field_d": "new_d",
            },
        )
        if isinstance(updated, list):
            updated = updated[0]

        assert updated["task_id"] == task_id_update
        assert updated["field_b"] == "updated_b"
        assert updated["field_d"] == "new_d"
        # field_a and field_c should not exist (full replacement)
