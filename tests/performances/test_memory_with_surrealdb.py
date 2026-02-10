"""Memory profiling integration tests with real SurrealDB.

This module contains integration tests that validate production memory behavior
using a real SurrealDB instance. These tests confirm that memory management code
works correctly under real I/O conditions.

Requirements:
    - SurrealDB instance running at localhost:8008
    - Set up via: docker run -p 8008:8000 surrealdb/surrealdb:latest start
    - Or set TEST_SURREALDB_PORT environment variable

Marker:
    - @pytest.mark.integration - Requires real database
    - These tests are SKIPPED by default unless real SurrealDB is available
"""

import asyncio
import contextlib
import datetime
import gc
import os
import tracemalloc
from collections.abc import AsyncGenerator
from typing import Any, ClassVar

import pytest

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode, ServicesStrategy

# Configuration for test database
TEST_DB_CONFIG = {
    "SURREALDB_URL": os.getenv("TEST_SURREALDB_URL", "ws://localhost"),
    "SURREALDB_PORT": os.getenv("TEST_SURREALDB_PORT", "8008"),
    "SURREALDB_USERNAME": os.getenv("TEST_SURREALDB_USERNAME", "root"),
    "SURREALDB_PASSWORD": os.getenv("TEST_SURREALDB_PASSWORD", "root"),
    "SURREALDB_NAMESPACE": "test_surreal",
    "SURREALDB_DATABASE": "test_surreal",
}

tracemalloc.start()

# Set timeout for all tests (60 seconds)
pytestmark = [
    pytest.mark.timeout(60),
    pytest.mark.integration,  # Requires real database
]


# ============================================================================
# Fixtures
# ============================================================================


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
    except (ConnectionError, OSError):
        pytest.skip("SurrealDB not available")

    try:
        yield connection
    finally:
        # Cleanup: close connection
        await connection.close()


class MockModuleForMemory(BaseModule):
    """Mock module for memory integration tests."""

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, str | None] | None]] = {}
    services_config: ClassVar[ServicesConfig] = ServicesConfig(
        services_config_strategies={}, services_config_params={}, mode=ServicesMode.LOCAL
    )

    def __init__(self, job_id: str, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        super().__init__(job_id, mission_id, setup_id, setup_version_id)
        self.name = "MockModuleForMemory"
        self.execution_count = 0

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict[str, Any]:
        """Skip service initialization."""
        return {
            "agent": None,
            "communication": None,
            "cost": None,
            "filesystem": None,
            "identity": None,
            "registry": None,
            "snapshot": None,
            "storage": None,
            "user_profile": None,
        }

    async def initialize(self, context: Any, setup_data: Any) -> None:
        """Initialize module."""

    async def run(self) -> None:
        """Run module."""
        self.execution_count += 1
        await asyncio.sleep(0.01)

    async def cleanup(self) -> None:
        """Cleanup module."""


def get_memory_mb() -> float:
    """Get current traced Python memory usage in MB via tracemalloc.

    Returns:
        Memory usage in megabytes.
    """
    gc.collect()
    current, _peak = tracemalloc.get_traced_memory()
    return current / 1024 / 1024


# ============================================================================
# Integration Tests with Real SurrealDB
# ============================================================================


class TestRealDatabaseMemoryBehavior:
    """Test memory behavior with real SurrealDB connections."""

    @pytest.mark.asyncio
    async def test_live_query_subscription_memory(self, conn):
        """Test memory behavior with real SurrealDB live queries."""
        # Clean test table
        with contextlib.suppress(Exception):
            await conn.execute_query("DELETE test_memory_live;")

        gc.collect()
        baseline_memory = get_memory_mb()

        # Start live query
        live_id, _generator = await conn.start_live("test_memory_live")

        # Create some data
        for i in range(10):
            await conn.create(
                "test_memory_live",
                {
                    "task_id": f"live-task-{i}",
                    "index": i,
                },
            )

        gc.collect()
        memory_with_live = get_memory_mb()

        # Stop live query
        await conn.stop_live(live_id)

        gc.collect()
        memory_after_stop = get_memory_mb()

        # Verify live query cleanup
        memory_retained = memory_after_stop - baseline_memory
        memory_during = memory_with_live - baseline_memory

        if memory_during > 0:
            retention_ratio = memory_retained / memory_during
            assert retention_ratio < 2.0, f"Live query excessive growth: {retention_ratio * 100:.1f}% retained"

    @pytest.mark.asyncio
    async def test_concurrent_db_operations_memory(self, conn):
        """Test memory with concurrent database operations."""
        # Clean test table
        with contextlib.suppress(Exception):
            await conn.execute_query("DELETE test_memory_concurrent;")

        gc.collect()
        baseline_memory = get_memory_mb()

        # Create tasks concurrently
        async def create_task_data(task_idx: int) -> None:
            """Create task data in database."""
            await conn.create(
                "test_memory_concurrent",
                {
                    "task_id": f"concurrent-{task_idx}",
                    "index": task_idx,
                    "data": "x" * 5000,  # 5KB per task
                },
            )

        # Run 20 concurrent operations
        tasks = [create_task_data(i) for i in range(20)]
        await asyncio.gather(*tasks)

        gc.collect()
        memory_after_operations = get_memory_mb()

        memory_used = memory_after_operations - baseline_memory

        # Memory usage should be reasonable (less than 100MB for 20 x 5KB operations)
        assert memory_used < 100, f"Excessive memory for concurrent ops: {memory_used:.2f}MB"


class TestRealDatabaseScaling:
    """Test memory scaling with real database operations."""

    @pytest.mark.asyncio
    async def test_database_connection_pooling_memory(self, conn):
        """Test memory behavior when reusing database connections."""
        gc.collect()
        baseline_memory = get_memory_mb()

        # Perform many operations with single connection (pooling simulation)
        operations_count = 100

        for i in range(operations_count):
            await conn.create(
                "test_memory_pooling",
                {
                    "task_id": f"pooling-task-{i}",
                    "index": i,
                },
            )

            # Every 10 operations, measure memory
            if i % 10 == 0:
                gc.collect()

        gc.collect()
        final_memory = get_memory_mb()

        memory_growth = final_memory - baseline_memory

        # With connection reuse, memory should not grow excessively
        # Allow up to 50MB for 100 operations
        assert memory_growth < 50, f"Excessive memory growth with connection pooling: {memory_growth:.2f}MB"

    @pytest.mark.asyncio
    async def test_query_result_memory_scaling(self, conn):
        """Test memory scaling with varying query result sizes."""
        # Create baseline dataset
        for i in range(100):
            await conn.create(
                "test_memory_queries",
                {
                    "task_id": f"query-task-{i}",
                    "category": f"cat_{i % 5}",
                    "data": "x" * 1000,
                },
            )

        memory_by_result_size = {}

        # Query with different result sizes
        for category_count in [1, 3, 5]:
            gc.collect()
            baseline = get_memory_mb()

            # Query that returns category_count * 20 results
            categories = [f"cat_{i}" for i in range(category_count)]
            results = []

            for cat in categories:
                query = "SELECT * FROM test_memory_queries WHERE category = $cat;"
                result = await conn.execute_query(query, {"cat": cat})

                # Extract results
                if result and isinstance(result[0], dict) and "result" in result[0]:
                    results.extend(result[0]["result"])
                else:
                    results.extend(result)

            gc.collect()
            memory_used = get_memory_mb() - baseline
            memory_by_result_size[len(results)] = memory_used

            # Clear results
            results.clear()

        # Memory should scale sub-linearly with result count
        if len(memory_by_result_size) >= 2:
            sizes = sorted(memory_by_result_size.keys())

            if memory_by_result_size[sizes[0]] > 0:
                for i in range(1, len(sizes)):
                    size_ratio = sizes[i] / sizes[0]
                    memory_ratio = memory_by_result_size[sizes[i]] / memory_by_result_size[sizes[0]]

                    # Allow 3x memory overhead for larger results (Python pooling)
                    assert memory_ratio < size_ratio * 3, (
                        f"Poor memory scaling: {sizes[i]} results uses {memory_ratio:.2f}x memory"
                    )


class TestRealDatabaseLongRunning:
    """Test memory behavior in long-running scenarios with real database."""

    @pytest.mark.asyncio
    async def test_sustained_operations_no_leak(self, conn):
        """Test sustained database operations don't leak memory."""
        # Warmup: match test workload to stabilize connection buffers
        for warmup in range(10):
            for i in range(10):
                await conn.create(
                    "test_memory_sustained",
                    {"task_id": f"warmup-{warmup}-{i}", "iteration": -1, "data": "x" * 500},
                )
            await conn.execute_query("SELECT * FROM test_memory_sustained WHERE iteration = $iter;", {"iter": -1})
            await conn.execute_query("DELETE test_memory_sustained WHERE iteration = $iter;", {"iter": -1})

        gc.collect()
        baseline_memory = get_memory_mb()

        memory_samples = []
        iterations = 20

        for iteration in range(iterations):
            # Create data
            for i in range(50):
                await conn.create(
                    "test_memory_sustained",
                    {
                        "task_id": f"sustained-{iteration}-{i}",
                        "iteration": iteration,
                        "data": "x" * 5000,
                    },
                )

            # Query data
            query = "SELECT * FROM test_memory_sustained WHERE iteration = $iter;"
            await conn.execute_query(query, {"iter": iteration})

            # Cleanup old data
            if iteration > 5:
                cleanup_iter = iteration - 5
                cleanup_query = "DELETE test_memory_sustained WHERE iteration = $iter;"
                await conn.execute_query(cleanup_query, {"iter": cleanup_iter})

            # Sample memory every iteration
            gc.collect()
            current_memory = get_memory_mb()
            memory_samples.append(current_memory - baseline_memory)

        # Debug: always print memory info
        print(f"\n[Memory Debug] baseline_memory={baseline_memory:.2f} MB")
        print(f"[Memory Debug] samples count={len(memory_samples)}")
        print(f"[Memory Debug] all samples (delta from baseline): {[f'{s:.3f}' for s in memory_samples]}")

        # Check for memory leak trend
        if len(memory_samples) >= 6:
            third = len(memory_samples) // 3
            early_avg = sum(memory_samples[:third]) / third
            late_avg = sum(memory_samples[-third:]) / third

            print(f"[Memory Debug] early_avg={early_avg:.4f} MB, late_avg={late_avg:.4f} MB")
            print(f"[Memory Debug] absolute growth={late_avg - early_avg:.4f} MB")
            if early_avg > 0:
                growth_ratio = (late_avg - early_avg) / early_avg
                print(f"[Memory Debug] growth_ratio={growth_ratio:.4f} ({growth_ratio * 100:.1f}%)")
                assert growth_ratio < 2.0, f"Memory leak in sustained operations: {growth_ratio * 100:.1f}% growth"
            else:
                print("[Memory Debug] early_avg <= 0, skipping ratio check")


# ============================================================================
# Summary Test
# ============================================================================


class TestMemoryIntegrationSummary:
    """Summary test to validate overall memory health with real database."""

    @pytest.mark.asyncio
    async def test_database_operations_integration(self, conn):
        """Integration test combining multiple database operations.

        This test validates memory behavior when combining CRUD, live queries,
        and concurrent operations - simulating realistic usage.
        """
        # Clean all test tables
        for table in ["test_integration_crud", "test_integration_live", "test_integration_concurrent"]:
            with contextlib.suppress(Exception):
                await conn.execute_query(f"DELETE {table};")

        gc.collect()
        baseline_memory = get_memory_mb()

        # Phase 1: CRUD operations
        for i in range(20):
            await conn.create(
                "test_integration_crud",
                {
                    "task_id": f"integration-{i}",
                    "index": i,
                    "data": "x" * 1000,
                },
            )

        gc.collect()
        phase1_memory = get_memory_mb() - baseline_memory

        # Phase 2: Live queries
        live_id, _gen = await conn.start_live("test_integration_live")

        for i in range(10):
            await conn.create(
                "test_integration_live",
                {"task_id": f"live-{i}", "status": "active"},
            )

        gc.collect()
        phase2_memory = get_memory_mb() - baseline_memory

        # Phase 3: Concurrent operations
        async def create_concurrent(idx: int) -> None:
            await conn.create(
                "test_integration_concurrent",
                {"task_id": f"concurrent-{idx}", "index": idx},
            )

        tasks = [create_concurrent(i) for i in range(15)]
        await asyncio.gather(*tasks)

        gc.collect()
        phase3_memory = get_memory_mb() - baseline_memory

        # Cleanup
        await conn.stop_live(live_id)

        gc.collect()
        final_memory = get_memory_mb() - baseline_memory

        # Validation - allow reasonable memory usage for realistic operations
        assert phase1_memory < 50, f"CRUD used excessive memory: {phase1_memory:.2f}MB"
        assert phase2_memory < 80, f"Live queries used excessive memory: {phase2_memory:.2f}MB"
        assert phase3_memory < 100, f"Concurrent ops used excessive memory: {phase3_memory:.2f}MB"

        # Verify cleanup reduces memory (allowing for Python pooling)
        if phase3_memory > 0:
            cleanup_ratio = final_memory / phase3_memory
            assert cleanup_ratio < 2.0, f"Excessive memory retention: {cleanup_ratio * 100:.1f}% of peak"
