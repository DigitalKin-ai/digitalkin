"""Tests for dynamic schema utilities."""

import asyncio
from typing import Annotated

import pytest
from pydantic import BaseModel, Field

from digitalkin.models.utils.dynamic_schema import ResolveResult
from digitalkin.utils.dynamic_schema import (
    DynamicField,
    get_dynamic_metadata,
    get_fetchers,
    has_dynamic,
    resolve,
    resolve_safe,
)

# Import alias for cleaner test code
Dynamic = DynamicField


class TestDynamicField:
    """Tests for the DynamicField metadata class."""

    def test_creates_fetchers_dict(self) -> None:
        """Test that DynamicField stores fetchers correctly."""
        def fetcher():
            return ["a", "b", "c"]
        meta = DynamicField(enum=fetcher)

        assert "enum" in meta.fetchers
        assert meta.fetchers["enum"] is fetcher

    def test_multiple_fetchers(self) -> None:
        """Test that multiple fetchers are supported."""
        def enum_fetcher():
            return ["opt1", "opt2"]

        def default_fetcher() -> str:
            return "opt1"

        meta = DynamicField(enum=enum_fetcher, default=default_fetcher)

        assert "enum" in meta.fetchers
        assert "default" in meta.fetchers

    def test_empty_fetchers(self) -> None:
        """Test that empty fetchers work."""
        meta = DynamicField()
        assert meta.fetchers == {}

    def test_repr(self) -> None:
        """Test string representation."""
        meta = DynamicField(enum=list, default=lambda: "x")
        repr_str = repr(meta)
        assert "DynamicField(" in repr_str
        assert "enum" in repr_str or "default" in repr_str

    def test_equality(self) -> None:
        """Test equality comparison."""
        def fetcher():
            return ["a"]
        meta1 = DynamicField(enum=fetcher)
        meta2 = DynamicField(enum=fetcher)
        meta3 = DynamicField(other=fetcher)

        assert meta1 == meta2
        assert meta1 != meta3

    def test_hash(self) -> None:
        """Test that DynamicField is hashable."""
        meta = DynamicField(enum=list)
        # Should not raise
        hash(meta)


class TestGetDynamicMetadata:
    """Tests for get_dynamic_metadata function."""

    def test_extracts_dynamic_from_annotated(self) -> None:
        """Test extraction from Annotated field."""
        dynamic_meta = DynamicField(enum=lambda: ["a"])

        class Model(BaseModel):
            field: Annotated[str, dynamic_meta] = "a"

        result = get_dynamic_metadata(Model.model_fields["field"])
        assert result is dynamic_meta

    def test_returns_none_without_dynamic(self) -> None:
        """Test returns None when no DynamicField metadata."""

        class Model(BaseModel):
            field: str = "a"

        result = get_dynamic_metadata(Model.model_fields["field"])
        assert result is None

    def test_returns_none_with_other_metadata(self) -> None:
        """Test returns None with other Annotated metadata."""

        class Model(BaseModel):
            field: Annotated[str, "some_other_metadata"] = "a"

        result = get_dynamic_metadata(Model.model_fields["field"])
        assert result is None


class TestHasDynamic:
    """Tests for the has_dynamic function."""

    def test_returns_true_with_dynamic_metadata(self) -> None:
        """Test detection when DynamicField is present."""

        class Model(BaseModel):
            field: Annotated[str, DynamicField(enum=lambda: ["a"])] = "a"

        assert has_dynamic(Model.model_fields["field"]) is True

    def test_returns_false_without_dynamic(self) -> None:
        """Test returns False when no DynamicField."""

        class Model(BaseModel):
            field: str = "a"

        assert has_dynamic(Model.model_fields["field"]) is False

    def test_returns_false_with_field_info_only(self) -> None:
        """Test returns False with Field but no DynamicField."""

        class Model(BaseModel):
            field: str = Field(default="a", description="A field")

        assert has_dynamic(Model.model_fields["field"]) is False


class TestGetFetchers:
    """Tests for the get_fetchers function."""

    def test_extracts_fetchers(self) -> None:
        """Test extraction of fetchers."""
        def fetcher():
            return ["a", "b"]

        class Model(BaseModel):
            field: Annotated[str, Dynamic(enum=fetcher)] = "a"

        fetchers = get_fetchers(Model.model_fields["field"])

        assert "enum" in fetchers
        assert fetchers["enum"] is fetcher

    def test_returns_empty_dict_without_dynamic(self) -> None:
        """Test returns empty dict when no Dynamic."""

        class Model(BaseModel):
            field: str = "a"

        assert get_fetchers(Model.model_fields["field"]) == {}

    def test_multiple_fetchers(self) -> None:
        """Test extraction of multiple fetchers."""
        def enum_fetcher():
            return ["a", "b"]

        def default_fetcher() -> str:
            return "a"

        class Model(BaseModel):
            field: Annotated[str, Dynamic(enum=enum_fetcher, default=default_fetcher)] = "a"

        fetchers = get_fetchers(Model.model_fields["field"])

        assert fetchers["enum"] is enum_fetcher
        assert fetchers["default"] is default_fetcher


class TestResolve:
    """Tests for the resolve function."""

    @pytest.mark.asyncio
    async def test_resolves_sync_fetcher(self) -> None:
        """Test resolution of sync fetcher."""
        fetchers = {"enum": lambda: ["a", "b", "c"]}

        resolved = await resolve(fetchers)

        assert resolved == {"enum": ["a", "b", "c"]}

    @pytest.mark.asyncio
    async def test_resolves_async_fetcher(self) -> None:
        """Test resolution of async fetcher."""

        async def async_enum() -> list[str]:
            return ["x", "y", "z"]

        fetchers = {"enum": async_enum}

        resolved = await resolve(fetchers)

        assert resolved == {"enum": ["x", "y", "z"]}

    @pytest.mark.asyncio
    async def test_resolves_mixed_fetchers(self) -> None:
        """Test resolution of mixed sync/async fetchers."""

        async def async_default() -> str:
            return "default_value"

        fetchers = {
            "enum": lambda: ["a", "b"],
            "default": async_default,
        }

        resolved = await resolve(fetchers)

        assert resolved == {"enum": ["a", "b"], "default": "default_value"}

    @pytest.mark.asyncio
    async def test_resolves_empty_fetchers(self) -> None:
        """Test resolution of empty fetchers."""
        resolved = await resolve({})
        assert resolved == {}

    @pytest.mark.asyncio
    async def test_fetcher_error_propagates(self) -> None:
        """Test that fetcher errors propagate."""

        def failing_fetcher() -> list[str]:
            msg = "Fetcher failed"
            raise ValueError(msg)

        fetchers = {"enum": failing_fetcher}

        with pytest.raises(ValueError, match="Fetcher failed"):
            await resolve(fetchers)


class TestIntegrationWithPydantic:
    """Integration tests with Pydantic models."""

    def test_dynamic_in_annotated_field(self) -> None:
        """Test that Dynamic works in Pydantic Annotated fields."""

        class TestModel(BaseModel):
            name: Annotated[str, Dynamic(enum=lambda: ["a", "b", "c"])] = "a"

        field_info = TestModel.model_fields["name"]

        assert has_dynamic(field_info)
        fetchers = get_fetchers(field_info)
        assert "enum" in fetchers

    def test_dynamic_with_field_and_json_schema_extra(self) -> None:
        """Test Dynamic alongside Field with json_schema_extra."""

        class TestModel(BaseModel):
            name: Annotated[str, Dynamic(enum=lambda: ["x", "y"])] = Field(
                default="x",
                json_schema_extra={"config": True},
            )

        field_info = TestModel.model_fields["name"]

        # Dynamic should be detected
        assert has_dynamic(field_info)

        # Static json_schema_extra should still be present
        assert field_info.json_schema_extra == {"config": True}

    def test_multiple_annotated_metadata(self) -> None:
        """Test Dynamic with other Annotated metadata."""

        class TestModel(BaseModel):
            name: Annotated[str, "description", Dynamic(enum=lambda: ["a"])] = "a"

        field_info = TestModel.model_fields["name"]

        # Dynamic should still be found
        assert has_dynamic(field_info)

    @pytest.mark.asyncio
    async def test_end_to_end_resolution(self) -> None:
        """Test full flow from model definition to resolution."""

        async def fetch_options() -> list[str]:
            return ["option1", "option2", "option3"]

        class TestModel(BaseModel):
            choice: Annotated[str, Dynamic(enum=fetch_options)] = "option1"

        field_info = TestModel.model_fields["choice"]
        fetchers = get_fetchers(field_info)
        resolved = await resolve(fetchers)

        assert resolved["enum"] == ["option1", "option2", "option3"]


class TestResolveResult:
    """Tests for the ResolveResult dataclass."""

    def test_success_when_no_errors(self) -> None:
        """Test success property returns True with no errors."""
        result = ResolveResult(values={"key": "value"}, errors={})
        assert result.success is True

    def test_success_when_errors_exist(self) -> None:
        """Test success property returns False with errors."""
        result = ResolveResult(values={}, errors={"key": ValueError("fail")})
        assert result.success is False

    def test_partial_when_both_values_and_errors(self) -> None:
        """Test partial property returns True with mixed results."""
        result = ResolveResult(
            values={"good": "value"},
            errors={"bad": ValueError("fail")},
        )
        assert result.partial is True

    def test_partial_false_when_all_success(self) -> None:
        """Test partial property returns False when all succeed."""
        result = ResolveResult(values={"key": "value"}, errors={})
        assert result.partial is False

    def test_partial_false_when_all_fail(self) -> None:
        """Test partial property returns False when all fail."""
        result = ResolveResult(values={}, errors={"key": ValueError("fail")})
        assert result.partial is False

    def test_get_returns_value(self) -> None:
        """Test get method returns existing value."""
        result = ResolveResult(values={"key": "value"})
        assert result.get("key") == "value"

    def test_get_returns_default_for_missing(self) -> None:
        """Test get method returns default for missing key."""
        result = ResolveResult(values={})
        assert result.get("missing", "default") == "default"

    def test_get_returns_none_by_default(self) -> None:
        """Test get method returns None when no default."""
        result = ResolveResult(values={})
        assert result.get("missing") is None

    def test_empty_result(self) -> None:
        """Test empty ResolveResult defaults."""
        result = ResolveResult()
        assert result.values == {}
        assert result.errors == {}
        assert result.success is True
        assert result.partial is False


class TestResolveSafe:
    """Tests for the resolve_safe function with error handling."""

    @pytest.mark.asyncio
    async def test_success_all_fetchers(self) -> None:
        """Test resolve_safe with all successful fetchers."""
        fetchers = {
            "enum": lambda: ["a", "b"],
            "default": lambda: "a",
        }

        result = await resolve_safe(fetchers)

        assert result.success is True
        assert result.values == {"enum": ["a", "b"], "default": "a"}
        assert result.errors == {}

    @pytest.mark.asyncio
    async def test_partial_failure(self) -> None:
        """Test resolve_safe handles partial failures."""

        def failing_fetcher() -> list[str]:
            msg = "Failed"
            raise ValueError(msg)

        fetchers = {
            "good": lambda: ["a", "b"],
            "bad": failing_fetcher,
        }

        result = await resolve_safe(fetchers)

        assert result.partial is True
        assert result.values == {"good": ["a", "b"]}
        assert "bad" in result.errors
        assert isinstance(result.errors["bad"], ValueError)

    @pytest.mark.asyncio
    async def test_all_failure(self) -> None:
        """Test resolve_safe when all fetchers fail."""

        def failing1() -> list[str]:
            msg = "Fail 1"
            raise ValueError(msg)

        def failing2() -> str:
            msg = "Fail 2"
            raise RuntimeError(msg)

        fetchers = {"a": failing1, "b": failing2}

        result = await resolve_safe(fetchers)

        assert result.success is False
        assert result.partial is False
        assert result.values == {}
        assert len(result.errors) == 2

    @pytest.mark.asyncio
    async def test_empty_fetchers(self) -> None:
        """Test resolve_safe with empty fetchers."""
        result = await resolve_safe({})
        assert result.success is True
        assert result.values == {}
        assert result.errors == {}

    @pytest.mark.asyncio
    async def test_async_fetcher_failure(self) -> None:
        """Test resolve_safe with async fetcher that fails."""

        async def async_failing() -> list[str]:
            msg = "Async failure"
            raise ValueError(msg)

        result = await resolve_safe({"async_key": async_failing})

        assert result.success is False
        assert "async_key" in result.errors


class TestResolveParallel:
    """Tests for parallel resolution behavior."""

    @pytest.mark.asyncio
    async def test_fetchers_run_in_parallel(self) -> None:
        """Test that multiple fetchers run concurrently, not sequentially."""
        call_times: list[float] = []
        start_time = asyncio.get_event_loop().time()

        async def slow_fetcher_1() -> str:
            call_times.append(asyncio.get_event_loop().time() - start_time)
            await asyncio.sleep(0.1)
            return "result1"

        async def slow_fetcher_2() -> str:
            call_times.append(asyncio.get_event_loop().time() - start_time)
            await asyncio.sleep(0.1)
            return "result2"

        async def slow_fetcher_3() -> str:
            call_times.append(asyncio.get_event_loop().time() - start_time)
            await asyncio.sleep(0.1)
            return "result3"

        fetchers = {
            "a": slow_fetcher_1,
            "b": slow_fetcher_2,
            "c": slow_fetcher_3,
        }

        before = asyncio.get_event_loop().time()
        result = await resolve(fetchers)
        elapsed = asyncio.get_event_loop().time() - before

        # If parallel, should complete in ~0.1s. If sequential, ~0.3s
        assert elapsed < 0.25, f"Expected parallel execution, but took {elapsed:.2f}s"
        assert result == {"a": "result1", "b": "result2", "c": "result3"}

        # All fetchers should start at roughly the same time
        if len(call_times) == 3:
            max_start_diff = max(call_times) - min(call_times)
            assert max_start_diff < 0.05, "Fetchers didn't start concurrently"

    @pytest.mark.asyncio
    async def test_resolve_safe_parallel(self) -> None:
        """Test that resolve_safe also runs in parallel."""

        async def slow_fetcher() -> str:
            await asyncio.sleep(0.1)
            return "result"

        fetchers = {f"key{i}": slow_fetcher for i in range(3)}

        before = asyncio.get_event_loop().time()
        result = await resolve_safe(fetchers)
        elapsed = asyncio.get_event_loop().time() - before

        assert elapsed < 0.25, f"Expected parallel execution, but took {elapsed:.2f}s"
        assert result.success is True
        assert len(result.values) == 3


class TestResolveTimeout:
    """Tests for timeout behavior in resolve functions."""

    @pytest.mark.asyncio
    async def test_resolve_with_timeout_success(self) -> None:
        """Test resolve completes within timeout."""

        async def fast_fetcher() -> str:
            await asyncio.sleep(0.01)
            return "fast"

        result = await resolve({"key": fast_fetcher}, timeout=1.0)
        assert result == {"key": "fast"}

    @pytest.mark.asyncio
    async def test_resolve_timeout_exceeded(self) -> None:
        """Test resolve raises TimeoutError when timeout exceeded."""

        async def slow_fetcher() -> str:
            await asyncio.sleep(10)
            return "slow"

        with pytest.raises(asyncio.TimeoutError):
            await resolve({"key": slow_fetcher}, timeout=0.05)

    @pytest.mark.asyncio
    async def test_resolve_safe_timeout_records_error(self) -> None:
        """Test resolve_safe records timeout as error."""

        async def slow_fetcher() -> str:
            await asyncio.sleep(10)
            return "slow"

        result = await resolve_safe({"slow": slow_fetcher}, timeout=0.05)

        # Timeout should be recorded as error
        assert result.success is False
        assert "slow" in result.errors
        assert isinstance(result.errors["slow"], asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_resolve_safe_partial_timeout(self) -> None:
        """Test resolve_safe with some fast and some slow fetchers."""

        async def fast_fetcher() -> str:
            await asyncio.sleep(0.01)
            return "fast"

        async def slow_fetcher() -> str:
            await asyncio.sleep(10)
            return "slow"

        fetchers = {"fast": fast_fetcher, "slow": slow_fetcher}
        result = await resolve_safe(fetchers, timeout=0.1)

        # Fast one should succeed, slow one should timeout
        assert result.partial is True
        assert result.values.get("fast") == "fast"
        assert "slow" in result.errors

    @pytest.mark.asyncio
    async def test_resolve_no_timeout_by_default(self) -> None:
        """Test that timeout=None allows indefinite execution."""

        async def fetcher() -> str:
            await asyncio.sleep(0.05)
            return "done"

        # Should not timeout with timeout=None
        result = await resolve({"key": fetcher}, timeout=None)
        assert result == {"key": "done"}
