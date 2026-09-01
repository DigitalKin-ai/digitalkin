"""Tests for request-ID propagation: RequestContext + interceptors + log filter."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock

import grpc
import grpc.aio
import pytest

from digitalkin.grpc_servers.interceptors.request_ids import (
    RequestContext,
    RequestIdClientInterceptor,
    RequestIdServerInterceptor,
)
from digitalkin.logger import RequestIdLogFilter

pytestmark = [pytest.mark.timeout(10)]


def _details(metadata: Any = None) -> grpc.aio.ClientCallDetails:
    return grpc.aio.ClientCallDetails(
        method="/svc/Method",
        timeout=None,
        metadata=metadata,
        credentials=None,
        wait_for_ready=None,
    )


class TestRequestContext:
    def test_bind_reset_and_current(self) -> None:
        token = RequestContext.bind(task_id="t1", setup_id="setups:s", mission_id="missions:m")
        try:
            assert RequestContext.current() == {"task_id": "t1", "setup_id": "setups:s", "mission_id": "missions:m"}
        finally:
            RequestContext.reset(token)
        assert RequestContext.current() == {}

    def test_bind_drops_empty(self) -> None:
        token = RequestContext.bind(task_id="t1")
        try:
            assert RequestContext.current() == {"task_id": "t1"}
        finally:
            RequestContext.reset(token)

    def test_as_metadata_only_non_empty(self) -> None:
        token = RequestContext.bind(task_id="t1", mission_id="missions:m")
        try:
            assert RequestContext.as_metadata() == [("x-task-id", "t1"), ("x-mission-id", "missions:m")]
        finally:
            RequestContext.reset(token)


class TestClientInterceptor:
    def test_augment_appends_headers(self) -> None:
        token = RequestContext.bind(task_id="t1", setup_id="setups:s", mission_id="missions:m")
        try:
            new = RequestIdClientInterceptor()._augment(_details())
            keys = {k for k, _ in new.metadata}
            assert {"x-task-id", "x-setup-id", "x-mission-id"} <= keys
        finally:
            RequestContext.reset(token)

    def test_augment_passthrough_when_unbound(self) -> None:
        details = _details()
        assert RequestIdClientInterceptor()._augment(details) is details

    def test_augment_skips_existing_key(self) -> None:
        token = RequestContext.bind(task_id="ctx-task")
        try:
            md = grpc.aio.Metadata()
            md.add("x-task-id", "existing")
            new = RequestIdClientInterceptor()._augment(_details(md))
            values = [v for k, v in new.metadata if k == "x-task-id"]
            assert values == ["existing"]
        finally:
            RequestContext.reset(token)


class TestServerInterceptor:
    async def test_binds_ids_from_metadata_and_resets(self) -> None:
        captured: dict[str, str] = {}

        async def behavior(request: Any, context: Any) -> str:  # noqa: ARG001
            captured.update(RequestContext.current())
            return "ok"

        handler = grpc.unary_unary_rpc_method_handler(behavior)

        async def continuation(hcd: Any) -> Any:  # noqa: ARG001
            return handler

        hcd = MagicMock()
        hcd.invocation_metadata = [("x-task-id", "t1"), ("x-mission-id", "missions:m")]

        wrapped = await RequestIdServerInterceptor().intercept_service(continuation, hcd)
        result = await wrapped.unary_unary("req", MagicMock())

        assert result == "ok"
        assert captured == {"task_id": "t1", "mission_id": "missions:m"}
        assert RequestContext.current() == {}  # reset after the call

    async def test_no_ids_returns_handler_unwrapped(self) -> None:
        handler = grpc.unary_unary_rpc_method_handler(lambda r, c: "ok")

        async def continuation(hcd: Any) -> Any:  # noqa: ARG001
            return handler

        hcd = MagicMock()
        hcd.invocation_metadata = []
        wrapped = await RequestIdServerInterceptor().intercept_service(continuation, hcd)
        assert wrapped is handler


class TestLogFilter:
    def _record(self) -> logging.LogRecord:
        return logging.LogRecord("n", logging.INFO, "p", 1, "msg", None, None)

    def test_injects_ambient_ids(self) -> None:
        token = RequestContext.bind(task_id="t1", setup_id="setups:s")
        try:
            record = self._record()
            assert RequestIdLogFilter().filter(record) is True
            assert record.task_id == "t1"  # type: ignore[attr-defined]
            assert record.setup_id == "setups:s"  # type: ignore[attr-defined]
        finally:
            RequestContext.reset(token)

    def test_does_not_clobber_explicit_extra(self) -> None:
        token = RequestContext.bind(task_id="ctx-task")
        try:
            record = self._record()
            record.task_id = "explicit"  # type: ignore[attr-defined]
            RequestIdLogFilter().filter(record)
            assert record.task_id == "explicit"  # type: ignore[attr-defined]
        finally:
            RequestContext.reset(token)
