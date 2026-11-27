"""ToolHttpClient: retries, size cap, SSRF guard and the ToolError each failure becomes."""

import asyncio
import socket
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from digitalkin.modules import ToolError, ToolHttpClient


class _Server:
    """MockTransport handler replaying one response per call and recording the requests."""

    def __init__(self, *responses: httpx.Response | Exception) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.responses.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def _client(server: _Server, **kwargs: Any) -> ToolHttpClient:
    return ToolHttpClient("Demo", base_url="https://api.demo", transport=httpx.MockTransport(server), **kwargs)


@pytest.fixture(autouse=True)
def waits(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    waits: list[float] = []

    async def sleep(delay: float) -> None:  # ruff: ignore[unused-async]  # stands in for asyncio.sleep
        waits.append(delay)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    return waits


class TestSuccess:
    async def test_returns_the_decoded_json(self) -> None:
        server = _Server(httpx.Response(200, json={"hits": [1]}))
        assert await _client(server).get_json("/search", params={"q": "a"}) == {"hits": [1]}
        assert server.requests[0].url.params["q"] == "a"

    async def test_sends_the_client_headers(self) -> None:
        server = _Server(httpx.Response(200, json={}))
        await ToolHttpClient(
            "Demo", headers={"Authorization": "Bearer K"}, transport=httpx.MockTransport(server)
        ).get_json("https://api.demo/x")
        assert server.requests[0].headers["authorization"] == "Bearer K"


class TestStatusFailures:
    @pytest.mark.parametrize(
        ("status", "retryable", "words"),
        [
            (400, False, "HTTP 400"),
            (401, False, "credentials were refused"),
            (403, False, "access is forbidden"),
            (404, False, "not found"),
            (500, True, "HTTP 500"),
        ],
    )
    async def test_maps_the_status(self, status: int, retryable: bool, words: str) -> None:
        server = _Server(*[httpx.Response(status, text="detail") for _ in range(3)])
        with pytest.raises(ToolError) as exc:
            await _client(server, retries=0).get_json("/search", params={"api_key": "SECRET"})
        assert exc.value.retryable is retryable
        assert words in exc.value.message
        assert exc.value.message.startswith("Demo GET /search: ")
        assert "SECRET" not in exc.value.message
        assert "Server says: detail" in exc.value.message

    async def test_redacts_a_key_echoed_in_the_body(self) -> None:
        server = _Server(httpx.Response(400, text="bad request api_key=SECRET"))
        with pytest.raises(ToolError) as exc:
            await _client(server, retries=0).get_json("/x")
        assert "SECRET" not in exc.value.message

    async def test_non_json_body_is_retryable(self) -> None:
        server = _Server(httpx.Response(200, text="<html>gateway</html>"))
        with pytest.raises(ToolError, match="non-JSON body") as exc:
            await _client(server).get_json("/x")
        assert exc.value.retryable is True


class TestRetries:
    async def test_a_429_is_resent_after_its_retry_after(self, waits: list[float]) -> None:
        server = _Server(httpx.Response(429, headers={"Retry-After": "3"}), httpx.Response(200, json={"ok": 1}))
        assert await _client(server).post_json("/x", json={}) == {"ok": 1}
        assert len(server.requests) == 2
        assert 3 <= waits[0] < 3.3

    async def test_a_429_asking_too_long_is_not_waited_for(self, waits: list[float]) -> None:
        server = _Server(httpx.Response(429, headers={"Retry-After": "120"}))
        with pytest.raises(ToolError, match="Retry after 120 s") as exc:
            await _client(server).get_json("/x")
        assert exc.value.retryable is True
        assert waits == []

    async def test_a_503_is_resent_for_a_get(self) -> None:
        server = _Server(httpx.Response(503), httpx.Response(503), httpx.Response(200, json=[]))
        assert await _client(server).get_json("/x") == []
        assert len(server.requests) == 3

    async def test_a_503_is_not_resent_for_a_post(self) -> None:
        server = _Server(httpx.Response(503))
        with pytest.raises(ToolError) as exc:
            await _client(server).post_json("/x", json={})
        assert exc.value.retryable is True
        assert len(server.requests) == 1

    async def test_gives_up_after_the_retries(self) -> None:
        server = _Server(*[httpx.Response(502) for _ in range(3)])
        with pytest.raises(ToolError, match="HTTP 502"):
            await _client(server, retries=2).get_json("/x")
        assert len(server.requests) == 3

    async def test_a_timeout_is_resent_then_reported_with_its_budget(self) -> None:
        server = _Server(*[httpx.ReadTimeout("slow") for _ in range(2)])
        with pytest.raises(ToolError) as exc:
            await _client(server, retries=1, timeout_s=5).get_json("/search?api_key=SECRET")
        assert exc.value.message == "Demo GET /search: no answer within 5 s"
        assert exc.value.retryable is True
        assert len(server.requests) == 2

    async def test_a_timeout_names_the_full_path_like_a_status_failure(self) -> None:
        server = _Server(httpx.ReadTimeout("slow"), httpx.Response(404))
        client = ToolHttpClient(
            "Demo", base_url="https://api.demo/v2", transport=httpx.MockTransport(server), retries=0
        )
        with pytest.raises(ToolError, match=r"^Demo GET /v2/items: no answer"):
            await client.get_json("/items")
        with pytest.raises(ToolError, match=r"^Demo GET /v2/items: HTTP 404"):
            await client.get_json("/items")

    async def test_a_connection_error_is_retryable(self) -> None:
        server = _Server(httpx.ConnectError("refused"))
        with pytest.raises(ToolError, match=r"connection failed \(ConnectError\)") as exc:
            await _client(server, retries=0).get_json("/x")
        assert exc.value.retryable is True


class TestRateLimitHeaders:
    @pytest.mark.parametrize("header", ["Retry-After", "RateLimit-Reset", "X-RateLimit-Reset-In"])
    async def test_reads_the_wait_from_each_header(self, header: str, waits: list[float]) -> None:
        server = _Server(httpx.Response(429, headers={header: "4"}), httpx.Response(200, json={}))
        await _client(server).get_json("/x")
        assert 4 <= waits[0] < 4.3


class TestOverride:
    async def test_a_subclass_adds_service_guidance(self) -> None:
        class Tinybird(ToolHttpClient):
            def status_failure(self, response: httpx.Response) -> ToolError:
                error = super().status_failure(response)
                if response.status_code == httpx.codes.FORBIDDEN:
                    return ToolError(f"{error.message}. The token lacks the READ scope", retryable=False)
                return error

        server = httpx.MockTransport(_Server(httpx.Response(403)))
        with pytest.raises(ToolError, match="lacks the READ scope"):
            await Tinybird("Tinybird", transport=server).get_json("https://api.tinybird.co/v0/sql")


class TestSizeCap:
    async def test_a_declared_large_body_is_refused_unread(self) -> None:
        server = _Server(httpx.Response(200, headers={"Content-Length": "999"}, content=b"x" * 999))
        with pytest.raises(ToolError, match="larger than 100 bytes") as exc:
            await _client(server, max_bytes=100).request("GET", "/x")
        assert exc.value.retryable is False

    async def test_a_streamed_large_body_is_cut(self) -> None:
        async def body() -> Any:  # ruff: ignore[unused-async]  # httpx needs an async iterable body
            for _ in range(10):
                yield b"x" * 50

        server = _Server(httpx.Response(200, content=body()))
        with pytest.raises(ToolError, match="larger than 100 bytes"):
            await _client(server, max_bytes=100).request("GET", "/x")

    async def test_a_body_under_the_cap_is_whole(self) -> None:
        server = _Server(httpx.Response(200, content=b"x" * 100))
        assert (await _client(server, max_bytes=100).request("GET", "/x")).content == b"x" * 100


class TestSsrfGuard:
    @pytest.fixture
    def resolve(self, monkeypatch: pytest.MonkeyPatch) -> Callable[[dict[str, str]], None]:
        def install(table: dict[str, str]) -> None:
            async def getaddrinfo(host: str, *_args: Any, **_kwargs: Any) -> list[Any]:  # ruff: ignore[unused-async]
                if host not in table:
                    raise socket.gaierror
                return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", (table[host], 443))]

            monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", getaddrinfo)

        return install

    @pytest.mark.parametrize("address", ["127.0.0.1", "10.0.0.5", "169.254.169.254", "192.168.1.1"])
    async def test_refuses_a_private_host(self, resolve: Callable[[dict[str, str]], None], address: str) -> None:
        resolve({"evil.test": address})
        server = _Server()
        client = ToolHttpClient("Fetch", ssrf_guard=True, transport=httpx.MockTransport(server))
        with pytest.raises(ToolError, match="not a public address") as exc:
            await client.request("GET", "https://evil.test/x")
        assert exc.value.retryable is False
        assert server.requests == []

    async def test_checks_every_redirect_hop(self, resolve: Callable[[dict[str, str]], None]) -> None:
        resolve({"public.test": "93.184.216.34", "internal.test": "10.0.0.1"})
        server = _Server(httpx.Response(302, headers={"Location": "https://internal.test/admin"}))
        client = ToolHttpClient("Fetch", ssrf_guard=True, transport=httpx.MockTransport(server))
        with pytest.raises(ToolError, match=r"internal\.test is not a public address"):
            await client.request("GET", "https://public.test/x")
        assert len(server.requests) == 1

    async def test_follows_a_public_redirect(self, resolve: Callable[[dict[str, str]], None]) -> None:
        resolve({"a.test": "93.184.216.34", "b.test": "93.184.216.35"})
        server = _Server(httpx.Response(301, headers={"Location": "https://b.test/y"}), httpx.Response(200, text="ok"))
        client = ToolHttpClient("Fetch", ssrf_guard=True, transport=httpx.MockTransport(server))
        assert (await client.request("GET", "https://a.test/x")).text == "ok"
        assert server.requests[1].url.host == "b.test"

    async def test_stops_after_max_redirects(self, resolve: Callable[[dict[str, str]], None]) -> None:
        resolve({"a.test": "93.184.216.34"})
        server = _Server(*[httpx.Response(302, headers={"Location": "https://a.test/loop"}) for _ in range(3)])
        client = ToolHttpClient("Fetch", ssrf_guard=True, max_redirects=2, transport=httpx.MockTransport(server))
        with pytest.raises(ToolError, match="more than 2 redirects"):
            await client.request("GET", "https://a.test/x")
        assert len(server.requests) == 3

    async def test_refuses_a_non_http_scheme(self) -> None:
        client = ToolHttpClient("Fetch", ssrf_guard=True, transport=httpx.MockTransport(_Server()))
        with pytest.raises(ToolError, match="only http and https"):
            await client.request("GET", "file:///etc/passwd")
