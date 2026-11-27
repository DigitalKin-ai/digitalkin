"""HTTP client for tool modules: every failure becomes a ToolError the calling agent can act on."""

import asyncio
import ipaddress
import random
import socket
import time
from http import HTTPStatus
from typing import Any

import httpx

from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.logger import logger
from digitalkin.modules.tool_error import ToolError


class ToolHttpClient:
    """Pooled ``httpx.AsyncClient`` with retries, a streamed size cap, an optional SSRF guard and typed failures.

    A 429, a 502/503/504 and a transport error or timeout are resent with backoff (``Retry-After`` first); 5xx and
    transport errors only for idempotent methods. A body over ``max_bytes`` stops the download. Every failure is a
    ``ToolError`` naming the method and path, never the query string. Create one per module and ``aclose`` it in
    ``cleanup``.
    """

    def __init__(  # ruff: ignore[too-many-arguments]  # keyword-only client options
        self,
        name: str,
        *,
        base_url: str = "",
        headers: dict[str, str] | None = None,
        timeout_s: float = 30.0,
        max_bytes: int = 10_000_000,
        retries: int = 2,
        max_retry_wait_s: float = 30.0,
        ssrf_guard: bool = False,
        max_redirects: int = 5,
        http2: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            name: The service named in messages and logs (``"PubMed"``).
            base_url: Prefix of relative request URLs.
            headers: Headers sent on every request (credentials belong here, not in the query string).
            timeout_s: Default timeout of one attempt.
            max_bytes: Largest response body read.
            retries: Resends after the first attempt.
            max_retry_wait_s: Longest wait before a resend; a longer ``Retry-After`` is not waited for.
            ssrf_guard: Refuse hosts that resolve to private, loopback, link-local or reserved addresses, on every
                redirect hop. Set it when the URL comes from the user or the model.
            max_redirects: Redirects followed before the request fails.
            http2: Negotiate HTTP/2 with servers that offer it.
            transport: Transport override, for tests.
        """
        self.name = name
        self.max_bytes = max_bytes
        self.retries = retries
        self.max_retry_wait_s = max_retry_wait_s
        self.ssrf_guard = ssrf_guard
        self.max_redirects = max_redirects
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=timeout_s,
            follow_redirects=not ssrf_guard,
            max_redirects=max_redirects,
            http2=http2,
            transport=transport,
        )

    async def aclose(self) -> None:
        """Close the connection pool."""
        await self._client.aclose()

    async def get_json(self, url: str, **kwargs: Any) -> Any:
        """GET ``url`` and decode its JSON body.

        Args:
            url: Absolute URL, or a path under ``base_url``.
            **kwargs: Passed to :meth:`request`.

        Returns:
            The decoded JSON.
        """
        return self.json_of(await self.request("GET", url, **kwargs))

    async def post_json(self, url: str, **kwargs: Any) -> Any:
        """POST to ``url`` and decode its JSON body.

        Args:
            url: Absolute URL, or a path under ``base_url``.
            **kwargs: Passed to :meth:`request`.

        Returns:
            The decoded JSON.
        """
        return self.json_of(await self.request("POST", url, **kwargs))

    def json_of(self, response: httpx.Response) -> Any:
        """Decode a response body as JSON.

        Args:
            response: A response returned by :meth:`request`.

        Returns:
            The decoded JSON.

        Raises:
            ToolError: The body is not JSON (retryable: a proxy or an outage page answered).
        """
        try:
            return response.json()
        except ValueError:
            msg = f"{self.name} {response.request.method} {response.request.url.path}: answered a non-JSON body"
            msg_0 = f"{msg} ({response.text[:200]!r})"
            raise ToolError(msg_0, retryable=True) from None

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        data: Any = None,
        files: Any = None,
        headers: dict[str, str] | None = None,
        timeout_s: float | None = None,
    ) -> httpx.Response:
        """Send a request with retries and return the successful response, body read.

        Args:
            method: HTTP method.
            url: Absolute URL, or a path under ``base_url``.
            params: Query parameters.
            json: JSON body.
            data: Form body.
            files: Multipart files.
            headers: Extra headers for this request.
            timeout_s: Timeout of each attempt, instead of the client's.

        Returns:
            The 2xx response, its body read (at most ``max_bytes``).

        Raises:
            ToolError: On a non-2xx answer, a timeout, a transport error, a body over ``max_bytes`` or a refused host.
        """
        method = method.upper()
        idempotent = method in {"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}
        attempt = 0
        while True:
            started = time.perf_counter()
            try:
                response = await self._send(method, url, params, json, data, files, headers, timeout_s)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                failure = self._transport_failure(method, url, exc, timeout_s)
                if attempt < self.retries and idempotent:
                    attempt += 1
                    await self._wait(attempt, None, failure.message)
                    continue
                raise failure from None
            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.debug(
                "%s %s %s -> %d in %.0f ms (attempt %d)",
                self.name,
                method,
                response.request.url.path,
                response.status_code,
                elapsed_ms,
                attempt + 1,
                extra=RequestContext.current(),
            )
            if response.is_success:
                return response
            status = response.status_code
            if attempt < self.retries and (
                status == HTTPStatus.TOO_MANY_REQUESTS or (idempotent and status in {502, 503, 504})
            ):
                retry_after = self.retry_after(response)
                if retry_after is None or retry_after <= self.max_retry_wait_s:
                    attempt += 1
                    await self._wait(attempt, retry_after, f"HTTP {status}")
                    continue
            raise self.status_failure(response)

    async def _send(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json: Any,
        data: Any,
        files: Any,
        headers: dict[str, str] | None,
        timeout_s: float | None,
    ) -> httpx.Response:
        """Send one attempt, following redirects through the SSRF guard when it is on, and read the body.

        Returns:
            The final response with its body read.

        Raises:
            ToolError: A refused host, too many redirects, or a body over ``max_bytes``.
        """
        timeout = self._client.timeout if timeout_s is None else httpx.Timeout(timeout_s)
        request = self._client.build_request(
            method, url, params=params, json=json, data=data, files=files, headers=headers, timeout=timeout
        )
        for _ in range(self.max_redirects + 1):
            if self.ssrf_guard:
                await self._check_host(request.url)
            response = await self._client.send(request, stream=True)
            if not (self.ssrf_guard and response.is_redirect):
                return await self._read(response)
            await response.aclose()
            request = self._client.build_request(
                "GET" if response.status_code == HTTPStatus.SEE_OTHER else method,
                request.url.join(response.headers["location"]),
                headers=headers,
                timeout=timeout,
            )
        msg = f"{self.name} {method} {request.url.path}: more than {self.max_redirects} redirects"
        raise ToolError(msg, retryable=False)

    async def _read(self, response: httpx.Response) -> httpx.Response:
        """Read a streamed body, stopping past ``max_bytes``.

        Returns:
            A response holding the whole body.

        Raises:
            ToolError: The body is larger than ``max_bytes`` (not retryable: the same call answers the same body).
        """
        try:
            declared = int(response.headers.get("content-length", "0") or 0)
            chunks: list[bytes] = []
            size = declared if declared > self.max_bytes else 0
            if not size:
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.max_bytes:
                        break
                    chunks.append(chunk)
        finally:
            await response.aclose()
        if size > self.max_bytes:
            msg = (
                f"{self.name} {response.request.method} {response.request.url.path}: the answer is larger than "
                f"{self.max_bytes} bytes; narrow the request"
            )
            raise ToolError(msg, retryable=False)
        headers = response.headers.copy()
        headers.pop("content-encoding", None)
        headers.pop("content-length", None)
        return httpx.Response(response.status_code, headers=headers, content=b"".join(chunks), request=response.request)

    async def _check_host(self, url: httpx.URL) -> None:
        """Refuse a URL whose host resolves to a non-public address.

        Raises:
            ToolError: The scheme is not http(s), the host does not resolve, or it resolves to a non-public address.
        """
        if url.scheme not in {"http", "https"} or not url.host:
            msg = f"{self.name}: only http and https URLs with a host are fetched"
            raise ToolError(msg, retryable=False)
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(url.host, url.port, type=socket.SOCK_STREAM)
        except socket.gaierror:
            msg = f"{self.name}: the host {url.host} does not resolve"
            raise ToolError(msg, retryable=False) from None
        for info in infos:
            address = ipaddress.ip_address(info[4][0])
            if not address.is_global or address.is_multicast:
                logger.warning(
                    "%s: refused %s, which resolves to %s", self.name, url.host, address, extra=RequestContext.current()
                )
                msg = f"{self.name}: the host {url.host} is not a public address and is not fetched"
                raise ToolError(msg, retryable=False)

    async def _wait(self, attempt: int, retry_after: float | None, reason: str) -> None:
        """Sleep before a resend: ``Retry-After`` when given, else exponential backoff with jitter."""
        delay = retry_after if retry_after is not None else min(self.max_retry_wait_s, 0.5 * 2**attempt)
        delay += random.uniform(0, 0.25)  # ruff: ignore[suspicious-non-cryptographic-random-usage]  # jitter only
        logger.warning(
            "%s: %s, resend %d/%d in %.1f s",
            self.name,
            reason,
            attempt,
            self.retries,
            delay,
            extra=RequestContext.current(),
        )
        await asyncio.sleep(delay)

    @staticmethod
    def retry_after(response: httpx.Response) -> float | None:
        """Seconds the server asks to wait: ``Retry-After``, else ``RateLimit-Reset`` or ``X-RateLimit-Reset-In``.

        Args:
            response: The failed response.

        Returns:
            The delay in seconds, or None when no header carries a number.
        """
        headers = response.headers
        value = headers.get("retry-after") or headers.get("ratelimit-reset") or headers.get("x-ratelimit-reset-in")
        try:
            return None if value is None else max(0.0, float(value))
        except ValueError:
            return None

    def status_failure(self, response: httpx.Response) -> ToolError:
        """Turn a non-2xx answer into a ToolError; override it to add service-specific guidance.

        Args:
            response: The final non-2xx response, body read.

        Returns:
            The error: 408, 425, 429 and 5xx are retryable, every other status is not.
        """
        status = response.status_code
        where = f"{self.name} {response.request.method} {response.request.url.path}"
        reason = {
            401: "the credentials were refused",
            403: "access is forbidden",
            404: "not found",
            429: "rate limit reached",
        }.get(status, response.reason_phrase or "error")
        body = ToolError.redact(response.text[:300].strip())
        retry_after = self.retry_after(response)
        message = f"{where}: HTTP {status} {reason}"
        if body:
            message += f". Server says: {body}"
        if retry_after is not None:
            message += f". Retry after {retry_after:.0f} s"
        retryable = status in {408, 425, 429} or status >= HTTPStatus.INTERNAL_SERVER_ERROR
        (logger.warning if retryable else logger.info)("%s", message, extra=RequestContext.current())
        return ToolError(message, retryable=retryable)

    def _transport_failure(self, method: str, url: str, exc: Exception, timeout_s: float | None) -> ToolError:
        """Turn a timeout or a transport error into a retryable ToolError.

        Returns:
            The error, naming the path and the timeout, never the query string.
        """
        path = self._client.build_request(method, url).url.path
        if isinstance(exc, httpx.TimeoutException):
            seconds = timeout_s if timeout_s is not None else self._client.timeout.read
            message = f"{self.name} {method} {path}: no answer within {seconds} s"
        else:
            message = f"{self.name} {method} {path}: connection failed ({type(exc).__name__})"
        logger.warning("%s", message, extra=RequestContext.current())
        return ToolError(message, retryable=True)
