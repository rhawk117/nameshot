import contextlib
import dataclasses as dc
import os
import ssl
from collections.abc import AsyncGenerator, Mapping
from typing import Any, Literal, Self

import aiohttp
import anyio
import msgspec


def _get_open_file_soft_limit(posix_os_name: str = 'posix') -> int | None:
    soft_limit: int | None = None
    if os.name != posix_os_name:
        return soft_limit

    with contextlib.suppress(ImportError, OSError, ValueError):
        import resource

        soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)  # type: ignore

    return None if soft_limit < 0 else int(soft_limit)  # type: ignore


class ConcurrencyOptions(msgspec.Struct):
    global_limit: int
    per_host_limit: int
    worker_limit: int

    @classmethod
    def reccomended(cls) -> Self:
        cpu_count = os.cpu_count() or 4
        open_fd_limit = _get_open_file_soft_limit()

        cpu_based_limit = max(32, min(512, cpu_count * 32))
        if open_fd_limit is not None:
            fd_based_limit = max(16, min(512, open_fd_limit // 4))
            cpu_based_limit = min(cpu_based_limit, fd_based_limit)

        per_host_limit = max(2, min(16, cpu_based_limit // 16))
        return cls(
            global_limit=cpu_based_limit,
            per_host_limit=per_host_limit,
            worker_limit=cpu_based_limit,
        )


@dc.dataclass(slots=True, kw_only=True)
class ConnectorOptions:
    ttl_dns_cache: int | None = 300
    use_dns_cache: bool = True
    keepalive_timeout: float = 20.0
    force_close: bool = False
    enable_cleanup_closed: bool = False
    ssl: ssl.SSLContext | bool = True
    family: int = 0
    happy_eyeballs_delay: float = 0.25
    timeout_total: float | None = 20.0
    timeout_connect: float | None = 5.0
    timeout_sock_connect: float | None = 5.0
    timeout_sock_read: float | None = 8.0

    @property
    def timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            total=self.timeout_total,
            connect=self.timeout_connect,
            sock_connect=self.timeout_sock_connect,
            sock_read=self.timeout_sock_read,
        )


@dc.dataclass(slots=True)
class SessionOptions:
    headers: dict[str, str] = dc.field(default_factory=dict)
    read_buffer_size: int = 65_536
    auto_decompress: bool = True
    trust_env: bool = False
    requote_redirect_url: bool = True
    max_line_size: int = 8190
    max_field_size: int = 8190
    max_headers: int = 128
    use_dummy_cookie_jar: bool = True
    timeout_total: float | None = 20.0
    timeout_connect: float | None = 5.0
    timeout_sock_connect: float | None = 5.0
    timeout_sock_read: float | None = 8.0

    def setheaders(self, headers: Mapping[str, str]) -> Self:
        self.headers.update(headers)
        return self

    def build_timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            total=self.timeout_total,
            connect=self.timeout_connect,
            sock_connect=self.timeout_sock_connect,
            sock_read=self.timeout_sock_read,
        )

class HTTPConnection(msgspec.Struct, frozen=True):
    connector: aiohttp.TCPConnector
    limiter: anyio.CapacityLimiter
    session: aiohttp.ClientSession

    @property
    def is_closed(self) -> bool:
        return self.session.closed

    async def aclose(self) -> None:
        if self.session.closed:
            return

        await self.session.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        await self.aclose()

    @contextlib.asynccontextmanager
    async def _sessionslot(self) -> AsyncGenerator[aiohttp.ClientSession]:
        await self.limiter.acquire()
        try:
            yield self.session
        finally:
            self.limiter.release()

    @contextlib.asynccontextmanager
    async def stream_request(
        self,
        *,
        method: Literal['GET', 'POST', 'HEAD'],
        url: str,
        headers: Mapping[str, str] | None = None,
        body: str | bytes | None = None,
        allow_redirects: bool = True,
    ) -> AsyncGenerator[aiohttp.ClientResponse]:
        if self.session.closed:
            raise RuntimeError('connection closed')

        request_kwargs = {
            'method': method,
            'url': url,
            'headers': headers,
            'data': body,
            'allow_redirects': allow_redirects,
        }
        await self.limiter.acquire()
        try:
            async with self.session.request(**request_kwargs) as response:
                yield response
        finally:
            self.limiter.release()





def create_http_connection(
    *,
    session: SessionOptions | None = None,
    connector: ConnectorOptions | None = None,
    concurrency: ConcurrencyOptions | None = None,
) -> HTTPConnection:
    session = session or SessionOptions()
    connector = connector or ConnectorOptions()
    concurrency = concurrency or ConcurrencyOptions.reccomended()

    cookie_jar = None if not session.use_dummy_cookie_jar else aiohttp.DummyCookieJar()

    tcp_connector = aiohttp.TCPConnector(
        ttl_dns_cache=connector.ttl_dns_cache,
        use_dns_cache=connector.use_dns_cache,
        keepalive_timeout=connector.keepalive_timeout,
        force_close=connector.force_close,
        enable_cleanup_closed=connector.enable_cleanup_closed,
        ssl=connector.ssl,
        happy_eyeballs_delay=connector.happy_eyeballs_delay,
        limit_per_host=concurrency.per_host_limit,
        limit=concurrency.global_limit,
    )
    http_session = aiohttp.ClientSession(
        cookie_jar=cookie_jar,
        headers=session.headers,
        auto_decompress=session.auto_decompress,
        trust_env=session.trust_env,
        requote_redirect_url=session.requote_redirect_url,
        read_bufsize=session.read_buffer_size,
        max_line_size=session.max_line_size,
        max_field_size=session.max_field_size,
        max_headers=session.max_headers,
        timeout=session.build_timeout(),
    )
    limiter = anyio.CapacityLimiter(concurrency.worker_limit)

    return HTTPConnection(
        connector=tcp_connector,
        limiter=limiter,
        session=http_session,
    )

async def stream_http_request(
    http_conn: HTTPConnection,
    *,
    method: Literal['GET', 'POST', 'HEAD'],
    url: str,
    headers: Mapping[str, str] | None = None,
    body: str | bytes | None = None,
    allow_redirects: bool = True,
) -> AsyncGenerator[aiohttp.ClientResponse]:
    if http_conn.is_closed:
        raise RuntimeError('connection closed')

    request_kwargs = {
        'method': method,
        'url': url,
        'headers': headers,
        'data': body,
        'allow_redirects': allow_redirects,
    }
    await http_conn.limiter.acquire()
    try:
        async with http_conn.session.request(**request_kwargs) as response:
            yield response
    finally:
        http_conn.limiter.release()
