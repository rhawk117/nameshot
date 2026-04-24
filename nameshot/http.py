import contextlib
import os
import ssl
from collections.abc import AsyncGenerator, Mapping
from typing import Any, Literal, Self

import aiohttp
import anyio
import msgspec
from aiohttp.abc import AbstractCookieJar


def _get_open_file_soft_limit(posix_os_name: str = 'posix') -> int | None:
    soft_limit: int | None = None
    if os.name != posix_os_name:
        return soft_limit

    with contextlib.suppress(ImportError, OSError, ValueError):
        import resource

        soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)  # type: ignore

    return None if soft_limit < 0 else int(soft_limit)  # type: ignore


class HTTPConcurrency(msgspec.Struct, frozen=True):
    limit: int
    per_host_limit: int
    worker_limit: int


def get_reccomended_concurrency() -> HTTPConcurrency:
    """
    uses context clues to derive / calculate concurrency based on the
    operating system.
    """
    cpu_count = os.cpu_count() or 4
    open_fd_limit = _get_open_file_soft_limit()

    cpu_based_limit = max(32, min(512, cpu_count * 32))
    if open_fd_limit is not None:
        fd_based_limit = max(16, min(512, open_fd_limit // 4))
        cpu_based_limit = min(cpu_based_limit, fd_based_limit)

    per_host_limit = max(2, min(16, cpu_based_limit // 16))
    return HTTPConcurrency(
        limit=cpu_based_limit,
        per_host_limit=per_host_limit,
        worker_limit=cpu_based_limit,
    )


class HTTPTimeout(msgspec.Struct):
    total: float | None = 20.0
    connect: float | None = 5.0
    sock_connect: float | None = 5.0
    sock_read: float | None = 8.0


class ConnectorOptions(msgspec.Struct):
    ttl_dns_cache: int = 300
    use_dns_cache: bool = True
    keepalive_timeout: float = 20.0
    force_close: bool = False
    enable_cleanup_closed: bool = False
    ssl: ssl.SSLContext | bool = True
    family: int = 0
    happy_eyeballs_delay: float = 0.25


class HTTPSessionOptions(msgspec.Struct, frozen=True):
    """the options for http client"""
    read_buffer_size: int = 65_536
    auto_decompress: bool = True
    trust_env: bool = False
    requote_redirect_url: bool = True
    max_line_size: int = 8_190
    max_field_size: int = 8_190
    max_headers: int = 128
    use_dummy_cookie_jar: bool = True
    headers: dict[str, str] = msgspec.field(default_factory=dict)
    timeout: HTTPTimeout = msgspec.field(default_factory=HTTPTimeout)
    connector: ConnectorOptions = msgspec.field(default_factory=ConnectorOptions)
    concurrency: HTTPConcurrency = msgspec.field(
        default_factory=get_reccomended_concurrency
    )

    def setheaders(self, headers: Mapping[str, str]) -> Self:
        self.headers.update(headers)
        return self

    @property
    def cookie_jar(self) -> AbstractCookieJar | None:
        return None if not self.use_dummy_cookie_jar else aiohttp.DummyCookieJar()

    def build_timeout(self) -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(
            total=self.timeout.total,
            connect=self.timeout.connect,
            sock_connect=self.timeout.sock_connect,
            sock_read=self.timeout.sock_read,
        )

    def reccomended_concurrency(self) -> Self:
        reccomended = get_reccomended_concurrency()
        return msgspec.structs.replace(self, concurrency=reccomended)

    def build_tcp_connector(self) -> aiohttp.TCPConnector:
        return aiohttp.TCPConnector(
            ttl_dns_cache=self.connector.ttl_dns_cache,
            use_dns_cache=self.connector.use_dns_cache,
            keepalive_timeout=self.connector.keepalive_timeout,
            force_close=self.connector.force_close,
            enable_cleanup_closed=self.connector.enable_cleanup_closed,
            ssl=self.connector.ssl,
            happy_eyeballs_delay=self.connector.happy_eyeballs_delay,
            limit_per_host=self.concurrency.per_host_limit,
            limit=self.concurrency.limit,
        )


def json_serialize_hook(data: Any) -> str:
    return msgspec.json.encode(data).decode('utf-8')


class HTTPClient:
    """
    aiohttp client wrapper, with context manager, lifecycle methods
    and concurrency controls / limits in place. built via the provided
    options.
    """

    __slots__ = ('_bulkhead', '_connector', '_options', '_session')

    def __init__(self, options: HTTPSessionOptions | None = None) -> None:
        self._options = options or HTTPSessionOptions()
        self._connector = self._options.build_tcp_connector()
        self._bulkhead = anyio.CapacityLimiter(self._options.concurrency.worker_limit)
        self._session = aiohttp.ClientSession(
            cookie_jar=self._options.cookie_jar,
            timeout=self._options.build_timeout(),
            headers=self._options.headers,
            connector=self._connector,
            auto_decompress=self._options.auto_decompress,
            trust_env=self._options.trust_env,
            requote_redirect_url=self._options.requote_redirect_url,
            read_bufsize=self._options.read_buffer_size,
            max_line_size=self._options.max_line_size,
            max_field_size=self._options.max_field_size,
            max_headers=self._options.max_headers,
            json_serialize=json_serialize_hook,
        )

    @property
    def is_closed(self) -> bool:
        return self._session.closed

    @property
    def options(self) -> HTTPSessionOptions:
        return self._options

    async def aclose(self) -> None:
        """closes the session"""
        if self._session.closed:
            return

        await self._session.close()

    @contextlib.asynccontextmanager
    async def request(
        self,
        *,
        method: Literal['GET', 'POST', 'HEAD'],
        url: str,
        headers: Mapping[str, str] | None = None,
        body: str | bytes | None = None,
        allow_redirects: bool = True,
    ) -> AsyncGenerator[aiohttp.ClientResponse]:
        """
        Streams a request and yields the respose, aquires the bulkhead

        Yields
        ------
        aiohttp.ClientResponse
            The client response
        """
        if body:
            body = msgspec.json.encode(body)

        kwargs = {
            'method': method,
            'url': url,
            'headers': headers,
            'data': body,
            'allow_redirects': allow_redirects,
        }

        await self._bulkhead.acquire()
        try:
            async with self._session.request(**kwargs) as response:
                yield response
        finally:
            self._bulkhead.release()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        await self.aclose()
