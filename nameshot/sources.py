from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import msgspec

from nameshot.wmn import SiteFilter, WMNSiteCollection, create_site_collection

if TYPE_CHECKING:
    from nameshot.http import HTTPConnection


class DataSource(msgspec.Struct, frozen=True):
    """a known wmn-compatible data source."""
    name: str
    url: str
    description: str = ''


KNOWN_SOURCES: frozenset[DataSource] = frozenset({
    DataSource(
        name='whatsmyname-canonical',
        url='https://raw.githubusercontent.com/WebBreacher/WhatsMyName/main/wmn-data.json',
        description='canonical wmn dataset maintained by webbreacher. primary source of truth.',
    ),
    DataSource(
        name='whatsmyname-thomas-kaiser-boyer',
        url='https://raw.githubusercontent.com/thomas-kaiser-boyer/WhatsMyName/main/wmn-data.json',
        description='community fork of the canonical wmn dataset. may include entries pending upstream merge.',
    ),
})


_decoder = msgspec.json.Decoder(type=dict[str, Any])


class DataSourceError(Exception):
    """raised when a data source fetch or decode fails."""

    def __init__(self, source: DataSource, reason: str) -> None:
        self.source = source
        self.reason = reason
        super().__init__(f'datasource {source.name!r} failed: {reason}')


async def fetch_source(
    conn: HTTPConnection,
    source: DataSource,
    *,
    site_filter: SiteFilter | None = None,
) -> WMNSiteCollection:
    """
    fetch and decode a single wmn data source into a site collection.

    parameters
    ----------
    conn : HTTPConnection
        active http connection to use for the request.
    source : DataSource
        data source to fetch.
    site_filter : SiteFilter, optional
        if provided, the returned collection is pre-filtered.

    returns
    -------
    WMNSiteCollection
        decoded and optionally filtered site collection.

    raises
    ------
    DataSourceError
        if the http request fails, returns a non-200 status, or the
        response body cannot be decoded as a wmn data dict.
    """
    async with conn.stream_request(method='GET', url=source.url) as response:
        if response.status != 200:
            raise DataSourceError(source, f'unexpected http status {response.status}')

        raw = await response.read()

    try:
        data: dict[str, Any] = _decoder.decode(raw)
    except msgspec.DecodeError as exc:
        raise DataSourceError(source, f'json decode failed: {exc}') from exc

    try:
        return create_site_collection(data, site_filter=site_filter)
    except (ValueError, TypeError) as exc:
        raise DataSourceError(source, f'collection build failed: {exc}') from exc


def merge_collections(
    collections: Iterable[WMNSiteCollection],
    *,
    deduplicate: bool = True,
) -> WMNSiteCollection:
    """
    merge multiple site collections into one.

    parameters
    ----------
    collections : iterable of WMNSiteCollection
        collections to merge, in priority order (first wins on deduplication).
    deduplicate : bool, optional
        if true, removes duplicate site entries by name, keeping the first
        occurrence. default is true.

    returns
    -------
    WMNSiteCollection
        merged collection of raw site dicts.
    """
    merged: list[dict[str, Any]] = []

    if not deduplicate:
        for collection in collections:
            merged.extend(collection.sites)
        return WMNSiteCollection(merged)

    seen: set[str] = set()
    for collection in collections:
        for site in collection.sites:
            name: str = site.get('name', '')
            if name and name not in seen:
                seen.add(name)
                merged.append(site)

    return WMNSiteCollection(merged)


async def fetch_sources(
    conn: HTTPConnection,
    sources: Iterable[DataSource] = KNOWN_SOURCES,
    *,
    site_filter: SiteFilter | None = None,
    deduplicate: bool = True,
    skip_errors: bool = False,
) -> WMNSiteCollection:
    """
    fetch and merge multiple wmn data sources into one collection.

    parameters
    ----------
    conn : HTTPConnection
        active http connection to use for all requests.
    sources : iterable of DataSource, optional
        data sources to fetch. defaults to KNOWN_SOURCES.
    site_filter : SiteFilter, optional
        if provided, applied to each source before merging.
    deduplicate : bool, optional
        if true, removes duplicate site entries by name after merging.
        default is true.
    skip_errors : bool, optional
        if true, datasource errors are silently skipped rather than raised.
        default is false.

    returns
    -------
    WMNSiteCollection
        merged collection from all fetched sources.

    raises
    ------
    DataSourceError
        if any source fails and skip_errors is false.
    """
    collections: list[WMNSiteCollection] = []

    for source in sources:
        try:
            collection = await fetch_source(conn, source, site_filter=site_filter)
            collections.append(collection)
        except DataSourceError:
            if not skip_errors:
                raise

    return merge_collections(collections, deduplicate=deduplicate)
