import urllib.parse
from collections.abc import Generator, Iterator
from typing import Annotated, Any, Literal, Self

import msgspec

WMN_CATEGORIES = frozenset({
    'social',
    'gaming',
    'coding',
    'images',
    'news',
    'music',
    'financial',
    'adult',
    'dating',
    'forums',
    'political',
    'professional',
    'crypto',
    'tracking',
    'hobbyist',
})

HttpStatus = Annotated[int, msgspec.Meta(ge=100, le=599)]
NonEmptyStr = Annotated[str, msgspec.Meta(min_length=1)]


class WMNSite(msgspec.Struct):
    """a single site entry from the whatsmyname dataset."""

    name: NonEmptyStr
    uri_check: Annotated[str, msgspec.Meta(min_length=1, pattern=r'.*\{account\}.*')]
    e_code: HttpStatus
    e_string: NonEmptyStr
    m_code: HttpStatus
    m_string: NonEmptyStr
    known: Annotated[list[NonEmptyStr], msgspec.Meta(min_length=1)]
    cat: NonEmptyStr

    uri_pretty: str | None = None
    post_body: str | None = None
    headers: dict[str, str] | None = None
    strip_bad_char: str | None = None
    protection: list[str] | None = None
    valid: Literal[False] | None = None

    def __post_init__(self) -> None:
        if self.uri_pretty and '{account}' not in self.uri_pretty:
            raise ValueError('uri_pretty must contain `{account}`')

        if self.post_body and '{account}' not in self.post_body:
            raise ValueError('post_body must contain `{account}`')

    @property
    def method(self) -> Literal['GET', 'POST']:
        """http method inferred from whether a post body is defined."""
        return 'POST' if self.post_body else 'GET'

    def is_json(self) -> bool:
        """
        check if the site uses json content-type.

        returns
        -------
        bool
            true if a content-type header is present and contains application/json.
        """
        if not self.headers:
            return False
        content_type = self.headers.get('Content-Type') or self.headers.get(
            'content-type'
        )
        return content_type is not None and 'application/json' in content_type.lower()

    def _apply_account(self, url: str, account: str, *, encode: bool) -> str:
        """
        substitute {account} into a url template.

        parameters
        ----------
        url : str
            template string containing {account}.
        account : str
            username to substitute.
        encode : bool
            whether to percent-encode the account before substitution.

        returns
        -------
        str
            url with {account} replaced.
        """
        if self.strip_bad_char:
            account = account.replace(self.strip_bad_char, '')
        if encode:
            account = urllib.parse.quote(account, safe='')
        return url.replace('{account}', account)

    def get_check_url(self, account: str) -> str:
        """
        build the request url for a given account.

        parameters
        ----------
        account : str
            username to check.

        returns
        -------
        str
            fully substituted and encoded check url.
        """
        return self._apply_account(self.uri_check, account, encode=True)

    def get_pretty_url(self, account: str) -> str | None:
        """
        build the human-readable profile url for a given account.

        parameters
        ----------
        account : str
            username to substitute.

        returns
        -------
        str or None
            substituted pretty url, or none if not defined.
        """
        if not self.uri_pretty:
            return None
        return self._apply_account(self.uri_pretty, account, encode=True)

    def get_body(self, account: str) -> str | None:
        """
        build the post body for a given account.

        parameters
        ----------
        account : str
            username to substitute.

        returns
        -------
        str or None
            substituted post body, or none if this is a get site.
        """
        if not self.post_body:
            return None
        return self._apply_account(self.post_body, account, encode=False)


class SiteFilter(msgspec.Struct):
    """filter criteria applied to a wmn site collection."""

    include_categories: frozenset[str] = msgspec.field(default_factory=frozenset)
    exclude_categories: frozenset[str] = msgspec.field(default_factory=frozenset)
    require_known_accounts: bool = False
    require_protection_any_of: frozenset[str] = msgspec.field(default_factory=frozenset)
    ignore_protected: bool = False
    get_method_only: bool = False

    def allows(self, site: dict[str, Any]) -> bool:
        """
        check whether a raw site dict passes this filter.

        parameters
        ----------
        site : dict
            raw site dict from the wmn data file.

        returns
        -------
        bool
            true if the site passes all filter criteria.
        """
        cat = site.get('cat')
        if cat:
            if self.include_categories and cat not in self.include_categories:
                return False
            if self.exclude_categories and cat in self.exclude_categories:
                return False

        if site.get('post_body') and self.get_method_only:
            return False

        if not site.get('known') and self.require_known_accounts:
            return False

        protection = site.get('protection') or []

        if protection and self.ignore_protected:
            return False

        if self.require_protection_any_of:
            have = {p.lower() for p in protection}
            if not (have & self.require_protection_any_of):
                return False

        return True


def convert_site(site: dict[str, Any]) -> WMNSite:
    """
    convert a raw site dict to a validated wmnsite struct.

    parameters
    ----------
    site : dict
        raw site dict from the wmn data file.

    returns
    -------
    WMNSite
        validated struct instance.

    raises
    ------
    msgspec.ValidationError
        if the site dict fails schema or post-init validation.
    """
    return msgspec.convert(site, type=WMNSite, strict=False)


class WMNSiteCollection(msgspec.Struct):
    """lazy collection of raw wmn site dicts with filtering and iteration."""

    sites: list[dict[str, Any]] = msgspec.field(default_factory=list)

    def _iter_filtered(self, site_filter: SiteFilter) -> Iterator[dict[str, Any]]:
        return filter(site_filter.allows, self.sites)

    def filter_sites(self, site_filter: SiteFilter) -> Self:
        """
        return a new collection containing only sites that pass the filter.

        parameters
        ----------
        site_filter : SiteFilter
            criteria to apply.

        returns
        -------
        WMNSiteCollection
            filtered collection of raw site dicts.
        """
        return msgspec.structs.replace(self, sites=list(self._iter_filtered(site_filter)))

    def walk_sites(self, site_filter: SiteFilter | None = None) -> Iterator[WMNSite]:
        """
        iterate over sites as validated wmnsite structs.

        parameters
        ----------
        site_filter : SiteFilter, optional
            if provided, only sites passing the filter are yielded.

        yields
        ------
        WMNSite
            validated site struct.
        """
        source = self._iter_filtered(site_filter) if site_filter else iter(self.sites)
        yield from map(convert_site, source)

    def build_sites(self) -> list[WMNSite]:
        """
        convert all sites to validated wmnsite structs eagerly.

        returns
        -------
        list[WMNSite]
            all sites as validated structs.
        """
        return list(map(convert_site, self.sites))

    @property
    def site_names(self) -> set[str]:
        """set of all site name values in the collection."""
        return {site.get('name', '<unknown>') for site in self.sites}

    @property
    def site_categories(self) -> set[str]:
        """set of all category values in the collection."""
        return {site.get('cat', 'none') for site in self.sites}

    def chunk_sites(
        self,
        chunk_size: int = 50,
        site_filter: SiteFilter | None = None,
    ) -> Generator[tuple[WMNSite, ...]]:
        """
        yield sites in fixed-size chunks of validated structs.

        parameters
        ----------
        chunk_size : int, optional
            number of sites per chunk, by default 50.
        site_filter : SiteFilter, optional
            if provided, only matching sites are included.

        yields
        ------
        tuple[WMNSite, ...]
            chunk of validated site structs.
        """
        if chunk_size <= 1:
            raise ValueError('invalid chunk size')

        chunk: list[WMNSite] = []
        for site in self.walk_sites(site_filter):
            chunk.append(site)
            if len(chunk) >= chunk_size:
                yield tuple(chunk)
                chunk.clear()

        if chunk:
            yield tuple(chunk)


def create_site_collection(
    data_source: dict[str, Any],
    *,
    site_filter: SiteFilter | None = None,
) -> WMNSiteCollection:
    """
    construct a wmnsitecollection from a wmn data dict.

    parameters
    ----------
    data_source : dict
        parsed contents of wmn-data.json.
    site_filter : SiteFilter, optional
        if provided, the collection is pre-filtered before returning.

    returns
    -------
    WMNSiteCollection
        collection of raw site dicts, optionally pre-filtered.

    raises
    ------
    ValueError
        if the 'sites' key is missing or empty.
    TypeError
        if 'sites' is not a list.
    """
    if not (sites := data_source.get('sites')):
        raise ValueError('sites not found in wmn datasource')

    if not isinstance(sites, list):
        raise TypeError(f'expected sites to be a list, got {type(sites)!r}')

    collection = WMNSiteCollection(sites)
    if site_filter:
        collection = collection.filter_sites(site_filter)

    return collection
