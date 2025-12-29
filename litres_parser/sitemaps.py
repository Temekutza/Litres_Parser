from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable, Iterator
from urllib.parse import urljoin

from .http import FetchConfig, fetch_text, make_session


SITEMAP_RE = re.compile(r"^\s*Sitemap:\s*(\S+)\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class SitemapDiscovery:
    base_url: str = "https://www.litres.ru/"
    robots_path: str = "robots.txt"


def discover_sitemaps(d: SitemapDiscovery, *, cfg: FetchConfig) -> list[str]:
    """
    Read robots.txt and return all Sitemap: URLs found there.
    """
    session = make_session()
    robots_url = urljoin(d.base_url, d.robots_path)
    txt = fetch_text(session, robots_url, cfg=cfg, ok_statuses=(200, 404))
    sitemaps: list[str] = []
    for line in txt.splitlines():
        m = SITEMAP_RE.match(line)
        if m:
            sitemaps.append(m.group(1).strip())
    return sorted(set(sitemaps))


def _iter_sitemap_locs(xml_text: str) -> Iterator[str]:
    """
    Supports both <urlset> and <sitemapindex>.
    """
    root = ET.fromstring(xml_text)
    # namespaces are common; handle via wildcard
    for loc in root.findall(".//{*}loc"):
        if loc.text:
            yield loc.text.strip()


def iter_urls_from_sitemaps(sitemap_urls: Iterable[str], *, cfg: FetchConfig) -> Iterator[str]:
    """
    Given a list of sitemap URLs (could include sitemap indexes), yields URLs found inside.
    This is a best-effort, recursive 1-level expansion: if a sitemap contains other *.xml
    links, we fetch those too.
    """
    session = make_session()
    seen: set[str] = set()
    queue: list[str] = []

    for u in sitemap_urls:
        if u and u not in seen:
            seen.add(u)
            queue.append(u)

    while queue:
        sm = queue.pop(0)
        try:
            xml_text = fetch_text(session, sm, cfg=cfg)
        except Exception:
            # sitemap might be missing/blocked; skip it
            continue
        for loc in _iter_sitemap_locs(xml_text):
            if loc.endswith(".xml") and loc not in seen:
                seen.add(loc)
                queue.append(loc)
            else:
                yield loc


def is_probable_book_url(url: str) -> bool:
    """
    Heuristic filter: keep likely book / audiobook pages.
    Adjust patterns as needed for LitRes URL taxonomy.
    """
    u = url.lower()
    if "litres.ru" not in u:
        return False
    if any(x in u for x in ("/book/", "/audiobook/")):
        return True
    return False


