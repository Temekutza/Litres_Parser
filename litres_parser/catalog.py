from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .http import FetchConfig, fetch_text, make_session


@dataclass(frozen=True)
class CatalogDiscovery:
    base_url: str = "https://www.litres.ru/"
    genres_path: str = "/pages/new_genres/"


def iter_genre_urls(d: CatalogDiscovery, *, cfg: FetchConfig) -> Iterator[str]:
    """
    Yields absolute URLs for genre/category pages.

    Notes:
    - litres.ru uses `/genre/.../`
    - litres.com uses category-like paths such as `/knigi-fentezi/`, `/biznes-knigi/.../`, etc.
    """
    session = make_session()
    html = fetch_text(session, urljoin(d.base_url, d.genres_path), cfg=cfg)
    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not isinstance(href, str):
            continue
        # Normalize
        href = href.strip()
        if not href.startswith("/"):
            continue
        if "?" in href:
            href = href.split("?", 1)[0]

        # Exclude obvious non-categories and direct book URLs
        if any(x in href for x in ("/book/", "/audiobook/", "/pages/", "/download_book/")):
            continue

        is_ru_genre = href.startswith("/genre/")
        is_com_category = ("knigi" in href) or ("audiobooks" in href)

        if not (is_ru_genre or is_com_category):
            continue

        u = urljoin(d.base_url, href)
        if u not in seen:
            seen.add(u)
            yield u


def iter_book_urls_from_genre(
    genre_url: str,
    *,
    cfg: FetchConfig,
    max_pages: int = 10000,
) -> Iterator[str]:
    """
    Crawl one genre with ?page=N and extract book/audiobook URLs.
    Stops when a page yields 0 book links.
    """
    session = make_session()
    seen_in_genre: set[str] = set()
    for page in range(1, max_pages + 1):
        url = genre_url if page == 1 else f"{genre_url}?page={page}"
        try:
            html = fetch_text(session, url, cfg=cfg)
        except Exception:
            break
        soup = BeautifulSoup(html, "lxml")
        found: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not isinstance(href, str):
                continue
            if "/book/" in href or "/audiobook/" in href:
                u = urljoin(genre_url, href.split("?")[0])
                if u not in seen_in_genre:
                    seen_in_genre.add(u)
                    found.append(u)
        if not found:
            break
        for u in found:
            yield u


def iter_book_urls_from_catalog(
    d: CatalogDiscovery,
    *,
    cfg: FetchConfig,
    max_pages_per_genre: int = 200,
) -> Iterator[str]:
    """
    Yields book URLs by iterating all genres and paging inside each genre.
    """
    for genre_url in iter_genre_urls(d, cfg=cfg):
        yield from iter_book_urls_from_genre(genre_url, cfg=cfg, max_pages=max_pages_per_genre)


