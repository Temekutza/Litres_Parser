from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from bs4 import BeautifulSoup

from .http import FetchConfig, fetch_text, make_session
from .storage import utc_now_iso


@dataclass(frozen=True)
class BookParseResult:
    ok: bool
    data: dict[str, Any]
    error: str | None = None


def _as_list(x: Any) -> list[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _extract_jsonld(soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not tag.string:
            continue
        try:
            payload = json.loads(tag.string)
        except Exception:
            continue
        for obj in _as_list(payload):
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _find_bookish_jsonld(jsonlds: list[dict[str, Any]]) -> dict[str, Any] | None:
    for obj in jsonlds:
        t = obj.get("@type")
        if isinstance(t, str) and t.lower() in {"book", "product"}:
            return obj
        if isinstance(t, list) and any(isinstance(x, str) and x.lower() == "book" for x in t):
            return obj
    # sometimes wrapped in @graph
    for obj in jsonlds:
        graph = obj.get("@graph")
        if isinstance(graph, list):
            for g in graph:
                if not isinstance(g, dict):
                    continue
                t = g.get("@type")
                if isinstance(t, str) and t.lower() in {"book", "product"}:
                    return g
    return None


def _get_meta(soup: BeautifulSoup, name: str) -> str | None:
    tag = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
    if not tag:
        return None
    c = tag.get("content")
    return c.strip() if isinstance(c, str) else None


def _first_text(soup: BeautifulSoup, selectors: list[str]) -> str | None:
    for sel in selectors:
        try:
            el = soup.select_one(sel)
        except Exception:
            continue
        if el:
            t = el.get_text(" ", strip=True)
            if t:
                return t
    return None


def _list_texts(soup: BeautifulSoup, selectors: list[str]) -> list[str]:
    out: list[str] = []
    for sel in selectors:
        try:
            els = soup.select(sel)
        except Exception:
            continue
        for el in els:
            t = el.get_text(" ", strip=True)
            if t:
                out.append(t)
        if out:
            break
    # unique, preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for x in out:
        x = x.strip()
        if not x or x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def parse_book_page(
    url: str,
    *,
    cfg: FetchConfig,
    session=None,
) -> BookParseResult:
    """
    Fetch book page HTML and extract metadata.
    Best effort: JSON-LD -> meta tags -> light HTML fallbacks.
    """
    session = session or make_session()
    try:
        html = fetch_text(session, url, cfg=cfg)
    except Exception as e:  # noqa: BLE001
        return BookParseResult(ok=False, data={"url": url}, error=str(e))

    soup = BeautifulSoup(html, "lxml")
    jsonlds = _extract_jsonld(soup)
    j = _find_bookish_jsonld(jsonlds)

    title: str | None = None
    authors: list[str] = []
    price: str | None = None
    rating: str | None = None
    rating_count: str | None = None
    genres: list[str] = []
    description: str | None = None

    if j:
        title = j.get("name") if isinstance(j.get("name"), str) else None

        for a in _as_list(j.get("author")):
            if isinstance(a, str):
                authors.append(a)
            elif isinstance(a, dict) and isinstance(a.get("name"), str):
                authors.append(a["name"])

        ar = j.get("aggregateRating")
        if isinstance(ar, dict):
            rv = ar.get("ratingValue")
            rc = ar.get("ratingCount") or ar.get("reviewCount")
            if rv is not None:
                rating = str(rv)
            if rc is not None:
                rating_count = str(rc)

        offers = j.get("offers")
        if isinstance(offers, dict):
            p = offers.get("price")
            cur = offers.get("priceCurrency")
            if p is not None and cur:
                price = f"{p} {cur}"
            elif p is not None:
                price = str(p)
        elif isinstance(offers, list) and offers:
            o0 = offers[0]
            if isinstance(o0, dict):
                p = o0.get("price")
                cur = o0.get("priceCurrency")
                if p is not None and cur:
                    price = f"{p} {cur}"
                elif p is not None:
                    price = str(p)

        g = j.get("genre")
        for gg in _as_list(g):
            if isinstance(gg, str) and gg.strip():
                genres.append(gg.strip())

        description = j.get("description") if isinstance(j.get("description"), str) else None

    # meta tag fallbacks
    title = title or _get_meta(soup, "og:title") or _get_meta(soup, "title")
    description = description or _get_meta(soup, "og:description") or _get_meta(soup, "description")

    # HTML fallback for <h1>
    if not title:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            title = h1.get_text(strip=True)

    # HTML fallbacks for fields often missing in JSON-LD on LitRes
    if not authors:
        a = _first_text(soup, [".art__author--details a", ".book__author a"])
        if a:
            authors = [a]

    if not genres:
        genres = _list_texts(
            soup,
            [
                ".book-genres-and-tags__wrapper a",
                "a[href^='/genre/']",
            ],
        )

    formats = _list_texts(
        soup,
        [
            ".book-tabs-format__element",
            "[data-testid*='format']",
        ],
    )

    if not description:
        description = _first_text(soup, [".book__infoAboutBook--wrapper"])

    data = {
        "url": url,
        "title": title or "",
        "authors": ", ".join(dict.fromkeys([a.strip() for a in authors if a.strip()])),
        "price": price or "",
        "rating": rating or "",
        "rating_count": rating_count or "",
        "genres": ", ".join(dict.fromkeys([g.strip() for g in genres if g.strip()])),
        "formats": ", ".join(formats),
        "description": (description or "").strip(),
        "scraped_at": utc_now_iso(),
        "status": "ok",
        "error": None,
    }

    return BookParseResult(ok=True, data=data, error=None)


