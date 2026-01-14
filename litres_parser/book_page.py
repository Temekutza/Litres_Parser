from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from bs4 import BeautifulSoup
from urllib.parse import urljoin

from .http import FetchConfig, fetch_text, make_session
from .storage import utc_now_iso


@dataclass(frozen=True)
class BookParseResult:
    ok: bool
    data: dict[str, Any]
    reviews: list[dict[str, Any]] | None = None
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


_INT_RE = re.compile(r"(\d+)")
_FLOAT_RE = re.compile(r"(\d+(?:[.,]\d+)?)")


def _first_int(text: str | None) -> int | None:
    if not text:
        return None
    m = _INT_RE.search(text.replace("\xa0", " "))
    return int(m.group(1)) if m else None


def _first_float(text: str | None) -> float | None:
    if not text:
        return None
    m = _FLOAT_RE.search(text.replace("\xa0", " "))
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


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


def _first_text_by_any_class(soup: BeautifulSoup, class_names: list[str]) -> str | None:
    """
    Some LitRes hashed classnames start with digits (e.g. "8dfa70c8") which are not valid
    CSS identifiers unless escaped. Using BeautifulSoup class_ lookup avoids selector issues.
    """
    for cls in class_names:
        try:
            el = soup.find(class_=cls)
        except Exception:
            el = None
        if not el:
            continue
        t = el.get_text(" ", strip=True)
        if t:
            return t
    return None


def _extract_formats_flags(soup: BeautifulSoup) -> tuple[list[str], int, int, int]:
    """
    Extract visible formats + derive three boolean flags:
    - text on site
    - audio
    - paper

    Prefer semantic classnames; fall back to text heuristics.
    """
    formats = _list_texts(
        soup,
        [
            ".book-tabs-format__element",
            ".book-tabs-format__wrapper .book-tabs-format__element",
            "[data-testid*='format']",
        ],
    )

    # Attempt: classnames like book-tabs-format__element_текст/аудио/бумага.
    # We search by class tokens to avoid CSS escaping issues with non-latin suffixes.
    has_text = 0
    has_audio = 0
    has_paper = 0
    for tag in soup.find_all(True):
        classes = tag.get("class")
        if not classes:
            continue
        if isinstance(classes, str):
            classes = [classes]
        if not isinstance(classes, list):
            continue
        for c in classes:
            if not isinstance(c, str):
                continue
            if c.startswith("book-tabs-format__element_"):
                suffix = c[len("book-tabs-format__element_") :].lower()
                if "текст" in suffix or "text" in suffix:
                    has_text = 1
                elif "аудио" in suffix or "audio" in suffix:
                    has_audio = 1
                elif "бума" in suffix or "paper" in suffix:
                    has_paper = 1

    # Text fallback: analyze collected labels
    if not (has_text or has_audio or has_paper):
        ftxt = ", ".join(formats).lower()
        has_text = 1 if any(x in ftxt for x in ("текст", "text", "читать")) else 0
        has_audio = 1 if any(x in ftxt for x in ("аудио", "audio")) else 0
        has_paper = 1 if any(x in ftxt for x in ("бума", "paper")) else 0

    return formats, has_text, has_audio, has_paper


def _find_first_attr(soup: BeautifulSoup, selectors: list[str], attrs: list[str]) -> str | None:
    for sel in selectors:
        try:
            el = soup.select_one(sel)
        except Exception:
            continue
        if not el:
            continue
        for a in attrs:
            v = el.get(a)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _extract_cover_url(soup: BeautifulSoup, base_url: str) -> str | None:
    # Prefer stable meta tags
    u = _get_meta(soup, "og:image") or _get_meta(soup, "twitter:image")
    if u:
        return urljoin(base_url, u)

    # Fallback: look for an <img> inside known cover wrappers (hash-classes from user list)
    for cls in ("_0bd8490c", "_78d816e0"):
        wrap = soup.find(class_=cls)
        if not wrap:
            continue
        img = wrap.find("img")
        if img:
            src = img.get("src") or img.get("data-src")
            if isinstance(src, str) and src.strip():
                return urljoin(base_url, src.strip())
            srcset = img.get("srcset")
            if isinstance(srcset, str) and srcset.strip():
                # take first URL from srcset
                first = srcset.split(",")[0].strip().split(" ")[0].strip()
                if first:
                    return urljoin(base_url, first)
    return None


def _extract_series(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    """
    Extract series information. Returns (in_series, series_title).
    in_series: "1" if book is part of a series, "0" otherwise.
    series_title: Name of the series (e.g., "Freedom. Интернет-бестселлеры Л. Дж. Шэн").
    """
    series_title = None
    
    # 1. Try JSON-LD first (most reliable)
    # Note: series info is often NOT in JSON-LD on LitRes
    
    # 2. Try semantic HTML classes (explicit series markers)
    series_title = _first_text(
        soup,
        [
            ".art__inSeries--title",  # Old layout
            ".art__inSeries a",
            "a.art__inSeries",
        ],
    )
    
    # 3. Try data-testid
    if not series_title:
        series_block = soup.select_one("[data-testid='art__inSeries--title']")
        if series_block:
            series_title = series_block.get_text(" ", strip=True)
    
    # 4. Hashed-class fallback (less reliable)
    if not series_title:
        series_title = _first_text_by_any_class(soup, ["31109fa8", "aed4345d", "_694da326"])
    
    # 5. Filter out obvious false positives (categories, not series)
    if series_title and any(word in series_title.lower() for word in ["каталог", "жанр", "легкое чтение", "серьезное чтение", "бизнес"]):
        series_title = None
    
    in_series = "1" if series_title else "0"
    return in_series, series_title


def _extract_pages_and_age(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    """Extract page count and age restriction."""
    pages_text = None
    age_text = None
    
    # 1. Try data-testid
    volume_block = soup.select_one("[data-testid='book-volume__wrapper']") or soup.select_one(".book-volume__wrapper")
    
    if volume_block:
        # Pages
        pages_el = volume_block.select_one(".a918461a")
        if pages_el:
            pages_text = pages_el.get_text(" ", strip=True)
        
        # Age
        age_el = volume_block.select_one("._6c4e649e._32449fb1") or volume_block.select_one("._6c4e649e") or volume_block.select_one("._32449fb1")
        if age_el:
            age_text = age_el.get_text(" ", strip=True)
        
        # Heuristic fallback
        if not pages_text or not age_text:
            txt = volume_block.get_text(" ", strip=True).lower()
            if not pages_text and any(k in txt for k in ("стр", "страниц", "page")):
                # Extract just the pages part
                for line in volume_block.get_text("\n").split("\n"):
                    if any(k in line.lower() for k in ("стр", "страниц", "page")):
                        pages_text = line.strip()
                        break
            if not age_text and any(k in txt for k in ("+", "лет", "age", "ограничение")):
                for line in volume_block.get_text("\n").split("\n"):
                    if any(k in line.lower() for k in ("+", "лет", "age")):
                        age_text = line.strip()
                        break
    
    pages = _first_int(pages_text)
    age = _first_int(age_text)
    return (str(pages) if pages is not None else None, f"{age}+" if age is not None and "+" not in (age_text or "") else (age_text.strip() if isinstance(age_text, str) else None))


def _extract_factoids(soup: BeautifulSoup) -> dict[str, str]:
    """
    Extract ratings, reviews count, quotations count from the page.
    Sources: __NEXT_DATA__ JSON (priority) → HTML fallbacks.
    IMPORTANT: Clearly separate LitRes vs LiveLib data.
    """
    out: dict[str, str] = {}

    # 1. Try __NEXT_DATA__ JSON first
    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if next_data_tag and next_data_tag.string:
        try:
            js = json.loads(next_data_tag.string)
            props = js.get("props", {}).get("pageProps", {})
            initial_state = props.get("initialState", {})
            if isinstance(initial_state, str):
                initial_state = json.loads(initial_state)
            
            rtk = initial_state.get("rtkqApi", {})
            queries = rtk.get("queries", {})
            
            art_data = None
            for k, v in queries.items():
                if k.startswith("getArt("):
                    art_data = v.get("data", {}).get("payload", {}).get("data", {})
                    break
            
            if art_data:
                # LitRes Data (primary site rating)
                out["rating_litres"] = str(art_data.get("rating") or "")
                out["rating_count_litres"] = str(art_data.get("votes_count") or "")
                out["reviews_count"] = str(art_data.get("reviews_count") or "")
                out["quotations_count"] = str(art_data.get("quotes_count") or "")
                
                # LiveLib Data (nested object)
                ll = art_data.get("livelib")
                if isinstance(ll, dict):
                    ll_rating = ll.get("rating") or ll.get("average_rating")
                    ll_count = ll.get("marks_count") or ll.get("votes_count") or ll.get("reviews_count")
                    if ll_rating:
                        out["livelib_rating"] = str(ll_rating)
                    if ll_count:
                        out["livelib_rating_count"] = str(ll_count)
        except Exception:
            pass

    # 2. HTML Fallbacks (only if JSON didn't provide data)
    
    # LitRes Rating Block
    if not out.get("rating_litres"):
        litres_block = soup.select_one("[data-testid='book-factoids__rating']")
        if litres_block:
            out["rating_litres"] = _first_text(litres_block, [".book-factoids__total-rating"]) or ""
            out["rating_count_litres"] = _first_text(litres_block, [".book-factoids__counter"]) or ""

    # Reviews Count
    if not out.get("reviews_count"):
        rev_block = soup.select_one("[data-testid='book-factoids__reviews']")
        if rev_block:
            # Try to get number from the block text
            txt = rev_block.get_text(" ", strip=True)
            cnt = _first_int(txt)
            if cnt:
                out["reviews_count"] = str(cnt)
        if not out.get("reviews_count"):
            # Fallback: look for link to /otzyvy/
            rev_link = soup.select_one("a[href*='/otzyvy/']")
            if rev_link:
                txt = rev_link.get_text(" ", strip=True)
                cnt = _first_int(txt)
                if cnt:
                    out["reviews_count"] = str(cnt)

    # Quotations Count
    if not out.get("quotations_count"):
        quot_block = soup.select_one("[data-testid='book-factoids__quotations']")
        if quot_block:
            # Try to get number from the block text
            txt = quot_block.get_text(" ", strip=True)
            cnt = _first_int(txt)
            if cnt:
                out["quotations_count"] = str(cnt)
        if not out.get("quotations_count"):
            # Fallback: look for link to /citati/
            quot_link = soup.select_one("a[href*='/citati/']")
            if quot_link:
                txt = quot_link.get_text(" ", strip=True)
                cnt = _first_int(txt)
                if cnt:
                    out["quotations_count"] = str(cnt)

    # LiveLib Block (SEPARATE from LitRes!)
    if not out.get("livelib_rating"):
        ll_block = soup.select_one("[data-testid='book-factoids__livelib']")
        if ll_block:
            # Rating: <span>4,6</span>
            rating_span = ll_block.select_one("span")
            if rating_span:
                rv = _first_float(rating_span.get_text())
                if rv:
                    out["livelib_rating"] = str(rv)
            
            # Count: <div class="_63638bfa">31 оценка</div>
            count_div = ll_block.select_one("._63638bfa")
            if count_div:
                cnt = _first_int(count_div.get_text())
                if cnt:
                    out["livelib_rating_count"] = str(cnt)

    return out


def _extract_chapters(soup: BeautifulSoup) -> list[str]:
    chapters: list[str] = []

    # 1. Try JSON state first (most reliable)
    next_data_tag = soup.find("script", id="__NEXT_DATA__")
    if next_data_tag and next_data_tag.string:
        try:
            js = json.loads(next_data_tag.string)
            props = js.get("props", {}).get("pageProps", {})
            initial_state = props.get("initialState", {})
            if isinstance(initial_state, str):
                initial_state = json.loads(initial_state)
            
            queries = initial_state.get("rtkqApi", {}).get("queries", {})
            for k, v in queries.items():
                if k.startswith("getArtTOC("):
                    toc = v.get("data", {}).get("payload", {}).get("data", [])
                    for item in toc:
                        t = item.get("title")
                        if t: chapters.append(t)
                    break
        except Exception:
            pass

    if chapters:
        return chapters

    # 2. Try multiple common selectors for TOC (Fallback)
    selectors = [
        "[data-testid='book-watch-table-content__wrapper']",
        ".book__tableContent--chapter",
        ".book-toc__item-title",
        ".book-toc__title",
        "._98980327", # Known hashed class for chapters
    ]
    for sel in selectors:
        for el in soup.select(sel):
            t = el.get_text(" ", strip=True)
            if t:
                chapters.append(t)
        if chapters:
            break
    
    # ...
    # unique preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for c in chapters:
        if c in seen:
            continue
        seen.add(c)
        uniq.append(c)
    return uniq


def _discover_reviews_url(page_url: str, soup: BeautifulSoup) -> str | None:
    # Try to find a direct link
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not isinstance(href, str):
            continue
        h = href.lower()
        if "otzyv" in h or "review" in h:
            return urljoin(page_url, href)
    # Guess common pattern: append /otzyvy/
    if page_url.endswith("/"):
        return urljoin(page_url, "otzyvy/")
    return urljoin(page_url + "/", "otzyvy/")


def _parse_reviews_from_soup(book_url: str, soup: BeautifulSoup) -> list[dict[str, Any]]:
    """
    Best-effort parser for reviews and replies.
    The site often uses dynamic/hashed classnames; we anchor on semantic ones when possible.
    """
    reviews: list[dict[str, Any]] = []
    # find review texts; then walk up to a container
    text_nodes = soup.select(".review__text")
    seen_containers: set[int] = set()
    for tn in text_nodes:
        container = tn
        for _ in range(6):
            if container is None:
                break
            # if it contains date/like buttons, treat as review root
            if container.select_one(".review__date--wrapper") or container.select_one(".reviews__like--button") or container.select_one(".reviews__dislike--button"):
                break
            container = container.parent  # type: ignore[assignment]
        if container is None:
            continue
        key = id(container)
        if key in seen_containers:
            continue
        seen_containers.add(key)

        text = tn.get_text(" ", strip=True)
        if not text:
            continue

        author = None
        author_el = container.select_one(".userInfo__notVerified--null") or container.select_one(".e2a9dc33")
        if author_el:
            author = author_el.get_text(" ", strip=True) or None
        date = _first_text(container, [".review__date--wrapper"])
        rating = _first_text(container, [".df420865"])
        likes = _first_text(container, [".reviews__like--button"])
        dislikes = _first_text(container, [".reviews__dislike--button"])
        # Fix for comments count
        comments_cnt = _first_text(container, [".author__reviews--count", ".author__reviews-count"])
        replies_cnt = _first_text(container, [".comment__thread--button"])
        avatar = _find_first_attr(container, ["._29dcaf5c img", "img._29dcaf5c"], ["src", "data-src"])

        is_livelib = bool(container.select_one("._1c30556e"))

        replies: list[dict[str, Any]] = []
        for rep in container.select(".reply__wrapper"):
            rep_author = _first_text(rep, [".userInfo__notVerified--null", ".e2a9dc33"]) or ""
            rep_date = _first_text(rep, [".reply__date--wrapper"]) or ""
            rep_text = _first_text(rep, [".reply__text", ".reply__message", ".comment__text"]) or ""
            rep_likes = _first_text(rep, [".reviews__like--button"]) or ""
            rep_dislikes = _first_text(rep, [".reviews__dislike--button"]) or ""
            rep_avatar = _find_first_attr(rep, ["._29dcaf5c img", "img._29dcaf5c"], ["src", "data-src"]) or ""
            
            replies.append({
                "author": rep_author,
                "avatar": rep_avatar,
                "date": rep_date,
                "text": rep_text,
                "likes": rep_likes,
                "dislikes": rep_dislikes,
            })

        reviews.append(
            {
                "book_url": book_url,
                "author": author or "",
                "author_avatar": avatar or "",
                "published_at": date or "",
                "rating": rating or "",
                "text": text,
                "likes": likes or "",
                "dislikes": dislikes or "",
                "comments_count": comments_cnt or "",
                "replies_count": replies_cnt or "",
                "replies": replies,
                "is_livelib": 1 if is_livelib else 0,
            }
        )
    return reviews


def _parse_reviews_from_next_data(book_url: str, soup: BeautifulSoup) -> list[dict[str, Any]]:
    """
    Fallback for pages where reviews are not present as HTML nodes but are embedded into
    Next.js state (`<script id="__NEXT_DATA__">`).
    """
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return []
    try:
        data = json.loads(tag.string)
    except Exception:
        return []
    try:
        page_props = data.get("props", {}).get("pageProps", {})
        init_state = page_props.get("initialState")
        if isinstance(init_state, str):
            init_state = json.loads(init_state)
        if not isinstance(init_state, dict):
            return []
        rtk = init_state.get("rtkqApi", {})
        queries = rtk.get("queries", {}) if isinstance(rtk, dict) else {}
        if not isinstance(queries, dict):
            return []
    except Exception:
        return []

    # Look for cached getArtReviews(...) query
    key = None
    for k in queries.keys():
        if isinstance(k, str) and k.lower().startswith("getartreviews("):
            key = k
            break
    if not key:
        return []

    entry = queries.get(key, {})
    if not isinstance(entry, dict):
        return []
    payload = (entry.get("data") or {}).get("payload") if isinstance(entry.get("data"), dict) else None
    if not isinstance(payload, dict):
        return []
    items = payload.get("data")
    if not isinstance(items, list):
        return []

    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        replies_src = it.get("replies") if isinstance(it.get("replies"), list) else []
        replies: list[dict[str, Any]] = []
        for rep in replies_src:
            if not isinstance(rep, dict):
                continue
            replies.append(
                {
                    "author": str(rep.get("user_display_name") or ""),
                    "avatar": str(rep.get("user_avatar_url") or ""),
                    "date": str(rep.get("created_at") or rep.get("createdAt") or ""),
                    "text": str(rep.get("text") or ""),
                    "likes": str(rep.get("likes_count") or ""),
                    "dislikes": str(rep.get("dislikes_count") or ""),
                }
            )

        source = str(it.get("source") or "")
        out.append(
            {
                "book_url": book_url,
                "author": str(it.get("user_display_name") or ""),
                "author_avatar": str(it.get("user_avatar_url") or ""),
                "published_at": str(it.get("created_at") or ""),
                "rating": str(it.get("item_rating") or ""),
                "text": str(it.get("text") or ""),
                "likes": str(it.get("likes_count") or ""),
                "dislikes": str(it.get("dislikes_count") or ""),
                # The API payload doesn't expose "comments_count" the same way as the HTML.
                "comments_count": "",
                "replies_count": str(it.get("replies_count") or ""),
                "replies": replies,
                "is_livelib": 1 if "livelib" in source.lower() else 0,
            }
        )
    return out


def parse_book_page(
    url: str,
    *,
    cfg: FetchConfig,
    session=None,
    with_reviews: bool = False,
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

    # Guardrails: sometimes litres serves a DDoS-Guard challenge page or redirects users to
    # non-book pages (e.g. author pages). In those cases we MUST NOT treat the HTML as a book.
    title_tag = soup.title.get_text(" ", strip=True) if soup.title else ""
    if "ddos-guard" in title_tag.lower():
        return BookParseResult(ok=False, data={"url": url}, error="Blocked by DDoS-Guard (challenge page)")

    og_url = _get_meta(soup, "og:url") or _get_meta(soup, "twitter:url")
    if og_url and any(x in og_url for x in ("/author/", "/account/", "/cart/")):
        return BookParseResult(ok=False, data={"url": url}, error=f"Not a book page (og:url={og_url})")

    jsonlds = _extract_jsonld(soup)
    j = _find_bookish_jsonld(jsonlds)

    title: str | None = None
    authors: list[str] = []
    price: str | None = None
    rating: str | None = None
    rating_count: str | None = None
    genres: list[str] = []
    description: str | None = None
    cover_url: str | None = None

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
        img = j.get("image")
        if isinstance(img, str) and img.strip():
            cover_url = urljoin(url, img.strip())
        elif isinstance(img, list) and img and isinstance(img[0], str):
            cover_url = urljoin(url, img[0].strip())

    # meta tag fallbacks
    title = title or _get_meta(soup, "og:title") or _get_meta(soup, "title")
    description = description or _get_meta(soup, "og:description") or _get_meta(soup, "description")
    cover_url = cover_url or _extract_cover_url(soup, url)

    # HTML fallback for <h1>
    if not title:
        title = (
            _first_text(soup, [".book__name--wrapper h1", ".book__name--wrapper"])
            or _first_text_by_any_class(soup, ["ed20f469", "8dfa70c8"])
        )
        if not title:
            h1 = soup.find("h1")
            if h1 and h1.get_text(strip=True):
                title = h1.get_text(strip=True)

    # If we still didn't find any book-ish structure, treat this as non-book HTML
    # (e.g. author page, listing page, etc.).
    if not j and not soup.select_one(".book__name--wrapper") and not soup.select_one(".book-factoids__total-rating"):
        extra = f" (og:url={og_url})" if og_url else ""
        return BookParseResult(ok=False, data={"url": url}, error="Not a book page" + extra)

    # HTML fallbacks for fields often missing in JSON-LD on LitRes
    if not authors:
        a = _first_text(soup, [".art__author--details a", ".book__author a"]) or _first_text_by_any_class(soup, ["4cd65209"])
        if a:
            authors = [a]

    if not price:
        price = _first_text(
            soup,
            [
                ".book__saleBlock--discountPrice",
                ".book__saleBlock--price",
                "[data-testid*='price']",
            ],
        )

    if not genres:
        # IMPORTANT: Limit search to genres block to avoid grabbing navigation links
        genres_block = soup.select_one("[data-testid='book-genres-and-tags__wrapper']") or soup.select_one(".book-genres-and-tags__wrapper")
        if genres_block:
            raw_genres = _list_texts(
                genres_block,  # Search INSIDE the block, not whole page
                [
                    "a[href^='/genre/']",
                    "a",
                ],
            )
            # Filter out navigation/menu items
            stopwords = {
                "каталог", "найти", "бонусы", "корзина", "мои книги", "войти",
                "подписка", "промокод", "новинки", "популярное", "подборки",
                "аудиокниги", "эксклюзивы", "черновики", "лекции", "комиксы",
                "вебтуны", "журнал", "сертификаты", "фанфики", "бесплатные",
                "легкое чтение", "серьезное чтение", "бизнес-книги", "детские книги",
                "родителям", "публицистика", "все жанры", "ещё"
            }
            genres = [g for g in raw_genres if g.lower() not in stopwords and len(g) > 2]

    formats, has_text, has_audio, has_paper = _extract_formats_flags(soup)

    if not description:
        description = _first_text(soup, [".book__infoAboutBook--wrapper"])

    # extra fields requested
    pages, age = _extract_pages_and_age(soup)
    in_series, series_title = _extract_series(soup)
    fact = _extract_factoids(soup)
    chapters = _extract_chapters(soup)

    data = {
        "url": url,
        "title": title or "",
        "authors": ", ".join(dict.fromkeys([a.strip() for a in authors if a.strip()])),
        "price": price or "",
        "rating": rating or fact.get("rating_litres", ""),
        "rating_count": rating_count or fact.get("rating_count_litres", ""),
        "genres": ", ".join(dict.fromkeys([g.strip() for g in genres if g.strip()])),
        "formats": ", ".join(formats),
        "format_text": str(has_text),
        "format_audio": str(has_audio),
        "format_paper": str(has_paper),
        "description": (description or "").strip(),
        "cover_url": cover_url or "",
        "pages": pages or "",
        "age_restriction": age or "",
        "in_series": (in_series or "").strip(),
        "series_title": (series_title or "").strip(),
        "reviews_count": fact.get("reviews_count", ""),
        "quotations_count": fact.get("quotations_count", ""),
        "livelib_rating": fact.get("livelib_rating", ""),
        "livelib_rating_count": fact.get("livelib_rating_count", ""),
        "chapters": "\n".join(chapters),
        "scraped_at": utc_now_iso(),
        "status": "ok",
        "error": None,
    }

    reviews: list[dict[str, Any]] | None = None
    if with_reviews:
        # first: try parse from the current page
        reviews = _parse_reviews_from_soup(url, soup)
        # if still nothing, try embedded Next.js state
        if not reviews:
            reviews = _parse_reviews_from_next_data(url, soup)
        # if still nothing, try dedicated reviews page (best effort)
        if not reviews:
            rev_url = _discover_reviews_url(url, soup)
            if rev_url:
                try:
                    rhtml = fetch_text(session, rev_url, cfg=cfg)
                    rsoup = BeautifulSoup(rhtml, "lxml")
                    reviews = _parse_reviews_from_soup(url, rsoup)
                    if not reviews:
                        reviews = _parse_reviews_from_next_data(url, rsoup)
                except Exception:
                    reviews = reviews or []

    return BookParseResult(ok=True, data=data, reviews=reviews, error=None)


