from __future__ import annotations

import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator

import openpyxl

from .book_page import parse_book_page
from .http import FetchConfig, polite_sleep
from .catalog import CatalogDiscovery, iter_book_urls_from_catalog
from .sitemaps import SitemapDiscovery, discover_sitemaps, is_probable_book_url, iter_urls_from_sitemaps
from .storage import DbConfig, connect_db, enqueue_urls, init_db, iter_books, mark_queue_done, mark_queue_failed, upsert_book, utc_now_iso


_thread_local = threading.local()


def _get_session():
    from .http import make_session

    s = getattr(_thread_local, "session", None)
    if s is None:
        s = make_session()
        _thread_local.session = s
    return s


def _stream_book_urls(
    *,
    base_url: str,
    cfg: FetchConfig,
    method: str,
    max_pages_per_genre: int,
) -> Iterator[str]:
    if method == "sitemap":
        sitemaps = discover_sitemaps(SitemapDiscovery(base_url=base_url), cfg=cfg)
        for url in iter_urls_from_sitemaps(sitemaps, cfg=cfg):
            if is_probable_book_url(url):
                yield url
        return

    # default: catalog via genres -> paging
    yield from iter_book_urls_from_catalog(
        CatalogDiscovery(base_url=base_url),
        cfg=cfg,
        max_pages_per_genre=max_pages_per_genre,
    )


def _enqueue_streaming(
    *,
    con,
    urls: Iterator[str],
    limit: int | None,
    batch_size: int = 5000,
) -> int:
    buf: list[str] = []
    total = 0
    for u in urls:
        buf.append(u)
        if limit is not None and (total + len(buf)) >= limit:
            buf = buf[: max(0, limit - total)]
            if buf:
                enqueue_urls(con, buf)
                total += len(buf)
            return total
        if len(buf) >= batch_size:
            enqueue_urls(con, buf)
            total += len(buf)
            buf = []
    if buf:
        enqueue_urls(con, buf)
        total += len(buf)
    return total


def cmd_discover(args: argparse.Namespace) -> None:
    cfg = FetchConfig(
        timeout_s=args.timeout,
        min_delay_s=args.min_delay,
        max_delay_s=args.max_delay,
        max_retries=args.retries,
    )
    con = connect_db(DbConfig(path=Path(args.db)))
    init_db(con)
    count = _enqueue_streaming(
        con=con,
        urls=_stream_book_urls(
            base_url=args.base_url,
            cfg=cfg,
            method=args.method,
            max_pages_per_genre=args.max_pages_per_genre,
        ),
        limit=args.limit,
    )
    print(f"OK: discovered & enqueued: {count} book URLs")


def cmd_crawl(args: argparse.Namespace) -> None:
    cfg = FetchConfig(
        timeout_s=args.timeout,
        min_delay_s=args.min_delay,
        max_delay_s=args.max_delay,
        max_retries=args.retries,
    )
    con = connect_db(DbConfig(path=Path(args.db)))
    init_db(con)

    # If a previous run was interrupted, some rows can remain in_progress.
    # Reset them back to pending so crawl can continue.
    con.execute("UPDATE queue SET status='pending' WHERE status='in_progress'")
    con.commit()

    if args.discover:
        count = _enqueue_streaming(
            con=con,
            urls=_stream_book_urls(
                base_url=args.base_url,
                cfg=cfg,
                method=args.method,
                max_pages_per_genre=args.max_pages_per_genre,
            ),
            limit=args.discover_limit,
        )
        print(f"OK: discovered & enqueued: {count} book URLs")

    processed = 0
    while True:
        if args.limit is not None and processed >= args.limit:
            break
        remaining = None if args.limit is None else max(0, args.limit - processed)
        batch_limit = max(1, args.workers * 3)
        if remaining is not None:
            batch_limit = min(batch_limit, remaining)
        batch = con.execute(
            """
            SELECT url FROM queue
            WHERE status='pending'
            ORDER BY discovered_at
            LIMIT ?
            """,
            (batch_limit,),
        ).fetchall()
        urls = [r["url"] for r in batch]
        if not urls:
            break
        con.executemany(
            "UPDATE queue SET status='in_progress', attempts=attempts+1 WHERE url=?",
            [(u,) for u in urls],
        )
        con.commit()

        def worker(u: str):
            polite_sleep(cfg)
            return parse_book_page(u, cfg=cfg, session=_get_session())

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(worker, u): u for u in urls}
            for fut in as_completed(futs):
                u = futs[fut]
                try:
                    res = fut.result()
                except Exception as e:  # noqa: BLE001
                    err = str(e)
                    upsert_book(
                        con,
                        {
                            "url": u,
                            "title": "",
                            "authors": "",
                            "price": "",
                            "rating": "",
                            "rating_count": "",
                            "genres": "",
                            "formats": "",
                            "description": "",
                            "scraped_at": utc_now_iso(),
                            "status": "error",
                            "error": err,
                        },
                    )
                    mark_queue_failed(con, u, err)
                else:
                    if res.ok:
                        upsert_book(con, res.data)
                        mark_queue_done(con, u)
                    else:
                        err = res.error or "unknown error"
                        d = dict(res.data)
                        d.update(
                            {
                                "title": "",
                                "authors": "",
                                "price": "",
                                "rating": "",
                                "rating_count": "",
                                "genres": "",
                                "formats": "",
                                "description": "",
                                "scraped_at": utc_now_iso(),
                                "status": "error",
                                "error": err,
                            }
                        )
                        upsert_book(con, d)
                        mark_queue_failed(con, u, err)
                processed += 1
                if processed % 50 == 0:
                    print(f"processed {processed} pages")

    print(f"OK: crawl finished. Processed: {processed}. DB: {args.db}")


def cmd_single(args: argparse.Namespace) -> None:
    cfg = FetchConfig(
        timeout_s=args.timeout,
        min_delay_s=args.min_delay,
        max_delay_s=args.max_delay,
        max_retries=args.retries,
    )
    res = parse_book_page(args.url, cfg=cfg)
    if not res.ok:
        raise SystemExit(f"Failed: {res.error}")
    print("OK")
    for k, v in res.data.items():
        if k in {"description"}:
            continue
        print(f"{k}: {v}")


def cmd_export(args: argparse.Namespace) -> None:
    con = connect_db(DbConfig(path=Path(args.db)))
    init_db(con)
    rows = iter_books(con)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "LitRes"

    headers = [
        "url",
        "title",
        "authors",
        "price",
        "rating",
        "rating_count",
        "genres",
        "formats",
        "description",
        "scraped_at",
    ]
    ws.append(headers)
    for r in rows:
        ws.append([r[h] if h in r.keys() else "" for h in headers])
    wb.save(str(out))
    print(f"OK: exported {len(rows)} rows to {out}")


def cmd_status(args: argparse.Namespace) -> None:
    con = connect_db(DbConfig(path=Path(args.db)))
    init_db(con)
    q = con.execute(
        """
        SELECT status, COUNT(*) AS cnt
        FROM queue
        GROUP BY status
        ORDER BY status
        """
    ).fetchall()
    b = con.execute(
        """
        SELECT status, COUNT(*) AS cnt
        FROM books
        GROUP BY status
        ORDER BY status
        """
    ).fetchall()
    last_ok = con.execute("SELECT MAX(scraped_at) AS ts FROM books WHERE status='ok'").fetchone()["ts"]
    print("Queue:")
    for r in q:
        print(f"  {r['status']}: {r['cnt']}")
    print("Books:")
    for r in b:
        print(f"  {r['status']}: {r['cnt']}")
    print(f"Last OK scraped_at: {last_ok or '-'}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="litres-parser")
    p.add_argument("--db", default="litres.sqlite", help="SQLite DB path (default: litres.sqlite)")
    p.add_argument("--base-url", default="https://www.litres.ru/", help="Base URL for discovery (default: https://www.litres.ru/)")
    p.add_argument("--method", choices=["catalog", "sitemap"], default="catalog", help="Discovery method (default: catalog)")
    p.add_argument("--max-pages-per-genre", type=int, default=50, help="Catalog discovery limit per genre (default: 50)")
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--min-delay", type=float, default=0.5)
    p.add_argument("--max-delay", type=float, default=1.5)
    p.add_argument("--retries", type=int, default=4)

    # In Windows/CLI usage it's common to run just `python main.py`.
    # Make subcommand optional and print help ourselves if missing.
    sub = p.add_subparsers(dest="cmd", required=False)

    d = sub.add_parser("discover", help="Discover book URLs via robots.txt+sitemaps and enqueue them")
    d.add_argument("--limit", type=int, default=None, help="Stop after enqueuing N URLs")
    d.set_defaults(func=cmd_discover)

    c = sub.add_parser("crawl", help="Crawl queued URLs and parse book metadata")
    c.add_argument("--discover", action="store_true", help="Also run discovery before crawling")
    c.add_argument("--discover-limit", type=int, default=None, help="Discovery limit (only with --discover)")
    c.add_argument("--workers", type=int, default=5)
    c.add_argument("--limit", type=int, default=None, help="Stop after processing N pages (crawl stage)")
    c.set_defaults(func=cmd_crawl)

    s = sub.add_parser("single", help="Parse one book URL and print extracted metadata")
    s.add_argument("url")
    s.set_defaults(func=cmd_single)

    e = sub.add_parser("export", help="Export parsed books from DB to XLSX")
    e.add_argument("--out", default="litres.xlsx")
    e.set_defaults(func=cmd_export)

    st = sub.add_parser("status", help="Show current progress from SQLite DB")
    st.set_defaults(func=cmd_status)

    return p


def main(argv: list[str] | None = None) -> None:
    import sys

    # Best-effort fix for Windows console encodings (avoid mojibake for Cyrillic).
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    def _normalize_global_flags(args: list[str]) -> list[str]:
        """
        Allow placing global flags either before or after the subcommand.
        Example:
          python main.py crawl --limit 10 --min-delay 0.5
        becomes:
          python main.py --min-delay 0.5 crawl --limit 10
        """
        global_flags = {
            "--db",
            "--base-url",
            "--method",
            "--max-pages-per-genre",
            "--timeout",
            "--min-delay",
            "--max-delay",
            "--retries",
        }

        head: list[str] = []
        tail: list[str] = []
        i = 0
        while i < len(args):
            a = args[i]
            # support --flag=value form
            if any(a.startswith(f"{f}=") for f in global_flags):
                head.append(a)
                i += 1
                continue
            if a in global_flags:
                head.append(a)
                if i + 1 < len(args):
                    head.append(args[i + 1])
                    i += 2
                else:
                    i += 1
                continue
            tail.append(a)
            i += 1
        return head + tail

    p = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    argv = _normalize_global_flags(list(argv))
    args = p.parse_args(argv)
    if getattr(args, "cmd", None) is None:
        p.print_help()
        print("\nExamples:")
        print("  python main.py discover --limit 5000")
        print("  python main.py crawl --workers 5 --limit 1000")
        print("  python main.py export --out litres.xlsx")
        print("  python main.py single https://www.litres.ru/book/...")
        raise SystemExit(0)
    # mypy/argparse: func exists when cmd is set via set_defaults
    args.func(args)


