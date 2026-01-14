from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True)
class DbConfig:
    path: Path


def connect_db(cfg: DbConfig) -> sqlite3.Connection:
    cfg.path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(cfg.path))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.row_factory = sqlite3.Row
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS queue (
          url TEXT PRIMARY KEY,
          status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          discovered_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS books (
          url TEXT PRIMARY KEY,
          title TEXT,
          authors TEXT,
          price TEXT,
          rating TEXT,
          rating_count TEXT,
          genres TEXT,
          formats TEXT,
          description TEXT,
          cover_url TEXT,
          pages TEXT,
          age_restriction TEXT,
          in_series TEXT,
          series_title TEXT,
          format_text TEXT,
          format_audio TEXT,
          format_paper TEXT,
          reviews_count TEXT,
          quotations_count TEXT,
          livelib_rating TEXT,
          livelib_rating_count TEXT,
          chapters TEXT,
          scraped_at TEXT NOT NULL,
          status TEXT NOT NULL,
          error TEXT
        );

        CREATE TABLE IF NOT EXISTS reviews (
          review_id TEXT PRIMARY KEY,
          book_url TEXT NOT NULL,
          author TEXT,
          author_avatar TEXT,
          published_at TEXT,
          rating TEXT,
          text TEXT,
          likes TEXT,
          dislikes TEXT,
          comments_count TEXT,
          replies_count TEXT,
          replies_json TEXT,
          is_livelib INTEGER NOT NULL DEFAULT 0,
          scraped_at TEXT NOT NULL,
          FOREIGN KEY(book_url) REFERENCES books(url)
        );
        """
    )
    _ensure_schema(con)
    con.commit()


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return {r["name"] for r in rows}


def _ensure_columns(con: sqlite3.Connection, table: str, columns_sql: dict[str, str]) -> None:
    existing = _table_columns(con, table)
    for name, ddl in columns_sql.items():
        if name in existing:
            continue
        con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


def _ensure_schema(con: sqlite3.Connection) -> None:
    """
    Migrations for existing DBs (CREATE TABLE IF NOT EXISTS doesn't add new columns).
    """
    _ensure_columns(
        con,
        "books",
        {
            "cover_url": "TEXT",
            "pages": "TEXT",
            "age_restriction": "TEXT",
            "in_series": "TEXT",
            "series_title": "TEXT",
            "format_text": "TEXT",
            "format_audio": "TEXT",
            "format_paper": "TEXT",
            "reviews_count": "TEXT",
            "quotations_count": "TEXT",
            "livelib_rating": "TEXT",
            "livelib_rating_count": "TEXT",
            "chapters": "TEXT",
        },
    )


def enqueue_urls(con: sqlite3.Connection, urls: Iterable[str]) -> int:
    now = utc_now_iso()
    rows = [(u, now) for u in urls]
    con.executemany(
        "INSERT OR IGNORE INTO queue(url, discovered_at) VALUES(?, ?)",
        rows,
    )
    con.commit()
    return con.total_changes


def claim_batch(con: sqlite3.Connection, *, batch_size: int) -> list[str]:
    """
    Atomically move N pending URLs to in_progress and return them.
    """
    cur = con.cursor()
    cur.execute(
        """
        SELECT url FROM queue
        WHERE status='pending'
        ORDER BY discovered_at
        LIMIT ?
        """,
        (batch_size,),
    )
    urls = [r["url"] for r in cur.fetchall()]
    if not urls:
        return []
    cur.executemany(
        "UPDATE queue SET status='in_progress', attempts=attempts+1 WHERE url=?",
        [(u,) for u in urls],
    )
    con.commit()
    return urls


def mark_queue_done(con: sqlite3.Connection, url: str) -> None:
    con.execute("UPDATE queue SET status='done', last_error=NULL WHERE url=?", (url,))
    con.commit()


def mark_queue_failed(con: sqlite3.Connection, url: str, error: str) -> None:
    con.execute(
        "UPDATE queue SET status='failed', last_error=? WHERE url=?",
        (error[:2000], url),
    )
    con.commit()


def upsert_book(con: sqlite3.Connection, book: dict[str, Any]) -> None:
    con.execute(
        """
        INSERT INTO books(
          url, title, authors, price, rating, rating_count, genres, formats, description,
          cover_url, pages, age_restriction, in_series, series_title,
          format_text, format_audio, format_paper,
          reviews_count, quotations_count, livelib_rating, livelib_rating_count, chapters,
          scraped_at, status, error
        )
        VALUES(
          :url, :title, :authors, :price, :rating, :rating_count, :genres, :formats, :description,
          :cover_url, :pages, :age_restriction, :in_series, :series_title,
          :format_text, :format_audio, :format_paper,
          :reviews_count, :quotations_count, :livelib_rating, :livelib_rating_count, :chapters,
          :scraped_at, :status, :error
        )
        ON CONFLICT(url) DO UPDATE SET
          title=excluded.title,
          authors=excluded.authors,
          price=excluded.price,
          rating=excluded.rating,
          rating_count=excluded.rating_count,
          genres=excluded.genres,
          formats=excluded.formats,
          description=excluded.description,
          cover_url=excluded.cover_url,
          pages=excluded.pages,
          age_restriction=excluded.age_restriction,
          in_series=excluded.in_series,
          series_title=excluded.series_title,
          format_text=excluded.format_text,
          format_audio=excluded.format_audio,
          format_paper=excluded.format_paper,
          reviews_count=excluded.reviews_count,
          quotations_count=excluded.quotations_count,
          livelib_rating=excluded.livelib_rating,
          livelib_rating_count=excluded.livelib_rating_count,
          chapters=excluded.chapters,
          scraped_at=excluded.scraped_at,
          status=excluded.status,
          error=excluded.error
        """,
        book,
    )
    con.commit()


def iter_books(con: sqlite3.Connection) -> list[sqlite3.Row]:
    cur = con.execute(
        """
        SELECT
          url, title, authors, price, rating, rating_count, genres, formats, description,
          cover_url, pages, age_restriction, in_series, series_title,
          format_text, format_audio, format_paper,
          reviews_count, quotations_count, livelib_rating, livelib_rating_count, chapters,
          scraped_at
        FROM books
        WHERE status='ok'
        ORDER BY scraped_at
        """
    )
    return cur.fetchall()


def _make_review_id(book_url: str, author: str, published_at: str, text: str) -> str:
    src = f"{book_url}|{author}|{published_at}|{text[:200]}".encode("utf-8", errors="ignore")
    return hashlib.sha1(src).hexdigest()[:16]


def upsert_reviews(con: sqlite3.Connection, reviews: list[dict[str, Any]]) -> int:
    if not reviews:
        return 0
    now = utc_now_iso()
    rows: list[dict[str, Any]] = []
    for r in reviews:
        book_url = str(r.get("book_url") or "")
        author = str(r.get("author") or "")
        published_at = str(r.get("published_at") or "")
        text = str(r.get("text") or "")
        review_id = str(r.get("review_id") or "") or _make_review_id(book_url, author, published_at, text)
        rows.append(
            {
                "review_id": review_id,
                "book_url": book_url,
                "author": author,
                "author_avatar": str(r.get("author_avatar") or ""),
                "published_at": published_at,
                "rating": str(r.get("rating") or ""),
                "text": text,
                "likes": str(r.get("likes") or ""),
                "dislikes": str(r.get("dislikes") or ""),
                "comments_count": str(r.get("comments_count") or ""),
                "replies_count": str(r.get("replies_count") or ""),
                "replies_json": json.dumps(r.get("replies") or [], ensure_ascii=False),
                "is_livelib": int(r.get("is_livelib") or 0),
                "scraped_at": now,
            }
        )

    con.executemany(
        """
        INSERT INTO reviews(
          review_id, book_url, author, author_avatar, published_at, rating, text,
          likes, dislikes, comments_count, replies_count, replies_json, is_livelib,
          scraped_at
        )
        VALUES(
          :review_id, :book_url, :author, :author_avatar, :published_at, :rating, :text,
          :likes, :dislikes, :comments_count, :replies_count, :replies_json, :is_livelib,
          :scraped_at
        )
        ON CONFLICT(review_id) DO UPDATE SET
          book_url=excluded.book_url,
          author=excluded.author,
          author_avatar=excluded.author_avatar,
          published_at=excluded.published_at,
          rating=excluded.rating,
          text=excluded.text,
          likes=excluded.likes,
          dislikes=excluded.dislikes,
          comments_count=excluded.comments_count,
          replies_count=excluded.replies_count,
          replies_json=excluded.replies_json,
          is_livelib=excluded.is_livelib,
          scraped_at=excluded.scraped_at
        """,
        rows,
    )
    con.commit()
    return len(rows)


def iter_reviews(con: sqlite3.Connection) -> list[sqlite3.Row]:
    cur = con.execute(
        """
        SELECT
          review_id, book_url, author, author_avatar, published_at, rating, text,
          likes, dislikes, comments_count, replies_count, replies_json, is_livelib, scraped_at
        FROM reviews
        ORDER BY scraped_at
        """
    )
    return cur.fetchall()


