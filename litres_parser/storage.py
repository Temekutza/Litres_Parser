from __future__ import annotations

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
          scraped_at TEXT NOT NULL,
          status TEXT NOT NULL,
          error TEXT
        );
        """
    )
    con.commit()


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
          scraped_at, status, error
        )
        VALUES(
          :url, :title, :authors, :price, :rating, :rating_count, :genres, :formats, :description,
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
        SELECT url, title, authors, price, rating, rating_count, genres, formats, description, scraped_at
        FROM books
        WHERE status='ok'
        ORDER BY scraped_at
        """
    )
    return cur.fetchall()


