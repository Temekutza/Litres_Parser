from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from typing import Iterable

import requests


DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class FetchConfig:
    timeout_s: float = 30.0
    min_delay_s: float = 0.5
    max_delay_s: float = 1.5
    max_retries: int = 4


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": DEFAULT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }
    )
    return s


_rate_lock = threading.Lock()
_next_allowed_ts = 0.0


def polite_sleep(cfg: FetchConfig) -> None:
    """
    Global (process-wide) polite sleep.

    With concurrency (ThreadPoolExecutor) per-thread sleeps can amplify the request rate.
    This function serializes the delays across threads to keep the aggregate rate polite.
    """
    global _next_allowed_ts
    delay = random.uniform(cfg.min_delay_s, cfg.max_delay_s)
    now = time.monotonic()
    with _rate_lock:
        # ensure at least `delay` between *any* two requests globally
        wake_at = max(now, _next_allowed_ts)
        _next_allowed_ts = wake_at + delay
    sleep_s = max(0.0, wake_at - now)
    if sleep_s:
        time.sleep(sleep_s)


def fetch_text(
    session: requests.Session,
    url: str,
    *,
    cfg: FetchConfig,
    ok_statuses: Iterable[int] = (200,),
) -> str:
    last_err: Exception | None = None
    for attempt in range(cfg.max_retries):
        if attempt:
            polite_sleep(cfg)
        try:
            resp = session.get(url, timeout=cfg.timeout_s)
            if resp.status_code in ok_statuses:
                resp.encoding = resp.encoding or "utf-8"
                return resp.text
            raise RuntimeError(f"HTTP {resp.status_code} for {url}")
        except Exception as e:  # noqa: BLE001 - deliberate retry wrapper
            last_err = e
    raise RuntimeError(f"Failed to fetch {url}") from last_err


def fetch_bytes(
    session: requests.Session,
    url: str,
    *,
    cfg: FetchConfig,
    ok_statuses: Iterable[int] = (200,),
) -> bytes:
    last_err: Exception | None = None
    for attempt in range(cfg.max_retries):
        if attempt:
            polite_sleep(cfg)
        try:
            resp = session.get(url, timeout=cfg.timeout_s)
            if resp.status_code in ok_statuses:
                return resp.content
            raise RuntimeError(f"HTTP {resp.status_code} for {url}")
        except Exception as e:  # noqa: BLE001 - deliberate retry wrapper
            last_err = e
    raise RuntimeError(f"Failed to fetch {url}") from last_err


