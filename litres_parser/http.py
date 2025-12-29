from __future__ import annotations

import random
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


def polite_sleep(cfg: FetchConfig) -> None:
    time.sleep(random.uniform(cfg.min_delay_s, cfg.max_delay_s))


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


