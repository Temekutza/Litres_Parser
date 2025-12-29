from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from .storage import utc_now_iso


@dataclass(frozen=True)
class SeleniumConfig:
    chromedriver_path: Path
    headless: bool = True
    wait_s: float = 3.0


def parse_book_selenium(url: str, *, cfg: SeleniumConfig) -> dict[str, Any]:
    """
    Heavy fallback: open the page in a real browser and scrape visible DOM.
    Use for debugging / when HTML is highly JS-driven.
    """
    options = webdriver.ChromeOptions()
    if cfg.headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    service = Service(str(cfg.chromedriver_path))
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(url)
        time.sleep(cfg.wait_s)

        data: dict[str, Any] = {
            "url": url,
            "title": "",
            "authors": "",
            "price": "",
            "rating": "",
            "rating_count": "",
            "genres": "",
            "formats": "",
            "description": "",
            "scraped_at": utc_now_iso(),
            "status": "ok",
            "error": None,
        }

        try:
            data["title"] = driver.find_element(By.CSS_SELECTOR, ".book__name--wrapper h1").text
        except Exception:
            pass
        try:
            data["authors"] = driver.find_element(By.CSS_SELECTOR, ".art__author--details a").text
        except Exception:
            pass
        try:
            data["price"] = driver.find_element(By.CSS_SELECTOR, ".book__saleBlock--discountPrice").text
        except Exception:
            pass
        try:
            data["rating"] = driver.find_element(By.CSS_SELECTOR, ".book-factoids__total-rating").text
        except Exception:
            pass
        try:
            data["rating_count"] = driver.find_element(By.CSS_SELECTOR, ".book-factoids__marks").text
        except Exception:
            pass

        try:
            genres = driver.find_elements(By.CSS_SELECTOR, ".book-genres-and-tags__wrapper a")
            data["genres"] = ", ".join([g.text for g in genres if g.text])
        except Exception:
            pass

        try:
            formats = driver.find_elements(By.CSS_SELECTOR, ".book-tabs-format__element")
            data["formats"] = ", ".join([f.text for f in formats if f.text])
        except Exception:
            pass

        try:
            data["description"] = driver.find_element(By.CSS_SELECTOR, ".book__infoAboutBook--wrapper").text
        except Exception:
            pass

        return data
    finally:
        driver.quit()


