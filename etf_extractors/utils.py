from __future__ import annotations

from io import StringIO
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": "Mozilla/5.0 ETFExtractor/1.0"
}


def fetch_html(url: str, timeout: int = 30) -> str:
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def make_soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def absolute_url(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def read_csv_from_url(url: str, timeout: int = 30) -> pd.DataFrame:
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    text = response.content.decode("utf-8", errors="replace")
    return pd.read_csv(StringIO(text))


def find_csv_link(base_url: str, html: str) -> str | None:
    soup = make_soup(html)

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        text = a_tag.get_text(" ", strip=True).lower()
        combined = f"{text} {href}".lower()

        if "csv" in combined or "download" in combined:
            return absolute_url(base_url, href)

    return None