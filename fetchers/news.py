"""
News fetcher — aggregates top recent news for a stock from:
  1. NSE corporate filings / announcements
  2. Google News RSS (no API key needed)
  3. Moneycontrol news search
Deduplicates by title similarity and returns the 5 most relevant items.
"""

import re
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any
from urllib.parse import quote_plus

import httpx

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

NSE_BASE = "https://www.nseindia.com"
GOOGLE_NEWS_RSS = (
    "https://news.google.com/rss/search?q={query}+stock+India"
    "&hl=en-IN&gl=IN&ceid=IN:en"
)
MC_NEWS_URL = "https://www.moneycontrol.com/news/tags/{slug}.html"


def _slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def _deduplicate(items: list[dict], key: str = "title", max_items: int = 5) -> list[dict]:
    """Remove near-duplicate titles; keep first occurrence."""
    seen: list[str] = []
    out: list[dict] = []
    for item in items:
        title = (item.get(key) or "").lower()
        title_words = set(re.findall(r"\w+", title))
        is_dup = False
        for s in seen:
            s_words = set(re.findall(r"\w+", s))
            if len(s_words) and len(title_words & s_words) / max(len(title_words), 1) > 0.6:
                is_dup = True
                break
        if not is_dup:
            seen.append(title)
            out.append(item)
        if len(out) >= max_items:
            break
    return out


class NewsFetcher:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            headers=HEADERS,
            follow_redirects=True,
            timeout=25.0,
        )

    async def _get_text(self, url: str) -> str | None:
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            # Respect the server's declared encoding; fall back to utf-8 with replacement
            encoding = resp.encoding or "utf-8"
            try:
                return resp.content.decode(encoding)
            except (UnicodeDecodeError, LookupError):
                return resp.content.decode("utf-8", errors="replace")
        except Exception as exc:
            logger.debug("News GET %s failed: %s", url, exc)
            return None

    async def _get_json(self, url: str) -> Any:
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("News JSON %s failed: %s", url, exc)
            return None

    # ------------------------------------------------------------------ #
    #  Individual source scrapers                                          #
    # ------------------------------------------------------------------ #

    async def _from_nse_announcements(self, symbol: str, company_name: str = "") -> list[dict]:
        """NSE corporate announcements feed."""
        url = (
            f"{NSE_BASE}/api/corporate-announcements"
            f"?index=equities&symbol={symbol}&issuer=&number=10"
        )
        # NSE needs prior cookie session — reuse same approach as NSEFetcher
        data = await self._get_json(url)
        if not data:
            return []
        rows = data if isinstance(data, list) else data.get("data", [])
        results = []
        for r in rows[:10]:
            results.append(
                {
                    "title": r.get("subject") or r.get("desc") or r.get("headline", ""),
                    "date": r.get("broadcastDate") or r.get("date") or r.get("an_dt"),
                    "url": r.get("attchmntFile") or r.get("link") or "",
                    "source": "NSE Announcement",
                }
            )
        return [x for x in results if x["title"]]

    async def _from_google_rss(self, symbol: str, company_name: str = "") -> list[dict]:
        """Google News RSS for recent mentions."""
        query = quote_plus(f'"{company_name or symbol}" NSE India stock')
        url = GOOGLE_NEWS_RSS.format(query=query)
        xml_text = await self._get_text(url)
        if not xml_text:
            return []
        results = []
        try:
            root = ET.fromstring(xml_text)
            channel = root.find("channel")
            if channel is None:
                return []
            for item in channel.findall("item")[:10]:
                title = (item.findtext("title") or "").strip()
                link = (item.findtext("link") or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()
                if title:
                    results.append(
                        {
                            "title": title,
                            "date": pub_date,
                            "url": link,
                            "source": "Google News",
                        }
                    )
        except ET.ParseError as exc:
            logger.debug("Google RSS parse error: %s", exc)
        return results

    async def _from_moneycontrol(self, symbol: str, company_name: str = "") -> list[dict]:
        """Moneycontrol news tag page scraper."""
        from bs4 import BeautifulSoup

        slug = _slugify(company_name or symbol)
        url = MC_NEWS_URL.format(slug=slug)
        html = await self._get_text(url)
        if not html:
            return []
        soup = BeautifulSoup(html, "lxml")
        results = []
        for li in soup.select("li.clearfix, .news-list li, article")[:10]:
            a_tag = li.find("a", href=True)
            if not a_tag:
                continue
            title = a_tag.get_text(strip=True) or a_tag.get("title", "")
            if not title or len(title) < 10:
                continue
            date_el = li.select_one("span.ago, span.date, time")
            results.append(
                {
                    "title": title,
                    "date": date_el.get_text(strip=True) if date_el else "",
                    "url": a_tag["href"],
                    "source": "Moneycontrol",
                }
            )
        return results

    # ------------------------------------------------------------------ #
    #  Public API                                                           #
    # ------------------------------------------------------------------ #

    async def get_top_news(
        self, symbol: str, company_name: str = "", top_n: int = 5
    ) -> list[dict]:
        """
        Aggregate news from NSE announcements, Google News RSS,
        and Moneycontrol. Deduplicate and return top_n items.
        """
        import asyncio

        nse_items, google_items, mc_items = await asyncio.gather(
            self._from_nse_announcements(symbol, company_name),
            self._from_google_rss(symbol, company_name),
            self._from_moneycontrol(symbol, company_name),
            return_exceptions=True,
        )

        all_items: list[dict] = []
        for bucket in (nse_items, google_items, mc_items):
            if isinstance(bucket, list):
                all_items.extend(bucket)

        deduped = _deduplicate(all_items, key="title", max_items=top_n)
        return deduped

    async def close(self) -> None:
        await self._client.aclose()
