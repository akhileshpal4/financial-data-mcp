"""
Web search fetcher — DuckDuckGo + Google News RSS.

Provides qualitative context for Indian stocks that structured API sources
do not cover:
  • Competitive moat, brand strength, pricing power, switching costs
  • Sector tailwinds / headwinds — 5–10 year outlook
  • Regulatory and policy risks (SEBI, RBI, government)
  • Latest earnings call highlights and management commentary
  • Governance flags — controversies, SEBI actions, audit qualifications

No API keys required.  DuckDuckGo is primary; Google News RSS is fallback.
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

DDGS_URL = "https://duckduckgo.com/html/?q={query}&kl=in-en"
GOOGLE_NEWS_RSS = (
    "https://news.google.com/rss/search?q={query}"
    "&hl=en-IN&gl=IN&ceid=IN:en"
)


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text).strip())


def _deduplicate(items: list[dict], max_items: int = 6) -> list[dict]:
    """Remove near-duplicate titles; keep first occurrence."""
    seen: list[str] = []
    out: list[dict] = []
    for item in items:
        title = _clean(item.get("title", "")).lower()
        words = set(re.findall(r"\w+", title))
        is_dup = any(
            len(words) > 0
            and len(words & set(re.findall(r"\w+", s))) / max(len(words), 1) > 0.6
            for s in seen
        )
        if not is_dup and title:
            seen.append(title)
            out.append(item)
        if len(out) >= max_items:
            break
    return out


class WebSearchFetcher:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            headers=HEADERS,
            follow_redirects=True,
            timeout=30.0,
        )

    # ------------------------------------------------------------------ #
    #  Private search backends                                             #
    # ------------------------------------------------------------------ #

    async def _ddg_search(self, query: str, top_n: int = 6) -> list[dict]:
        """DuckDuckGo HTML endpoint — no API key needed."""
        from bs4 import BeautifulSoup

        url = DDGS_URL.format(query=quote_plus(query))
        results: list[dict] = []
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            for el in soup.select(".result__body, .results_links_deep, .web-result")[:top_n * 2]:
                title_el = el.select_one(".result__title a, .result__a")
                snip_el = el.select_one(".result__snippet")
                link_el = el.select_one("a.result__url, a.result__a")
                title = _clean(title_el.get_text() if title_el else "")
                if not title:
                    continue
                results.append({
                    "title": title,
                    "snippet": _clean(snip_el.get_text() if snip_el else ""),
                    "url": _clean(link_el.get("href", "") if link_el else ""),
                    "source": "DuckDuckGo",
                    "published": "",
                })
                if len(results) >= top_n:
                    break
        except Exception as exc:
            logger.debug("DDG search failed for %r: %s", query, exc)
        return results

    async def _google_news_rss(self, query: str, top_n: int = 6) -> list[dict]:
        """Google News RSS — reliable fallback, no API key."""
        url = GOOGLE_NEWS_RSS.format(query=quote_plus(query))
        results: list[dict] = []
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item")[:top_n]:
                title = _clean(item.findtext("title") or "")
                if not title:
                    continue
                source_el = item.find("source")
                results.append({
                    "title": title,
                    "snippet": _clean(item.findtext("description") or ""),
                    "url": _clean(item.findtext("link") or ""),
                    "source": source_el.text if source_el is not None else "Google News",
                    "published": _clean(item.findtext("pubDate") or ""),
                })
        except Exception as exc:
            logger.debug("Google News RSS failed for %r: %s", query, exc)
        return results

    async def _search(self, query: str, top_n: int = 5) -> list[dict]:
        """Try DuckDuckGo first; fall back to Google News RSS if no results."""
        results = await self._ddg_search(query, top_n)
        if not results:
            results = await self._google_news_rss(query, top_n)
        return _deduplicate(results, max_items=top_n)

    # ------------------------------------------------------------------ #
    #  Public qualitative research methods                                 #
    # ------------------------------------------------------------------ #

    async def get_moat_analysis(
        self, symbol: str, company_name: str, sector: str
    ) -> dict[str, Any]:
        """
        Competitive moat: pricing power, brand, switching costs, market share,
        competitive advantages.
        """
        query = (
            f"{company_name} India competitive advantage moat market share "
            f"pricing power brand value {sector}"
        )
        results = await self._search(query, top_n=6)
        moat_kws = [
            "moat", "market share", "pricing power", "brand", "switching",
            "dominant", "leader", "advantage", "competitive", "monopoly",
        ]
        ranked = sorted(
            results,
            key=lambda r: sum(
                1 for kw in moat_kws
                if kw in (r["title"] + r["snippet"]).lower()
            ),
            reverse=True,
        )
        return {
            "symbol": symbol,
            "query": query,
            "results": ranked[:5],
            "fetched_at": datetime.now().strftime("%d-%b-%Y %H:%M"),
        }

    async def get_sector_context(
        self, sector: str, company_name: str
    ) -> dict[str, Any]:
        """Sector tailwinds, headwinds, and 5–10 year outlook."""
        query = (
            f"India {sector} sector outlook tailwinds headwinds 2025 2030 "
            f"growth opportunities challenges"
        )
        results = await self._search(query, top_n=5)
        return {
            "sector": sector,
            "query": query,
            "results": results,
            "fetched_at": datetime.now().strftime("%d-%b-%Y %H:%M"),
        }

    async def get_regulatory_risks(
        self, sector: str, company_name: str
    ) -> dict[str, Any]:
        """Sector and company-specific regulatory and policy risks."""
        query = (
            f"{company_name} OR {sector} India regulatory risk SEBI RBI "
            f"government policy 2024 2025"
        )
        results = await self._search(query, top_n=5)
        return {
            "company": company_name,
            "sector": sector,
            "query": query,
            "results": results,
            "fetched_at": datetime.now().strftime("%d-%b-%Y %H:%M"),
        }

    async def get_earnings_call_highlights(
        self, symbol: str, company_name: str
    ) -> dict[str, Any]:
        """Latest earnings call transcript excerpts and management commentary."""
        q1 = (
            f'"{company_name}" earnings call management commentary '
            f"quarterly results guidance 2025"
        )
        q2 = f"{symbol} NSE quarterly earnings management guidance outlook FY25 FY26"
        results1 = await self._search(q1, top_n=5)
        results2 = await self._search(q2, top_n=3)
        combined = _deduplicate(results1 + results2, max_items=6)
        return {
            "symbol": symbol,
            "company_name": company_name,
            "query": q1,
            "results": combined,
            "fetched_at": datetime.now().strftime("%d-%b-%Y %H:%M"),
        }

    async def get_management_governance_flags(
        self, symbol: str, company_name: str
    ) -> dict[str, Any]:
        """
        Governance red flags: SEBI actions, audit qualifications, management
        controversies, insider trading allegations.
        """
        query = (
            f"{company_name} India SEBI governance controversy audit "
            f"qualification insider trading management change penalty"
        )
        results = await self._search(query, top_n=6)
        flag_kws = [
            "controversy", "sebi", "fraud", "audit", "penalty",
            "insider", "manipulation", "governance", "probe", "investigation",
            "qualified", "concern", "resignation",
        ]
        flagged = [
            r for r in results
            if any(
                kw in (r["title"] + r["snippet"]).lower()
                for kw in flag_kws
            )
        ]
        return {
            "symbol": symbol,
            "company_name": company_name,
            "governance_flags": flagged,
            "all_results": results,
            "fetched_at": datetime.now().strftime("%d-%b-%Y %H:%M"),
        }

    async def search(
        self, symbol: str, query: str, top_n: int = 5
    ) -> dict[str, Any]:
        """
        General-purpose qualitative search about a stock.
        Automatically appends 'India' context to the query.
        """
        enriched = f"{symbol} India {query}"
        results = await self._search(enriched, top_n=top_n)
        return {
            "symbol": symbol,
            "query": enriched,
            "results": results,
            "fetched_at": datetime.now().strftime("%d-%b-%Y %H:%M"),
        }

    async def close(self) -> None:
        await self._client.aclose()
