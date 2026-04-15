"""
Screener.in fetcher — scrapes historical P&L, Balance Sheet, Cash Flow,
Ratios, Shareholding, and Quarterly Results for an NSE-listed company.
This is the richest source of multi-year financial data.
"""

import re
import logging
from typing import Any

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://www.screener.in"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.screener.in/",
}


def _to_float(text: str) -> float | None:
    """Convert Screener's number strings ('1,23,456.78', '12.3%') to float."""
    if text is None:
        return None
    cleaned = re.sub(r"[,%₹\s]", "", str(text).strip())
    if cleaned in ("", "-", "--", "N/A"):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_table(table_el) -> dict[str, list]:
    """
    Parse a Screener financial table into {row_label: [v1, v2, ...]} dict.
    The first <thead> row is used as header (years/quarters); rows in <tbody> map
    to financial line items.
    """
    if table_el is None:
        return {}

    # Headers (years or quarter labels)
    header_row = table_el.select_one("thead tr")
    headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])] if header_row else []

    data: dict[str, list] = {}
    for row in table_el.select("tbody tr"):
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        label = cells[0].get_text(strip=True).rstrip(" +").rstrip(" -").strip()
        values = [_to_float(c.get_text(strip=True)) for c in cells[1:]]
        if label:
            data[label] = values

    data["__headers__"] = headers[1:] if headers else []
    return data


class ScreenerFetcher:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            headers=HEADERS,
            follow_redirects=True,
            timeout=35.0,
        )
        self._cache: dict[str, BeautifulSoup] = {}

    async def _fetch_page(self, symbol: str, consolidated: bool = True) -> BeautifulSoup | None:
        """
        Fetch and cache the Screener company page.
        Tries consolidated first; falls back to standalone if not available.
        """
        cache_key = f"{symbol}_{consolidated}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        suffix = "consolidated" if consolidated else "standalone"
        url = f"{BASE_URL}/company/{symbol}/{suffix}/"
        try:
            resp = await self._client.get(url)
            if resp.status_code == 404 and consolidated:
                # company may not have consolidation — try standalone
                url = f"{BASE_URL}/company/{symbol}/standalone/"
                resp = await self._client.get(url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            self._cache[cache_key] = soup
            return soup
        except Exception as exc:
            logger.debug("Screener fetch failed for %s: %s", symbol, exc)
            return None

    def _get_ratio_value(self, soup: BeautifulSoup, label_pattern: str) -> float | None:
        """Extract a single ratio from the top key-ratios section."""
        for li in soup.select("#top-ratios li, .company-ratios li"):
            name_el = li.select_one(".name, span[class*='name']")
            val_el = li.select_one(".value, span[class*='value'], .number")
            if not name_el or not val_el:
                # Try simple text extraction
                text = li.get_text(" ", strip=True)
                if re.search(label_pattern, text, re.IGNORECASE):
                    # last number in line
                    nums = re.findall(r"[\d,]+\.?\d*", text)
                    return _to_float(nums[-1]) if nums else None
                continue
            if re.search(label_pattern, name_el.get_text(strip=True), re.IGNORECASE):
                return _to_float(val_el.get_text(strip=True))
        return None

    # ------------------------------------------------------------------ #
    #  Public methods                                                       #
    # ------------------------------------------------------------------ #

    async def get_key_ratios(self, symbol: str, consolidated: bool = True) -> dict[str, Any] | None:
        """P/E, P/B, EV/EBITDA, ROE, ROCE, D/E, current ratio, market cap, CMP."""
        soup = await self._fetch_page(symbol, consolidated)
        if soup is None:
            return None

        ratios: dict[str, Any] = {"symbol": symbol, "source": "Screener.in"}
        patterns = {
            "pe": r"P/E|Price.*Earning",
            "pb": r"P/B|Price.*Book",
            "ev_ebitda": r"EV.*EBITDA",
            "roce": r"ROCE|Return.*Capital",
            "roe": r"ROE|Return.*Equity",
            "de_ratio": r"Debt.*Equity|D/E",
            "current_ratio": r"Current Ratio",
            "cmp": r"Current Price|Market Price|CMP",
            "market_cap_cr": r"Market Cap",
            "dividend_yield": r"Div.*Yield",
            "book_value": r"Book Value",
            "eps": r"\bEPS\b",
        }
        for key, pat in patterns.items():
            ratios[key] = self._get_ratio_value(soup, pat)

        # Derive P/B from CMP / Book Value when not directly available
        if not ratios.get("pb") and ratios.get("cmp") and ratios.get("book_value"):
            try:
                ratios["pb"] = round(ratios["cmp"] / ratios["book_value"], 2)
            except (TypeError, ZeroDivisionError):
                pass

        return ratios

    async def get_profit_loss(self, symbol: str, consolidated: bool = True) -> dict[str, list] | None:
        """Annual P&L table (Revenue, EBITDA, Net Profit, EPS, etc.)."""
        soup = await self._fetch_page(symbol, consolidated)
        if soup is None:
            return None
        table = soup.select_one("#profit-loss table, section#profit-loss table")
        result = _parse_table(table)
        result["source"] = "Screener.in"
        return result if len(result) > 2 else None

    async def get_balance_sheet(self, symbol: str, consolidated: bool = True) -> dict[str, list] | None:
        """Annual balance sheet (Borrowings, Total Assets, Net Worth, etc.)."""
        soup = await self._fetch_page(symbol, consolidated)
        if soup is None:
            return None
        table = soup.select_one("#balance-sheet table, section#balance-sheet table")
        result = _parse_table(table)
        result["source"] = "Screener.in"
        return result if len(result) > 2 else None

    async def get_cash_flow(self, symbol: str, consolidated: bool = True) -> dict[str, list] | None:
        """Annual cash flow statement."""
        soup = await self._fetch_page(symbol, consolidated)
        if soup is None:
            return None
        table = soup.select_one("#cash-flow table, section#cash-flow table")
        result = _parse_table(table)
        result["source"] = "Screener.in"
        return result if len(result) > 2 else None

    async def get_quarterly_results(self, symbol: str) -> dict[str, list] | None:
        """Last 8+ quarters of results."""
        soup = await self._fetch_page(symbol, consolidated=True)
        if soup is None:
            return None
        table = soup.select_one("#quarters table, section#quarters table")
        result = _parse_table(table)
        result["source"] = "Screener.in"
        return result if len(result) > 2 else None

    async def get_ratios_table(self, symbol: str, consolidated: bool = True) -> dict[str, list] | None:
        """Multi-year ratios table (ROE, ROCE, D/E, ICR, etc.)."""
        soup = await self._fetch_page(symbol, consolidated)
        if soup is None:
            return None
        table = soup.select_one("#ratios table, section#ratios table")
        result = _parse_table(table)
        result["source"] = "Screener.in"
        return result if len(result) > 2 else None

    async def get_shareholding(self, symbol: str) -> dict[str, list] | None:
        """Shareholding pattern table (promoter, FII, DII, public)."""
        soup = await self._fetch_page(symbol, consolidated=True)
        if soup is None:
            return None
        table = soup.select_one(
            "#shareholding table, section#shareholding table, "
            ".shareholding-pattern table"
        )
        result = _parse_table(table)
        result["source"] = "Screener.in"
        return result if len(result) > 2 else None

    async def get_peers(self, symbol: str) -> list[dict] | None:
        """Peer companies via Screener's lazy-loaded peers API."""
        import re as _re

        soup = await self._fetch_page(symbol, consolidated=True)
        if soup is None:
            return None

        # Extract warehouseId from page HTML (present in /alerts/stock-{id}/ data-url)
        html = str(soup)
        m = _re.search(r"/alerts/stock-(\d+)/", html)
        if not m:
            m = _re.search(r"warehouseId[^\d]*(\d+)", html)
        if not m:
            return None
        warehouse_id = m.group(1)

        url = f"{BASE_URL}/api/company/{warehouse_id}/peers/"
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            peer_soup = BeautifulSoup(resp.text, "lxml")
        except Exception as exc:
            logger.debug("Screener peers API failed for %s: %s", symbol, exc)
            return None

        table = peer_soup.find("table")
        if not table:
            return None

        # First <tr> is the header row
        header_row = table.find("tr")
        headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])] if header_row else []

        peers: list[dict] = []
        for row in table.find_all("tr")[1:7]:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            peer: dict[str, Any] = {}
            for i, cell in enumerate(cells):
                key = headers[i] if i < len(headers) else f"col_{i}"
                val = cell.get_text(strip=True)
                # First two columns (S.No., Name) are text; rest are numeric
                peer[key] = val if i < 2 else _to_float(val)
            if peer:
                peers.append(peer)
        return peers[:6] if peers else None

    def invalidate_cache(self, symbol: str | None = None) -> None:
        if symbol:
            self._cache = {k: v for k, v in self._cache.items() if not k.startswith(symbol)}
        else:
            self._cache.clear()

    async def close(self) -> None:
        await self._client.aclose()
