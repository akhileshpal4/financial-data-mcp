"""
Tickertape fetcher — valuation, earnings estimates, sector info,
and peer metrics via Tickertape's internal JSON API.
Acts as a second cross-check source alongside Screener.in.
"""

import re
import json
import logging
from typing import Any

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

TT_BASE = "https://www.tickertape.in"
TT_API = "https://api.tickertape.in"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.tickertape.in/",
    "Origin": "https://www.tickertape.in",
}


class TickertapeFetcher:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            headers=HEADERS,
            follow_redirects=True,
            timeout=30.0,
        )
        self._sid_cache: dict[str, str] = {}

    async def _get_json(self, url: str) -> Any:
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("Tickertape GET %s failed: %s", url, exc)
            return None

    async def _resolve_sid(self, symbol: str) -> str | None:
        """Resolve Tickertape's internal stock ID (sid) from NSE symbol."""
        if symbol in self._sid_cache:
            return self._sid_cache[symbol]
        url = f"{TT_API}/stocks/suggest?query={symbol}&type=stock"
        data = await self._get_json(url)
        if not data:
            return None
        suggestions = (
            data if isinstance(data, list)
            else data.get("data", data.get("suggestions", []))
        )
        for s in suggestions:
            ticker = s.get("ticker") or s.get("symbol") or ""
            if ticker.upper() == symbol.upper():
                sid = s.get("sid") or s.get("id") or s.get("slug") or ""
                if sid:
                    self._sid_cache[symbol] = str(sid)
                    return str(sid)
        # Fallback: first result
        if suggestions:
            first = suggestions[0]
            sid = first.get("sid") or first.get("id") or first.get("slug") or symbol
            self._sid_cache[symbol] = str(sid)
            return str(sid)
        return None

    # ------------------------------------------------------------------ #
    #  Public methods                                                       #
    # ------------------------------------------------------------------ #

    async def get_stock_info(self, symbol: str) -> dict[str, Any] | None:
        """General stock info including sector, industry, market cap."""
        sid = await self._resolve_sid(symbol)
        if not sid:
            return None
        url = f"{TT_API}/stocks/info/{sid}"
        data = await self._get_json(url)
        if not data:
            return None
        d = data.get("data", data)
        return {
            "symbol": symbol,
            "company_name": d.get("name") or d.get("companyName"),
            "sector": d.get("sector"),
            "industry": d.get("industry"),
            "market_cap_cr": d.get("marketCap") or d.get("mktCap"),
            "cmp": d.get("price") or d.get("lastPrice"),
            "pe": d.get("pe") or d.get("priceToEarnings"),
            "pb": d.get("pb") or d.get("priceToBook"),
            "eps": d.get("eps"),
            "roe": d.get("roe"),
            "roce": d.get("roce"),
            "de_ratio": d.get("debtToEquity"),
            "dividend_yield": d.get("dividendYield"),
            "source": "Tickertape",
        }

    async def get_financials(self, symbol: str) -> dict[str, Any] | None:
        """Multi-period financials for CAGR and trend calculations."""
        sid = await self._resolve_sid(symbol)
        if not sid:
            return None
        url = f"{TT_API}/stocks/financials/{sid}?period=annual"
        data = await self._get_json(url)
        if not data:
            return None
        rows = data.get("data", data) if isinstance(data, dict) else data
        if not isinstance(rows, list):
            return None
        result: dict[str, list] = {
            "years": [],
            "revenue": [],
            "ebitda": [],
            "net_profit": [],
            "eps": [],
            "operating_cf": [],
            "capex": [],
            "free_cf": [],
        }
        for row in rows:
            result["years"].append(row.get("year") or row.get("period"))
            result["revenue"].append(row.get("revenue") or row.get("totalRevenue"))
            result["ebitda"].append(row.get("ebitda"))
            result["net_profit"].append(row.get("netProfit") or row.get("profit"))
            result["eps"].append(row.get("eps") or row.get("basicEPS"))
            result["operating_cf"].append(row.get("operatingCashFlow") or row.get("cfOperations"))
            result["capex"].append(row.get("capex") or row.get("capitalExpenditure"))
            fcf = None
            ocf = result["operating_cf"][-1]
            cx = result["capex"][-1]
            if ocf is not None and cx is not None:
                fcf = round(float(ocf) - abs(float(cx)), 2)
            result["free_cf"].append(fcf)
        result["source"] = "Tickertape"
        return result

    async def get_shareholding(self, symbol: str) -> list[dict] | None:
        """Quarterly shareholding pattern from Tickertape."""
        sid = await self._resolve_sid(symbol)
        if not sid:
            return None
        url = f"{TT_API}/stocks/shareholding/{sid}"
        data = await self._get_json(url)
        if not data:
            return None
        rows = data.get("data", data) if isinstance(data, dict) else data
        if not isinstance(rows, list):
            return None
        result = []
        for r in rows[:12]:
            result.append(
                {
                    "period": r.get("period") or r.get("quarter"),
                    "promoter_pct": r.get("promoter"),
                    "fii_pct": r.get("fii") or r.get("foreignInstitutional"),
                    "dii_pct": r.get("dii") or r.get("domesticInstitutional"),
                    "public_pct": r.get("public") or r.get("retail"),
                    "source": "Tickertape",
                }
            )
        return result

    async def get_sector_peers(self, symbol: str) -> list[dict] | None:
        """Peer valuation from Tickertape for cross-checking Screener peers."""
        sid = await self._resolve_sid(symbol)
        if not sid:
            return None
        url = f"{TT_API}/stocks/peers/{sid}"
        data = await self._get_json(url)
        if not data:
            return None
        peers_raw = data.get("data", data) if isinstance(data, dict) else data
        if not isinstance(peers_raw, list):
            return None
        peers = []
        for p in peers_raw[:6]:
            peers.append(
                {
                    "symbol": p.get("ticker") or p.get("symbol"),
                    "company_name": p.get("name"),
                    "cmp": p.get("price") or p.get("lastPrice"),
                    "pe": p.get("pe"),
                    "pb": p.get("pb"),
                    "roe": p.get("roe"),
                    "revenue_growth": p.get("revenueGrowth"),
                    "de_ratio": p.get("debtToEquity"),
                    "market_cap_cr": p.get("marketCap"),
                    "source": "Tickertape",
                }
            )
        return peers

    async def close(self) -> None:
        await self._client.aclose()
