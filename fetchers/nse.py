"""
NSE India fetcher — live quotes, shareholding, dividends, corporate actions.
Session cookies are silently refreshed on every call to bypass NSE's auth wall.
"""

import asyncio
import re
import logging
from datetime import datetime, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

NSE_BASE = "https://www.nseindia.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
    "DNT": "1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


class NSEFetcher:
    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None
        self._session_valid_until: datetime = datetime.min

    async def _ensure_session(self) -> httpx.AsyncClient:
        """
        Create (or refresh) an httpx client with valid NSE session cookies.
        NSE requires a prior visit to the homepage before accepting API calls.
        """
        if self._client is None or datetime.utcnow() >= self._session_valid_until:
            if self._client:
                await self._client.aclose()
            self._client = httpx.AsyncClient(
                headers=HEADERS,
                follow_redirects=True,
                timeout=30.0,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            )
            # Warm-up: seed cookies from the homepage
            try:
                await self._client.get(NSE_BASE + "/")
                await asyncio.sleep(0.5)
            except Exception:
                pass
            self._session_valid_until = datetime.utcnow() + timedelta(minutes=25)
        return self._client

    async def _get(self, url: str, retries: int = 3) -> dict | list | None:
        client = await self._ensure_session()
        for attempt in range(retries):
            try:
                resp = await client.get(url)
                if resp.status_code == 401:
                    # Force session refresh on next attempt
                    self._session_valid_until = datetime.min
                    client = await self._ensure_session()
                    continue
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.debug("NSE GET %s attempt %d failed: %s", url, attempt + 1, exc)
                if attempt < retries - 1:
                    await asyncio.sleep(1.5 ** attempt)
        return None

    # ------------------------------------------------------------------ #
    #  Public methods                                                       #
    # ------------------------------------------------------------------ #

    async def get_quote(self, symbol: str) -> dict[str, Any] | None:
        """Full NSE equity quote (price, 52W, market cap, face value, sector P/E)."""
        url = f"{NSE_BASE}/api/quote-equity?symbol={symbol}"
        data = await self._get(url)
        if not data:
            return None
        try:
            pi = data.get("priceInfo", {})
            meta = data.get("metadata", {})
            sec_info = data.get("securityInfo", {})
            whl = pi.get("weekHighLow", {})
            intra = pi.get("intraDayHighLow", {})
            issued_size = sec_info.get("issuedSize", 0)
            cmp = pi.get("lastPrice", 0)
            market_cap_cr = round((cmp * issued_size) / 1e7, 2)  # ₹ Crore
            return {
                "symbol": symbol,
                "cmp": cmp,
                "open": pi.get("open"),
                "prev_close": pi.get("previousClose"),
                "day_high": intra.get("max"),
                "day_low": intra.get("min"),
                "week52_high": whl.get("max"),
                "week52_high_date": whl.get("maxDate"),
                "week52_low": whl.get("min"),
                "week52_low_date": whl.get("minDate"),
                "vwap": pi.get("vwap"),
                "market_cap_cr": market_cap_cr,
                "face_value": sec_info.get("faceValue") or meta.get("pdFaceValue"),
                "sector_pe": meta.get("pdSectorPe"),
                "symbol_pe": meta.get("pdSymbolPe"),
                "sector": meta.get("pdSectorInd"),
                "isin": data.get("info", {}).get("isin"),
                "company_name": data.get("info", {}).get("companyName"),
                "last_updated": meta.get("lastUpdateTime"),
                "source": "NSE",
            }
        except Exception as exc:
            logger.debug("NSE quote parse error for %s: %s", symbol, exc)
            return None

    async def get_shareholding(self, symbol: str) -> list[dict] | None:
        """Last 12 quarters of shareholding pattern from NSE."""
        url = (
            f"{NSE_BASE}/api/corporate-shareholding-patterns"
            f"?symbol={symbol}&tabName=Quarterly"
        )
        data = await self._get(url)
        if not data:
            return None
        try:
            records = []
            for item in (data if isinstance(data, list) else data.get("data", [])):
                date_str = item.get("quarter") or item.get("period") or item.get("date", "")
                promoter = item.get("promoterAndPromoterGroupTotal") or item.get("promoter", 0)
                fii = item.get("foreignInstitutionalInvestors") or item.get("fii", 0)
                dii = item.get("domesticInstitutionalInvestors") or item.get("dii", 0)
                public = item.get("publicShareholdingTotal") or item.get("public", 0)
                pledge = (
                    item.get("promoterAndPromoterGroupPledge")
                    or item.get("pledged", 0)
                )
                records.append(
                    {
                        "period": date_str,
                        "promoter_pct": float(promoter or 0),
                        "fii_pct": float(fii or 0),
                        "dii_pct": float(dii or 0),
                        "public_pct": float(public or 0),
                        "promoter_pledged_pct": float(pledge or 0),
                    }
                )
            return records[:12]
        except Exception as exc:
            logger.debug("NSE shareholding parse error for %s: %s", symbol, exc)
            return None

    async def get_corporate_actions(self, symbol: str) -> list[dict] | None:
        """Dividends, bonus, splits from NSE corporate actions."""
        url = (
            f"{NSE_BASE}/api/corporates-corporateActions"
            f"?index=equities&symbol={symbol}"
        )
        data = await self._get(url)
        if not data:
            return None
        actions = data if isinstance(data, list) else data.get("data", [])
        result = []
        for a in actions[:30]:
            result.append(
                {
                    "ex_date": a.get("exDate"),
                    "action": a.get("subject") or a.get("action"),
                    "record_date": a.get("recordDate"),
                }
            )
        return result

    async def get_quarterly_results(self, symbol: str) -> list[dict] | None:
        """NSE financial results (quarterly)."""
        url = (
            f"{NSE_BASE}/api/financials-results"
            f"?index=equities&symbol={symbol}&period=Quarterly"
        )
        data = await self._get(url)
        if not data:
            return None
        try:
            rows = data if isinstance(data, list) else data.get("data", [])
            results = []
            for r in rows[:8]:
                results.append(
                    {
                        "period": r.get("period") or r.get("fromDate"),
                        "revenue": r.get("totalIncome") or r.get("income"),
                        "net_profit": r.get("netProfit") or r.get("profit"),
                        "eps": r.get("basicEPS") or r.get("eps"),
                    }
                )
            return results
        except Exception as exc:
            logger.debug("NSE quarterly results parse error: %s", exc)
            return None

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
