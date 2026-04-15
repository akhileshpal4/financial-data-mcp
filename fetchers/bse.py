"""
BSE India fetcher — backup quotes, corporate actions, financial results.
Used as cross-check source when NSE data is available and as primary
fallback when NSE returns errors.
"""

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BSE_API_BASE = "https://api.bseindia.com/BseIndiaAPI/api"
BSE_WEB_BASE = "https://www.bseindia.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
}

# Well-known NSE ↔ BSE code mapping (extended at runtime via BSE search)
NSE_TO_BSE: dict[str, str] = {
    "RELIANCE": "500325",
    "TCS": "532540",
    "INFY": "500209",
    "HDFCBANK": "500180",
    "ICICIBANK": "532174",
    "HINDUNILVR": "500696",
    "BHARTIARTL": "532454",
    "ITC": "500875",
    "KOTAKBANK": "500247",
    "LT": "500510",
    "AXISBANK": "532215",
    "MARUTI": "532500",
    "SUNPHARMA": "524715",
    "TITAN": "500114",
    "WIPRO": "507685",
    "NESTLEIND": "500790",
    "ULTRACEMCO": "532538",
    "POWERGRID": "532898",
    "NTPC": "532555",
    "ONGC": "500312",
    "BAJFINANCE": "500034",
    "BAJAJFINSV": "532978",
    "TATAMOTORS": "500570",
    "TATASTEEL": "500470",
    "ADANIPORTS": "532921",
    "ASIANPAINT": "500820",
    "TECHM": "532755",
    "HCLTECH": "532281",
    "DRREDDY": "500124",
    "CIPLA": "500087",
    "DIVISLAB": "532488",
    "SBILIFE": "540719",
    "HDFCLIFE": "540777",
    "HDFC": "500010",
    "SBIN": "500112",
}


class BSEFetcher:
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            headers=HEADERS,
            follow_redirects=True,
            timeout=25.0,
        )

    async def _get(self, url: str) -> dict | list | None:
        try:
            resp = await self._client.get(url)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("BSE GET %s failed: %s", url, exc)
            return None

    async def resolve_bse_code(self, nse_symbol: str) -> str | None:
        """
        Return BSE script code for an NSE symbol.
        First check the static map; if not found, query BSE search API.
        """
        code = NSE_TO_BSE.get(nse_symbol.upper())
        if code:
            return code
        # Try BSE search
        search_url = (
            f"{BSE_API_BASE}/getScripSearch/w"
            f"?strSearch={nse_symbol}&strsearchtext=&fromDate=&toDate=&mypage=1&Action=0"
        )
        data = await self._get(search_url)
        if data and isinstance(data, dict):
            table = data.get("Table", [])
            if table:
                return str(table[0].get("SCRIP_CD", ""))
        return None

    async def get_quote(self, nse_symbol: str) -> dict[str, Any] | None:
        """Live BSE quote for cross-checking NSE price."""
        bse_code = await self.resolve_bse_code(nse_symbol)
        if not bse_code:
            return None
        url = (
            f"{BSE_API_BASE}/getScripHeaderData/w"
            f"?Debtflag=&scripcode={bse_code}&seriesid="
        )
        data = await self._get(url)
        if not data:
            return None
        try:
            d = data[0] if isinstance(data, list) and data else data
            return {
                "symbol": nse_symbol,
                "bse_code": bse_code,
                "cmp": float(d.get("CurrRate") or d.get("LTP") or 0),
                "week52_high": float(d.get("High52") or 0),
                "week52_low": float(d.get("Low52") or 0),
                "market_cap_cr": float(d.get("Mktcap") or 0),
                "face_value": float(d.get("FaceValue") or 0),
                "pe": float(d.get("PE") or 0),
                "pb": float(d.get("PBV") or 0),
                "source": "BSE",
            }
        except Exception as exc:
            logger.debug("BSE quote parse error for %s: %s", nse_symbol, exc)
            return None

    async def get_corporate_actions(self, nse_symbol: str) -> list[dict] | None:
        """Dividends and other corporate actions from BSE."""
        bse_code = await self.resolve_bse_code(nse_symbol)
        if not bse_code:
            return None
        url = (
            f"{BSE_API_BASE}/CorporateAction/w"
            f"?scripcode={bse_code}&Flag=C&fromdate=&todate=&mypage=1"
        )
        data = await self._get(url)
        if not data:
            return None
        rows = data if isinstance(data, list) else data.get("Table", [])
        results = []
        for r in rows[:30]:
            results.append(
                {
                    "ex_date": r.get("ExDate"),
                    "action": r.get("Purpose"),
                    "payout": r.get("Remarks"),
                    "source": "BSE",
                }
            )
        return results

    async def get_financials(self, nse_symbol: str) -> dict[str, Any] | None:
        """Annual financial summary from BSE."""
        bse_code = await self.resolve_bse_code(nse_symbol)
        if not bse_code:
            return None
        url = (
            f"{BSE_API_BASE}/Fundamentals/w"
            f"?scrip_cd={bse_code}"
        )
        data = await self._get(url)
        if not data:
            return None
        try:
            d = data[0] if isinstance(data, list) and data else data
            return {
                "eps_ttm": float(d.get("EPSTTM") or 0),
                "book_value": float(d.get("BookValue") or 0),
                "dividend_yield": float(d.get("DividendYield") or 0),
                "roce": float(d.get("ROCE") or 0),
                "roe": float(d.get("ROE") or 0),
                "source": "BSE",
            }
        except Exception as exc:
            logger.debug("BSE fundamentals parse error: %s", exc)
            return None

    async def close(self) -> None:
        await self._client.aclose()
