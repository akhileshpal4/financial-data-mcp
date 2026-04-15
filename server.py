#!/usr/bin/env python3
"""
Financial Data MCP Server
=========================
Silently cross-checks Indian stock data from NSE, BSE, Screener.in,
Tickertape, and Moneycontrol before returning any output.

Sources (priority order):
  NSE India → BSE India → Screener.in → Tickertape → Moneycontrol
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import Any

import mcp.types as types
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server

from fetchers.nse import NSEFetcher
from fetchers.bse import BSEFetcher
from fetchers.screener import ScreenerFetcher
from fetchers.tickertape import TickertapeFetcher
from fetchers.news import NewsFetcher
from utils.cross_check import CrossChecker
from utils.calculator import FinancialCalculator

logging.basicConfig(level=logging.ERROR, stream=sys.stderr)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  Server & singleton fetchers                                                  #
# --------------------------------------------------------------------------- #

server = Server("financial-data-mcp")

_nse: NSEFetcher | None = None
_bse: BSEFetcher | None = None
_screener: ScreenerFetcher | None = None
_tickertape: TickertapeFetcher | None = None
_news: NewsFetcher | None = None
_cc = CrossChecker()
_calc = FinancialCalculator()


def nse() -> NSEFetcher:
    global _nse
    if _nse is None:
        _nse = NSEFetcher()
    return _nse


def bse() -> BSEFetcher:
    global _bse
    if _bse is None:
        _bse = BSEFetcher()
    return _bse


def screener() -> ScreenerFetcher:
    global _screener
    if _screener is None:
        _screener = ScreenerFetcher()
    return _screener


def tickertape() -> TickertapeFetcher:
    global _tickertape
    if _tickertape is None:
        _tickertape = TickertapeFetcher()
    return _tickertape


def news_fetcher() -> NewsFetcher:
    global _news
    if _news is None:
        _news = NewsFetcher()
    return _news


# --------------------------------------------------------------------------- #
#  Tool definitions                                                             #
# --------------------------------------------------------------------------- #

TOOLS: list[types.Tool] = [
    types.Tool(
        name="fetch_live_market_data",
        description=(
            "Fetch live CMP, 52-week high/low, market cap, and face value "
            "for an NSE-listed Indian stock. Cross-checks NSE and BSE."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "NSE symbol e.g. RELIANCE, TCS, HDFCBANK",
                }
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_valuation_metrics",
        description=(
            "Fetch P/E, P/B, EV/EBITDA for the stock — current value, "
            "sector average, and the stock's own 5-year historical average. "
            "Cross-checks NSE, Screener.in, and Tickertape."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "consolidated": {
                    "type": "boolean",
                    "description": "Use consolidated financials (default true)",
                },
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_growth_metrics",
        description=(
            "Fetch Revenue CAGR, Net Profit CAGR, and EPS CAGR "
            "for 3-year and 5-year periods."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "consolidated": {"type": "boolean"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_profitability_trends",
        description=(
            "Fetch EBITDA margin and net profit margin trends over the last 5 years."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "consolidated": {"type": "boolean"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_quarterly_performance",
        description=(
            "Fetch last 8 quarters of EPS data with year-on-year percentage change."
        ),
        inputSchema={
            "type": "object",
            "properties": {"symbol": {"type": "string"}},
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_cashflow_data",
        description=(
            "Fetch Operating Cash Flow, CapEx, and Free Cash Flow for the last 3–5 years."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "consolidated": {"type": "boolean"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_balance_sheet_health",
        description=(
            "Fetch Debt-to-Equity ratio (5-year trend), Interest Coverage Ratio, "
            "and Current Ratio."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "consolidated": {"type": "boolean"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_return_ratios",
        description=(
            "Fetch ROE and ROCE: current value, 3-year average, and 5-year average."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "consolidated": {"type": "boolean"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_dividend_history",
        description=("Fetch dividend history and dividend payout ratio."),
        inputSchema={
            "type": "object",
            "properties": {"symbol": {"type": "string"}},
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_shareholding_pattern",
        description=(
            "Fetch promoter holding (last 12 quarters), FII and DII holding "
            "(last 8 quarters), and flag promoter pledging if above 10%."
        ),
        inputSchema={
            "type": "object",
            "properties": {"symbol": {"type": "string"}},
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_peer_comparison",
        description=(
            "Fetch the 3 closest peer companies with P/E, P/B, ROE, "
            "Revenue Growth, and D/E for side-by-side comparison."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "consolidated": {"type": "boolean"},
            },
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_recent_news",
        description=(
            "Fetch the top 5 recent news items relevant to long-term investors "
            "from NSE announcements, Google News, and Moneycontrol."
        ),
        inputSchema={
            "type": "object",
            "properties": {"symbol": {"type": "string"}},
            "required": ["symbol"],
        },
    ),
    types.Tool(
        name="fetch_full_analysis",
        description=(
            "COMPREHENSIVE ANALYSIS TOOL. "
            "Fetches and cross-checks ALL of the following from NSE, BSE, "
            "Screener.in, Tickertape, and Moneycontrol in one call:\n"
            "• Live CMP, 52W H/L, Market Cap, Face Value\n"
            "• P/E, P/B, EV/EBITDA — current + sector avg + 5Y historical\n"
            "• Revenue / NP / EPS CAGR — 3Y and 5Y\n"
            "• EBITDA margin & NP margin trends — 5 years\n"
            "• EPS — last 8 quarters with YoY change\n"
            "• Free Cash Flow — last 3–5 years\n"
            "• D/E trend (5Y), Interest Coverage Ratio, Current Ratio\n"
            "• ROE and ROCE — current + 3Y avg + 5Y avg\n"
            "• Dividend history and payout ratio\n"
            "• Promoter holding (12Q), FII/DII (8Q), pledging flag >10%\n"
            "• 3 closest peers: P/E, P/B, ROE, Revenue Growth, D/E\n"
            "• Top 5 recent news items\n"
            "Returns a single structured JSON for complete investment analysis."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "NSE stock symbol e.g. RELIANCE, TCS, HDFCBANK",
                },
                "consolidated": {
                    "type": "boolean",
                    "description": "Use consolidated financials (default true)",
                },
            },
            "required": ["symbol"],
        },
    ),
]


# --------------------------------------------------------------------------- #
#  Individual tool implementations                                              #
# --------------------------------------------------------------------------- #

async def _live_market_data(symbol: str) -> dict[str, Any]:
    nse_q, bse_q = await asyncio.gather(
        nse().get_quote(symbol),
        bse().get_quote(symbol),
        return_exceptions=True,
    )
    nse_q = nse_q if isinstance(nse_q, dict) else None
    bse_q = bse_q if isinstance(bse_q, dict) else None

    cmp_check = _cc.reconcile_price(
        nse_q.get("cmp") if nse_q else None,
        bse_q.get("cmp") if bse_q else None,
    )
    w52h = _cc.reconcile_price(
        nse_q.get("week52_high") if nse_q else None,
        bse_q.get("week52_high") if bse_q else None,
    )
    w52l = _cc.reconcile_price(
        nse_q.get("week52_low") if nse_q else None,
        bse_q.get("week52_low") if bse_q else None,
    )
    mcap = _cc.reconcile_financial(
        nse_q.get("market_cap_cr") if nse_q else None,
        bse_q.get("market_cap_cr") if bse_q else None,
        name="market_cap",
    )
    fv = _cc.reconcile(
        [nse_q.get("face_value") if nse_q else None,
         bse_q.get("face_value") if bse_q else None],
        tolerance=0,
    )

    return {
        "symbol": symbol,
        "company_name": (nse_q or {}).get("company_name"),
        "sector": (nse_q or {}).get("sector"),
        "isin": (nse_q or {}).get("isin"),
        "cmp": cmp_check["value"],
        "week52_high": w52h["value"],
        "week52_high_date": (nse_q or {}).get("week52_high_date"),
        "week52_low": w52l["value"],
        "week52_low_date": (nse_q or {}).get("week52_low_date"),
        "market_cap_cr": mcap["value"],
        "face_value": fv["value"],
        "last_updated": (nse_q or {}).get("last_updated"),
        "as_of": datetime.now().strftime("%d-%b-%Y %H:%M"),
    }


async def _valuation_metrics(symbol: str, consolidated: bool) -> dict[str, Any]:
    nse_q, scr_ratios, scr_ratios_tbl, tt_info = await asyncio.gather(
        nse().get_quote(symbol),
        screener().get_key_ratios(symbol, consolidated),
        screener().get_ratios_table(symbol, consolidated),
        tickertape().get_stock_info(symbol),
        return_exceptions=True,
    )
    nse_q = nse_q if isinstance(nse_q, dict) else {}
    scr_ratios = scr_ratios if isinstance(scr_ratios, dict) else {}
    scr_ratios_tbl = scr_ratios_tbl if isinstance(scr_ratios_tbl, dict) else {}
    tt_info = tt_info if isinstance(tt_info, dict) else {}

    pe = _cc.reconcile_ratio(
        nse_q.get("symbol_pe"), scr_ratios.get("pe"), tt_info.get("pe"), name="PE"
    )
    pb = _cc.reconcile_ratio(
        scr_ratios.get("pb"), tt_info.get("pb"), name="PB"
    )
    # 5-year historical P/E from ratios table
    pe_history = scr_ratios_tbl.get("Price to Earning") or scr_ratios_tbl.get("P/E")
    pe_5y_avg = _calc.avg(pe_history or [], last_n=5)
    roe_history = scr_ratios_tbl.get("ROE %") or scr_ratios_tbl.get("Return on Equity")
    headers = scr_ratios_tbl.get("__headers__", [])

    return {
        "symbol": symbol,
        "pe_current": pe["value"],
        "pe_sector_avg": nse_q.get("sector_pe"),
        "pe_5y_avg": pe_5y_avg,
        "pe_confidence": pe["confidence"],
        "pb_current": pb["value"],
        "pb_5y_avg": _calc.avg(
            scr_ratios_tbl.get("Price to Book") or scr_ratios_tbl.get("P/B") or [],
            last_n=5,
        ),
        "ev_ebitda_current": scr_ratios.get("ev_ebitda") or tt_info.get("ev_ebitda"),
        "sector": nse_q.get("sector"),
        "pe_history": {"headers": headers, "values": pe_history},
        "roe_history": {"headers": headers, "values": roe_history},
    }


async def _growth_metrics(symbol: str, consolidated: bool) -> dict[str, Any]:
    scr_pl, tt_fin = await asyncio.gather(
        screener().get_profit_loss(symbol, consolidated),
        tickertape().get_financials(symbol),
        return_exceptions=True,
    )
    scr_pl = scr_pl if isinstance(scr_pl, dict) else {}
    tt_fin = tt_fin if isinstance(tt_fin, dict) else {}

    # Screener P&L keys — after stripping trailing " +" in _parse_table
    rev = (
        scr_pl.get("Sales") or scr_pl.get("Revenue from Operations") or
        scr_pl.get("Revenue") or scr_pl.get("Total Revenue") or []
    )
    np_ = (
        scr_pl.get("Net Profit") or scr_pl.get("Profit after tax") or
        scr_pl.get("PAT") or []
    )
    eps = scr_pl.get("EPS in Rs") or scr_pl.get("EPS") or []

    def _cagr(series: list, n: int) -> float | None:
        clean = [v for v in series if v is not None]
        if len(clean) <= n:
            return _calc.cagr_from_series(clean)
        return _calc.cagr(clean[-n - 1], clean[-1], n)

    # Cross-check revenue CAGR with Tickertape
    tt_rev = tt_fin.get("revenue", [])
    rev_3y_scr = _cagr(rev, 3)
    rev_3y_tt = _cagr(tt_rev, 3)
    rev_3y = _cc.reconcile_ratio(rev_3y_scr, rev_3y_tt, name="Revenue CAGR 3Y")

    rev_5y_scr = _cagr(rev, 5)
    rev_5y_tt = _cagr(tt_rev, 5)
    rev_5y = _cc.reconcile_ratio(rev_5y_scr, rev_5y_tt, name="Revenue CAGR 5Y")

    tt_np = tt_fin.get("net_profit", [])
    np_3y = _cc.reconcile_ratio(_cagr(np_, 3), _cagr(tt_np, 3), name="NP CAGR 3Y")
    np_5y = _cc.reconcile_ratio(_cagr(np_, 5), _cagr(tt_np, 5), name="NP CAGR 5Y")

    tt_eps = tt_fin.get("eps", [])
    eps_3y = _cc.reconcile_ratio(_cagr(eps, 3), _cagr(tt_eps, 3), name="EPS CAGR 3Y")
    eps_5y = _cc.reconcile_ratio(_cagr(eps, 5), _cagr(tt_eps, 5), name="EPS CAGR 5Y")

    headers = scr_pl.get("__headers__", [])
    return {
        "symbol": symbol,
        "revenue_cagr_3y_pct": rev_3y["value"],
        "revenue_cagr_5y_pct": rev_5y["value"],
        "net_profit_cagr_3y_pct": np_3y["value"],
        "net_profit_cagr_5y_pct": np_5y["value"],
        "eps_cagr_3y_pct": eps_3y["value"],
        "eps_cagr_5y_pct": eps_5y["value"],
        "revenue_series": {"headers": headers, "values": rev},
        "net_profit_series": {"headers": headers, "values": np_},
        "eps_series": {"headers": headers, "values": eps},
    }


async def _profitability_trends(symbol: str, consolidated: bool) -> dict[str, Any]:
    scr_pl, tt_fin = await asyncio.gather(
        screener().get_profit_loss(symbol, consolidated),
        tickertape().get_financials(symbol),
        return_exceptions=True,
    )
    scr_pl = scr_pl if isinstance(scr_pl, dict) else {}
    tt_fin = tt_fin if isinstance(tt_fin, dict) else {}

    headers = scr_pl.get("__headers__", [])
    rev = (
        scr_pl.get("Sales") or scr_pl.get("Revenue from Operations") or
        scr_pl.get("Revenue") or scr_pl.get("Total Revenue") or []
    )
    # OPM % is the EBITDA/operating margin on Screener
    ebitda = (
        scr_pl.get("OPM %") or scr_pl.get("EBITDA %") or
        scr_pl.get("Operating Profit Margin %") or []
    )
    np_ = scr_pl.get("Net Profit") or scr_pl.get("Profit after tax") or scr_pl.get("PAT") or []
    np_margin = scr_pl.get("NPM %") or scr_pl.get("Net Profit Margin %") or []

    # If margin series not directly available, derive it
    if not ebitda:
        ebitda_abs = scr_pl.get("Operating Profit") or scr_pl.get("EBITDA") or []
        ebitda = _calc.margin_series(ebitda_abs, rev)
    if not np_margin:
        np_margin = _calc.margin_series(np_, rev)

    # Tickertape cross-check
    tt_rev = tt_fin.get("revenue", [])
    tt_ebitda = tt_fin.get("ebitda", [])
    tt_np = tt_fin.get("net_profit", [])
    tt_ebitda_margin = _calc.margin_series(tt_ebitda, tt_rev)
    tt_np_margin = _calc.margin_series(tt_np, tt_rev)

    return {
        "symbol": symbol,
        "headers": headers,
        "ebitda_margin_pct": ebitda[-5:] if ebitda else [],
        "net_profit_margin_pct": np_margin[-5:] if np_margin else [],
        "ebitda_margin_tt": tt_ebitda_margin[-5:],
        "net_profit_margin_tt": tt_np_margin[-5:],
        "ebitda_margin_5y_avg": _calc.avg(ebitda, last_n=5),
        "net_profit_margin_5y_avg": _calc.avg(np_margin, last_n=5),
    }


async def _quarterly_performance(symbol: str) -> dict[str, Any]:
    nse_q_res, scr_q = await asyncio.gather(
        nse().get_quarterly_results(symbol),
        screener().get_quarterly_results(symbol),
        return_exceptions=True,
    )
    nse_q_res = nse_q_res if isinstance(nse_q_res, list) else []
    scr_q = scr_q if isinstance(scr_q, dict) else {}

    headers = scr_q.get("__headers__", [])
    eps_vals = scr_q.get("EPS in Rs") or scr_q.get("Diluted EPS") or scr_q.get("EPS") or []
    rev_vals = (
        scr_q.get("Sales") or scr_q.get("Revenue from Operations") or
        scr_q.get("Revenue") or []
    )
    np_vals = scr_q.get("Net Profit") or []

    eps_with_yoy = _calc.eps_yoy_series(
        headers[-8:] if headers else [f"Q{i}" for i in range(len(eps_vals[-8:]))],
        eps_vals[-8:],
    )

    return {
        "symbol": symbol,
        "eps_quarterly": eps_with_yoy,
        "revenue_quarterly": {"headers": headers[-8:], "values": rev_vals[-8:]},
        "net_profit_quarterly": {"headers": headers[-8:], "values": np_vals[-8:]},
        "nse_results": nse_q_res[:8],
    }


async def _cashflow_data(symbol: str, consolidated: bool) -> dict[str, Any]:
    scr_cf, tt_fin = await asyncio.gather(
        screener().get_cash_flow(symbol, consolidated),
        tickertape().get_financials(symbol),
        return_exceptions=True,
    )
    scr_cf = scr_cf if isinstance(scr_cf, dict) else {}
    tt_fin = tt_fin if isinstance(tt_fin, dict) else {}

    headers = scr_cf.get("__headers__", [])
    ocf = (
        scr_cf.get("Cash from Operating Activity") or
        scr_cf.get("Operating Activities") or
        scr_cf.get("Net Cash from Operating Activities") or []
    )
    investing_cf = (
        scr_cf.get("Cash from Investing Activity") or
        scr_cf.get("Cash from Investing Activities") or []
    )
    # Screener provides Free Cash Flow directly — use it; fall back to derived
    fcf_direct = scr_cf.get("Free Cash Flow") or []
    fcf_scr = fcf_direct if fcf_direct else _calc.free_cash_flow_series(ocf, investing_cf)
    tt_fcf = tt_fin.get("free_cf", [])

    return {
        "symbol": symbol,
        "headers": headers[-5:],
        "operating_cashflow": ocf[-5:],
        "investing_cashflow": investing_cf[-5:],
        "free_cashflow": fcf_scr[-5:],
        "free_cashflow_tickertape": tt_fcf[-5:] if tt_fcf else [],
        "avg_fcf_3y": _calc.avg(fcf_scr, last_n=3),
        "avg_fcf_5y": _calc.avg(fcf_scr, last_n=5),
    }


async def _balance_sheet_health(symbol: str, consolidated: bool) -> dict[str, Any]:
    scr_bs, scr_pl, scr_rat = await asyncio.gather(
        screener().get_balance_sheet(symbol, consolidated),
        screener().get_profit_loss(symbol, consolidated),
        screener().get_ratios_table(symbol, consolidated),
        return_exceptions=True,
    )
    scr_bs = scr_bs if isinstance(scr_bs, dict) else {}
    scr_pl = scr_pl if isinstance(scr_pl, dict) else {}
    scr_rat = scr_rat if isinstance(scr_rat, dict) else {}

    headers = scr_bs.get("__headers__", [])
    borrowings = scr_bs.get("Borrowings") or scr_bs.get("Borrowing") or scr_bs.get("Total Debt") or []
    # Net Worth = Equity Capital + Reserves (Screener splits them)
    equity_cap = scr_bs.get("Equity Capital") or []
    reserves = scr_bs.get("Reserves") or []
    if equity_cap and reserves and len(equity_cap) == len(reserves):
        net_worth = [round((e or 0) + (r or 0), 2) for e, r in zip(equity_cap, reserves)]
    else:
        net_worth = scr_bs.get("Net Worth") or scr_bs.get("Total Equity") or equity_cap or []
    de_trend = _calc.de_ratio_series(borrowings, net_worth)

    # D/E from ratios table (cross-check)
    de_ratio_series_rat = scr_rat.get("Debt to equity") or scr_rat.get("D/E Ratio") or []
    de_crosscheck = _cc.reconcile_ratio(
        de_trend[-1] if de_trend else None,
        de_ratio_series_rat[-1] if de_ratio_series_rat else None,
        name="D/E",
    )

    # ICR — for banks use Financing Profit; for others use Operating Profit
    ebit = (
        scr_pl.get("Operating Profit") or scr_pl.get("EBIT") or
        scr_pl.get("Financing Profit") or []
    )
    interest = scr_pl.get("Interest") or scr_pl.get("Finance Costs") or []
    icr = None
    if ebit and interest:
        icr = _calc.interest_coverage(ebit[-1], interest[-1]) if ebit[-1] and interest[-1] else None

    # Current ratio from ratios table
    cr_series = scr_rat.get("Current ratio") or scr_rat.get("Current Ratio") or []
    current_ratio = cr_series[-1] if cr_series else None

    return {
        "symbol": symbol,
        "headers": headers[-5:],
        "de_ratio_trend": de_trend[-5:],
        "de_ratio_current": de_crosscheck["value"],
        "de_5y_avg": _calc.avg(de_trend, last_n=5),
        "interest_coverage_ratio": icr,
        "current_ratio": current_ratio,
        "borrowings_series": borrowings[-5:],
        "net_worth_series": net_worth[-5:],
    }


async def _return_ratios(symbol: str, consolidated: bool) -> dict[str, Any]:
    scr_rat, tt_info = await asyncio.gather(
        screener().get_ratios_table(symbol, consolidated),
        tickertape().get_stock_info(symbol),
        return_exceptions=True,
    )
    scr_rat = scr_rat if isinstance(scr_rat, dict) else {}
    tt_info = tt_info if isinstance(tt_info, dict) else {}

    headers = scr_rat.get("__headers__", [])
    # Screener ratios table has ROCE % but not ROE — fetch key_ratios for ROE
    scr_key = await screener().get_key_ratios(symbol, consolidated)
    scr_key = scr_key if isinstance(scr_key, dict) else {}

    roe_series = scr_rat.get("ROE %") or scr_rat.get("Return on Equity") or scr_rat.get("ROE") or []
    roce_series = scr_rat.get("ROCE %") or scr_rat.get("Return on Capital Employed") or scr_rat.get("ROCE") or []

    roe_current = roe_series[-1] if roe_series else scr_key.get("roe") or tt_info.get("roe")
    roce_current = roce_series[-1] if roce_series else scr_key.get("roce") or tt_info.get("roce")

    roe_xchk = _cc.reconcile_ratio(roe_current, tt_info.get("roe"), name="ROE")
    roce_xchk = _cc.reconcile_ratio(roce_current, tt_info.get("roce"), name="ROCE")

    return {
        "symbol": symbol,
        "headers": headers,
        "roe_series": roe_series,
        "roce_series": roce_series,
        "roe_current": roe_xchk["value"],
        "roe_3y_avg": _calc.avg(roe_series, last_n=3),
        "roe_5y_avg": _calc.avg(roe_series, last_n=5),
        "roce_current": roce_xchk["value"],
        "roce_3y_avg": _calc.avg(roce_series, last_n=3),
        "roce_5y_avg": _calc.avg(roce_series, last_n=5),
    }


async def _dividend_history(symbol: str) -> dict[str, Any]:
    nse_actions, bse_actions, scr_pl = await asyncio.gather(
        nse().get_corporate_actions(symbol),
        bse().get_corporate_actions(symbol),
        screener().get_profit_loss(symbol, consolidated=True),
        return_exceptions=True,
    )
    nse_actions = nse_actions if isinstance(nse_actions, list) else []
    bse_actions = bse_actions if isinstance(bse_actions, list) else []
    scr_pl = scr_pl if isinstance(scr_pl, dict) else {}

    # Merge dividend actions — prefer NSE
    div_actions = [
        a for a in nse_actions
        if a.get("action") and "dividend" in str(a.get("action", "")).lower()
    ]
    if not div_actions:
        div_actions = [
            a for a in bse_actions
            if a.get("action") and "dividend" in str(a.get("action", "")).lower()
        ]

    # Payout ratio from screener
    div_paid = scr_pl.get("Dividend Payout %") or scr_pl.get("Dividend Paid") or []
    headers = scr_pl.get("__headers__", [])

    return {
        "symbol": symbol,
        "dividend_actions": div_actions[:10],
        "dividend_payout_pct_series": {
            "headers": headers[-5:],
            "values": div_paid[-5:] if isinstance(div_paid, list) else [],
        },
    }


async def _shareholding_pattern(symbol: str) -> dict[str, Any]:
    nse_sh, tt_sh, scr_sh = await asyncio.gather(
        nse().get_shareholding(symbol),
        tickertape().get_shareholding(symbol),
        screener().get_shareholding(symbol),
        return_exceptions=True,
    )
    nse_sh = nse_sh if isinstance(nse_sh, list) else []
    tt_sh = tt_sh if isinstance(tt_sh, list) else []
    scr_sh = scr_sh if isinstance(scr_sh, dict) else {}

    # Prefer NSE data; fall back to Tickertape
    records = nse_sh if nse_sh else tt_sh
    trend = _cc.build_shareholding_trend(records)

    # Screener shareholding table labels (after + stripping)
    sh_headers = scr_sh.get("__headers__", [])
    promoter_scr = (
        scr_sh.get("Promoters") or scr_sh.get("Promoter & Promoter Group") or
        scr_sh.get("Promoter and Promoter Group") or []
    )
    fii_scr = (
        scr_sh.get("FIIs") or scr_sh.get("Foreign Institutional Investors") or
        scr_sh.get("Foreign Institutions") or []
    )
    dii_scr = (
        scr_sh.get("DIIs") or scr_sh.get("Domestic Institutional Investors") or
        scr_sh.get("Domestic Institutions") or []
    )
    # If NSE/Tickertape records are empty, build trend from Screener data
    if not records and sh_headers and promoter_scr:
        records = [
            {
                "period": sh_headers[i],
                "promoter_pct": promoter_scr[i] if i < len(promoter_scr) else None,
                "fii_pct": fii_scr[i] if i < len(fii_scr) else None,
                "dii_pct": dii_scr[i] if i < len(dii_scr) else None,
                "public_pct": None,
                "promoter_pledged_pct": None,
            }
            for i in range(len(sh_headers))
        ]
        trend = _cc.build_shareholding_trend(records)

    return {
        "symbol": symbol,
        "trend": trend,
        "records": records[:12],
        "screener_supplement": {
            "headers": sh_headers,
            "promoter": promoter_scr,
            "fii": fii_scr,
            "dii": dii_scr,
        },
    }


async def _peer_comparison(symbol: str, consolidated: bool) -> dict[str, Any]:
    scr_peers, tt_peers = await asyncio.gather(
        screener().get_peers(symbol),
        tickertape().get_sector_peers(symbol),
        return_exceptions=True,
    )
    scr_peers = scr_peers if isinstance(scr_peers, list) else []
    tt_peers = tt_peers if isinstance(tt_peers, list) else []

    # Merge: Screener peers are more complete; Tickertape fills gaps
    peer_map: dict[str, dict] = {}
    for p in scr_peers:
        name = str(p.get("Name") or p.get("Company") or p.get("col_0", "")).strip()
        if name:
            peer_map[name] = {**p, "source": "Screener.in"}

    for p in tt_peers:
        t = str(p.get("symbol") or p.get("ticker") or "").strip()
        if t and t not in peer_map:
            peer_map[t] = {**p, "source": "Tickertape"}

    # Exclude the queried stock itself
    peers_final = [
        v for k, v in peer_map.items()
        if symbol.upper() not in k.upper()
    ][:3]

    return {
        "symbol": symbol,
        "peers": peers_final,
    }


async def _recent_news(symbol: str) -> dict[str, Any]:
    nse_q = await nse().get_quote(symbol)
    company_name = (nse_q or {}).get("company_name", symbol)
    items = await news_fetcher().get_top_news(symbol, company_name, top_n=5)
    return {
        "symbol": symbol,
        "company_name": company_name,
        "news": items,
        "fetched_at": datetime.now().strftime("%d-%b-%Y %H:%M"),
    }


async def _full_analysis(symbol: str, consolidated: bool) -> dict[str, Any]:
    """
    Silently fetch everything in parallel, cross-check, and return
    a single comprehensive JSON payload.
    """
    (
        live,
        valuations,
        growth,
        margins,
        quarterly,
        cashflow,
        bs_health,
        returns,
        dividends,
        shareholding,
        peers,
        news,
    ) = await asyncio.gather(
        _live_market_data(symbol),
        _valuation_metrics(symbol, consolidated),
        _growth_metrics(symbol, consolidated),
        _profitability_trends(symbol, consolidated),
        _quarterly_performance(symbol),
        _cashflow_data(symbol, consolidated),
        _balance_sheet_health(symbol, consolidated),
        _return_ratios(symbol, consolidated),
        _dividend_history(symbol),
        _shareholding_pattern(symbol),
        _peer_comparison(symbol, consolidated),
        _recent_news(symbol),
        return_exceptions=True,
    )

    def _safe(result: Any, fallback: dict) -> dict:
        return result if isinstance(result, dict) else {**fallback, "error": str(result)}

    return {
        "symbol": symbol,
        "analysis_date": datetime.now().strftime("%d-%b-%Y"),
        "financials_type": "consolidated" if consolidated else "standalone",
        "live_market_data": _safe(live, {"symbol": symbol}),
        "valuation_metrics": _safe(valuations, {"symbol": symbol}),
        "growth_metrics": _safe(growth, {"symbol": symbol}),
        "profitability_trends": _safe(margins, {"symbol": symbol}),
        "quarterly_performance": _safe(quarterly, {"symbol": symbol}),
        "cashflow_data": _safe(cashflow, {"symbol": symbol}),
        "balance_sheet_health": _safe(bs_health, {"symbol": symbol}),
        "return_ratios": _safe(returns, {"symbol": symbol}),
        "dividend_history": _safe(dividends, {"symbol": symbol}),
        "shareholding_pattern": _safe(shareholding, {"symbol": symbol}),
        "peer_comparison": _safe(peers, {"symbol": symbol}),
        "recent_news": _safe(news, {"symbol": symbol}),
    }


# --------------------------------------------------------------------------- #
#  MCP handler registration                                                     #
# --------------------------------------------------------------------------- #

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return TOOLS


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent]:
    args = arguments or {}
    symbol: str = args.get("symbol", "").upper().strip()
    if not symbol:
        return [types.TextContent(type="text", text='{"error": "symbol is required"}')]

    consolidated: bool = bool(args.get("consolidated", True))

    try:
        dispatch = {
            "fetch_live_market_data": lambda: _live_market_data(symbol),
            "fetch_valuation_metrics": lambda: _valuation_metrics(symbol, consolidated),
            "fetch_growth_metrics": lambda: _growth_metrics(symbol, consolidated),
            "fetch_profitability_trends": lambda: _profitability_trends(symbol, consolidated),
            "fetch_quarterly_performance": lambda: _quarterly_performance(symbol),
            "fetch_cashflow_data": lambda: _cashflow_data(symbol, consolidated),
            "fetch_balance_sheet_health": lambda: _balance_sheet_health(symbol, consolidated),
            "fetch_return_ratios": lambda: _return_ratios(symbol, consolidated),
            "fetch_dividend_history": lambda: _dividend_history(symbol),
            "fetch_shareholding_pattern": lambda: _shareholding_pattern(symbol),
            "fetch_peer_comparison": lambda: _peer_comparison(symbol, consolidated),
            "fetch_recent_news": lambda: _recent_news(symbol),
            "fetch_full_analysis": lambda: _full_analysis(symbol, consolidated),
        }
        if name not in dispatch:
            return [types.TextContent(type="text", text=f'{{"error": "unknown tool: {name}"}}')]

        result = await dispatch[name]()
        return [
            types.TextContent(
                type="text",
                text=json.dumps(result, indent=2, ensure_ascii=False),
            )
        ]
    except Exception as exc:
        logger.error("Tool %s failed for %s: %s", name, symbol, exc, exc_info=True)
        return [
            types.TextContent(
                type="text",
                text=json.dumps({"error": str(exc), "symbol": symbol, "tool": name}),
            )
        ]


# --------------------------------------------------------------------------- #
#  Entry point                                                                  #
# --------------------------------------------------------------------------- #

async def main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="financial-data-mcp",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


def main_sync() -> None:
    """Synchronous entry point used by the installed CLI command."""
    asyncio.run(main())


if __name__ == "__main__":
    main_sync()
