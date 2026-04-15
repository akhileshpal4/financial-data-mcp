"""
Financial calculator — CAGR, averages, margin derivation, FCF derivation,
EPS YoY change, and multi-year trend helpers.
All inputs expected in consistent units (₹ Crore or % as floats).
"""

from __future__ import annotations

import math
import statistics
import logging
from typing import Any

logger = logging.getLogger(__name__)


def safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


class FinancialCalculator:

    # ------------------------------------------------------------------ #
    #  CAGR                                                                 #
    # ------------------------------------------------------------------ #

    def cagr(self, start: Any, end: Any, years: float) -> float | None:
        """Compound Annual Growth Rate in percent."""
        s, e = safe_float(start), safe_float(end)
        if s is None or e is None or years <= 0:
            return None
        if s == 0 or s < 0:
            return None
        rate = (e / s) ** (1.0 / years) - 1.0
        return round(rate * 100, 2)

    def cagr_from_series(self, series: list[Any], years: int | None = None) -> float | None:
        """
        Compute CAGR from a time-ordered list (oldest → newest).
        `years` defaults to len(series) - 1 if not supplied.
        """
        clean = [safe_float(v) for v in series]
        clean = [v for v in clean if v is not None]
        if len(clean) < 2:
            return None
        n = years if years is not None else (len(clean) - 1)
        return self.cagr(clean[0], clean[-1], n)

    # ------------------------------------------------------------------ #
    #  Averages                                                             #
    # ------------------------------------------------------------------ #

    def avg(self, values: list[Any], last_n: int | None = None) -> float | None:
        clean = [safe_float(v) for v in (values[-last_n:] if last_n else values)]
        clean = [v for v in clean if v is not None]
        if not clean:
            return None
        return round(statistics.mean(clean), 2)

    # ------------------------------------------------------------------ #
    #  Margins                                                              #
    # ------------------------------------------------------------------ #

    def margin_series(
        self,
        numerator_series: list[Any],
        revenue_series: list[Any],
    ) -> list[float | None]:
        """Compute margin % for each period."""
        result = []
        for num, rev in zip(numerator_series, revenue_series):
            n, r = safe_float(num), safe_float(rev)
            if n is None or r is None or r == 0:
                result.append(None)
            else:
                result.append(round(n / r * 100, 2))
        return result

    # ------------------------------------------------------------------ #
    #  FCF                                                                  #
    # ------------------------------------------------------------------ #

    def free_cash_flow_series(
        self,
        operating_cf: list[Any],
        capex: list[Any],
    ) -> list[float | None]:
        """FCF = Operating CF − CapEx (CapEx treated as positive spend)."""
        result = []
        for ocf, cx in zip(operating_cf, capex):
            o, c = safe_float(ocf), safe_float(cx)
            if o is None:
                result.append(None)
            else:
                cx_val = abs(c) if c is not None else 0.0
                result.append(round(o - cx_val, 2))
        return result

    # ------------------------------------------------------------------ #
    #  EPS quarterly YoY                                                   #
    # ------------------------------------------------------------------ #

    def eps_yoy_series(
        self,
        quarters: list[str],
        eps_values: list[Any],
    ) -> list[dict]:
        """
        Return list of {quarter, eps, yoy_change_pct} for the last 8 quarters.
        YoY requires eps from the same quarter one year prior (index - 4).
        """
        result = []
        for i, (q, e) in enumerate(zip(quarters, eps_values)):
            eps = safe_float(e)
            yoy = None
            if i >= 4 and eps is not None:
                prior = safe_float(eps_values[i - 4])
                if prior is not None and prior != 0:
                    yoy = round((eps - prior) / abs(prior) * 100, 2)
            result.append({"quarter": q, "eps": eps, "yoy_pct": yoy})
        return result[-8:]

    # ------------------------------------------------------------------ #
    #  D/E and return ratio helpers                                         #
    # ------------------------------------------------------------------ #

    def de_ratio_series(
        self,
        borrowings: list[Any],
        net_worth: list[Any],
    ) -> list[float | None]:
        result = []
        for b, nw in zip(borrowings, net_worth):
            bv, nwv = safe_float(b), safe_float(nw)
            if bv is None or nwv is None or nwv == 0:
                result.append(None)
            else:
                result.append(round(bv / nwv, 2))
        return result

    def interest_coverage(self, ebit: Any, interest: Any) -> float | None:
        e, i = safe_float(ebit), safe_float(interest)
        if e is None or i is None or i == 0:
            return None
        return round(e / i, 2)

    def current_ratio(self, current_assets: Any, current_liabilities: Any) -> float | None:
        ca, cl = safe_float(current_assets), safe_float(current_liabilities)
        if ca is None or cl is None or cl == 0:
            return None
        return round(ca / cl, 2)

    def roe(self, net_profit: Any, avg_equity: Any) -> float | None:
        np_, eq = safe_float(net_profit), safe_float(avg_equity)
        if np_ is None or eq is None or eq == 0:
            return None
        return round(np_ / eq * 100, 2)

    def roce(self, ebit: Any, capital_employed: Any) -> float | None:
        e, ce = safe_float(ebit), safe_float(capital_employed)
        if e is None or ce is None or ce == 0:
            return None
        return round(e / ce * 100, 2)
