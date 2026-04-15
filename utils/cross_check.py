"""
Cross-checker utility — reconciles the same metric from multiple sources.
Silently validates, flags large discrepancies, and returns a best-estimate value.
"""

from __future__ import annotations

import statistics
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Tolerance thresholds
PRICE_TOL = 0.005       # 0.5 % — live price must be very close
RATIO_TOL = 0.08        # 8 %  — P/E, P/B, etc.
FINANCIAL_TOL = 0.05    # 5 %  — revenue, profit numbers


def _safe_float(value: Any) -> float | None:
    try:
        v = float(value)
        return v if v == v else None   # filter NaN
    except (TypeError, ValueError):
        return None


class CrossChecker:
    """
    Compares values from multiple sources and returns a reconciled result.
    All cross-checking is performed silently — no source names are exposed
    in the user-facing output.
    """

    def reconcile(
        self,
        values: list[Any],
        tolerance: float = RATIO_TOL,
        metric_name: str = "",
    ) -> dict[str, Any]:
        """
        Given a list of values (possibly None / from different sources),
        return a dict with:
          - 'value'     : best estimate (median of valid values)
          - 'confidence': 'high' | 'medium' | 'low'
          - 'discrepancy': True if spread > tolerance
        """
        valid: list[float] = [v for v in (_safe_float(x) for x in values) if v is not None]
        if not valid:
            return {"value": None, "confidence": "unavailable", "discrepancy": False}

        if len(valid) == 1:
            return {"value": round(valid[0], 4), "confidence": "low", "discrepancy": False}

        median_val = statistics.median(valid)
        if median_val == 0:
            return {"value": 0, "confidence": "high", "discrepancy": False}

        spread = (max(valid) - min(valid)) / abs(median_val)
        discrepancy = spread > tolerance

        if discrepancy:
            logger.debug(
                "Discrepancy in '%s': values=%s spread=%.1f%%",
                metric_name, valid, spread * 100
            )

        confidence = "high" if (not discrepancy and len(valid) >= 2) else (
            "medium" if len(valid) >= 2 else "low"
        )

        return {
            "value": round(median_val, 4),
            "confidence": confidence,
            "discrepancy": discrepancy,
        }

    def reconcile_price(self, *values: Any) -> dict[str, Any]:
        return self.reconcile(list(values), tolerance=PRICE_TOL, metric_name="price")

    def reconcile_ratio(self, *values: Any, name: str = "") -> dict[str, Any]:
        return self.reconcile(list(values), tolerance=RATIO_TOL, metric_name=name)

    def reconcile_financial(self, *values: Any, name: str = "") -> dict[str, Any]:
        return self.reconcile(list(values), tolerance=FINANCIAL_TOL, metric_name=name)

    def check_pledging(self, promoter_pledged_pct: float | None) -> dict[str, Any]:
        """Flag if promoter pledging exceeds 10%."""
        if promoter_pledged_pct is None:
            return {"pledged_pct": None, "flag": False, "alert": "Data unavailable"}
        flag = float(promoter_pledged_pct) > 10.0
        return {
            "pledged_pct": promoter_pledged_pct,
            "flag": flag,
            "alert": "HIGH PLEDGING — exceeds 10%" if flag else "Within safe limits",
        }

    def build_shareholding_trend(
        self, records: list[dict]
    ) -> dict[str, Any]:
        """
        Build trend analysis for promoter, FII, DII over quarters.
        Detects rising / falling / stable trends.
        """
        if not records:
            return {}

        def trend_label(vals: list[float | None]) -> str:
            clean = [v for v in vals if v is not None]
            if len(clean) < 2:
                return "insufficient data"
            delta = clean[-1] - clean[0]
            if delta > 1.5:
                return "rising"
            if delta < -1.5:
                return "falling"
            return "stable"

        promoter = [r.get("promoter_pct") for r in records]
        fii = [r.get("fii_pct") for r in records]
        dii = [r.get("dii_pct") for r in records]
        pledged = [r.get("promoter_pledged_pct") for r in records]

        latest_pledge = next((p for p in reversed(pledged) if p is not None), None)

        return {
            "quarters": [r.get("period") for r in records],
            "promoter": promoter,
            "fii": fii,
            "dii": dii,
            "pledged": pledged,
            "promoter_trend": trend_label(promoter),
            "fii_trend": trend_label(fii),
            "dii_trend": trend_label(dii),
            "pledging_alert": self.check_pledging(latest_pledge),
        }
