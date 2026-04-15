# Financial Data MCP Server

A **Model Context Protocol (MCP) server** for deep, cross-checked analysis of Indian NSE/BSE-listed stocks.

Every data point is silently verified against **at least 2 independent sources** before being returned:

> **NSE India → BSE India → Screener.in → Tickertape → Moneycontrol**

---

## What it does

One call to `fetch_full_analysis` returns:

| Category | Detail |
|---|---|
| **Live market data** | CMP, 52W high/low, market cap, face value (NSE × BSE cross-check) |
| **Valuation** | P/E, P/B, EV/EBITDA — current + sector average + stock's own 5-year historical |
| **Growth** | Revenue / Net Profit / EPS CAGR — 3-year and 5-year |
| **Profitability** | EBITDA margin & net profit margin trends — 5 years |
| **Quarterly EPS** | Last 8 quarters with year-on-year % change |
| **Cash Flow** | Operating CF, CapEx, Free Cash Flow — last 3–5 years |
| **Balance sheet** | Debt-to-Equity trend (5Y), Interest Coverage Ratio, Current Ratio |
| **Return ratios** | ROE and ROCE — current + 3-year avg + 5-year avg |
| **Dividends** | Full dividend history + payout ratio trend |
| **Shareholding** | Promoter (12Q), FII/DII (8Q), pledging flag if > 10% |
| **Peer comparison** | 3 closest peers — P/E, P/B, ROE, Revenue Growth, D/E |
| **News** | Top 5 recent news items relevant to long-term investors |

---

## Installation

```bash
git clone https://github.com/akhileshpal4/financial-data-mcp.git
cd financial-data-mcp
pip install -r requirements.txt
```

---

## Register with Claude Desktop

Open `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows) and add:

```json
{
  "mcpServers": {
    "financial-data": {
      "command": "python",
      "args": ["/absolute/path/to/financial-data-mcp/server.py"],
      "env": {}
    }
  }
}
```

Restart Claude Desktop. The server will appear as **financial-data** in the tools panel.

---

## Register with VS Code (GitHub Copilot)

Add to your `.vscode/mcp.json` or user-level MCP settings:

```json
{
  "servers": {
    "financial-data": {
      "type": "stdio",
      "command": "python",
      "args": ["/absolute/path/to/financial-data-mcp/server.py"]
    }
  }
}
```

---

## Available tools

| Tool | Description |
|---|---|
| `fetch_full_analysis` | All metrics in one shot |
| `fetch_live_market_data` | CMP, 52W H/L, market cap, face value |
| `fetch_valuation_metrics` | P/E, P/B, EV/EBITDA with historical context |
| `fetch_growth_metrics` | Revenue / NP / EPS CAGR |
| `fetch_profitability_trends` | EBITDA & NP margin trends |
| `fetch_quarterly_performance` | 8Q EPS with YoY change |
| `fetch_cashflow_data` | OCF, CapEx, FCF |
| `fetch_balance_sheet_health` | D/E, ICR, Current Ratio |
| `fetch_return_ratios` | ROE & ROCE averages |
| `fetch_dividend_history` | Dividends + payout ratio |
| `fetch_shareholding_pattern` | Promoter/FII/DII + pledging alert |
| `fetch_peer_comparison` | 3 peers side-by-side |
| `fetch_recent_news` | Top 5 long-term–relevant news |

**Example usage in Claude:**
```
Analyse RELIANCE for long-term investment
```
```
fetch_full_analysis with symbol="HDFCBANK"
```

---

## Data sources

1. **NSE India** (nseindia.com) — live quotes, shareholding, corporate actions
2. **BSE India** (bseindia.com) — backup quotes and cross-check
3. **Screener.in** — multi-year P&L, balance sheet, cash flow, ratios, peers
4. **Tickertape** — valuation, sector peers, shareholding cross-check
5. **Moneycontrol** — news aggregation
6. **Google News RSS** — recent news cross-check

---

## Requirements

- Python ≥ 3.11
- No API keys required — all public data sources

---

## License

MIT
