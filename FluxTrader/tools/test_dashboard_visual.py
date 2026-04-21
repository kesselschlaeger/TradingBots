"""Visueller Smoke-Test: Dashboard mit Fake-Symbol-Status starten.

    python tools/test_dashboard_visual.py

Öffnet anschließend http://localhost:8181 im Browser.
Bot-Cards sind alle als RUNNING markiert. Symbol-Status-Tabelle
öffnet per Klick auf den Card-Header.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.config import load_config
from dashboard.app import create_app
from live.health import HealthState

# ── Fake-HealthState aufbauen ──────────────────────────────────────────────

hs = HealthState()

FAKE_SYMBOLS: dict[str, list[tuple[str, str, str]]] = {
    "orb": [
        ("AAPL",  "WAIT_BREAKOUT",     "Preis 182.00 in [181.20..183.50]"),
        ("NVDA",  "GAP_BLOCK",          "gap 3.52%"),
        ("SPY",   "SIGNAL",             "ORB Breakout: 520.10 > 519.80 +Vol 2.1x"),
        ("QQQ",   "WAIT_ORB",           "ORB-Periode, 15m"),
        ("MSFT",  "TREND_BLOCK",        "SPY-Trend nicht bullish"),
        ("TSLA",  "MIT_BLOCK",          "Gruppe high_beta_growth belegt (PLTR)"),
        ("META",  "OUTSIDE_HOURS",      "außerhalb Handelszeiten"),
        ("AMZN",  "MIT_OVERLAY_REJECT", "EV 0.21 < Schwelle 0.30"),
    ],
    "botti": [
        ("AAPL",  "SIGNAL",         "botti_trend BUY, VIX-Faktor 1.0"),
        ("SPY",   "WAIT_BREAKOUT",  "MR-Setup warten"),
        ("QQQ",   "GAP_BLOCK",      "gap 2.10%"),
    ],
}

for strat_name, symbols in FAKE_SYMBOLS.items():
    for sym, code, reason in symbols:
        hs.set_symbol_status(strat_name, sym, code, reason)

# ── Equity-ähnliche Fake-Daten damit Portfolio-Panel nicht leer bleibt ────

import asyncio

async def _seed_health():
    await hs.set_broker_status(connected=True, adapter="paper")
    await hs.update_portfolio(
        equity=102_450.0, cash=48_200.0,
        drawdown_pct=-1.3, open_positions=2, peak_equity=103_100.0,
    )

asyncio.run(_seed_health())

# ── App starten ────────────────────────────────────────────────────────────

cfg = load_config("configs/botti.yaml")
app = create_app(cfg, health_state=hs)

if __name__ == "__main__":
    import uvicorn
    print("\n  Dashboard: http://localhost:8181\n")
    uvicorn.run(app, host="127.0.0.1", port=8181, log_level="warning")
