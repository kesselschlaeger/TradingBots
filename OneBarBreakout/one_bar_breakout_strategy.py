#!/usr/bin/env python3
"""
one_bar_breakout_strategy.py – Single Source of Truth für die
50-Bar High/Low Momentum Strategie (auch „One-Bar Breakout" genannt).

Strategie-Regeln (exakt nach Original-Video):
  - Long:  aktueller Close > höchstes High der letzten 50 Bars
  - Short: aktueller Close < tiefstes  Low  der letzten 50 Bars
  - Haltedauer: IMMER genau 1 Bar → Exit zum nächsten Market Open (Market Order)
  - KEINE Stop-Loss, KEINE Take-Profit, KEINE zusätzlichen Filter

Architektur-Entscheidung (Variante A – eigenständige Datei):
  Die Strategie ist NICHT in orb_bot_alpaca.py integriert, weil:
    1. Fundamental andere Exit-Logik: kein Bracket-Order, kein SL/TP,
       Exit via Market Order am nächsten Open statt intraday.
    2. Andere Datengranularität: Daily Bars statt 5m-Bars.
    3. Zero-Touch-Policy: ORB-Bot bleibt 100% unberührt.
    4. Single Responsibility: 1 Datei = 1 Strategie = 1 Verantwortung.
    5. Sauberer Vergleich: zwei unabhängige Equity-Kurven im Report.

Imports in anderen Dateien:
    from one_bar_breakout_strategy import (
        OBB_DEFAULT_CONFIG,
        compute_obb_signals,
        generate_obb_live_signal,
        calculate_obb_position_size,
    )
"""

from __future__ import annotations

from datetime import datetime, time
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

ET = pytz.timezone("America/New_York")


# ============================= Default-Konfiguration ========================

OBB_DEFAULT_CONFIG: dict = {
    # ── Symbole ────────────────────────────────────────────────────────────
    "symbols": [
        #"SPY", "QQQ", "IWM", "DIA","NVDA", "TSLA", "AMD", "AVGO","AAPL", "MSFT", "META", "AMZN","PLTR", "GOOGL", "NFLX",
        "ORCL","NOW","TSM","INTC"
                ],
    #"symbols": ["SPY", "QQQ", "IWM", "TSLA", "NVDA", "AAPL", "AMD"], ##für Vergleich mit Gemini-Implementierung (gleiche Symbole)

    # ── Kern-Strategie-Parameter (exakt nach Original) ────────────────────
    # Lookback für Rolling-High/Low: Anzahl der vergangenen Bars
    # (nicht inklusive des aktuellen Bars)
    "lookback_bars": 50,

    # ── Short-Seite ───────────────────────────────────────────────────────
    "allow_shorts": True,

    # ── Position Sizing (kein SL → fixer Anteil am Kapital) ──────────────
    # position_size_pct: Anteil des Eigenkapitals pro Trade
    # z.B. 0.10 = 10% des Kapitals je Position
    "position_size_pct": 0.10,

    # Kelly-basiertes Sizing aktivieren (nutzt rolling Win-Rate der letzten
    # kelly_lookback_trades als Proxy für die Gewinnwahrscheinlichkeit)
    "use_kelly_sizing": False,
    "kelly_fraction": 0.50,         # Half-Kelly
    "kelly_lookback_trades": 50,    # Trades für Rolling Win-Rate
    "kelly_min_trades": 20,         # Mindestanzahl für Kelly-Aktivierung
    "kelly_payoff_ratio": 1.0,      # Geschätztes durchschnittliches Win/Loss-Verhältnis

    # ── Marktzeiten ────────────────────────────────────────────────────────
    "market_open":  time(9, 30),
    "market_close": time(16, 0),

    # ── Risiko-Guards ─────────────────────────────────────────────────────
    "max_daily_trades":          3,
    "max_concurrent_positions":  3,

    # ── Kosten (realistisch für Backtest) ─────────────────────────────────
    "commission_pct": 0.00005,   # 0.5 Basispunkte je Seite
    "slippage_pct":   0.0005,    # Gemini hat hier 5 Basispunkte 3 Basispunkte (höher als ORB: Daily Open-Slippage)

    # ── Kapital & Ausgabe ──────────────────────────────────────────────────
    "initial_capital": 10_000.0,
    "currency": "USD",

    # ── Alpaca ─────────────────────────────────────────────────────────────
    "alpaca_paper":     True,
    "alpaca_data_feed": "iex",

    # ── Benchmark ──────────────────────────────────────────────────────────
    "benchmark": "SPY",

    # ── Logging ────────────────────────────────────────────────────────────
    "debug_signals": False,
}


# ============================= Signal-Generierung ===========================

def compute_obb_signals(
    df: pd.DataFrame,
    cfg: dict,
) -> pd.DataFrame:
    """
    Berechne One-Bar-Breakout-Signale für einen DataFrame mit täglichen Bars.

    Exakte Implementierung der Original-Regeln:
      - Long:  Close[i] > max(High[i-lookback : i])
      - Short: Close[i] < min(Low[i-lookback  : i])

    Das Rolling-Fenster beginnt NICHT beim aktuellen Bar (shift(1)),
    um Look-Ahead-Bias zu vermeiden.

    Parameter
    ----------
    df  : DataFrame mit OHLCV-Spalten (Index = DatetimeIndex)
    cfg : Konfigurationsdict

    Rückgabe
    --------
    DataFrame mit Spalten:
      - signal:         "BUY", "SHORT", "HOLD"
      - lookback_high:  höchstes High der letzten N Bars
      - lookback_low:   tiefstes Low der letzten N Bars
      - close:          aktueller Close
    """
    lookback = int(cfg.get("lookback_bars", 50))
    allow_shorts = bool(cfg.get("allow_shorts", True))

    out = pd.DataFrame(index=df.index)
    out["close"] = df["Close"].astype(float)

    # Rolling High/Low der vergangenen `lookback` Bars (exkl. aktueller Bar)
    # shift(1): aktueller Bar wird NICHT in das Fenster einbezogen → kein Look-Ahead-Bias
    out["lookback_high"] = (
        df["High"].shift(1).rolling(lookback, min_periods=lookback).max()
    )
    out["lookback_low"] = (
        df["Low"].shift(1).rolling(lookback, min_periods=lookback).min()
    )

    # Signale setzen
    out["signal"] = "HOLD"

    long_mask = out["close"] > out["lookback_high"]
    out.loc[long_mask, "signal"] = "BUY"

    if allow_shorts:
        short_mask = out["close"] < out["lookback_low"]
        # Short hat niedrigere Priorität als Long (kann nicht gleichzeitig feuern,
        # aber Safety-Check: Long überschreibt Short wenn beide True)
        out.loc[short_mask & ~long_mask, "signal"] = "SHORT"

    # NaN-Rows (erste `lookback` Bars ohne ausreichend Historie) → HOLD
    no_data = out["lookback_high"].isna() | out["lookback_low"].isna()
    out.loc[no_data, "signal"] = "HOLD"

    return out


def generate_obb_live_signal(
    df: pd.DataFrame,
    cfg: dict,
) -> Tuple[str, float, float, float, str]:
    """
    Generiere Signal für den aktuellen (letzten) Bar – Live-Bot.

    Nutzt tägliche Bars. Der letzte Bar im DataFrame ist der heutige
    Handelsschluss (oder der letzte verfügbare Close).

    Parameter
    ----------
    df  : DataFrame mit täglichen OHLCV-Bars (mindestens lookback+2 Bars)
    cfg : Konfigurationsdict

    Rückgabe
    --------
    (signal, current_close, lookback_high, lookback_low, reason)
      - signal: "BUY", "SHORT", "HOLD"
      - current_close: aktueller Close
      - lookback_high: 50-Bar-High (Breakout-Level für Long)
      - lookback_low:  50-Bar-Low  (Breakout-Level für Short)
      - reason: Erklärung für Log/Telegram
    """
    lookback = int(cfg.get("lookback_bars", 50))
    min_bars = lookback + 2

    if len(df) < min_bars:
        return (
            "HOLD", 0.0, 0.0, 0.0,
            f"Zu wenig Daten ({len(df)} < {min_bars} benötigt)"
        )

    current_close = float(df["Close"].iloc[-1])

    # Letzten N Bars EXKL. aktuellem Bar (exakter Signal-Lookup)
    hist = df.iloc[-(lookback + 1):-1]  # genau lookback Bars
    lookback_high = float(hist["High"].max())
    lookback_low  = float(hist["Low"].min())

    allow_shorts = bool(cfg.get("allow_shorts", True))

    if current_close > lookback_high:
        reason = (
            f"OBB Long: Close {current_close:.2f} > "
            f"{lookback}-Bar-High {lookback_high:.2f} "
            f"(+{(current_close / lookback_high - 1) * 100:.2f}%)"
        )
        return "BUY", current_close, lookback_high, lookback_low, reason

    if allow_shorts and current_close < lookback_low:
        reason = (
            f"OBB Short: Close {current_close:.2f} < "
            f"{lookback}-Bar-Low {lookback_low:.2f} "
            f"({(current_close / lookback_low - 1) * 100:.2f}%)"
        )
        return "SHORT", current_close, lookback_high, lookback_low, reason

    reason = (
        f"HOLD: Close {current_close:.2f} "
        f"[{lookback_low:.2f} – {lookback_high:.2f}]"
    )
    return "HOLD", current_close, lookback_high, lookback_low, reason


# ============================= Position Sizing ==============================

def calculate_obb_position_size(
    equity: float,
    price: float,
    cfg: dict,
    rolling_win_rate: Optional[float] = None,
) -> int:
    """
    Position Sizing für die One-Bar-Breakout-Strategie.

    Da die Original-Strategie KEINEN Stop-Loss hat, kann kein klassisches
    R-basiertes Sizing genutzt werden. Zwei Alternativen:

    1. Fixer Anteil (Default): shares = int(equity × position_size_pct / price)
    2. Kelly (optional): nutzt historische Win-Rate als Schätzung für P(Win)

    Parameter
    ----------
    equity          : aktuelles Eigenkapital
    price           : Kurs für den Entry (nächster Open)
    cfg             : Konfigurationsdict
    rolling_win_rate: falls use_kelly_sizing=True, empirische Win-Rate [0,1]

    Rückgabe
    --------
    Anzahl Aktien (int, mind. 0)
    """
    if price <= 0 or equity <= 0:
        return 0

    if cfg.get("use_kelly_sizing", False) and rolling_win_rate is not None:
        # Kelly-Formel: f* = (p × b - q) / b
        # b = Payoff-Ratio (avg_win / avg_loss), p = Win-Prob, q = 1-p
        b = float(cfg.get("kelly_payoff_ratio", 1.0))
        p = float(np.clip(rolling_win_rate, 0.05, 0.95))
        q = 1.0 - p
        kelly_full = max(0.0, (p * b - q) / b)
        kelly_fraction = float(cfg.get("kelly_fraction", 0.50))
        position_pct = kelly_full * kelly_fraction
        # Cap auf position_size_pct als Sicherheitsnetz
        max_pct = float(cfg.get("position_size_pct", 0.10))
        position_pct = min(position_pct, max_pct)
    else:
        position_pct = float(cfg.get("position_size_pct", 0.10))

    notional = equity * position_pct
    shares = int(notional / price)
    return max(0, shares)


# ============================= Hilfsfunktionen ==============================

def is_market_day_et(dt_obj: datetime) -> bool:
    """Einfache Prüfung auf Wochentag (Mo–Fr). Keine Feiertagsprüfung."""
    return dt_obj.weekday() < 5


def to_et(dt_obj: datetime) -> datetime:
    """Konvertiere datetime nach Eastern Time."""
    if dt_obj.tzinfo is None:
        dt_obj = pytz.utc.localize(dt_obj)
    return dt_obj.astimezone(ET)


def compute_rolling_win_rate(trades: list, lookback: int = 50) -> Optional[float]:
    """
    Berechne Win-Rate der letzten `lookback` abgeschlossenen Trades.

    trades: Liste von Dicts mit "pnl" Key
    Rückgabe: Win-Rate [0,1] oder None wenn zu wenig Daten
    """
    closed = [t for t in trades if t.get("pnl", 0) != 0]
    if len(closed) < lookback:
        return None
    recent = closed[-lookback:]
    wins = sum(1 for t in recent if t["pnl"] > 0)
    return wins / len(recent)


def obb_metrics_summary(cfg: dict) -> str:
    """Gibt eine kompakte Zusammenfassung der aktiven Konfiguration zurück."""
    lines = [
        f"  Lookback:          {cfg.get('lookback_bars', 50)} Bars",
        f"  Shorts:            {'AN' if cfg.get('allow_shorts') else 'AUS'}",
        f"  Position Sizing:   {'Kelly' if cfg.get('use_kelly_sizing') else 'Fixed'} "
        f"({cfg.get('position_size_pct', 0.10) * 100:.0f}%)",
        f"  Symbole:           {len(cfg.get('symbols', []))}",
        f"  Kapital:           {cfg.get('initial_capital', 10000):,.0f} {cfg.get('currency', 'USD')}",
    ]
    return "\n".join(lines)
