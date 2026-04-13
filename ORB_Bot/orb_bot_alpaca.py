#!/usr/bin/env python3
"""
ORB_Bot – Opening Range Breakout Strategy
Alpaca Markets Edition: Daten + Orderausführung via alpaca-py
OpenClaw-kompatibel: läuft als CLI-Skript, Keys aus Umgebungsvariablen

Installation:
    pip install alpaca-py pytz pandas numpy

Umgebungsvariablen (OpenClaw setzt diese automatisch):
    APCA_API_KEY_ID      – Alpaca API Key
    APCA_API_SECRET_KEY  – Alpaca Secret Key
    APCA_PAPER           – "true" für Paper Trading (Standard), "false" für Live
    APCA_DATA_FEED       – "iex" (kostenlos, Standard) oder "sip" (Echtzeit, kostenpflichtig)

OpenClaw-Befehle:
    python orb_bot.py --mode scan        # Signalsuche + Orderausführung
    python orb_bot.py --mode status      # Portfolio-Status (JSON-Ausgabe)
    python orb_bot.py --mode eod         # Alle Positionen schließen
    python orb_bot.py --mode backtest    # Historischen Backtest laufen lassen

Hinweis zu Futures (ES=F, NQ=F etc.):
    Alpaca unterstützt keine Futures. Diese Symbole werden in symbols_watchonly
    geführt – Signale werden generiert, aber keine Orders ausgeführt.
"""

import json
import os
import sys
import argparse
import copy
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import pytz
import time as time_module

from orb_broker_base import BrokerBase

# Shared strategy module – Single Source of Truth
from orb_strategy import (
    ORB_DEFAULT_CONFIG,
    ET,
    to_et,
    to_et_time,
    is_market_hours,
    is_trading_day,
    is_orb_period,
    get_opening_range,
    calculate_atr,
    compute_indicators,
    check_breakout,
    calculate_stop,
    calculate_position_size,
    check_trend_filter,
    check_gap_filter,
    generate_signal as _strategy_generate_signal,
    # MIT Overlay – Single Source of Truth (nicht mehr lokal dupliziert)
    mit_apply_overlay as _mit_apply_overlay,
    mit_group_for_symbol as _mit_group_for_symbol,
)

## für eine lokale Ausführung ohne OpenClaw-Umgebung können die
## .env-Dateien genutzt werden. Das Laden erfolgt in main() nach dem
## Parsen der CLI-Argumente, damit die richtige Datei gewählt wird.
try:
    from dotenv import load_dotenv as _load_dotenv
    _DOTENV_AVAILABLE = True
except ImportError:
    _load_dotenv = None
    _DOTENV_AVAILABLE = False

# ── Alpaca-py (pip install alpaca-py) ──────────────────────────────────────
try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import (
        MarketOrderRequest,
        GetOrdersRequest,
        StopLossRequest,
        TakeProfitRequest,
    )
    from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    try:
        from alpaca.data.timeframe import TimeFrameUnit
    except ImportError:
        TimeFrameUnit = None
    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_AVAILABLE = False
    print("[WARN] alpaca-py fehlt -> pip install alpaca-py", file=sys.stderr)


def _resolve_5m_timeframe():
    """Kompatibel mit mehreren alpaca-py Versionen."""
    if not ALPACA_AVAILABLE:
        return None

    # Ältere Versionen
    if hasattr(TimeFrame, "Minute5"):
        return TimeFrame.Minute5

    # Neuere Versionen
    if TimeFrameUnit is not None:
        try:
            return TimeFrame(5, TimeFrameUnit.Minute)
        except Exception:
            pass

    # Fallback: 1-Minuten TimeFrame als letzte Option
    if hasattr(TimeFrame, "Minute"):
        return TimeFrame.Minute

    raise RuntimeError("Keine kompatible 5m-TimeFrame-Konstante in alpaca-py gefunden.")

# ============================= Konfiguration ================================

TELEGRAM_TOKEN_PATH  = Path.home() / ".secrets" / "telegram.token"
TELEGRAM_CHAT_ID_PATH = Path.home() / ".secrets" / "telegram.chat_id"

# Erbe Defaults aus orb_strategy und erweitere um Live-Bot-spezifische Keys
ORB_CONFIG = copy.deepcopy(ORB_DEFAULT_CONFIG)
ORB_CONFIG.update({
    # IEX = kostenlos aber 15 Min. verzögert; SIP = Echtzeit,  kostenpflichtig
    # Für Live-Trading unbedingt SIP verwenden!
    "alpaca_data_feed": "iex",

    # Lokale Dateien
    "data_dir":         Path(__file__).parent / "orb_trading_data",
    "portfolio_file":   Path(__file__).parent / "orb_trading_data" / "portfolio.json",
    "memory_file":      Path(__file__).parent / "orb_trading_data" / "memory.md",
    "daily_stats_file": Path(__file__).parent / "orb_trading_data" / "daily_stats.json",
    "daily_event_log_dir": Path(__file__).parent / "orb_trading_data" / "daily_logs",
    
    # Sicherheitsparameter für Live-Trading
    "buying_power_safety_factor": 0.95,  # Cap Qty wenn Buying Power < 95% erforderlich

    # Stale-Data-Blocker:
    # IEX liefert Daten mit ~15 Min. Verzögerung. Threshold auf 20 Min. gesetzt,
    # damit normale IEX-Latenz akzeptiert wird, aber echte Staleness (Verbindungsprobleme,
    # API-Ausfall, >20 Min. alt) den Trade blockt.
    # Für SIP-Feed empfehlenswert: max_bar_delay_minutes auf 5-6 Min. senken.
    "max_bar_delay_minutes": 20,
    # True  → Symbol wird übersprungen wenn Daten zu alt (empfohlen für Live-Trading)
    # False → nur Warnung, Trade geht trotzdem durch (altes Verhalten)
    "block_trades_on_stale_data": True,

    # EOD-Close: Minuten vor Handelsschluss (16:00 ET) alle Positionen schließen
    "eod_close_minutes_before": 33,

    # Buy-Cutoff: ab dieser Uhrzeit (ET) werden keine neuen BUY/SHORT-Signale
    # mehr ausgeführt (verhindert späte Intraday-Entries kurz vor Marktschluss)
    "buy_cutoff_time_et": time(15, 0),

    # Kleine Debug-Ausgabe im Scanmodus (z.B. für Volume-/Signal-Validierung)
    "debug_scan_enabled": True,
    "debug_scan_symbols": ["SPY", "QQQ"], ##Verwendung von [] dann wird jedes Symbol ins Debug geschrieben

    # Interactive Brokers Verbindungsparameter
    "ibkr_host":      "192.168.188.93",
    "ibkr_port":      4002,        # 4002 = Gateway Paper, 4001 = Gateway Live
    "ibkr_client_id": 1,
    "ibkr_bot_id":    "ORB",
    "ibkr_paper":     True,
})

ORB_CONFIG["data_dir"].mkdir(exist_ok=True)
ORB_CONFIG["daily_event_log_dir"].mkdir(exist_ok=True)


# ============================= Telegram =====================================

def send_telegram(message: str) -> None:
    try:
        token   = TELEGRAM_TOKEN_PATH.read_text().strip()
        chat_id = TELEGRAM_CHAT_ID_PATH.read_text().strip()
        import urllib.request, urllib.parse
        url  = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": message}).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10).read()
    except Exception as e:
        print(f"[Telegram] {e}")


def _build_client_order_id(symbol: str, side: str, overlay_reason: str = "",
                           prefix: str = "") -> str:
    """
    Erzeugt eine lesbare client_order_id ≤ 128 Zeichen für Alpaca.

    Format ohne MIT-Overlay:  "[PREFIX|]ORB|SPY|BUY|2026-04-08 14:30 ET"
    Format mit MIT-Overlay:   "[PREFIX|]ORB|SPY|BUY|2026-04-08 14:30 ET|P=0.82|EV=+0.15R|Kelly=0.75x"

    Bei aktivem MIT-Overlay enthält die ID die komplette Signalbewertung
    (Win-Probability, Expected-Value in R, Kelly-Faktor) aus apply_mit_overlay().
    side: "BUY" oder "SHORT"
    Ist der String zu lang, wird von rechts gekürzt.
    """
    side_str = "BUY" if side.upper() == "BUY" else "SHORT"
    now_et = datetime.now(pytz.UTC).astimezone(ET)
    ts = now_et.strftime("%Y-%m-%d %H:%M ET")
    base = f"ORB|{symbol}|{side_str}|{ts}"
    if prefix:
        base = f"{prefix}|{base}"
    if overlay_reason:
        # overlay_reason Beispiel: "MIT Overlay: P=0.82 EV=+0.15R Kelly=0.75x"
        # Präfix entfernen und Leerzeichen als Trennzeichen durch | ersetzen
        detail = overlay_reason.replace("MIT Overlay: ", "").replace(" ", "|")
        full = f"{base}|{detail}"
    else:
        full = base
    return full[:128]


# ============================= Helper =======================================
# Timezone-sichere Helper, Indikatoren, ORB-Levels etc.
# werden aus orb_strategy importiert (Single Source of Truth).
# Siehe Imports oben.


# ============================= AlpacaClient =================================

class AlpacaClient(BrokerBase):
    """
    Zentrale Klasse für alle Alpaca-Interaktionen.
    Trennt sauber zwischen Datenabruf (StockHistoricalDataClient)
    und Orderausführung (TradingClient).
    """

    def __init__(self, api_key: str, secret_key: str,
                 paper: bool = True, data_feed: str = "iex"):
        if not ALPACA_AVAILABLE:
            raise RuntimeError("alpaca-py fehlt – pip install alpaca-py")
        self.paper     = paper
        self.data_feed = data_feed
        self.timeframe_5m = _resolve_5m_timeframe()
        self.trading   = TradingClient(api_key=api_key, secret_key=secret_key, paper=paper)
        self.data      = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)
        mode = "PAPER" if paper else "LIVE"
        print(f"[Alpaca] Verbunden  Modus={mode}  Feed={data_feed}")

    # ── Marktdaten ──────────────────────────────────────────────────────────

    def fetch_bars(self, symbol: str, days: int = 2) -> pd.DataFrame:
        """5m-Bars für ein Symbol mit business-day-aware Lookback."""
        try:
            now_utc = datetime.now(pytz.UTC)
            now_et = now_utc.astimezone(ET)
            start_et = (pd.Timestamp(now_et).normalize() - pd.tseries.offsets.BDay(days)).to_pydatetime()
            if start_et.tzinfo is None:
                start_et = ET.localize(start_et)

            req = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=self.timeframe_5m,
                start=start_et.astimezone(pytz.UTC),
                end=now_utc,
                adjustment="raw",
                feed=self.data_feed,
            )
            bars = self.data.get_stock_bars(req)
            if bars.df.empty:
                return pd.DataFrame()
            df = bars.df.loc[symbol].copy() if isinstance(bars.df.index, pd.MultiIndex) \
                 else bars.df.copy()
            return self._rename(df)
        except Exception as e:
            print(f"[Alpaca] Datenfehler {symbol}: {e}")
            return pd.DataFrame()

    def check_bar_freshness(self, df: pd.DataFrame,
                             max_delay_minutes: int = 20) -> bool:
        """
        Prüft ob der letzte Bar aktuell genug ist.

        Rückgabe:
            True  → Daten frisch genug, Trade kann fortgesetzt werden.
            False → Daten zu alt (> max_delay_minutes), Trade sollte geblockt werden.

        Threshold-Empfehlung:
            IEX-Feed:  20 Min. (akzeptiert normale ~15 Min. IEX-Verzögerung,
                       blockt bei echten Verbindungsproblemen / API-Ausfall)
            SIP-Feed:   5-6 Min. (Echtzeit-Feed, strengere Freshness-Anforderung)

        Der Rückgabewert MUSS vom Aufrufer ausgewertet werden!
        Wird er ignoriert, handelt der Bot still auf stalen Daten.
        Siehe: run_orb_scan() → block_trades_on_stale_data Config-Key.
        """
        if df.empty:
            return False
        last_ts = df.index[-1]
        if hasattr(last_ts, 'to_pydatetime'):
            last_ts = last_ts.to_pydatetime()
        if last_ts.tzinfo is None:
            last_ts = pytz.utc.localize(last_ts)
        now = datetime.now(pytz.UTC)
        delay = (now - last_ts).total_seconds() / 60
        if delay > max_delay_minutes:
            print(f"[Alpaca] WARN Daten {delay:.0f} Min. alt "
                f"(max={max_delay_minutes}). IEX ist 15 Min. verzoegert - "
                  f"für Live-Trading SIP-Feed empfohlen!")
            return False
        return True

    def fetch_bars_bulk(self, symbols: List[str],
                        start: str, end: str) -> Dict[str, pd.DataFrame]:
        """
        Historische 5m-Bars für mehrere Symbole.

        Robust gegen serverseitige Pagination/Limits: lädt pro Symbol in
        Zeitfenster-Chunks und führt die Daten anschließend zusammen.
        """
        result: Dict[str, pd.DataFrame] = {}
        try:
            start_utc = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
            # Inklusive Enddatum bis Tagesende, nicht 00:00.
            end_utc = (
                datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
                + timedelta(days=1)
                - timedelta(microseconds=1)
            )
        except Exception as e:
            print(f"[Alpaca] Bulk-Fehler (Datum): {e}")
            return result

        # Robust gegen API-Limits/Pagination: je Symbol in Zeitfenstern laden.
        chunk_days = 30
        max_retries = 5

        for sym in symbols:
            sym_chunks: List[pd.DataFrame] = []
            chunk_count = 0
            failed_chunks = 0
            cur_start = start_utc

            while cur_start <= end_utc:
                cur_end = min(
                    cur_start + timedelta(days=chunk_days) - timedelta(microseconds=1),
                    end_utc,
                )

                chunk_ok = False
                last_err = None

                for attempt in range(1, max_retries + 1):
                    try:
                        req = StockBarsRequest(
                            symbol_or_symbols=sym,
                            timeframe=self.timeframe_5m,
                            start=cur_start,
                            end=cur_end,
                            adjustment="raw",
                            feed=self.data_feed,
                        )
                        bars = self.data.get_stock_bars(req)
                        if not bars.df.empty:
                            if isinstance(bars.df.index, pd.MultiIndex):
                                df_raw = bars.df.loc[sym].copy()
                            else:
                                df_raw = bars.df.copy()
                            sym_chunks.append(self._rename(df_raw))
                        chunk_ok = True
                        break
                    except Exception as e:
                        last_err = e
                        # Exponential Backoff gegen 429/temporäre Netzwerkfehler.
                        wait_s = min(2 ** (attempt - 1), 16)
                        print(
                            f"  [WARN] {sym} Chunk {cur_start.date()}->{cur_end.date()} "
                            f"fehlgeschlagen (Versuch {attempt}/{max_retries}): {e}"
                        )
                        if attempt < max_retries:
                            time_module.sleep(wait_s)

                if not chunk_ok:
                    failed_chunks += 1
                    print(
                        f"  [WARN] {sym} Chunk dauerhaft übersprungen "
                        f"({cur_start.date()}->{cur_end.date()}): {last_err}"
                    )

                chunk_count += 1
                cur_start = cur_end + timedelta(microseconds=1)

            if sym_chunks:
                merged = pd.concat(sym_chunks, axis=0)
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                result[sym] = merged
                warn_suffix = f", {failed_chunks} Fehl-Chunks" if failed_chunks else ""
                print(f"  [OK] {sym}: {len(merged)} Bars ({chunk_count} Chunks{warn_suffix})")
            else:
                print(f"  [NO DATA] {sym}: keine Daten")

        return result

    @staticmethod
    def _rename(df: pd.DataFrame) -> pd.DataFrame:
        """Alpaca-Spaltennamen → OHLCV-Standard."""
        return df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume",
        })[["Open", "High", "Low", "Close", "Volume"]]

    # ── Account & Positionen ────────────────────────────────────────────────

    def get_equity(self) -> float:
        try:
            return float(self.trading.get_account().equity)
        except Exception as e:
            print(f"[Alpaca] Equity-Fehler: {e}")
            return 0.0

    def get_cash(self) -> float:
        try:
            return float(self.trading.get_account().cash)
        except Exception:
            return 0.0

    def get_buying_power(self) -> float:
        try:
            return float(self.trading.get_account().buying_power)
        except Exception:
            return 0.0

    def sync_positions(self) -> Dict[str, dict]:
        """
        Aktuelle Positionen direkt von Alpaca holen.
        Gibt Wahrheit über offene Positionen – verhindert Doppel-Entries.
        """
        try:
            positions = self.trading.get_all_positions()
            return {
                p.symbol: {
                    "symbol":          p.symbol,
                    "qty":             float(p.qty),
                    "side":            p.side.value,        # "long" | "short"
                    "entry":           float(p.avg_entry_price),
                    "current_price":   float(p.current_price),
                    "unrealized_pnl":  float(p.unrealized_pl),
                    "market_value":    float(p.market_value),
                }
                for p in positions
            }
        except Exception as e:
            print(f"[Alpaca] Positions-Fehler: {e}")
            return {}

    def is_shortable(self, symbol: str) -> bool:
        """Prüft ob Alpaca das Symbol für Shorts freigibt."""
        try:
            asset = self.trading.get_asset(symbol)
            return bool(asset.shortable) and bool(asset.easy_to_borrow)
        except Exception:
            return False

    def get_open_orders(self) -> List[dict]:
        try:
            req    = GetOrdersRequest(status="open", limit=50)
            orders = self.trading.get_orders(req)
            return [{"id": str(o.id), "symbol": o.symbol,
                     "side": o.side.value, "qty": float(o.qty),
                     "status": o.status.value} for o in orders]
        except Exception as e:
            print(f"[Alpaca] Orders-Fehler: {e}")
            return []

    # ── Orderausführung ─────────────────────────────────────────────────────

    def place_long_bracket(self, symbol: str, qty: int,
                            stop_loss: float, take_profit: float,
                            client_order_id: str = None) -> dict:
        """
        Long-Entry als Bracket-Order.
        Alpaca verwaltet Stop-Loss und Take-Profit serverseitig –
        _manage_position() im Bot ist für Live-Trades nicht nötig.
        client_order_id: optionaler kompakter ID-String (max 64 Zeichen) mit Signalinfos.
        Gibt dict mit {"ok": True, ...} oder {"ok": False, "error": msg} zurück.
        """
        try:
            req_kwargs = dict(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                order_class=OrderClass.BRACKET,
                stop_loss=StopLossRequest(stop_price=round(stop_loss,    2)),
                take_profit=TakeProfitRequest(limit_price=round(take_profit, 2)),
            )
            if client_order_id:
                req_kwargs["client_order_id"] = client_order_id[:128]
            order = MarketOrderRequest(**req_kwargs)
            r = self.trading.submit_order(order)
            print(f"[Alpaca] LONG  {symbol} {qty} Aktien | SL {stop_loss:.2f} | TP {take_profit:.2f} -> {r.status.value}")
            return {"ok": True, "id": str(r.id), "client_id": client_order_id or "",
                    "symbol": symbol, "qty": qty, "side": "long",
                    "stop_loss": stop_loss, "take_profit": take_profit, "status": r.status.value}
        except Exception as e:
            print(f"[Alpaca] Long-Order {symbol} fehlgeschlagen: {e}")
            return {"ok": False, "error": str(e)}

    def place_short_bracket(self, symbol: str, qty: int,
                             stop_loss: float, take_profit: float,
                             client_order_id: str = None) -> dict:
        """
        Short-Entry als Bracket-Order.
        stop_loss liegt ÜBER dem Entry, take_profit DARUNTER.
        Erfordert Margin-Konto + Shortability-Check.
        client_order_id: optionaler kompakter ID-String (max 64 Zeichen) mit Signalinfos.
        Gibt dict mit {"ok": True, ...} oder {"ok": False, "error": msg} zurück.
        """
        if not self.is_shortable(symbol):
            print(f"[Alpaca] {symbol} nicht shortbar - Order abgebrochen")
            return {"ok": False, "error": f"{symbol} nicht shortbar"}
        try:
            req_kwargs = dict(
                symbol=symbol,
                qty=qty,
                side=OrderSide.SELL,          # Sell-to-Open = Short
                time_in_force=TimeInForce.DAY,
                order_class=OrderClass.BRACKET,
                stop_loss=StopLossRequest(stop_price=round(stop_loss,    2)),
                take_profit=TakeProfitRequest(limit_price=round(take_profit, 2)),
            )
            if client_order_id:
                req_kwargs["client_order_id"] = client_order_id[:128]
            order = MarketOrderRequest(**req_kwargs)
            r = self.trading.submit_order(order)
            print(f"[Alpaca] SHORT {symbol} {qty} Aktien | SL {stop_loss:.2f} | TP {take_profit:.2f} -> {r.status.value}")
            return {"ok": True, "id": str(r.id), "client_id": client_order_id or "",
                    "symbol": symbol, "qty": qty, "side": "short",
                    "stop_loss": stop_loss, "take_profit": take_profit, "status": r.status.value}
        except Exception as e:
            print(f"[Alpaca] Short-Order {symbol} fehlgeschlagen: {e}")
            return {"ok": False, "error": str(e)}

    def cancel_all_orders(self) -> None:
        try:
            self.trading.cancel_orders()
            print("[Alpaca] Alle offenen Orders storniert")
        except Exception as e:
            print(f"[Alpaca] Stornierungsfehler: {e}")

    def close_all_positions(self, verify: bool = True) -> dict:
        """
        EOD: alle Positionen schließen + offene Orders stornieren.
        Mit verify=True: wartet kurz und prüft danach ob alle wirklich geschlossen.
        Falls nicht, wird jede verbleibende Position einzeln geschlossen (Fallback).
        Gibt dict zurück: {"attempted": [syms], "remaining": [syms], "ok": bool}
        """
        result = {"attempted": [], "remaining": [], "ok": False}
        try:
            current = self.sync_positions()
            result["attempted"] = list(current.keys())

            if not current:
                print("[Alpaca] EOD: keine offenen Positionen")
                result["ok"] = True
                return result

            self.trading.close_all_positions(cancel_orders=True)
            print(f"[Alpaca] EOD-Close: {len(result['attempted'])} Position(en) "
                  f"schliessen -> {', '.join(result['attempted'])}")

            if verify:
                time_module.sleep(3)
                remaining = self.sync_positions()
                result["remaining"] = list(remaining.keys())

                # Fallback: verbleibende Positionen einzeln schließen
                if remaining:
                    print(f"[Alpaca] WARN: {len(remaining)} Position(en) nach Bulk-Close "
                          f"noch offen – Einzelschluss: {', '.join(remaining.keys())}")
                    for sym in list(remaining.keys()):
                        try:
                            self.trading.close_position(sym)
                            print(f"[Alpaca] {sym} einzeln geschlossen")
                        except Exception as e2:
                            print(f"[Alpaca] {sym} Einzelschluss fehlgeschlagen: {e2}")

                    time_module.sleep(2)
                    still_open = self.sync_positions()
                    result["remaining"] = list(still_open.keys())

            result["ok"] = len(result["remaining"]) == 0
            return result
        except Exception as e:
            print(f"[Alpaca] EOD-Close Fehler: {e}")
            result["remaining"] = result.get("attempted", [])
            return result


# ============================= Portfolio-Ledger =============================
# Im Live-Betrieb dient ORBPortfolio nur noch als lokales Log + Tagesstatistik.
# Die eigentliche Positionsverwaltung übernimmt Alpaca (Bracket-Orders).
# Für den Backtester bleibt die vollständige virtuelle Execution erhalten.

class ORBPortfolio:
    def __init__(self, config: dict, persist_files: bool = True):
        self.cfg = config
        self.persist_files = persist_files
        self.cfg["data_dir"].mkdir(exist_ok=True)
        self.data        = self._load()
        self.daily_stats = self._load_daily_stats()

    def _load(self) -> dict:
        if not self.persist_files:
            return {
                "cash": self.cfg.get("initial_capital", 0.0),
                "initial_capital": self.cfg.get("initial_capital", 0.0),
                "positions": {}, "short_positions": {},
                "trades": [], "equity_curve": [], "daily_pnl": {},
                "last_updated": None,
            }
        if self.cfg["portfolio_file"].exists():
            with open(self.cfg["portfolio_file"]) as f:
                return json.load(f)
        return {
            "cash": self.cfg.get("initial_capital", 0.0),
            "initial_capital": self.cfg.get("initial_capital", 0.0),
            "positions": {}, "short_positions": {},
            "trades": [], "equity_curve": [], "daily_pnl": {},
            "last_updated": None,
        }

    def _load_daily_stats(self) -> dict:
        if not self.persist_files:
            return {"trades_today": 0, "pnl_today": 0.0, "wins_today": 0,
                    "losses_today": 0, "win_rate_today": 0.0, "last_reset_date": None}
        if self.cfg["daily_stats_file"].exists():
            with open(self.cfg["daily_stats_file"]) as f:
                return json.load(f)
        return {"trades_today": 0, "pnl_today": 0.0, "wins_today": 0,
                "losses_today": 0, "win_rate_today": 0.0, "last_reset_date": None}

    def save(self):
        if not self.persist_files:
            return
        self.data["last_updated"] = datetime.now(pytz.UTC).isoformat()
        with open(self.cfg["portfolio_file"], "w") as f:
            json.dump(self.data, f, indent=2)

    def _save_daily_stats(self):
        if not self.persist_files:
            return
        with open(self.cfg["daily_stats_file"], "w") as f:
            json.dump(self.daily_stats, f, indent=2)

    def reset_daily_stats_if_needed(self):
        today = datetime.now(pytz.UTC).date().isoformat()
        if self.daily_stats.get("last_reset_date") != today:
            self.daily_stats = {"trades_today": 0, "pnl_today": 0.0,
                                "wins_today": 0, "losses_today": 0,
                                "win_rate_today": 0.0, "last_reset_date": today}
            self._save_daily_stats()

    def can_trade_today(self) -> bool:
        self.reset_daily_stats_if_needed()
        return self.daily_stats["trades_today"] < self.cfg["max_daily_trades"]

    def log_order(self, symbol: str, action: str, qty: int,
                  price: float, stop: float, target: float,
                  alpaca_order_id: str = "", reason: str = ""):
        """
        Speichert eine von Alpaca ausgeführte Order lokal.
        Kein virtuelles Cash-Update – Alpaca führt die Bücher.
        """
        record = {
            "time":     datetime.now(pytz.UTC).isoformat(),
            "symbol":   symbol,
            "action":   action,
            "qty":      qty,
            "price":    price,
            "stop":     stop,
            "target":   target,
            "order_id": alpaca_order_id,
            "reason":   reason,
            "strategy": "ORB",
            "pnl":      0.0,  # wird bei Schließung aktualisiert (optional)
        }
        self.data["trades"].append(record)
        self.daily_stats["trades_today"] += 1
        self._append_to_memory(
            f"**{datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M')}** "
            f"{symbol} {action} {qty} @ {price:.2f} | SL {stop:.2f} | TP {target:.2f} "
            f"| OrderID {alpaca_order_id} ({reason})"
        )
        self._save_daily_stats()
        self.save()

    # ── Virtuelles Buy/Sell für Backtester ──────────────────────────────────

    def has_pos(self, sym: str) -> bool:
        return sym in self.data["positions"]

    def get_pos(self, sym: str) -> dict:
        return self.data["positions"].get(sym)

    def calculate_position_size_virtual(self, entry: float, stop: float, equity: float) -> int:
        """Position sizing für virtuellen Backtester (delegiert an orb_strategy)."""
        return calculate_position_size(
            entry, stop, equity,
            self.cfg.get("risk_per_trade", 0.005),
            self.cfg.get("max_equity_at_risk", 0.02), #vorher 0.05),
            self.cfg.get("max_position_value_pct", 0.25),
        )

    def buy(self, sym: str, price: float, shares: int, stop: float, reason: str) -> dict:
        if shares <= 0 or price * shares > self.data["cash"]:
            return {"ok": False}
        if not self.can_trade_today():
            return {"ok": False, "msg": "Daily limit"}
        self.data["cash"] -= price * shares
        self.data["positions"][sym] = {
            "symbol": sym, "shares": shares, "entry": price,
            "stop_loss": stop, "price": price, "highest": price,
            "trail_stop": None, "reason": reason,
            "entry_time": datetime.now().isoformat(),
        }
        self._log_bt_trade(sym, "BUY", shares, price, 0.0, reason)
        self.daily_stats["trades_today"] += 1
        self._save_daily_stats()
        self.save()
        return {"ok": True}

    def sell(self, sym: str, price: float, shares: int, reason: str) -> dict:
        pos = self.data["positions"].get(sym)
        if not pos:
            return {"ok": False}
        pnl = (price - pos["entry"]) * shares
        self.data["cash"] += price * shares
        del self.data["positions"][sym]
        self._log_bt_trade(sym, "SELL", shares, price, pnl, reason)
        self._update_bt_stats(pnl)
        self.save()
        return {"ok": True, "pnl": pnl}

    def _log_bt_trade(self, sym, action, shares, price, pnl, reason):
        self.data["trades"].append({
            "time": datetime.now(pytz.UTC).isoformat(), "symbol": sym,
            "action": action, "shares": shares, "price": price,
            "pnl": pnl, "reason": reason, "strategy": "ORB",
        })

    def _update_bt_stats(self, pnl: float):
        self.daily_stats["pnl_today"] += pnl
        if pnl > 0:
            self.daily_stats["wins_today"] += 1
        elif pnl < 0:
            self.daily_stats["losses_today"] += 1
        total = self.daily_stats["wins_today"] + self.daily_stats["losses_today"]
        self.daily_stats["win_rate_today"] = (
            self.daily_stats["wins_today"] / max(total, 1) * 100
        ) if total > 0 else 0.0
        self._save_daily_stats()

    def _append_to_memory(self, content: str):
        if not self.persist_files:
            return
        mp = self.cfg["memory_file"]
        if not mp.exists():
            mp.write_text("# ORB_Bot Memory Log\n\n")
        with open(mp, "a") as f:
            f.write(f"{content}\n\n")

    def equity(self, price_dict: dict = None) -> float:
        price_dict = price_dict or {}
        return self.data["cash"] + sum(
            p["shares"] * price_dict.get(s, 0)
            for s, p in self.data["positions"].items()
        )


# ============================= ORB-Strategie ================================

class ORBStrategy:
    """Thin Wrapper – delegiert an orb_strategy.py (Single Source of Truth)."""
    def __init__(self, config: dict):
        self.cfg = config

    def calculate_orb_levels(self, df: pd.DataFrame) -> Tuple[float, float, float, dict]:
        orb_high, orb_low, orb_range = get_opening_range(df)
        vol     = df["Volume"].iloc[-1] if len(df) > 0 else 0
        vol_ma  = 1.0
        if "Volume_MA" in df.columns:
            v = df["Volume_MA"].iloc[-1]
            vol_ma = float(v) if not np.isnan(v) else 1.0
        vol_r   = vol / vol_ma if vol_ma > 0 else 0
        ctx = {
            "volume_ratio":     vol_r,
            "volume_confirmed": vol_r >= self.cfg.get("volume_multiplier", 1.3),
            "orb_range_pct":    (orb_range / orb_low * 100) if orb_low > 0 else 0,
        }
        return orb_high, orb_low, orb_range, ctx

    def generate_signal(self, df: pd.DataFrame,
                        spy_df: pd.DataFrame = None,
                        ) -> Tuple[str, float, str, dict]:
        """Delegiert an orb_strategy.generate_signal()."""
        return _strategy_generate_signal(df, self.cfg, spy_df=spy_df)

    def mit_group_for_symbol(self, symbol: str) -> str:
        """Delegiert an orb_strategy.mit_group_for_symbol (SSoT)."""
        return _mit_group_for_symbol(symbol, self.cfg)

    def apply_mit_overlay(
        self,
        signal: str,
        strength: float,
        ctx: dict,
        df: pd.DataFrame,
        current_drawdown: float = 0.0,
        vix_spot: Optional[float] = None,
        vix_3m: Optional[float] = None,
    ) -> Tuple[bool, float, str]:
        """Delegiert an orb_strategy.mit_apply_overlay (SSoT) mit DD + VIX."""
        return _mit_apply_overlay(signal, strength, ctx, df, self.cfg, current_drawdown, vix_spot, vix_3m)


# ============================= ORB_Bot (Live) ================================

class ORB_Bot:
    """
    Live-Bot: Datenabruf + Orderausführung via Alpaca.
    Positionsverwaltung (Stop / Target) übernimmt Alpaca serverseitig
    über Bracket-Orders → _manage_position() entfällt im Live-Betrieb.
    """

    def __init__(self, config: dict = None,
                 alpaca: "BrokerBase" = None, broker: "BrokerBase" = None):
        self.cfg       = config or ORB_CONFIG
        self.broker    = broker or alpaca
        self.alpaca    = self.broker   # Backward-Compat-Alias – NICHT entfernen!
        self.portfolio = ORBPortfolio(self.cfg)
        self.strategy  = ORBStrategy(self.cfg)
        self.reports_dir = self.cfg["data_dir"] / "reports"
        self.reports_dir.mkdir(exist_ok=True)

        # Event-Log-Verzeichnis sicherstellen
        self.event_log_dir = self.cfg["daily_event_log_dir"]
        self.event_log_dir.mkdir(exist_ok=True)
        self._eod_done_date: Optional[str] = None  # Verhindert mehrfaches EOD-Close pro Tag

        mode = "PAPER" if (self.alpaca and self.alpaca.paper) else "LIVE" if self.alpaca else "KEIN BROKER"
        print(f"ORB_Bot  Modus={mode}  Symbole={len(self.cfg['symbols'])}"
              f"  Shorts={'an' if self.cfg.get('allow_shorts') else 'aus'}")

    # ── Event-Logging und Benachrichtigungen ──────────────────────────────────

    def _daily_log_path(self) -> Path:
        """Returns die Tageslog-Datei für heute (YYYYMMDD-Format)."""
        today = datetime.now(pytz.UTC).strftime("%Y%m%d")
        return self.event_log_dir / f"events_{today}.log"

    def _log_event(self, level: str, event: str, symbol: str = "", details: dict = None) -> None:
        """
        Schreibt strukturierte Events in Daily Log.
        level: "INFO", "WARN", "ERROR", "ORDER_FILLED", "ORDER_REJECTED", "QTY_CAP"
        """
        if details is None:
            details = {}
        try:
            timestamp = datetime.now(pytz.UTC).isoformat()
            log_line = f"[{timestamp}] [{level}] {event}"
            if symbol:
                log_line += f" | {symbol}"
            if details:
                log_line += f" | {json.dumps(details)}"
            log_line += "\n"
            
            log_path = self._daily_log_path()
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(log_line)
        except Exception as e:
            print(f"[WARN] Event-Log Fehler: {e}")

    def _notify_and_log(self, level: str, event: str, symbol: str = "",
                        details: dict = None, telegram_msg: str = "") -> None:
        """
        Kombiniert Log-Eintrag + Telegram-Benachrichtigung.
        """
        self._log_event(level, event, symbol, details)
        if telegram_msg:
            send_telegram(telegram_msg)

    def _classify_order_error(self, error_text: str) -> str:
        """
        Kategorisiert Alpaca-Order-Fehler in erkannte Probleme.
        Gibt Kategorie zurück: "INSUFFICIENT_BP", "PDT", "NOT_SHORTABLE", "UNKNOWN"
        """
        error_lower = error_text.lower()
        if "buying power" in error_lower or "40310" in error_text:
            return "INSUFFICIENT_BP"
        if "pattern day" in error_lower or "pdt" in error_lower:
            return "PDT"
        if "shortable" in error_lower or "short" in error_lower:
            return "NOT_SHORTABLE"
        return "UNKNOWN"

    def _cap_qty_by_buying_power(self, symbol: str, side: str,
                                  qty: int, price: float) -> Tuple[int, dict]:
        """
        Prüft Buying Power und reduziert Qty bei Bedarf.
        Returns (adjusted_qty, detail_dict)
        detail_dict enthält: {"capped": bool, "original_qty": int, "adjusted_qty": int,
                              "bp_available": float, "required": float, ...}
        """
        detail = {
            "original_qty": qty,
            "capped": False,
            "adjusted_qty": qty,
        }
        
        if not self.alpaca:
            return qty, detail
        
        try:
            bp_available = self.alpaca.get_buying_power()
            # Alpaca Requote: Long kauft = qty * price wird belastet (approx)
            # Short verkauft = nur ~33% Margin für non-regulated accounts
            multiplier = 1.0 if side == "long" else 0.33  # Short benötigt Margin
            required_bp = qty * price * multiplier
            safety_factor = self.cfg.get("buying_power_safety_factor", 0.95)
            available_with_safety = bp_available * safety_factor
            
            detail["bp_available"] = bp_available
            detail["required_bp"] = required_bp
            detail["safety_factor"] = safety_factor
            detail["available_with_safety"] = available_with_safety
            
            if required_bp > available_with_safety:
                # Reduziere Qty proportional
                new_qty = int((available_with_safety / (price * multiplier)))
                if new_qty < 1:
                    new_qty = 0
                detail["capped"] = True
                detail["adjusted_qty"] = new_qty
                if new_qty < qty:
                    print(f"[WARN] {symbol} Qty gekürzt {qty} -> {new_qty} "
                          f"(BP: {bp_available:.2f} vs {required_bp:.2f} erforderlich)")
                return new_qty, detail
        except Exception as e:
            print(f"[WARN] Buying-Power-Check {symbol}: {e}")
        
        return qty, detail

    def _mit_group_blocked(
        self,
        symbol: str,
        open_positions: Dict[str, dict],
        reserved_groups: set,
    ) -> Tuple[bool, str]:
        if not self.cfg.get("use_mit_probabilistic_overlay", False):
            return False, ""
        if not self.cfg.get("use_mit_independence_guard", True):
            return False, ""

        group = self.strategy.mit_group_for_symbol(symbol)
        if not group:
            return False, ""

        for open_sym in open_positions.keys():
            if self.strategy.mit_group_for_symbol(open_sym) == group:
                return True, f"MIT Independence: Gruppe {group} bereits offen"
        if group in reserved_groups:
            return True, f"MIT Independence: Gruppe {group} heute bereits genutzt"
        return False, ""

    def _perform_eod_close(self) -> dict:
        """
        Zuverlässiger EOD-Close mit Verifikation, Fallback und Benachrichtigung.
        Wird pro Tag nur einmal ausgeführt – _eod_done_date verhindert Wiederholung
        beim nächsten Scan-Aufruf im gleichen EOD-Fenster.
        """
        today = datetime.now(pytz.UTC).strftime("%Y-%m-%d")
        if self._eod_done_date == today:
            return {"skipped": True}

        self._eod_done_date = today
        print("  [EOD] Starte EOD-Close...")

        if not self.alpaca:
            return {"ok": False, "error": "Kein Alpaca-Client"}

        result = self.alpaca.close_all_positions(verify=True)
        attempted = result.get("attempted", [])
        remaining = result.get("remaining", [])

        if not attempted:
            msg = "ORB_Bot EOD: keine offenen Positionen"
            self._notify_and_log("INFO", "EOD-Close: keine Positionen",
                                 details=result, telegram_msg=msg)
        elif result.get("ok"):
            syms = ", ".join(attempted)
            msg = f"ORB_Bot EOD: {len(attempted)} Position(en) geschlossen – {syms}"
            self._notify_and_log("INFO", "EOD-Close erfolgreich",
                                 details=result, telegram_msg=msg)
        else:
            still = ", ".join(remaining)
            msg = (f"ORB_Bot EOD WARNUNG: {len(remaining)} Position(en) "
                   f"noch offen! – {still}")
            self._notify_and_log("WARN", "EOD-Close: Positionen verbleiben",
                                 details=result, telegram_msg=msg)

        return result

    # ── Haupt-Scan ───────────────────────────────────────────────────────────

    def run_orb_scan(self) -> dict:
        now   = datetime.now(pytz.UTC)  # Fix #8: timezone-aware
        today = now.strftime("%Y-%m-%d")
        print(f"\n=== ORB Scan – {today} ===")

        # EOD-Close bereits durchgeführt → keine neuen Trades mehr heute
        if self._eod_done_date == today:
            print("  EOD-Close für heute bereits durchgeführt – keine neuen Trades möglich.")
            return self._empty_result(today)

        if not is_trading_day(now):
            print("  Wochenende – kein Scan.")
            return self._empty_result(today)

        if not is_market_hours(now):
            et  = pytz.timezone("America/New_York")
            t   = now.astimezone(et).strftime("%H:%M ET")
            print(f"  Außerhalb Handelszeiten ({t}) – übersprungen.")
            return self._empty_result(today)

        if self.cfg.get("avoid_fridays") and now.weekday() == 4:
            print("  Freitag-Filter aktiv – kein Scan.")
            return self._empty_result(today)
        if self.cfg.get("avoid_mondays") and now.weekday() == 0:
            print("  Montag-Filter aktiv – kein Scan.")
            return self._empty_result(today)

        # Aktuelle Alpaca-Positionen holen (verhindert Doppel-Entries)
        open_positions = self.alpaca.sync_positions() if self.alpaca else {}
        equity         = self.alpaca.get_equity()     if self.alpaca else 0.0
        signals        = []
        reserved_mit_groups = set()

        # Buy-Cutoff: nach dieser Uhrzeit werden keine neuen Entries mehr ausgeführt
        buy_cutoff = self.cfg.get("buy_cutoff_time_et", time(15, 0))
        current_et_time = now.astimezone(ET).time()
        no_more_buys = current_et_time >= buy_cutoff
        if no_more_buys:
            print(f"  Buy-Cutoff ({buy_cutoff.strftime('%H:%M')} ET) erreicht – keine neuen Entries.")

        # Fix #5: SPY-Daten für Trendfilter vorladen
        spy_df = None
        if self.alpaca and self.cfg.get("use_trend_filter", True):
            spy_df = self.alpaca.fetch_bars("SPY", days=5)
            if not spy_df.empty:
                spy_df = compute_indicators(spy_df)

        max_delay = int(self.cfg.get("max_bar_delay_minutes", 20))
        max_concurrent = int(self.cfg.get("max_concurrent_positions", 3))
        new_entries_this_scan = 0  # Zählt neue Entries in diesem Scan-Durchlauf
        block_on_stale = bool(self.cfg.get("block_trades_on_stale_data", True))

        # Fix: VIX/VIX3M und Drawdown für MIT-Overlay berechnen (Backtest-Parität)
        _vix_spot = None
        _vix_3m = None
        _current_dd = 0.0
        if self.cfg.get("use_mit_probabilistic_overlay", False):
            try:
                import yfinance as yf
                _vix_data = yf.Ticker("^VIX").history(period="5d")
                if not _vix_data.empty:
                    _vix_spot = float(_vix_data["Close"].iloc[-1])
                _vix3m_data = yf.Ticker("^VIX3M").history(period="5d")
                if not _vix3m_data.empty:
                    _vix_3m = float(_vix3m_data["Close"].iloc[-1])
            except Exception:
                pass  # Fallbacks in mit_apply_overlay greifen
            # Drawdown aus portfolio.json / Alpaca-Equity berechnen
            initial_cap = float(self.cfg.get("initial_capital", 10_000.0))
            if equity > 0 and initial_cap > 0:
                peak = max(equity, initial_cap)
                _current_dd = max(0.0, (peak - equity) / peak)

        for sym in self.cfg["symbols"]:
            # Concurrent-Positions-Guard: open_positions + neue Entries in diesem Scan
            if len(open_positions) + new_entries_this_scan >= max_concurrent:
                print(f"  {sym}: max_concurrent_positions ({max_concurrent}) erreicht – übersprungen")
                continue

            df = (self.alpaca.fetch_bars(sym, days=2) if self.alpaca
                  else pd.DataFrame())
            if df.empty:
                print(f"  {sym}: keine Daten von Alpaca erhalten")
                continue
            if len(df) < 20:
                print(f"  {sym}: zu wenig Historie ({len(df)} Bars)")
                continue

            # Fix #4: Freshness-Check – bei stale data Trade blocken, nicht nur warnen
            if self.alpaca and not self.alpaca.check_bar_freshness(df, max_delay):
                print(f"  {sym}: STALE DATA – übersprungen")
                self._log_event("STALE_DATA", "Bar zu alt – Trade blockiert", sym,
                                {"max_delay_min": max_delay})
                continue
            # Freshness-Check: Daten zu alt → Symbol überspringen wenn block_on_stale=True
            if self.alpaca:
                data_fresh = self.alpaca.check_bar_freshness(df, max_delay)
                if not data_fresh:
                    msg = (f"  {sym}: Daten zu alt (>{max_delay} Min.) – "
                           f"Trade übersprungen. "
                           f"{'(block_trades_on_stale_data=False zum Deaktivieren)' if block_on_stale else 'WARNUNG: Trade wird trotzdem ausgeführt!'}")
                    print(msg)
                    send_telegram(f"⚠️ {sym}: stale Daten (>{max_delay} Min.) – kein Trade")
                    if block_on_stale:
                        continue

            df = compute_indicators(df)

            # Bereits offen? → Status ausgeben, nichts tun (Alpaca managt Exit)
            if sym in open_positions:
                pos = open_positions[sym]
                print(f"  {sym}: offen {pos['side'].upper()} {pos['qty']} "
                      f"@ {pos['entry']:.2f}  uPnL {pos['unrealized_pnl']:+.2f}")
                continue

            if not self.portfolio.can_trade_today():
                print(f"  {sym}: Tageslimit erreicht")
                continue

            signal, strength, reason, ctx = self.strategy.generate_signal(df, spy_df=spy_df)
            qty_factor = 1.0
            overlay_reason = ""

            # Kompakte Debug-Ausgabe für ausgewählte Symbole.
            if self.cfg.get("debug_scan_enabled", False):
                dbg_syms = self.cfg.get("debug_scan_symbols", [])
                if (not dbg_syms) or (sym in dbg_syms):
                    last = df.iloc[-1]
                    vol = float(last.get("Volume", 0.0))
                    vol_ma = float(last["Volume_MA"]) if pd.notna(last.get("Volume_MA", np.nan)) else np.nan
                    vol_ratio = float(last["Volume_Ratio"]) if pd.notna(last.get("Volume_Ratio", np.nan)) else np.nan
                    orb_vol_ratio = float(ctx.get("volume_ratio", np.nan))
                    vol_ma_s = f"{vol_ma:.0f}" if np.isfinite(vol_ma) else "n/a"
                    vol_ratio_s = f"{vol_ratio:.2f}" if np.isfinite(vol_ratio) else "n/a"
                    orb_vol_ratio_s = f"{orb_vol_ratio:.2f}" if np.isfinite(orb_vol_ratio) else "n/a"
                    print(
                        f"  [DBG] {sym}: sig={signal} str={strength:.2f} "
                        f"vol={vol:.0f} vma={vol_ma_s} vRatio={vol_ratio_s} orbVRatio={orb_vol_ratio_s}"
                    )

            if signal in ("BUY", "SHORT") and self.cfg.get("use_mit_probabilistic_overlay", False):
                should_trade, qty_factor, overlay_reason = self.strategy.apply_mit_overlay(
                    signal, strength, ctx, df,
                    current_drawdown=_current_dd,
                    vix_spot=_vix_spot,
                    vix_3m=_vix_3m,
                )
                if not should_trade:
                    print(f"  {sym}: HOLD – {overlay_reason}")
                    self._log_event(
                        "MIT_OVERLAY_REJECTED",
                        "MIT Overlay abgelehnt",
                        sym,
                        {"signal": signal, "strength": round(strength, 2), "reason": overlay_reason},
                    )
                    continue

                blocked, block_reason = self._mit_group_blocked(sym, open_positions, reserved_mit_groups)
                if blocked:
                    print(f"  {sym}: HOLD – {block_reason}")
                    self._log_event(
                        "MIT_INDEPENDENCE_BLOCKED",
                        "MIT Independence Guard blockiert",
                        sym,
                        {"signal": signal, "reason": block_reason},
                    )
                    continue

                self._log_event(
                    "MIT_OVERLAY_APPROVED",
                    "MIT Overlay freigegeben",
                    sym,
                    {"signal": signal, "strength": round(strength, 2), "qty_factor": round(qty_factor, 2), "reason": overlay_reason},
                )

            # strength-Gating erfolgt zentral in orb_strategy via min_signal_strength.
            if signal == "BUY":
                if no_more_buys:
                    print(f"  {sym}: BUY_CUTOFF_REJECTED – nach {buy_cutoff.strftime('%H:%M')} ET")
                    self._log_event("BUY_CUTOFF_REJECTED", "Buy nach Cutoff-Zeit abgelehnt", sym,
                                    {"signal": signal, "cutoff": buy_cutoff.strftime("%H:%M")})
                    continue
                sig = self._execute_long(sym, df, equity, reason, strength, qty_factor=qty_factor, overlay_reason=overlay_reason)
                if sig:
                    signals.append(sig)
                    new_entries_this_scan += 1
                    if self.cfg.get("use_mit_probabilistic_overlay", False):
                        group = self.strategy.mit_group_for_symbol(sym)
                        if group:
                            reserved_mit_groups.add(group)

            elif signal == "SHORT":
                if no_more_buys:
                    print(f"  {sym}: BUY_CUTOFF_REJECTED – nach {buy_cutoff.strftime('%H:%M')} ET")
                    self._log_event("BUY_CUTOFF_REJECTED", "Short nach Cutoff-Zeit abgelehnt", sym,
                                    {"signal": signal, "cutoff": buy_cutoff.strftime("%H:%M")})
                    continue
                sig = self._execute_short(sym, df, equity, reason, strength, qty_factor=qty_factor, overlay_reason=overlay_reason)
                if sig:
                    signals.append(sig)
                    new_entries_this_scan += 1
                    if self.cfg.get("use_mit_probabilistic_overlay", False):
                        group = self.strategy.mit_group_for_symbol(sym)
                        if group:
                            reserved_mit_groups.add(group)

            else:
                label = f"HOLD (Stärke {strength:.2f})" if signal in ("BUY","SHORT") else signal
                print(f"  {sym}: {label} – {reason}")

        # ── EOD-Close ────────────────────────────────────────────────────────
        eod_minutes = int(self.cfg.get("eod_close_minutes_before", 33))
        et_now    = now.astimezone(pytz.timezone("America/New_York"))
        mins_left = (16 * 60) - (et_now.hour * 60 + et_now.minute)
        if 0 < mins_left <= eod_minutes:
            print(f"  {mins_left} Min bis Schluss – EOD Close (Trigger: {eod_minutes} Min)")
            self._perform_eod_close()

        self._write_report(today, signals, equity)
        return {
            "date":       today,
            "equity":     equity,
            "signals":    len(signals),
            "open":       list(open_positions.keys()),
            "trades_today": self.portfolio.daily_stats["trades_today"],
        }

    # ── Signal-Ausführung ────────────────────────────────────────────────────

    def _execute_long(self, sym: str, df: pd.DataFrame,
                      equity: float, reason: str, strength: float,
                      qty_factor: float = 1.0,
                      overlay_reason: str = "") -> Optional[dict]:
        orb_high, orb_low, orb_range, _ = self.strategy.calculate_orb_levels(df)
        current  = df["Close"].iloc[-1]
        # Fix #9: Stop an ORB-Range statt ATR
        stop     = calculate_stop("long", current, orb_high, orb_low, orb_range,
                                  self.cfg.get("stop_loss_r", 1.0))
        target   = current + self.cfg["profit_target_r"] * (current - stop)
        qty      = calculate_position_size(current, stop, equity,
                                           self.cfg.get("risk_per_trade", 0.005),
                                           self.cfg.get("max_equity_at_risk", 0.05),
                                           self.cfg.get("max_position_value_pct", 0.25))
        qty = max(0, int(qty * max(qty_factor, 0.0)))
        if qty <= 0:
            print(f"  {sym}: Positionsgröße = 0 – übersprungen")
            return None
        final_reason = f"{reason} | {overlay_reason}" if overlay_reason else reason

        # Prüfe Buying Power und reduziere Qty bei Bedarf
        qty, bp_detail = self._cap_qty_by_buying_power(sym, "long", qty, current)
        if bp_detail["capped"]:
            msg = (f"ORB {sym}: Qty gekürzt von {bp_detail['original_qty']} zu "
                   f"{qty} (Buying Power limit)")
            self._notify_and_log("QTY_CAP", "Order Qty aufgrund BP gekürzt", sym, bp_detail, msg)
        
        if qty <= 0:
            msg = (f"ORB {sym} ABGEBROCHEN: Insufficient Buying Power "
                   f"(verfügbar: {bp_detail.get('bp_available', 0):.2f})")
            self._notify_and_log("ORDER_REJECTED", "Insufficient BP", sym, bp_detail, msg)
            return None

        # Lesbare client_order_id mit Signalinfos für Alpaca-Dashboard (max 128 Zeichen)
        client_order_id = _build_client_order_id(sym, "BUY", overlay_reason,
                                                  self.cfg.get("order_prefix", ""))

        order = self.alpaca.place_long_bracket(sym, qty, stop, target,
                                               client_order_id=client_order_id) if self.alpaca else {"ok": True, "id": client_order_id}
        if self.alpaca and not order.get("ok"):
            error = order.get("error", "Unknown error")
            cat = self._classify_order_error(error)
            msg = f"ORB {sym} BUY FAILED ({cat}): {error}"
            self._notify_and_log("ORDER_REJECTED", f"Order failed: {cat}", sym,
                               {"error": error, "category": cat}, msg)
            return None

        self.portfolio.log_order(sym, "BUY", qty, current, stop, target,
                                  alpaca_order_id=order["id"] if order else "SIM",
                                  reason=final_reason)
        msg = (f"ORB BUY {sym} {qty} @ {current:.2f} | "
               f"SL {stop:.2f} | TP {target:.2f} [{strength:.2f}] {final_reason}")
        self._notify_and_log("ORDER_FILLED", "Long order executed", sym,
                           {"qty": qty, "price": current, "reason": final_reason, "qty_factor": round(qty_factor, 2)}, msg)
        return {"symbol": sym, "action": "BUY", "qty": qty,
                "price": current, "stop": stop, "target": target,
                "strength": strength, "reason": final_reason}

    def _execute_short(self, sym: str, df: pd.DataFrame,
                        equity: float, reason: str, strength: float,
                        qty_factor: float = 1.0,
                        overlay_reason: str = "") -> Optional[dict]:
        orb_high, orb_low, orb_range, _ = self.strategy.calculate_orb_levels(df)
        current  = df["Close"].iloc[-1]
        # Fix #9: Stop an ORB-Range statt ATR
        stop     = calculate_stop("short", current, orb_high, orb_low, orb_range,
                                  self.cfg.get("stop_loss_r", 1.0))
        target   = current - self.cfg["profit_target_r"] * (stop - current)
        qty      = calculate_position_size(current, stop, equity,
                                           self.cfg.get("risk_per_trade", 0.005),
                                           self.cfg.get("max_equity_at_risk", 0.05),
                                           self.cfg.get("max_position_value_pct", 0.25))
        qty = max(0, int(qty * max(qty_factor, 0.0)))
        if qty <= 0:
            print(f"  {sym}: Positionsgröße = 0 – übersprungen")
            return None
        final_reason = f"{reason} | {overlay_reason}" if overlay_reason else reason

        # Prüfe Buying Power und reduziere Qty bei Bedarf (shorting benötigt Margin)
        qty, bp_detail = self._cap_qty_by_buying_power(sym, "short", qty, current)
        if bp_detail["capped"]:
            msg = (f"ORB {sym}: Qty gekürzt von {bp_detail['original_qty']} zu "
                   f"{qty} (Buying Power limit)")
            self._notify_and_log("QTY_CAP", "Order Qty aufgrund BP gekürzt", sym, bp_detail, msg)
        
        if qty <= 0:
            msg = (f"ORB {sym} ABGEBROCHEN: Insufficient Buying Power / "
                   f"Not Shortable (verfügbar: {bp_detail.get('bp_available', 0):.2f})")
            self._notify_and_log("ORDER_REJECTED", "Insufficient BP or not shortable", sym,
                               bp_detail, msg)
            return None

        # Lesbare client_order_id mit Signalinfos für Alpaca-Dashboard (max 128 Zeichen)
        client_order_id = _build_client_order_id(sym, "SHORT", overlay_reason,
                                                  self.cfg.get("order_prefix", ""))

        order = self.alpaca.place_short_bracket(sym, qty, stop, target,
                                                client_order_id=client_order_id) if self.alpaca else {"ok": True, "id": client_order_id}
        if self.alpaca and not order.get("ok"):
            error = order.get("error", "Unknown error")
            cat = self._classify_order_error(error)
            msg = f"ORB {sym} SHORT FAILED ({cat}): {error}"
            self._notify_and_log("ORDER_REJECTED", f"Order failed: {cat}", sym,
                               {"error": error, "category": cat}, msg)
            return None

        self.portfolio.log_order(sym, "SHORT", qty, current, stop, target,
                                  alpaca_order_id=order["id"] if order else "SIM",
                  reason=final_reason)
        msg = (f"ORB SHORT {sym} {qty} @ {current:.2f} | "
           f"SL {stop:.2f} | TP {target:.2f} [{strength:.2f}] {final_reason}")
        self._notify_and_log("ORDER_FILLED", "Short order executed", sym,
               {"qty": qty, "price": current, "reason": final_reason, "qty_factor": round(qty_factor, 2)}, msg)
        return {"symbol": sym, "action": "SHORT", "qty": qty,
                "price": current, "stop": stop, "target": target,
        "strength": strength, "reason": final_reason}

    # ── Status & Report ──────────────────────────────────────────────────────

    def get_status(self) -> dict:
        positions = self.alpaca.sync_positions() if self.alpaca else {}
        equity    = self.alpaca.get_equity()     if self.alpaca else 0.0
        orders    = self.alpaca.get_open_orders()if self.alpaca else []
        return {
            "mode":         "PAPER" if (self.alpaca and self.alpaca.paper) else "LIVE",
            "equity":       equity,
            "cash":         self.alpaca.get_cash()         if self.alpaca else 0.0,
            "buying_power": self.alpaca.get_buying_power() if self.alpaca else 0.0,
            "open_positions": positions,
            "open_orders":    orders,
            "trades_today":   self.portfolio.daily_stats["trades_today"],
            "pnl_today":      self.portfolio.daily_stats["pnl_today"],
        }

    def _write_report(self, date_str: str, signals: list, equity: float):
        path  = self.reports_dir / f"orb_report_{date_str}.txt"
        lines = [
            "=" * 60,
            f"ORB_BOT – DAILY REPORT – {date_str}",
            f"Modus: {'PAPER' if (self.alpaca and self.alpaca.paper) else 'LIVE'}",
            "=" * 60,
            f"Eigenkapital:  {equity:,.2f} {self.cfg['currency']}",
            f"Trades heute:  {self.portfolio.daily_stats['trades_today']}/"
            f"{self.cfg['max_daily_trades']}",
            "",
            "Signale:",
        ]
        for s in signals:
            lines.append(
                f"  {s['symbol']}: {s['action']} {s['qty']} @ {s['price']:.2f} "
                f"| SL {s['stop']:.2f} | TP {s['target']:.2f} "
                f"[{s['strength']:.2f}] – {s['reason']}"
            )
        if not signals:
            lines.append("  (keine)")
        lines.append("=" * 60)
        path.write_text("\n".join(lines))
        print(f"  Report: {path}")

    @staticmethod
    def _empty_result(today: str) -> dict:
        return {"date": today, "equity": 0.0, "signals": 0,
                "open": [], "trades_today": 0}


# ============================= Backtester (delegiert an orb_backtest) =========
# Der Legacy-Backtester wurde entfernt. Stattdessen wird der kanonische
# Backtester aus orb_backtest.py verwendet, der Long+Short, alle Guards,
# MIT-Overlay und vollständige Metriken unterstützt.

def _run_canonical_backtest(cfg: dict, alpaca: "AlpacaClient",
                            start_date: str, end_date: str) -> None:
    """Wrapper für den kanonischen Backtester aus orb_backtest.py."""
    from orb_backtest import load_orb_data, run_orb_backtest, print_orb_report

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n{'=' * 60}")
    print(f"  ORB BOT – BACKTEST (kanonisch via orb_backtest.py)")
    print(f"  Zeitraum: {start_date} → {end_date}")
    print(f"  Symbole: {len(cfg['symbols'])} | Kapital: {cfg['initial_capital']:,.0f}")
    print(f"  Shorts: {'AN' if cfg.get('allow_shorts') else 'AUS'}")
    print(f"{'=' * 60}")

    data, vix, vix3m = load_orb_data(cfg["symbols"], start_date, end_date, alpaca=alpaca)
    if not data:
        print("[ERROR] Keine Daten geladen.")
        return

    _, report = run_orb_backtest(data, vix, cfg, vix3m_series=vix3m)
    output_dir = cfg.get("data_dir", Path(__file__).parent / "orb_trading_data")
    print_orb_report(report, output_dir=output_dir)


# ============================= CLI / OpenClaw-Einstieg ======================

def _build_alpaca_client(cfg: dict) -> Optional["AlpacaClient"]:
    """
    Liest Alpaca-Keys aus Umgebungsvariablen.
    OpenClaw setzt APCA_API_KEY_ID und APCA_API_SECRET_KEY automatisch,
    wenn der Nutzer den Alpaca-Skill installiert hat.
    """
    if not ALPACA_AVAILABLE:
        print("[ERROR] alpaca-py fehlt – pip install alpaca-py", file=sys.stderr)
        return None

    key    = os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY")

    if not key or not secret:
        print("[ERROR] APCA_API_KEY_ID / APCA_API_SECRET_KEY nicht gesetzt.\n"
              "  In OpenClaw: clawhub install alpaca-trading → Keys hinterlegen\n"
              "  Lokal:       export APCA_API_KEY_ID=pk_...\n"
              "               export APCA_API_SECRET_KEY=sk_...",
              file=sys.stderr)
        return None

    # Env-Var hat IMMER Vorrang über Config. Nur wenn nicht gesetzt → cfg-Fallback.
    paper_env = os.getenv("APCA_PAPER", "").lower()
    if paper_env == "false":
        paper = False
    elif paper_env == "true":
        paper = True
    else:
        paper = cfg.get("alpaca_paper", True)

    feed = os.getenv("APCA_DATA_FEED", cfg.get("alpaca_data_feed", "iex"))

    return AlpacaClient(api_key=key, secret_key=secret, paper=paper, data_feed=feed)


def _build_ibkr_client(cfg: dict) -> Optional["BrokerBase"]:
    """
    Erstellt einen IBKRClient aus Umgebungsvariablen.
    Gibt None zurück bei Fehler (keine Exception).
    """
    try:
        from orb_bot_ibkr import IBKRClient, IBKR_AVAILABLE
    except ImportError:
        IBKR_AVAILABLE = False

    if not IBKR_AVAILABLE:
        print("[ERROR] ib_insync fehlt – pip install ib_insync", file=sys.stderr)
        return None

    host      = os.getenv("IBKR_HOST", cfg.get("ibkr_host", "192.168.188.93"))
    port      = int(os.getenv("IBKR_PORT", str(cfg.get("ibkr_port", 4002))))
    client_id = int(os.getenv("IBKR_CLIENT_ID", str(cfg.get("ibkr_client_id", 1))))
    bot_id    = os.getenv("IBKR_BOT_ID", cfg.get("ibkr_bot_id", "ORB"))

    paper_env = os.getenv("IBKR_PAPER", "").lower()
    if paper_env == "false":
        paper = False
    elif paper_env == "true":
        paper = True
    else:
        paper = cfg.get("ibkr_paper", True)

    try:
        return IBKRClient(
            host=host, port=port, client_id=client_id,
            paper=paper, bot_id=bot_id,
            order_prefix=cfg.get("order_prefix", ""),
        )
    except Exception as e:
        print(f"[ERROR] IBKR-Verbindung fehlgeschlagen: {e}\n"
              f"  Ist TWS/Gateway gestartet auf {host}:{port}?\n"
              f"  Gateway Paper: Port 4002 | Gateway Live: Port 4001",
              file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="ORB_Bot – Opening Range Breakout",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--mode", choices=["scan", "status", "eod", "backtest"],
        default="scan",
        help=(
            "scan      – Signalsuche + Orderausführung  (Standard)\n"
            "status    – Portfolio-Status ausgeben (JSON)\n"
            "eod       – Alle Positionen sofort schließen\n"
            "backtest  – Historischen Backtest starten"
        ),
    )
    parser.add_argument("--start", default="2024-01-01",
                        help="Backtest-Start (YYYY-MM-DD)")
    parser.add_argument("--end",   default=None,
                        help="Backtest-Ende  (YYYY-MM-DD, Standard: heute)")
    parser.add_argument("--shorts", action="store_true",
                        help="Short-Signale aktivieren (Margin-Konto erforderlich)")
    parser.add_argument("--live", action="store_true",
                        help="Live-Modus – überschreibt Paper-Einstellung")
    parser.add_argument(
        "--broker", choices=["alpaca", "ibkr"], default=None,
        help="Broker-Auswahl (überschreibt ORB_BROKER Env-Variable)"
    )
    parser.add_argument(
        "--client-id", dest="client_id", type=int, default=None,
        help="IBKR Client-ID (Integer, überschreibt IBKR_CLIENT_ID Env-Variable)"
    )
    parser.add_argument(
        "--order-prefix", dest="order_prefix", default="",
        help="Optionaler Präfix für Order-IDs (max 20 Zeichen, wird vorangestellt)",
    )
    mit_group = parser.add_mutually_exclusive_group()
    mit_group.add_argument("--mit-overlay", dest="mit_overlay", action="store_true",
                           help="MIT probabilistic overlay aktivieren")
    mit_group.add_argument("--no-mit-overlay", dest="mit_overlay", action="store_false",
                           help="MIT probabilistic overlay deaktivieren")
    parser.set_defaults(mit_overlay=None)
    args = parser.parse_args()

    # ── Broker-Auswahl bestimmen ─────────────────────────────────────────────
    broker_name = (args.broker or os.getenv("ORB_BROKER", "alpaca")).lower()

    # ── .env-Datei wählen und laden ──────────────────────────────────────────
    if _DOTENV_AVAILABLE:
        if broker_name == "ibkr":
            _env_name = ".env_ORB_MIT_IBKR" if args.mit_overlay is True else ".env_ORB_IBKR"
        else:
            _env_name = ".env_ORB_MIT" if args.mit_overlay is True else ".env_ORB"
        _base = Path(__file__).parent
        _candidates = [_base / _env_name, _base / ".env"]
        _loaded = False
        for _candidate in _candidates:
            if _candidate.exists():
                _load_dotenv(_candidate, override=True)
                _label = _candidate.name
                if _label != _env_name:
                    _label = f".env (Fallback – {_env_name} nicht gefunden)"
                print(f"[Config] Umgebung geladen: {_label}")
                _loaded = True
                break
        if not _loaded:
            print(f"[WARN] Keine .env-Datei gefunden ({_env_name} oder .env) – "
                  f"Umgebungsvariablen müssen extern gesetzt sein")

    cfg = dict(ORB_CONFIG)
    if args.shorts:
        cfg["allow_shorts"] = True
    if args.live:
        cfg["alpaca_paper"] = False
        cfg["ibkr_paper"] = False
        os.environ["APCA_PAPER"] = "false"
        os.environ["IBKR_PAPER"] = "false"
    if args.client_id is not None:
        cfg["ibkr_client_id"] = args.client_id
        os.environ["IBKR_CLIENT_ID"] = str(args.client_id)
    if args.mit_overlay is not None:
        cfg["use_mit_probabilistic_overlay"] = args.mit_overlay
    if args.order_prefix:
        cfg["order_prefix"] = args.order_prefix

    # ── Broker instanziieren ─────────────────────────────────────────────────
    if broker_name == "ibkr":
        broker = _build_ibkr_client(cfg)
    else:
        broker = _build_alpaca_client(cfg)

    # ── Modus-Ausführung ─────────────────────────────────────────────────────

    if args.mode == "scan":
        bot    = ORB_Bot(config=cfg, broker=broker)
        result = bot.run_orb_scan()
        print(json.dumps(result, indent=2, default=str))

    elif args.mode == "status":
        bot    = ORB_Bot(config=cfg, broker=broker)
        status = bot.get_status()
        print(json.dumps(status, indent=2, default=str))

    elif args.mode == "eod":
        if broker:
            bot = ORB_Bot(config=cfg, broker=broker)
            result = bot._perform_eod_close()
            print(json.dumps(result, indent=2, default=str))
        else:
            print("[ERROR] Kein Broker-Client – EOD nicht möglich", file=sys.stderr)
            sys.exit(1)

    elif args.mode == "backtest":
        cfg["initial_capital"] = 10000.0
        # Backtest nutzt immer Alpaca für Datenabruf (IBKR hat Pacing-Limits)
        data_client = _build_alpaca_client(cfg)
        _run_canonical_backtest(cfg, data_client,
                                start_date=args.start, end_date=args.end)


if __name__ == "__main__":
    main()
