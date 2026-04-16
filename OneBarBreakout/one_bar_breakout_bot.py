#!/usr/bin/env python3
"""
one_bar_breakout_bot.py – Live/Paper-Bot für die 50-Bar High/Low Momentum Strategie.

Analog zu orb_bot_alpaca.py – gleiche CLI-Struktur, gleiche Alpaca-Integration,
aber komplett eigenständig (Variante A).

Strategie-Regeln (exakt nach Original, keine Modifikationen):
  - Long:  Close > höchstes High der letzten 50 Bars → BUY at next Open
  - Short: Close < tiefstes  Low  der letzten 50 Bars → SHORT at next Open
  - Exit:  immer 1 Bar halten → Close via Market Order am übernächsten Open
  - KEINE Stop-Loss, KEINE Take-Profit

Warum eigenständig statt integriert (Variante A vs. B)?
  → Siehe Kommentar in one_bar_breakout_strategy.py:
     Fundamental andere Exit-Logik (kein Bracket), andere Datengranularität
     (Daily statt 5m), Zero-Touch-Policy am ORB-Bot.

Verwendung:
    python one_bar_breakout_bot.py --mode scan       # Signal + Order (nach Market Close)
    python one_bar_breakout_bot.py --broker ibkr --mode scan # Signal + Order (nach Market Close) mit IBKR-Broker
    python one_bar_breakout_bot.py --mode morning    # Exit-Orders für offene Positionen
    python one_bar_breakout_bot.py --mode status     # Portfolio-Status (JSON)
    python one_bar_breakout_bot.py --mode backtest   # Historischer Backtest

Umgebungsvariablen (identisch mit ORB-Bot):
    APCA_API_KEY_ID      – Alpaca API Key
    APCA_API_SECRET_KEY  – Alpaca Secret Key
    APCA_PAPER           – "true" / "false"
    APCA_DATA_FEED       – "iex" / "sip"

Timing-Hinweis:
    scan   → nach Market Close (16:00–23:00 ET): analysiert heutigen Close,
             platziert Market-Orders für morgen Open
    morning → vor Market Open (07:00–09:28 ET): schließt gestrige Positionen
              via OPG-Orders (Opening Order, exekutiert am Opening Print)

    python one_bar_breakout_bot.py --mode scan
    python one_bar_breakout_bot.py --mode morning
    python one_bar_breakout_bot.py --mode status
    python one_bar_breakout_bot.py --mode backtest --start 2024-01-01 --end 2026-04-08
"""

import argparse
import copy
import json
import os
import sys
import time as time_module
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

# ── One-Bar-Breakout Strategy (SSoT) ─────────────────────────────────────
from one_bar_breakout_strategy import (
    OBB_DEFAULT_CONFIG,
    ET,
    calculate_obb_position_size,
    compute_obb_signals,
    compute_rolling_win_rate,
    generate_obb_live_signal,
    is_market_day_et,
    obb_metrics_summary,
    to_et,
)
from obb_broker_base import OBBBrokerBase

# ── .env Support ──────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv as _load_dotenv
    _DOTENV_AVAILABLE = True
except ImportError:
    _load_dotenv = None
    _DOTENV_AVAILABLE = False

# ── Alpaca-py ─────────────────────────────────────────────────────────────
try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import (
        GetOrdersRequest,
        MarketOrderRequest,
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
    print("[WARN] alpaca-py fehlt → pip install alpaca-py", file=sys.stderr)


# ============================= Konfiguration ================================

TELEGRAM_TOKEN_PATH   = Path.home() / ".secrets" / "telegram.token"
TELEGRAM_CHAT_ID_PATH = Path.home() / ".secrets" / "telegram.chat_id"

OBB_CONFIG = copy.deepcopy(OBB_DEFAULT_CONFIG)
OBB_CONFIG.update({
    "alpaca_data_feed":           "iex",
    "data_dir":                   Path(__file__).parent / "obb_trading_data",
    "portfolio_file":             Path(__file__).parent / "obb_trading_data" / "portfolio.json",
    "memory_file":                Path(__file__).parent / "obb_trading_data" / "memory.md",
    "daily_stats_file":           Path(__file__).parent / "obb_trading_data" / "daily_stats.json",
    "daily_event_log_dir":        Path(__file__).parent / "obb_trading_data" / "daily_logs",

    # Wie viele tägliche Bars für das Signal abrufen (lookback + Puffer)
    "daily_bars_fetch":           80,

    # Sicherheitsfaktor für Buying-Power-Check
    "buying_power_safety_factor": 0.95,

    # Exit-Order TIF: "opg" für Opening Print (nur wenn vor 09:28 ET eingereicht),
    # sonst automatisch Fallback auf "day".
    "exit_time_in_force": "opg",

    # Debug: welche Symbole detailliert loggen
    "debug_scan_enabled":         True,
    "debug_scan_symbols":         ["SPY", "QQQ"],

    # IBKR-spezifische Defaults
    "ibkr_host":                  "192.168.188.93",
    "ibkr_port":                  4002,
    "ibkr_client_id":             1,
    "ibkr_bot_id":                "OBB",
    "ibkr_paper":                 True,
})

OBB_CONFIG["data_dir"].mkdir(exist_ok=True)
OBB_CONFIG["daily_event_log_dir"].mkdir(exist_ok=True)


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


# ============================= AlpacaClient (Daily Extension) ===============

class AlpacaClientDaily(OBBBrokerBase):
    """
    Alpaca-Client spezialisiert für TÄGLICHE Bars (Daily TimeFrame).

    Eigenständige Klasse (kein Erben von orb_bot_alpaca.AlpacaClient),
    um die Abhängigkeit minimal zu halten. Gleiche Schnittstelle für
    Accounts/Orders, aber andere Datenabruf-Methoden.

    Trennung von 5m-Bars (ORB) und Daily-Bars (OBB) ist bewusst:
    unterschiedliche Fetch-Strategien, unterschiedliche Datenvolumen.
    """

    def __init__(self, api_key: str, secret_key: str,
                 paper: bool = True, data_feed: str = "iex"):
        if not ALPACA_AVAILABLE:
            raise RuntimeError("alpaca-py fehlt – pip install alpaca-py")
        self.paper     = paper
        self.data_feed = data_feed
        self.trading   = TradingClient(api_key=api_key, secret_key=secret_key, paper=paper)
        self.data      = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)
        mode = "PAPER" if paper else "LIVE"
        print(f"[AlpacaDaily] Verbunden  Modus={mode}  Feed={data_feed}")

    # ── Daily Bars ──────────────────────────────────────────────────────────

    def fetch_daily_bars(self, symbol: str, days: int = 80) -> pd.DataFrame:
        """Tägliche OHLCV-Bars für ein Symbol (letzte `days` Handelstage)."""
        try:
            now_utc   = datetime.now(pytz.UTC)
            start_utc = now_utc - timedelta(days=days * 2)  # Kalender-Puffer
            req = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Day,
                start=start_utc,
                end=now_utc,
                adjustment="raw",
                feed=self.data_feed,
            )
            bars = self.data.get_stock_bars(req)
            if bars.df.empty:
                return pd.DataFrame()
            df = (bars.df.loc[symbol].copy()
                  if isinstance(bars.df.index, pd.MultiIndex) else bars.df.copy())
            return self._rename(df).tail(days)
        except Exception as e:
            print(f"[AlpacaDaily] Datenfehler {symbol}: {e}")
            return pd.DataFrame()

    def fetch_daily_bars_bulk(
        self, symbols: List[str], start: str, end: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Historische tägliche Bars für mehrere Symbole (Backtesting).
        Chunking und Retry-Logik identisch zu orb_bot_alpaca.fetch_bars_bulk.
        """
        result: Dict[str, pd.DataFrame] = {}
        try:
            start_utc = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
            end_utc = (
                datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
                + timedelta(days=1) - timedelta(microseconds=1)
            )
        except Exception as e:
            print(f"[AlpacaDaily] Bulk-Datumsfehler: {e}")
            return result

        chunk_days  = 365   # Daily-Bars: größere Chunks möglich
        max_retries = 5

        for sym in symbols:
            chunks: List[pd.DataFrame] = []
            cur_start = start_utc
            failed = 0

            while cur_start <= end_utc:
                cur_end = min(
                    cur_start + timedelta(days=chunk_days) - timedelta(microseconds=1),
                    end_utc,
                )
                for attempt in range(1, max_retries + 1):
                    try:
                        req = StockBarsRequest(
                            symbol_or_symbols=sym,
                            timeframe=TimeFrame.Day,
                            start=cur_start,
                            end=cur_end,
                            adjustment="raw",
                            feed=self.data_feed,
                        )
                        bars = self.data.get_stock_bars(req)
                        if not bars.df.empty:
                            df_raw = (bars.df.loc[sym].copy()
                                      if isinstance(bars.df.index, pd.MultiIndex)
                                      else bars.df.copy())
                            chunks.append(self._rename(df_raw))
                        break
                    except Exception as e:
                        wait = min(2 ** (attempt - 1), 16)
                        print(f"  [WARN] {sym} Chunk {cur_start.date()}->{cur_end.date()} "
                              f"Versuch {attempt}/{max_retries}: {e}")
                        if attempt < max_retries:
                            time_module.sleep(wait)
                        else:
                            failed += 1
                cur_start = cur_end + timedelta(microseconds=1)

            if chunks:
                merged = pd.concat(chunks).pipe(
                    lambda d: d[~d.index.duplicated(keep="last")].sort_index()
                )
                result[sym] = merged
                print(f"  [OK] {sym}: {len(merged)} Daily-Bars"
                      + (f" ({failed} Fehl-Chunks)" if failed else ""))
            else:
                print(f"  [NO DATA] {sym}")

        return result

    @staticmethod
    def _rename(df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume",
        })[["Open", "High", "Low", "Close", "Volume"]]

    # ── Account ────────────────────────────────────────────────────────────

    def get_equity(self) -> float:
        try:
            return float(self.trading.get_account().equity)
        except Exception as e:
            print(f"[AlpacaDaily] Equity-Fehler: {e}")
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
        try:
            positions = self.trading.get_all_positions()
            return {
                p.symbol: {
                    "symbol":         p.symbol,
                    "qty":            float(p.qty),
                    "side":           p.side.value,
                    "entry":          float(p.avg_entry_price),
                    "current_price":  float(p.current_price),
                    "unrealized_pnl": float(p.unrealized_pl),
                    "market_value":   float(p.market_value),
                }
                for p in positions
            }
        except Exception as e:
            print(f"[AlpacaDaily] Positions-Fehler: {e}")
            return {}

    def is_shortable(self, symbol: str) -> bool:
        try:
            asset = self.trading.get_asset(symbol)
            return bool(asset.shortable) and bool(asset.easy_to_borrow)
        except Exception:
            return False

    def get_open_orders(self) -> List[dict]:
        try:
            req    = GetOrdersRequest(status="open", limit=50)
            orders = self.trading.get_orders(req)
            return [{
                "id": str(o.id), "symbol": o.symbol,
                "side": o.side.value, "qty": float(o.qty),
                "status": o.status.value,
                "time_in_force": o.time_in_force.value,
            } for o in orders]
        except Exception as e:
            print(f"[AlpacaDaily] Orders-Fehler: {e}")
            return []

    # ── Orders ─────────────────────────────────────────────────────────────

    def place_market_order(
        self,
        symbol: str,
        qty: int,
        side: str,        # "buy" | "sell"
        time_in_force: str = "day",  # "day" | "opg" | "gtc"
        client_order_id: str = "",
    ) -> dict:
        """
        Einfache Market-Order ohne Bracket.

        Timing:
          - Tagsüber / kurz vor Close + TimeInForce.CLS → MOC-Order (Market on Close),
            exekutiert exakt am Closing Print (22:00 ET / 16:00 ET).
            Kann den ganzen Tag eingereicht werden – Timing des Scans egal.
          - Vor Marktöffnung (07:00–09:28 ET) + TimeInForce.OPG → Opening Order,
            exekutiert exakt am Opening Print (empfohlen für Exit).

        Wann OPG vs. CLS?
          → CLS: `--mode scan` (Entry, läuft zum Closing Print)
          → OPG: `--mode morning` (Exit, läuft zum Opening Print)
        """
        try:
            tif_map = {
                "day": TimeInForce.DAY,
                "opg": TimeInForce.OPG,
                "cls": TimeInForce.CLS,
                "gtc": TimeInForce.GTC,
            }
            tif = tif_map.get(time_in_force.lower(), TimeInForce.DAY)

            order_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
            req_kwargs = dict(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=tif,
                order_class=OrderClass.SIMPLE,
            )
            if client_order_id:
                req_kwargs["client_order_id"] = client_order_id[:128]

            r = self.trading.submit_order(MarketOrderRequest(**req_kwargs))
            print(f"[AlpacaDaily] {side.upper()} {symbol} {qty} "
                  f"TIF={time_in_force.upper()} → {r.status.value}")
            return {"ok": True, "id": str(r.id), "symbol": symbol,
                    "qty": qty, "side": side, "status": r.status.value}
        except Exception as e:
            print(f"[AlpacaDaily] Order-Fehler {symbol}: {e}")
            return {"ok": False, "error": str(e)}

    def cancel_all_orders(self) -> None:
        try:
            self.trading.cancel_orders()
            print("[AlpacaDaily] Alle offenen Orders storniert")
        except Exception as e:
            print(f"[AlpacaDaily] Stornierungsfehler: {e}")

    def close_position(self, symbol: str) -> dict:
        """Schließe eine einzelne Position sofort (Market-Order)."""
        try:
            self.trading.close_position(symbol)
            print(f"[AlpacaDaily] Position {symbol} geschlossen")
            return {"ok": True}
        except Exception as e:
            print(f"[AlpacaDaily] Schließ-Fehler {symbol}: {e}")
            return {"ok": False, "error": str(e)}

    def close_all_positions(self) -> dict:
        """Schließe alle offenen Positionen."""
        try:
            positions = self.sync_positions()
            if not positions:
                print("[AlpacaDaily] Keine offenen Positionen")
                return {"ok": True, "closed": []}
            self.trading.close_all_positions(cancel_orders=True)
            print(f"[AlpacaDaily] {len(positions)} Position(en) geschlossen")
            return {"ok": True, "closed": list(positions.keys())}
        except Exception as e:
            print(f"[AlpacaDaily] Close-All-Fehler: {e}")
            return {"ok": False, "error": str(e)}


# ============================= Portfolio-Ledger =============================

class OBBPortfolio:
    """
    Lokales Ledger für den One-Bar-Breakout-Bot.
    Aufgabe: tägliche Statistiken, Trade-Log, Memory-Datei.
    Positionsverwaltung übernimmt Alpaca.
    """

    def __init__(self, config: dict, persist_files: bool = True):
        self.cfg = config
        self.persist_files = persist_files
        self.cfg["data_dir"].mkdir(exist_ok=True)
        self.data        = self._load()
        self.daily_stats = self._load_daily_stats()

    def _load(self) -> dict:
        if not self.persist_files:
            return self._empty_portfolio()
        pf = self.cfg.get("portfolio_file")
        if pf and Path(pf).exists():
            with open(pf) as f:
                return json.load(f)
        return self._empty_portfolio()

    def _empty_portfolio(self) -> dict:
        return {
            "cash":            self.cfg.get("initial_capital", 0.0),
            "initial_capital": self.cfg.get("initial_capital", 0.0),
            "trades":          [],
            "equity_curve":    [],
            "last_updated":    None,
            "strategy":        "OBB",
        }

    def _load_daily_stats(self) -> dict:
        if not self.persist_files:
            return self._empty_daily()
        dsf = self.cfg.get("daily_stats_file")
        if dsf and Path(dsf).exists():
            with open(dsf) as f:
                return json.load(f)
        return self._empty_daily()

    def _empty_daily(self) -> dict:
        return {
            "trades_today": 0, "pnl_today": 0.0,
            "wins_today": 0, "losses_today": 0,
            "win_rate_today": 0.0, "last_reset_date": None,
        }

    def save(self):
        if not self.persist_files:
            return
        self.data["last_updated"] = datetime.now(pytz.UTC).isoformat()
        with open(self.cfg["portfolio_file"], "w") as f:
            json.dump(self.data, f, indent=2, default=str)

    def _save_daily_stats(self):
        if not self.persist_files:
            return
        with open(self.cfg["daily_stats_file"], "w") as f:
            json.dump(self.daily_stats, f, indent=2)

    def reset_daily_if_needed(self):
        today = datetime.now(pytz.UTC).date().isoformat()
        if self.daily_stats.get("last_reset_date") != today:
            self.daily_stats = {**self._empty_daily(), "last_reset_date": today}
            self._save_daily_stats()

    def can_trade_today(self) -> bool:
        self.reset_daily_if_needed()
        return self.daily_stats["trades_today"] < self.cfg.get("max_daily_trades", 3)

    def log_order(self, symbol: str, action: str, qty: int,
                  price: float, alpaca_order_id: str = "",
                  reason: str = "", signal: str = "") -> None:
        record = {
            "time":     datetime.now(pytz.UTC).isoformat(),
            "symbol":   symbol,
            "action":   action,      # "BUY" | "SHORT" | "SELL" | "COVER"
            "qty":      qty,
            "price":    price,
            "order_id": alpaca_order_id,
            "reason":   reason,
            "signal":   signal,
            "strategy": "OBB",
            "pnl":      0.0,
        }
        self.data["trades"].append(record)
        self.daily_stats["trades_today"] += 1
        self._append_to_memory(
            f"**{datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M')}** "
            f"{symbol} {action} {qty} @ {price:.2f} | {reason}"
        )
        self._save_daily_stats()
        self.save()

    def _append_to_memory(self, content: str):
        if not self.persist_files:
            return
        mp = self.cfg.get("memory_file")
        if not mp:
            return
        mp = Path(mp)
        if not mp.exists():
            mp.write_text("# OBB_Bot Memory Log\n\n")
        with open(mp, "a") as f:
            f.write(f"{content}\n\n")

    def get_trades(self) -> list:
        return self.data.get("trades", [])


# ============================= One-Bar-Breakout-Bot =========================


def _et_to_de(now_utc: datetime, et_hour: int, et_min: int) -> str:
    """Hilfsfunktion: ET-Zeit → deutsche Uhrzeit als String (berücksichtigt Sommer/Winterzeit)."""
    import pytz as _pytz
    et = _pytz.timezone("America/New_York")
    de = _pytz.timezone("Europe/Berlin")
    # Nehme das heutige Datum in ET und ersetze Uhrzeit
    et_dt = now_utc.astimezone(et).replace(hour=et_hour, minute=et_min, second=0, microsecond=0)
    de_dt = et_dt.astimezone(de)
    return de_dt.strftime("%H:%M")

class OneBarBreakoutBot:
    """
    Live/Paper-Bot für die One-Bar-Breakout-Strategie.

    scan-Modus (nach Market Close):
      1. Tagesschlusskurse aller Symbole laden
      2. Signal prüfen (Close > 50-Bar-High oder Close < 50-Bar-Low)
      3. Falls Signal: Market-Order für nächsten Open einreichen
      4. Falls offene Position aus Vortag: Exit-Order einreichen

    morning-Modus (vor Market Open, 07:00–09:28 ET):
      1. Offene Positionen via OPG-Order schließen (exakt am Opening Print)

    Warum zwei Modi?
      OPG-Orders müssen VOR 09:28 ET submitted werden.
      Die Signal-Generierung braucht den heutigen CLOSE → nach 16:00 ET.
      Deshalb: 2 separate Scheduler-Jobs.
    """

    def __init__(self, config: dict = None,
                 alpaca: "OBBBrokerBase" = None, broker: "OBBBrokerBase" = None):
        self.cfg       = config or OBB_CONFIG
        self.broker    = broker or alpaca
        self.alpaca    = self.broker   # Backward-Compat-Alias – NICHT entfernen!
        self.portfolio = OBBPortfolio(self.cfg)
        self.reports_dir = self.cfg["data_dir"] / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        self.event_log_dir = self.cfg["daily_event_log_dir"]
        self.event_log_dir.mkdir(exist_ok=True)

        # Wenn ein Broker verbunden ist: echten Equity-Wert abholen und Portfolio + Config aktualisieren
        if self.broker:
            try:
                equity = self.broker.get_equity()
                if equity > 0:
                    self.cfg["initial_capital"] = equity
                    self.portfolio.data["cash"] = equity
                    self.portfolio.save()
                    print(f"  ✓ Equity vom Broker geholt: ${equity:,.2f}")
            except Exception as e:
                print(f"  [WARN] Equity vom Broker nicht abrufbar: {e}")

        mode = "PAPER" if (self.alpaca and self.alpaca.paper) else "LIVE" if self.alpaca else "KEIN BROKER"
        print(f"\nOneBarBreakoutBot  Modus={mode}  Symbole={len(self.cfg['symbols'])}")
        print(f"  Lookback: {self.cfg.get('lookback_bars', 50)} Bars  "
              f"Shorts: {'AN' if self.cfg.get('allow_shorts') else 'AUS'}")
        print(obb_metrics_summary(self.cfg))

    # ── Logging ──────────────────────────────────────────────────────────────

    def _daily_log_path(self) -> Path:
        today = datetime.now(pytz.UTC).strftime("%Y%m%d")
        return self.event_log_dir / f"obb_events_{today}.log"

    def _log_event(self, level: str, event: str, symbol: str = "",
                   details: dict = None) -> None:
        details = details or {}
        try:
            ts = datetime.now(pytz.UTC).isoformat()
            line = f"[{ts}] [{level}] {event}"
            if symbol:
                line += f" | {symbol}"
            if details:
                line += f" | {json.dumps(details, default=str)}"
            with open(self._daily_log_path(), "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception as e:
            print(f"[WARN] OBB Event-Log Fehler: {e}")

    def _notify(self, level: str, event: str, symbol: str = "",
                details: dict = None, telegram: str = "") -> None:
        self._log_event(level, event, symbol, details)
        if telegram:
            send_telegram(telegram)

    # ── Buying-Power-Guard ───────────────────────────────────────────────────

    def _cap_qty_by_bp(self, symbol: str, side: str,
                       qty: int, price: float) -> Tuple[int, dict]:
        detail = {"original_qty": qty, "capped": False, "adjusted_qty": qty}
        if not self.alpaca or qty <= 0:
            return qty, detail
        try:
            bp = self.alpaca.get_buying_power()
            multiplier = 1.0 if side == "long" else 0.33
            required   = qty * price * multiplier
            safety     = self.cfg.get("buying_power_safety_factor", 0.95)
            available  = bp * safety

            detail.update({"bp_available": bp, "required_bp": required,
                            "safety_factor": safety, "available_with_safety": available})

            if required > available:
                new_qty = int(available / (price * multiplier))
                detail["capped"] = True
                detail["adjusted_qty"] = max(0, new_qty)
                if new_qty < qty:
                    print(f"  [WARN] {symbol} Qty {qty} → {new_qty} (Buying Power)")
                return max(0, new_qty), detail
        except Exception as e:
            print(f"  [WARN] BP-Check {symbol}: {e}")
        return qty, detail

    # ── Morning Mode: Exit offener Positionen ─────────────────────────────────

    def run_morning_exits(self) -> dict:
        """
        Vor Market Open (07:00–09:28 ET):
        Alle offenen OBB-Positionen via OPG-Order schließen.

        OPG (Opening Order) → exekutiert exakt am Opening Print (9:30 ET).
        Das ist das präziseste Instrument für die 1-Bar-Exit-Logik.
        """
        now    = datetime.now(pytz.UTC)
        today  = now.strftime("%Y-%m-%d")
        et_now = now.astimezone(ET)

        print(f"\n=== OBB Morning-Exits – {today} (ET: {et_now.strftime('%H:%M')}) ===")

        if et_now.weekday() >= 5:
            print("  Wochenende – kein Exit.")
            return {"date": today, "closed": []}

        configured_tif = str(self.cfg.get("exit_time_in_force", "opg")).lower()
        now_minutes = et_now.hour * 60 + et_now.minute
        opg_cutoff_minutes = 9 * 60 + 28  # 09:28 ET

        exit_tif = configured_tif
        if configured_tif == "opg" and now_minutes >= opg_cutoff_minutes:
            print("  [WARN] Exit-TIF OPG angefordert, aber >= 09:28 ET. "
                  "Fallback auf DAY.")
            exit_tif = "day"
        elif configured_tif not in {"day", "opg", "cls", "gtc"}:
            print(f"  [WARN] Unbekannter exit_time_in_force='{configured_tif}'. "
                  "Fallback auf DAY.")
            exit_tif = "day"

        print(f"  [INFO] Exit Time-In-Force: {exit_tif.upper()} "
              f"(konfiguriert: {configured_tif.upper()})")

        positions = self.alpaca.sync_positions() if self.alpaca else {}
        closed = []

        for sym, pos in positions.items():
            if sym not in self.cfg["symbols"]:
                continue  # Nur OBB-Symbole verwalten

            side      = pos["side"]          # "long" | "short"
            qty       = int(abs(pos["qty"]))
            order_side = "sell" if side == "long" else "buy"

            if qty <= 0:
                print(f"  EXIT SKIP: {sym} – ungültige Qty {pos.get('qty')}")
                continue

            order = (self.alpaca.place_market_order(
                symbol=sym, qty=qty, side=order_side,
                time_in_force=exit_tif,
                client_order_id=f"OBB|EXIT|{sym}|{today}",
            ) if self.alpaca else {"ok": True, "id": "SIM"})

            if order.get("ok"):
                print(
                    f"  EXIT ORDER: {sym} {order_side.upper()} x{qty} "
                    f"| TIF={exit_tif.upper()} | ID={order.get('id', '?')} "
                    f"| STATUS={order.get('status', '?')}"
                )
                pnl_est = pos.get("unrealized_pnl", 0.0)
                msg = (f"OBB EXIT {sym}: {order_side.upper()} {qty} Aktien "
                       f"[{exit_tif.upper()}] uPnL≈{pnl_est:+.2f}")
                self._notify("ORDER_EXIT", "OBB Morning Exit", sym,
                             {
                                 "qty": qty,
                                 "side": order_side,
                                 "upnl": pnl_est,
                                 "time_in_force": exit_tif,
                                 "order_status": order.get("status", ""),
                                 "order_id": order.get("id", ""),
                             },
                             msg)
                self.portfolio.log_order(
                    sym, order_side.upper(), qty, pos["current_price"],
                    alpaca_order_id=order.get("id", ""),
                    reason=f"1-Bar Exit (Morning {exit_tif.upper()})", signal="EXIT"
                )
                closed.append(sym)
            else:
                msg = f"OBB EXIT FEHLER {sym}: {order.get('error', '?')}"
                self._notify("ORDER_ERROR", "OBB Exit fehlgeschlagen", sym,
                             order, msg)

        if not closed:
            print("  Keine offenen OBB-Positionen zu schließen.")
        else:
            print(f"  Exit-Orders eingereicht: {', '.join(closed)}")

        return {"date": today, "closed": closed}

    # ── Scan Mode: Signale + Entry-Orders ────────────────────────────────────

    def run_scan(self) -> dict:
        """
        Nach Market Close (16:00–23:59 ET):
        Signale berechnen → Entry-Orders für nächsten Open einreichen.

        Flow:
          1. Tägliche Bars laden
          2. OBB-Signal berechnen (Close vs. 50-Bar-High/Low)
          3. Falls BUY/SHORT + keine offene Position → Market-Order einreichen
             (wird am nächsten Handelstag ausgeführt)
          4. Offene Positionen aus Vortag werden vom morning-Modus geschlossen.

        Hinweis: Falls morning-Modus nicht genutzt wird, werden offene
        Positionen hier als "zu schließen" gemeldet (kein automatisches Close).
        """
        now   = datetime.now(pytz.UTC)
        today = now.strftime("%Y-%m-%d")
        et_now = now.astimezone(ET)

        print(f"\n=== OBB Signal-Scan – {today} (ET: {et_now.strftime('%H:%M')}) ===")

        if et_now.weekday() >= 5:
            print("  Wochenende – kein Scan.")
            return self._empty_result(today)

        open_positions = self.alpaca.sync_positions() if self.alpaca else {}
        equity         = self.alpaca.get_equity()     if self.alpaca else 0.0
        signals_fired  = []
        trades_today   = 0

        max_concurrent = int(self.cfg.get("max_concurrent_positions", 3))

        for sym in self.cfg["symbols"]:
            # Concurrent-Guard
            if len(open_positions) + trades_today >= max_concurrent:
                print(f"  {sym}: max_concurrent ({max_concurrent}) erreicht")
                continue

            # Tägliche Bars laden
            days_fetch = int(self.cfg.get("daily_bars_fetch", 80))
            df = (self.alpaca.fetch_daily_bars(sym, days=days_fetch)
                  if self.alpaca else pd.DataFrame())

            if df.empty or len(df) < self.cfg.get("lookback_bars", 50) + 2:
                print(f"  {sym}: zu wenig Daten ({len(df)} Bars)")
                continue

            # Signal berechnen
            signal, current_close, lbh, lbl, reason = generate_obb_live_signal(
                df, self.cfg
            )

            # Debug-Ausgabe
            if self.cfg.get("debug_scan_enabled", False):
                dbg = self.cfg.get("debug_scan_symbols", [])
                if not dbg or sym in dbg:
                    print(f"  [DBG] {sym}: {signal}  "
                          f"Close={current_close:.2f}  "
                          f"50H={lbh:.2f}  50L={lbl:.2f}")

            # Bereits offen?
            if sym in open_positions:
                pos = open_positions[sym]
                print(f"  {sym}: OFFEN {pos['side'].upper()} {pos['qty']:.0f} "
                      f"@ {pos['entry']:.2f}  uPnL {pos['unrealized_pnl']:+.2f}")
                continue

            if not self.portfolio.can_trade_today():
                print(f"  {sym}: Tageslimit erreicht")
                continue

            if signal == "BUY":
                sig = self._execute_entry(
                    sym, "buy", current_close, equity, reason
                )
                if sig:
                    signals_fired.append(sig)
                    trades_today += 1
            elif signal == "SHORT":
                if not self.alpaca or self.alpaca.is_shortable(sym):
                    sig = self._execute_entry(
                        sym, "sell", current_close, equity, reason
                    )
                    if sig:
                        signals_fired.append(sig)
                        trades_today += 1
                else:
                    print(f"  {sym}: SHORT – nicht shortbar")
            else:
                print(f"  {sym}: HOLD – {reason}")

        self._write_report(today, signals_fired, equity)
        return {
            "date":          today,
            "equity":        equity,
            "signals_fired": len(signals_fired),
            "open":          list(open_positions.keys()),
            "trades_today":  self.portfolio.daily_stats.get("trades_today", 0),
        }

    # ── Entry-Ausführung ──────────────────────────────────────────────────────

    def _execute_entry(
        self,
        sym: str,
        side: str,       # "buy" (long) | "sell" (short)
        price: float,
        equity: float,
        reason: str,
    ) -> Optional[dict]:
        """
        MOC-Order (Market on Close) → exekutiert am heutigen Closing Print.

        Entry = heutiger Closing Print (MOC, 16:00 ET)
        Exit  = morgiger Opening Print  (OPG, --mode morning)
        → echte Overnight-Position. Kann ganztägig eingereicht werden.

        TimeInForce.DAY nach Marktschluss → Alpaca queued die Order für
        den nächsten Handelstag (exekutiert nahe am Opening-Kurs).

        Für präzise Opening-Price-Execution: morning-Modus mit OPG nutzen.
        """
        # Rolling Win-Rate für Kelly (falls aktiviert)
        win_rate = None
        if self.cfg.get("use_kelly_sizing", False):
            win_rate = compute_rolling_win_rate(
                self.portfolio.get_trades(),
                self.cfg.get("kelly_lookback_trades", 50),
            )

        qty = calculate_obb_position_size(equity, price, self.cfg, win_rate)

        if qty <= 0:
            print(f"  {sym}: Positionsgröße = 0 – übersprungen")
            return None

        # Buying-Power-Check
        bp_side = "long" if side == "buy" else "short"
        qty, bp_detail = self._cap_qty_by_bp(sym, bp_side, qty, price)
        if bp_detail["capped"] and bp_detail.get("adjusted_qty", 0) < bp_detail["original_qty"]:
            msg = (f"OBB {sym}: Qty {bp_detail['original_qty']} → {qty} (BP)")
            self._notify("QTY_CAP", "Qty gekürzt", sym, bp_detail, msg)

        if qty <= 0:
            msg = f"OBB {sym}: Insufficient Buying Power – Trade abgebrochen"
            self._notify("ORDER_REJECTED", "Insufficient BP", sym, bp_detail, msg)
            return None

        action_label = "BUY" if side == "buy" else "SHORT"
        client_id = f"OBB|{action_label}|{sym}|{datetime.now(pytz.UTC).strftime('%Y%m%d-%H%M')}"

        order = (self.alpaca.place_market_order(
            symbol=sym, qty=qty, side=side,
            time_in_force="cls",  # MOC: exekutiert am Closing Print (16:00 ET)
            client_order_id=client_id,
        ) if self.alpaca else {"ok": True, "id": client_id})

        if not order.get("ok"):
            err = order.get("error", "?")
            msg = f"OBB {sym} {action_label} FAILED: {err}"
            self._notify("ORDER_REJECTED", "Order fehlgeschlagen", sym,
                         {"error": err}, msg)
            return None

        self.portfolio.log_order(
            sym, action_label, qty, price,
            alpaca_order_id=order.get("id", "SIM"),
            reason=reason, signal=action_label,
        )
        msg = (f"OBB {action_label} {sym} {qty} Stk @ ~{price:.2f} "
               f"(Closing Print heute, Exit morgen Open) | {reason}")
        self._notify("ORDER_FILLED", f"OBB {action_label}", sym,
                     {"qty": qty, "price": price, "reason": reason}, msg)
        print(f"  {sym}: {action_label} {qty} Stk  {reason}")
        return {
            "symbol": sym, "action": action_label, "qty": qty,
            "price": price, "reason": reason,
        }

    # ── Status & Reports ──────────────────────────────────────────────────────

    def get_status(self) -> dict:
        positions = self.alpaca.sync_positions() if self.alpaca else {}
        equity    = self.alpaca.get_equity()     if self.alpaca else 0.0
        orders    = self.alpaca.get_open_orders()if self.alpaca else []
        return {
            "strategy":       "OneBarBreakout",
            "mode":           "PAPER" if (self.alpaca and self.alpaca.paper) else "LIVE",
            "equity":         equity,
            "cash":           self.alpaca.get_cash()         if self.alpaca else 0.0,
            "buying_power":   self.alpaca.get_buying_power() if self.alpaca else 0.0,
            "open_positions": positions,
            "open_orders":    orders,
            "lookback_bars":  self.cfg.get("lookback_bars", 50),
            "trades_today":   self.portfolio.daily_stats.get("trades_today", 0),
        }

    def _write_report(self, date_str: str, signals: list, equity: float):
        path  = self.reports_dir / f"obb_report_{date_str}.txt"
        lines = [
            "=" * 60,
            f"ONE-BAR BREAKOUT BOT – DAILY REPORT – {date_str}",
            f"Modus: {'PAPER' if (self.alpaca and self.alpaca.paper) else 'LIVE'}",
            "=" * 60,
            f"Eigenkapital:    {equity:,.2f} {self.cfg['currency']}",
            f"Lookback:        {self.cfg.get('lookback_bars', 50)} Bars",
            f"Trades heute:    {self.portfolio.daily_stats.get('trades_today', 0)}/"
            f"{self.cfg['max_daily_trades']}",
            "",
            "Signale heute:",
        ]
        for s in signals:
            lines.append(f"  {s['symbol']}: {s['action']} {s['qty']} "
                         f"@ ~{s['price']:.2f} | {s['reason']}")
        if not signals:
            lines.append("  (keine)")
        lines.append("=" * 60)
        path.write_text("\n".join(lines))
        print(f"  Report: {path}")

    @staticmethod
    def _empty_result(today: str) -> dict:
        return {"date": today, "equity": 0.0, "signals_fired": 0,
                "open": [], "trades_today": 0}


# ============================= Backtest-Integration =========================

def run_obb_backtest_mode(cfg: dict, alpaca: Optional[AlpacaClientDaily],
                          start_date: str, end_date: str,
                          compare_with_orb: bool = False) -> None:
    """Wrapper: startet one_bar_backtest.py als Modul."""
    try:
        from one_bar_backtest import (
            load_obb_data,
            run_obb_backtest,
            print_obb_report,
        )
    except ImportError:
        print("[ERROR] one_bar_backtest.py nicht gefunden. "
              "Stelle sicher, dass die Datei im selben Verzeichnis liegt.")
        return

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  ONE-BAR BREAKOUT BOT – BACKTEST")
    print(f"  Zeitraum: {start_date} → {end_date}")
    print(f"  Symbole: {len(cfg['symbols'])} | Kapital: {cfg['initial_capital']:,.0f}")
    print(f"  Lookback: {cfg.get('lookback_bars', 50)} Bars")
    print(f"  Shorts: {'AN' if cfg.get('allow_shorts') else 'AUS'}")
    print(f"{'='*60}")

    data = load_obb_data(cfg["symbols"], start_date, end_date, alpaca=alpaca)
    if not data:
        print("[ERROR] Keine Daten geladen.")
        return

    report = run_obb_backtest(data, cfg)
    output_dir = cfg.get("data_dir", Path(__file__).parent / "obb_trading_data")
    print_obb_report(
        report,
        output_dir=output_dir,
        compare_with_orb=compare_with_orb,
        orb_cfg=cfg if compare_with_orb else None,
        orb_alpaca=alpaca if compare_with_orb else None,
        start_date=start_date,
        end_date=end_date,
    )


# ============================= CLI / Einstieg ================================

def _build_alpaca_client(cfg: dict) -> Optional[AlpacaClientDaily]:
    if not ALPACA_AVAILABLE:
        print("[ERROR] alpaca-py fehlt – pip install alpaca-py", file=sys.stderr)
        return None

    key    = os.getenv("APCA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY")

    if not key or not secret:
        print("[ERROR] APCA_API_KEY_ID / APCA_API_SECRET_KEY nicht gesetzt.\n"
              "  export APCA_API_KEY_ID=pk_...\n"
              "  export APCA_API_SECRET_KEY=sk_...", file=sys.stderr)
        return None

    paper_env = os.getenv("APCA_PAPER", "").lower()
    if paper_env == "false":
        paper = False
    elif paper_env == "true":
        paper = True
    else:
        paper = cfg.get("alpaca_paper", True)

    feed = os.getenv("APCA_DATA_FEED", cfg.get("alpaca_data_feed", "iex"))
    return AlpacaClientDaily(api_key=key, secret_key=secret, paper=paper, data_feed=feed)


def _build_ibkr_client(cfg: dict) -> Optional["OBBBrokerBase"]:
    """IBKRClientDaily aus orb_bot_ibkr erzeugen (lazy-import)."""
    try:
        from obb_bot_ibkr import IBKRClientDaily
    except ImportError as e:
        print(f"[ERROR] obb_bot_ibkr nicht ladbar: {e}", file=sys.stderr)
        return None

    host      = os.getenv("IBKR_HOST",      cfg.get("ibkr_host", "192.168.188.93"))
    port      = int(os.getenv("IBKR_PORT",   str(cfg.get("ibkr_port", 4002))))
    client_id = int(os.getenv("IBKR_CLIENT_ID", str(cfg.get("ibkr_client_id", 1))))
    bot_id    = os.getenv("IBKR_BOT_ID",     cfg.get("ibkr_bot_id", "OBB"))

    paper_env = os.getenv("IBKR_PAPER", "").lower()
    if paper_env == "false":
        paper = False
    elif paper_env == "true":
        paper = True
    else:
        paper = cfg.get("ibkr_paper", True)

    try:
        return IBKRClientDaily(host=host, port=port, client_id=client_id,
                               bot_id=bot_id, paper=paper)
    except Exception as e:
        print(f"[ERROR] IBKR-Verbindung fehlgeschlagen: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="One-Bar-Breakout Bot – 50-Bar High/Low Momentum",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["scan", "morning", "status", "backtest"],
        default="scan",
        help=(
            "scan      – Signal-Check + Entry-Orders (nach Market Close)\n"
            "morning   – Exit-Orders für offene Positionen (vor Market Open)\n"
            "status    – Portfolio-Status ausgeben (JSON)\n"
            "backtest  – Historischen Backtest starten"
        ),
    )
    parser.add_argument("--broker", choices=["alpaca", "ibkr"], default=None,
                        help="Broker-Backend (Standard: alpaca, per ORB_BROKER überschreibbar)")
    parser.add_argument("--start", default="2024-01-01",
                        help="Backtest-Start (YYYY-MM-DD)")
    parser.add_argument("--end", default=None,
                        help="Backtest-Ende (YYYY-MM-DD, Standard: heute)")
    parser.add_argument("--shorts", action="store_true",
                        help="Short-Signale aktivieren")
    parser.add_argument("--live", action="store_true",
                        help="Live-Modus (überschreibt APCA_PAPER=true)")
    parser.add_argument("--lookback", type=int, default=None,
                        help="Lookback in Bars (Standard: 50)")
    parser.add_argument("--compare-orb", action="store_true",
                        help="Backtest: Vergleich mit ORB-Bot einschließen")
    parser.add_argument("--kelly", action="store_true",
                        help="Kelly-basiertes Sizing aktivieren")
    args = parser.parse_args()

    broker_name = (args.broker or os.getenv("OBB_BROKER", "alpaca")).lower()

    # .env laden
    if _DOTENV_AVAILABLE:
        _base = Path(__file__).parent
        if broker_name == "ibkr":
            candidates = [_base / ".env_OBB_IBKR"]
        else:
            candidates = [_base / ".env_OBB", _base / ".env"]
        for candidate in candidates:
            if candidate.exists():
                _load_dotenv(candidate, override=True)
                print(f"[Config] Umgebung geladen: {candidate.name}")
                break

    cfg = dict(OBB_CONFIG)
    if args.shorts:
        cfg["allow_shorts"] = True
    if args.live:
        cfg["alpaca_paper"] = False
        os.environ["APCA_PAPER"] = "false"
    if args.lookback:
        cfg["lookback_bars"] = args.lookback
    if args.kelly:
        cfg["use_kelly_sizing"] = True

    # Broker instanziieren
    if broker_name == "ibkr":
        broker = _build_ibkr_client(cfg)
    else:
        broker = _build_alpaca_client(cfg)

    if args.mode == "scan":
        bot    = OneBarBreakoutBot(config=cfg, broker=broker)
        result = bot.run_scan()
        print(json.dumps(result, indent=2, default=str))

    elif args.mode == "morning":
        bot    = OneBarBreakoutBot(config=cfg, broker=broker)
        result = bot.run_morning_exits()
        print(json.dumps(result, indent=2, default=str))

    elif args.mode == "status":
        bot    = OneBarBreakoutBot(config=cfg, broker=broker)
        status = bot.get_status()
        print(json.dumps(status, indent=2, default=str))

    elif args.mode == "backtest":
        cfg["initial_capital"] = 10_000.0
        # Backtest nutzt immer Alpaca für Daten (IBKR-Pacing zu langsam)
        data_client = _build_alpaca_client(cfg) if broker_name == "ibkr" else broker
        run_obb_backtest_mode(
            cfg, data_client,
            start_date=args.start,
            end_date=args.end,
            compare_with_orb=args.compare_orb,
        )


if __name__ == "__main__":
    main()
