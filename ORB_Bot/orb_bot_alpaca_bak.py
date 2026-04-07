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
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import pytz
import time as time_module

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
    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_AVAILABLE = False
    print("[WARN] alpaca-py fehlt → pip install alpaca-py", file=sys.stderr)

# ============================= Konfiguration ================================

TELEGRAM_TOKEN_PATH  = Path.home() / ".secrets" / "telegram.token"
TELEGRAM_CHAT_ID_PATH = Path.home() / ".secrets" / "telegram.chat_id"

ORB_CONFIG = {
    # ── Handelbare Symbole via Alpaca (Stocks + ETFs) ──────────────────────
    "symbols": [
        "SPY", "QQQ", "IWM", "DIA",
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
        "NVDA", "META", "AMD", "NFLX",
    ],
    # ── Nur Signal, keine Ausführung (Alpaca unterstützt keine Futures) ────
    "symbols_watchonly": ["ES=F", "NQ=F", "MES=F", "MNQ=F"],

    # ── ORB-Parameter ──────────────────────────────────────────────────────
    "opening_range_minutes": 30,
    "orb_breakout_multiplier": 1.0,
    "volume_multiplier": 1.3,

    # ── Risiko-Management ──────────────────────────────────────────────────
    "risk_per_trade":    0.01,   # 1 % des Eigenkapitals pro Trade
    "max_daily_trades":  3,
    "max_equity_at_risk": 0.05,  # max. 5 % Gesamtrisiko gleichzeitig

    # ── Trade-Management ───────────────────────────────────────────────────
    "profit_target_r": 2.0,      # Take-Profit in R-Vielfachen
    "stop_loss_r":     1.0,      # Stop-Loss  in R-Vielfachen
    # Trailing Stop: wird als separater Alpaca-Aufruf gesetzt, sobald
    # Bracket-Order gefüllt ist (optional, see _activate_trailing_stop)
    "trail_after_r":    1.0,
    "trail_distance_r": 0.5,

    # ── Short-Seite ────────────────────────────────────────────────────────
    # Alpaca benötigt ein Margin-Konto für Shorts auf Stocks/ETFs.
    # Futures-Shorts werden über symbols_watchonly ohnehin nicht ausgeführt.
    "allow_shorts": False,

    # ── Marktzeiten (ET) ───────────────────────────────────────────────────
    "market_open":  time(9, 30),
    "market_close": time(16, 0),
    "orb_end_time": time(10, 0),

    # ── Alpaca-Verbindung ──────────────────────────────────────────────────
    # Werte aus Umgebungsvariablen; hier als Fallback-Defaults
    "alpaca_paper":     True,    # überschrieben durch APCA_PAPER env-var
    "alpaca_data_feed": "iex",   # "iex" = kostenlos | "sip" = Echtzeit (kostenpflichtig)

    # ── Filter ─────────────────────────────────────────────────────────────
    "avoid_fridays": True,
    "avoid_mondays": False,

    # ── Lokale Dateien ─────────────────────────────────────────────────────
    "currency":  "USD",
    "data_dir":         Path(__file__).parent / "orb_trading_data",
    "portfolio_file":   Path(__file__).parent / "orb_trading_data" / "portfolio.json",
    "memory_file":      Path(__file__).parent / "orb_trading_data" / "memory.md",
    "daily_stats_file": Path(__file__).parent / "orb_trading_data" / "daily_stats.json",
}

ORB_CONFIG["data_dir"].mkdir(exist_ok=True)


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


# ============================= Helper =======================================

def is_market_hours(dt: datetime) -> bool:
    et = pytz.timezone("America/New_York")
    try:
        et_dt = et.localize(dt) if dt.tzinfo is None else dt.astimezone(et)
        return time(9, 30) <= et_dt.time() < time(16, 0)
    except Exception:
        return True

def is_trading_day(dt: datetime = None) -> bool:
    return (dt or datetime.now()).weekday() < 5

def is_orb_period(dt: datetime) -> bool:
    et = pytz.timezone("America/New_York")
    try:
        et_dt = et.localize(dt) if dt.tzinfo is None else dt.astimezone(et)
        return time(9, 30) <= et_dt.time() < time(10, 0)
    except Exception:
        return False

def get_opening_range(df: pd.DataFrame) -> Tuple[float, float, float]:
    if df.empty or len(df) < 6:
        return 0.0, 0.0, 0.0
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert("America/New_York")
    else:
        df.index = df.index.tz_convert("America/New_York")
    df["date"] = df.index.date
    valid = df.groupby("date").size()
    valid = valid[valid >= 6]
    if valid.empty:
        b = df.iloc[-1]
        return b["High"], b["Low"], b["High"] - b["Low"]
    orb_df = df[df["date"] == valid.index[-1]]
    hhmm   = orb_df.index.hour * 100 + orb_df.index.minute
    period = orb_df[(hhmm >= 930) & (hhmm < 1000)]
    if len(period) >= 2:
        h, l = period["High"].max(), period["Low"].min()
        return h, l, h - l
    b = orb_df.iloc[-1]
    return b["High"], b["Low"], b["High"] - b["Low"]

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    hl  = df["High"] - df["Low"]
    hc  = np.abs(df["High"] - df["Close"].shift())
    lc  = np.abs(df["Low"]  - df["Close"].shift())
    return pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(period).mean()

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ATR"]       = calculate_atr(df)
    df["Volume_MA"] = df["Volume"].rolling(20).mean()
    df["Volume_Ratio"] = df["Volume"] / df["Volume_MA"]
    return df


# ============================= AlpacaClient =================================

class AlpacaClient:
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
        self.trading   = TradingClient(api_key=api_key, secret_key=secret_key, paper=paper)
        self.data      = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)
        mode = "PAPER" if paper else "LIVE ⚠️"
        print(f"[Alpaca] Verbunden  Modus={mode}  Feed={data_feed}")

    # ── Marktdaten ──────────────────────────────────────────────────────────

    def fetch_bars(self, symbol: str, days: int = 5) -> pd.DataFrame:
        """5m-Bars für ein Symbol – ersetzt yfinance.fetch_intraday_data()."""
        try:
            req = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Minute5,
                start=datetime.now(pytz.UTC) - timedelta(days=days),
                end=datetime.now(pytz.UTC),
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

    def fetch_bars_bulk(self, symbols: List[str],
                        start: str, end: str) -> Dict[str, pd.DataFrame]:
        """
        Historische 5m-Bars für mehrere Symbole auf einmal.
        Ersetzt yfinance._download_chunked_5m() im Backtester.
        Alpaca hat kein 60-Tage-Limit wie yfinance → kein Chunking nötig.
        """
        result: Dict[str, pd.DataFrame] = {}
        try:
            req = StockBarsRequest(
                symbol_or_symbols=symbols,
                timeframe=TimeFrame.Minute5,
                start=datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=pytz.UTC),
                end=datetime.strptime(end,   "%Y-%m-%d").replace(tzinfo=pytz.UTC),
                adjustment="raw",
                feed=self.data_feed,
            )
            bars = self.data.get_stock_bars(req)
            if bars.df.empty:
                return result
            for sym in symbols:
                try:
                    result[sym] = self._rename(bars.df.loc[sym].copy())
                    print(f"  ✓ {sym}: {len(result[sym])} Bars")
                except KeyError:
                    print(f"  ✗ {sym}: keine Daten")
        except Exception as e:
            print(f"[Alpaca] Bulk-Fehler: {e}")
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
                            stop_loss: float, take_profit: float) -> Optional[dict]:
        """
        Long-Entry als Bracket-Order.
        Alpaca verwaltet Stop-Loss und Take-Profit serverseitig –
        _manage_position() im Bot ist für Live-Trades nicht nötig.
        """
        try:
            order = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                order_class=OrderClass.BRACKET,
                stop_loss=StopLossRequest(stop_price=round(stop_loss,    2)),
                take_profit=TakeProfitRequest(limit_price=round(take_profit, 2)),
            )
            r = self.trading.submit_order(order)
            print(f"[Alpaca] LONG  {symbol} {qty} Aktien | SL {stop_loss:.2f} | TP {take_profit:.2f} → {r.status.value}")
            return {"id": str(r.id), "symbol": symbol, "qty": qty,
                    "side": "long", "stop_loss": stop_loss,
                    "take_profit": take_profit, "status": r.status.value}
        except Exception as e:
            print(f"[Alpaca] Long-Order {symbol} fehlgeschlagen: {e}")
            return None

    def place_short_bracket(self, symbol: str, qty: int,
                             stop_loss: float, take_profit: float) -> Optional[dict]:
        """
        Short-Entry als Bracket-Order.
        stop_loss liegt ÜBER dem Entry, take_profit DARUNTER.
        Erfordert Margin-Konto + Shortability-Check.
        """
        if not self.is_shortable(symbol):
            print(f"[Alpaca] {symbol} nicht shortbar – Order abgebrochen")
            return None
        try:
            order = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.SELL,          # Sell-to-Open = Short
                time_in_force=TimeInForce.DAY,
                order_class=OrderClass.BRACKET,
                stop_loss=StopLossRequest(stop_price=round(stop_loss,    2)),
                take_profit=TakeProfitRequest(limit_price=round(take_profit, 2)),
            )
            r = self.trading.submit_order(order)
            print(f"[Alpaca] SHORT {symbol} {qty} Aktien | SL {stop_loss:.2f} | TP {take_profit:.2f} → {r.status.value}")
            return {"id": str(r.id), "symbol": symbol, "qty": qty,
                    "side": "short", "stop_loss": stop_loss,
                    "take_profit": take_profit, "status": r.status.value}
        except Exception as e:
            print(f"[Alpaca] Short-Order {symbol} fehlgeschlagen: {e}")
            return None

    def cancel_all_orders(self) -> None:
        try:
            self.trading.cancel_orders()
            print("[Alpaca] Alle offenen Orders storniert")
        except Exception as e:
            print(f"[Alpaca] Stornierungsfehler: {e}")

    def close_all_positions(self) -> None:
        """EOD: alle Positionen schließen + offene Orders stornieren."""
        try:
            self.trading.close_all_positions(cancel_orders=True)
            print("[Alpaca] Alle Positionen geschlossen (EOD)")
        except Exception as e:
            print(f"[Alpaca] EOD-Close Fehler: {e}")


# ============================= Portfolio-Ledger =============================
# Im Live-Betrieb dient ORBPortfolio nur noch als lokales Log + Tagesstatistik.
# Die eigentliche Positionsverwaltung übernimmt Alpaca (Bracket-Orders).
# Für den Backtester bleibt die vollständige virtuelle Execution erhalten.

class ORBPortfolio:
    def __init__(self, config: dict):
        self.cfg = config
        self.cfg["data_dir"].mkdir(exist_ok=True)
        self.data        = self._load()
        self.daily_stats = self._load_daily_stats()

    def _load(self) -> dict:
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
        if self.cfg["daily_stats_file"].exists():
            with open(self.cfg["daily_stats_file"]) as f:
                return json.load(f)
        return {"trades_today": 0, "pnl_today": 0.0, "wins_today": 0,
                "losses_today": 0, "win_rate_today": 0.0, "last_reset_date": None}

    def save(self):
        self.data["last_updated"] = datetime.now().isoformat()
        with open(self.cfg["portfolio_file"], "w") as f:
            json.dump(self.data, f, indent=2)

    def _save_daily_stats(self):
        with open(self.cfg["daily_stats_file"], "w") as f:
            json.dump(self.daily_stats, f, indent=2)

    def reset_daily_stats_if_needed(self):
        today = datetime.now().date().isoformat()
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
            "time":     datetime.now().isoformat(),
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
            f"**{datetime.now().strftime('%Y-%m-%d %H:%M')}** "
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

    def calculate_position_size(self, entry: float, stop: float, equity: float) -> int:
        risk = abs(entry - stop)
        if risk <= 0:
            return 0
        shares = int((equity * self.cfg["risk_per_trade"]) / risk)
        max_sh  = int((equity * self.cfg["max_equity_at_risk"]) / risk)
        return min(shares, max_sh)

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
            "time": datetime.now().isoformat(), "symbol": sym,
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
    def __init__(self, config: dict):
        self.cfg = config

    def calculate_orb_levels(self, df: pd.DataFrame) -> Tuple[float, float, float, dict]:
        orb_high, orb_low, orb_range = get_opening_range(df)
        vol     = df["Volume"].iloc[-1] if len(df) > 0 else 0
        vol_ma  = df["Volume_MA"].iloc[-1] if len(df) > 0 and not np.isnan(df["Volume_MA"].iloc[-1]) else 1
        vol_r   = vol / vol_ma if vol_ma > 0 else 0
        ctx = {
            "volume_ratio":     vol_r,
            "volume_confirmed": vol_r >= self.cfg["volume_multiplier"],
            "orb_range_pct":    (orb_range / orb_low * 100) if orb_low > 0 else 0,
            "bars_in_orb":      self._count_orb_bars(df),
        }
        return orb_high, orb_low, orb_range, ctx

    def _count_orb_bars(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        try:
            idx = df.index
            if idx.tz is None:
                idx = pd.DatetimeIndex(idx).tz_localize("UTC").tz_convert("America/New_York")
            else:
                idx = idx.tz_convert("America/New_York")
            hhmm      = idx.hour * 100 + idx.minute
            last_date = idx.date[-1]
            return int(((idx.date == last_date) & (hhmm >= 930) & (hhmm < 1000)).sum())
        except Exception:
            return 6

    def generate_signal(self, df: pd.DataFrame) -> Tuple[str, float, str, dict]:
        if len(df) < 2:
            return "HOLD", 0.0, "Insufficient data", {}

        orb_high, orb_low, orb_range, ctx = self.calculate_orb_levels(df)
        if orb_range <= 0:
            return "HOLD", 0.0, "Invalid ORB range", ctx

        current_price = df["Close"].iloc[-1]
        current_time  = (df.index[-1].to_pydatetime()
                         if hasattr(df.index[-1], "to_pydatetime") else datetime.now())

        if not is_market_hours(current_time):
            return "HOLD", 0.0, "Outside market hours", ctx

        if not is_orb_period(current_time) and current_time.time() >= self.cfg["orb_end_time"]:
            if current_price > orb_high:
                strength = min((current_price - orb_high) / orb_range, 1.0)
                if ctx["volume_confirmed"]:
                    strength = min(strength * 1.2, 1.0)
                reason = f"ORB Breakout: {current_price:.2f} > {orb_high:.2f}"
                if ctx["volume_confirmed"]:
                    reason += f" +Vol {ctx['volume_ratio']:.1f}x"
                return "BUY", strength, reason, ctx

            elif current_price < orb_low:
                strength = min((orb_low - current_price) / orb_range, 1.0)
                if ctx["volume_confirmed"]:
                    strength = min(strength * 1.2, 1.0)
                reason = f"ORB Breakdown: {current_price:.2f} < {orb_low:.2f}"
                if ctx["volume_confirmed"]:
                    reason += f" +Vol {ctx['volume_ratio']:.1f}x"
                if self.cfg.get("allow_shorts"):
                    return "SHORT", strength, reason, ctx
                return "HOLD", strength, f"[SHORT disabled] {reason}", ctx

        return "HOLD", 0.0, "Waiting for ORB breakout", ctx


# ============================= ORB_Bot (Live) ================================

class ORB_Bot:
    """
    Live-Bot: Datenabruf + Orderausführung via Alpaca.
    Positionsverwaltung (Stop / Target) übernimmt Alpaca serverseitig
    über Bracket-Orders → _manage_position() entfällt im Live-Betrieb.
    """

    def __init__(self, config: dict = None, alpaca: AlpacaClient = None):
        self.cfg       = config or ORB_CONFIG
        self.alpaca    = alpaca
        self.portfolio = ORBPortfolio(self.cfg)
        self.strategy  = ORBStrategy(self.cfg)
        self.reports_dir = self.cfg["data_dir"] / "reports"
        self.reports_dir.mkdir(exist_ok=True)

        mode = "PAPER" if (alpaca and alpaca.paper) else "LIVE" if alpaca else "KEIN ALPACA"
        print(f"ORB_Bot  Modus={mode}  Symbole={len(self.cfg['symbols'])}"
              f"  Shorts={'an' if self.cfg.get('allow_shorts') else 'aus'}")

    # ── Haupt-Scan ───────────────────────────────────────────────────────────

    def run_orb_scan(self) -> dict:
        now   = datetime.now()
        today = now.strftime("%Y-%m-%d")
        print(f"\n=== ORB Scan – {today} ===")

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

        for sym in self.cfg["symbols"]:
            df = (self.alpaca.fetch_bars(sym, days=5) if self.alpaca
                  else pd.DataFrame())
            if df.empty or len(df) < 20:
                print(f"  {sym}: keine Daten")
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

            signal, strength, reason, ctx = self.strategy.generate_signal(df)

            if signal == "BUY" and strength > 0.3:
                sig = self._execute_long(sym, df, equity, reason, strength)
                if sig:
                    signals.append(sig)

            elif signal == "SHORT" and strength > 0.3:
                sig = self._execute_short(sym, df, equity, reason, strength)
                if sig:
                    signals.append(sig)

            else:
                label = f"HOLD (Stärke {strength:.2f})" if signal in ("BUY","SHORT") else signal
                print(f"  {sym}: {label} – {reason}")

        # ── EOD-Close (< 15 Min bis Marktschluss) ───────────────────────────
        et_now    = now.astimezone(pytz.timezone("America/New_York"))
        mins_left = (16 * 60) - (et_now.hour * 60 + et_now.minute)
        if 0 < mins_left <= 15:
            print(f"  {mins_left} Min bis Schluss – EOD Close")
            if self.alpaca:
                self.alpaca.close_all_positions()
            send_telegram("ORB_Bot EOD: alle Positionen geschlossen")

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
                      equity: float, reason: str, strength: float) -> Optional[dict]:
        orb_high, orb_low, orb_range, _ = self.strategy.calculate_orb_levels(df)
        current  = df["Close"].iloc[-1]
        atr      = df["ATR"].iloc[-1] if not np.isnan(df["ATR"].iloc[-1]) else orb_range
        stop     = max(orb_low, current - 1.5 * atr)
        if stop >= current:
            stop = current - atr
        target   = current + self.cfg["profit_target_r"] * (current - stop)
        qty      = self.portfolio.calculate_position_size(current, stop, equity)
        if qty <= 0:
            print(f"  {sym}: Positionsgröße = 0 – übersprungen")
            return None

        order = self.alpaca.place_long_bracket(sym, qty, stop, target) if self.alpaca else None
        if self.alpaca and order is None:
            return None

        self.portfolio.log_order(sym, "BUY", qty, current, stop, target,
                                  alpaca_order_id=order["id"] if order else "SIM",
                                  reason=reason)
        send_telegram(f"ORB BUY {sym} {qty} @ {current:.2f} | SL {stop:.2f} | TP {target:.2f}")
        return {"symbol": sym, "action": "BUY", "qty": qty,
                "price": current, "stop": stop, "target": target,
                "strength": strength, "reason": reason}

    def _execute_short(self, sym: str, df: pd.DataFrame,
                        equity: float, reason: str, strength: float) -> Optional[dict]:
        orb_high, orb_low, orb_range, _ = self.strategy.calculate_orb_levels(df)
        current  = df["Close"].iloc[-1]
        atr      = df["ATR"].iloc[-1] if not np.isnan(df["ATR"].iloc[-1]) else orb_range
        stop     = min(orb_high, current + 1.5 * atr)
        if stop <= current:
            stop = current + atr
        target   = current - self.cfg["profit_target_r"] * (stop - current)
        qty      = self.portfolio.calculate_position_size(current, stop, equity)
        if qty <= 0:
            print(f"  {sym}: Positionsgröße = 0 – übersprungen")
            return None

        order = self.alpaca.place_short_bracket(sym, qty, stop, target) if self.alpaca else None
        if self.alpaca and order is None:
            return None

        self.portfolio.log_order(sym, "SHORT", qty, current, stop, target,
                                  alpaca_order_id=order["id"] if order else "SIM",
                                  reason=reason)
        send_telegram(f"ORB SHORT {sym} {qty} @ {current:.2f} | SL {stop:.2f} | TP {target:.2f}")
        return {"symbol": sym, "action": "SHORT", "qty": qty,
                "price": current, "stop": stop, "target": target,
                "strength": strength, "reason": reason}

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


# ============================= Backtester ===================================
# Nutzt Alpaca für historische Daten, virtuelle Execution (kein echtes Geld).

class ORB_Backtester:
    def __init__(self, config: dict = None, alpaca: AlpacaClient = None):
        self.cfg       = config or ORB_CONFIG
        self.alpaca    = alpaca
        self.portfolio = ORBPortfolio(self.cfg)
        self.strategy  = ORBStrategy(self.cfg)
        self.commission = 0.00005
        self.slippage   = 0.0002

    def run_backtest(self, start_date: str = "2024-01-01", end_date: str = None):
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        print(f"\n=== ORB Backtest {start_date} → {end_date} ===")

        # ── Daten laden ──────────────────────────────────────────────────────
        # Nur Stocks (keine Futures – Alpaca liefert keine)
        tradeable = self.cfg["symbols"]
        if self.alpaca:
            print("Lade Daten via Alpaca...")
            raw = self.alpaca.fetch_bars_bulk(tradeable, start_date, end_date)
        else:
            print("[WARN] Kein Alpaca-Client – kein Datenabruf möglich")
            return []

        data_cache = {s: compute_indicators(df)
                      for s, df in raw.items() if len(df) > 100}
        if not data_cache:
            print("Keine Daten – Abbruch")
            return []

        # ── Portfolio zurücksetzen ───────────────────────────────────────────
        self.portfolio.data.update({
            "cash": self.cfg.get("initial_capital", 10000.0),
            "positions": {}, "short_positions": {},
            "trades": [], "equity_curve": [],
        })

        # ── Bar-by-Bar-Simulation ────────────────────────────────────────────
        for current_date in pd.date_range(start_date, end_date, freq="B"):
            if self.cfg.get("avoid_fridays") and current_date.weekday() == 4:
                continue
            if self.cfg.get("avoid_mondays") and current_date.weekday() == 0:
                continue

            price_dict: Dict[str, float] = {}

            for sym, df_full in data_cache.items():
                day_df = df_full[df_full.index.date == current_date.date()].sort_index()
                if day_df.empty or len(day_df) < 8:
                    continue

                idx_et = (day_df.index.tz_localize("UTC") if day_df.index.tz is None
                          else day_df.index).tz_convert("America/New_York")
                hhmm   = idx_et.hour * 100 + idx_et.minute

                if (hhmm >= 930) & (hhmm < 1000):
                    if ((hhmm >= 930) & (hhmm < 1000)).sum() < 2:
                        continue

                post_orb = day_df[hhmm >= 1000]
                if post_orb.empty:
                    continue

                entered = False

                for bar_idx, bar in post_orb.iterrows():
                    current_price       = bar["Close"]
                    price_dict[sym]     = current_price

                    if self.portfolio.has_pos(sym):
                        closed = self._manage_bar(sym, self.portfolio.get_pos(sym), bar)
                        if closed:
                            break
                        continue

                    if entered or not self.portfolio.can_trade_today():
                        continue

                    bars_so_far = day_df.loc[:bar_idx]
                    orb_high, orb_low, orb_range, _ = self.strategy.calculate_orb_levels(bars_so_far)
                    if orb_range <= 0:
                        continue

                    signal, strength, reason, _ = self.strategy.generate_signal(bars_so_far)
                    if signal == "BUY" and strength > 0.3:
                        entry = current_price * (1 + self.slippage)
                        atr   = bars_so_far["ATR"].iloc[-1]
                        if np.isnan(atr):
                            atr = orb_range
                        stop  = max(orb_low, entry - 1.5 * atr)
                        if stop >= entry:
                            stop = entry - atr
                        qty   = self.portfolio.calculate_position_size(
                                    entry, stop, self.portfolio.equity(price_dict))
                        if qty > 0:
                            cost = entry * qty * (1 + self.commission)
                            if cost <= self.portfolio.data["cash"]:
                                self.portfolio.buy(sym, entry, qty, stop, reason)
                                entered = True
                        break

            self.portfolio.data["equity_curve"].append({
                "date": current_date.strftime("%Y-%m-%d"),
                "equity": self.portfolio.equity(price_dict),
            })

        self._print_results()
        return self.portfolio.data["trades"]

    def _manage_bar(self, sym: str, pos: dict, bar: pd.Series) -> bool:
        """Bar-by-Bar Exit-Logik für Backtester."""
        entry = pos["entry"]
        stop  = pos["stop_loss"]
        risk  = entry - stop
        if risk <= 0:
            self.portfolio.sell(sym, bar["Close"], pos["shares"], "Invalid risk")
            return True

        target = entry + self.cfg["profit_target_r"] * risk

        if bar["Low"] <= stop:
            ep = stop * (1 - self.slippage)
            self.portfolio.sell(sym, ep, pos["shares"], "Stop Loss")
            return True
        if bar["High"] >= target:
            ep = target * (1 - self.slippage)
            self.portfolio.sell(sym, ep, pos["shares"], "Profit Target")
            return True

        # Trailing Stop
        r_mult = (bar["Close"] - entry) / risk
        if r_mult >= self.cfg["trail_after_r"]:
            trail = bar["Close"] - self.cfg["trail_distance_r"] * risk
            if trail > (pos.get("trail_stop") or stop):
                pos["trail_stop"] = trail
        if pos.get("trail_stop") and bar["Low"] <= pos["trail_stop"]:
            ep = pos["trail_stop"] * (1 - self.slippage)
            self.portfolio.sell(sym, ep, pos["shares"], "Trailing Stop")
            return True

        pos["price"] = bar["Close"]
        return False

    def _print_results(self):
        trades = self.portfolio.data["trades"]
        if not trades:
            print("Keine Trades.")
            return
        df   = pd.DataFrame(trades)
        wins = df[df["pnl"] > 0]
        init = self.cfg.get("initial_capital", 10000.0)
        eq   = self.portfolio.equity()
        ret  = (eq / init - 1) * 100
        wr   = len(wins) / len(df) * 100
        gp   = wins["pnl"].sum()
        gl   = df[df["pnl"] < 0]["pnl"].sum()
        pf   = abs(gp / gl) if gl != 0 else float("inf")
        ec   = pd.DataFrame(self.portfolio.data["equity_curve"])
        ec["date"] = pd.to_datetime(ec["date"])
        ec.set_index("date", inplace=True)
        mdd  = ((ec["equity"] / ec["equity"].cummax()) - 1).min() * 100
        print("\n" + "="*60)
        print("BACKTEST ERGEBNIS")
        print("="*60)
        print(f"Startkapital  : {init:,.0f}")
        print(f"Endkapital    : {eq:,.0f}")
        print(f"Rendite       : {ret:+.2f} %")
        print(f"Trades        : {len(df)}")
        print(f"Win-Rate      : {wr:.1f} %")
        print(f"Profit-Faktor : {pf:.2f}")
        print(f"Max. Drawdown : {mdd:.2f} %")
        print(f"Ø Trade       : {df['pnl'].mean():+.2f}")
        print("="*60)


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

    # APCA_PAPER=false → Live; alles andere → Paper
    paper_env = os.getenv("APCA_PAPER", "true").lower()
    paper     = paper_env != "false"

    # cfg-Wert als Fallback, aber Env-Var hat Vorrang
    paper     = paper and cfg.get("alpaca_paper", True)

    feed = os.getenv("APCA_DATA_FEED", cfg.get("alpaca_data_feed", "iex"))

    return AlpacaClient(api_key=key, secret_key=secret, paper=paper, data_feed=feed)


def main():
    parser = argparse.ArgumentParser(
        description="ORB_Bot – Opening Range Breakout (Alpaca Edition)",
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
                        help="Live-Modus – überschreibt APCA_PAPER=true")
    args = parser.parse_args()

    cfg = dict(ORB_CONFIG)
    if args.shorts:
        cfg["allow_shorts"] = True
    if args.live:
        cfg["alpaca_paper"] = False
        os.environ["APCA_PAPER"] = "false"

    alpaca = _build_alpaca_client(cfg)

    # ── Modus-Ausführung ─────────────────────────────────────────────────────

    if args.mode == "scan":
        bot    = ORB_Bot(config=cfg, alpaca=alpaca)
        result = bot.run_orb_scan()
        print(json.dumps(result, indent=2, default=str))

    elif args.mode == "status":
        bot    = ORB_Bot(config=cfg, alpaca=alpaca)
        status = bot.get_status()
        print(json.dumps(status, indent=2, default=str))

    elif args.mode == "eod":
        if alpaca:
            alpaca.close_all_positions()
            send_telegram("ORB_Bot: manueller EOD-Close ausgeführt")
        else:
            print("[ERROR] Kein Alpaca-Client – EOD nicht möglich", file=sys.stderr)
            sys.exit(1)

    elif args.mode == "backtest":
        cfg["initial_capital"] = 10000.0
        tester = ORB_Backtester(config=cfg, alpaca=alpaca)
        tester.run_backtest(start_date=args.start, end_date=args.end)


if __name__ == "__main__":
    main()
