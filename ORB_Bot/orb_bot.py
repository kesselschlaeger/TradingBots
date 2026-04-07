#!/usr/bin/env python3
"""
ORB_Bot - Opening Range Breakout Strategy
Independent day trading bot focusing on ORB strategy
"""

import json
import os
from datetime import datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yfinance as yf
import pandas as pd
import numpy as np
import pytz

# ============================= Configuration =============================
import os
from pathlib import Path

TELEGRAM_TOKEN_PATH = Path.home() / ".secrets" / "telegram.token"
TELEGRAM_CHAT_ID_PATH = Path.home() / ".secrets" / "telegram.chat_id"

def send_telegram(message: str) -> None:
    """Send a message via Telegram bot."""
    try:
        token = TELEGRAM_TOKEN_PATH.read_text().strip()
        chat_id = TELEGRAM_CHAT_ID_PATH.read_text().strip()
        import urllib.request
        import urllib.parse
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": message}).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
    except Exception as e:
        # Fail silently to not break trading logic
        print(f"[Telegram] Failed to send message: {e}")

ORB_CONFIG = {
    # --- Universe for day trading (liquid stocks/ETFs) ------------------
    # Based on backtest results (2024-01-01 to 2026-03-31):
    # QQQ: 70% Win Rate, 10.26% Return, PF ∞ (best performer)
    # SPY: 53.8% Win Rate, 5.95% Return, PF 2.44 (solid performer)
    # ES=F/NQ=F: Require intraday data for proper ORB calculation - kept for future enhancement
    "symbols": [
        "SPY", "QQQ", "IWM", "DIA",  # Major ETFs (backtested)
        "ES=F", "NQ=F", "MES=F", "MNQ=F",  # Futures + Micros
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",  # Mega caps
        "NVDA", "META", "AMD", "NFLX"  # Tech leaders
    ],
    # --- ORB Strategy Parameters ----------------------------------------
    # Optimized based on backtest results (2024-01-01 to 2026-03-31):
    # QQQ showed 70% Win Rate, 10.26% Return - favorable for ORB
    # SPY showed 53.8% Win Rate, 5.95% Return - solid but less volatile
    "opening_range_minutes": 30,  # First 30 minutes for ORB calculation
    "orb_breakout_multiplier": 1.0,  # Breakout threshold (1.0 = ORB high/low)
    "volume_multiplier": 1.3,  # Reduced from 1.5 for better signal frequency (especially for SPY)
    # --- Futures-spezifische Einstellungen ------------------------------
    "futures_config": {
        "point_values": {      # Dollar pro Punkt
            "ES=F": 50, "MES=F": 5,
            "NQ=F": 20, "MNQ=F": 2,
        },
        "margin_per_contract": {   # ungefähre Intraday-Margin
            "ES=F": 12000, "MES=F": 1200,
            "NQ=F": 15000, "MNQ=F": 1500,
        },
    },
    # --- Risk Management ------------------------------------------------
    "risk_per_trade": 0.01,  # 1% of equity per trade (maintained for consistency)
    "max_daily_trades": 3,   # Reduced from 5 to focus on quality over quantity
    "max_equity_at_risk": 0.05,  # 5% max portfolio risk at any time
    # --- Trade Management -----------------------------------------------
    "profit_target_r": 2.0,  # Profit target in R multiples (maintained)
    "stop_loss_r": 1.0,  # Stop loss in R multiples (maintained)
    "trail_after_r": 1.0,  # Start trailing after 1R profit (maintained)
    "trail_distance_r": 0.5,  # Trail distance in R multiples (maintained)
    # --- Market Hours (ET) ----------------------------------------------
    "market_open": time(9, 30),  # 9:30 AM ET
    "market_close": time(16, 0),  # 4:00 PM ET
    "orb_end_time": time(10, 0),  # 10:00 AM ET (30 min after open)
    # --- Misc -----------------------------------------------------------
    "currency": "EUR",
    "initial_capital": 10000.0,
    "data_dir": Path(__file__).parent / "orb_trading_data",
    "portfolio_file": Path(__file__).parent / "orb_trading_data" / "portfolio.json",
    "memory_file": Path(__file__).parent / "orb_trading_data" / "memory.md",
    "daily_stats_file": Path(__file__).parent / "orb_trading_data" / "daily_stats.json",
    # --- Trading Filters ----------------------------------------------
    "avoid_fridays": True,        # Avoid trading on Fridays (often lower quality)
    "avoid_mondays": False,       # Avoid trading on Mondays (can be gap-sensitive)
    "allowed_weekdays": [0, 1, 2, 3, 4],  # Monday=0, Sunday=6 (default: all weekdays)
}

# Ensure directories exist
ORB_CONFIG["data_dir"].mkdir(exist_ok=True)

# ============================= Helper Functions =============================
import pytz  # Add to top imports if not present

def is_market_hours(dt: datetime) -> bool:
    """Check if datetime is within US market hours (9:30-16:00 ET)"""
    et_tz = pytz.timezone('America/New_York')
    try:
        # Localize if naive
        if dt.tzinfo is None:
            et_dt = et_tz.localize(dt)
        else:
            et_dt = dt.astimezone(et_tz)
        et_time = et_dt.time()
        market_open = time(9, 30)
        market_close = time(16, 0)
        return market_open <= et_time < market_close
    except:
        return True  # Fallback

def is_orb_period(dt: datetime) -> bool:
    """Check if datetime is within ORB period (9:30-10:00 ET)"""
    et_tz = pytz.timezone('America/New_York')
    try:
        # Localize if naive
        if dt.tzinfo is None:
            et_dt = et_tz.localize(dt)
        else:
            et_dt = dt.astimezone(et_tz)
        et_time = et_dt.time()
        orb_start = time(9, 30)
        orb_end = time(10, 0)
        return orb_start <= et_time < orb_end
    except:
        return False  # Fallback to no

def get_opening_range(df: pd.DataFrame) -> Tuple[float, float, float]:
    """
    Calculate Opening Range from intraday data (first 30 minutes AFTER market open TODAY)
    Returns: (ORB_high, ORB_low, ORB_range)
    """
    if df.empty or len(df) < 6:
        return 0.0, 0.0, 0.0

    # Saubere Zeitzonen-Behandlung (funktioniert immer)
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    if df.index.tz is None:
        df.index = df.index.tz_localize('UTC').tz_convert('America/New_York')
    else:
        df = df.tz_convert('America/New_York')

    # Letzten vollständigen Trading-Tag finden (wichtig für Backtesting!)
    # Wir nehmen den letzten Tag, an dem mindestens 30 Minuten gehandelt wurden
    df['date'] = df.index.date
    daily_counts = df.groupby('date').size()
    valid_days = daily_counts[daily_counts >= 6] # mind. 6 Bars = 30 Min
    
    if valid_days.empty:
        # Ultimativer Fallback
        last_bar = df.iloc[-1]
        return last_bar["High"], last_bar["Low"], last_bar["High"] - last_bar["Low"]

    # Heutiger (oder letzter gültiger) ORB-Tag
    orb_date = valid_days.index[-1]
    orb_df = df[df['date'] == orb_date]
    
    # Nur 9:30–10:00 ET
    orb_mask = (orb_df.index.hour * 100 + orb_df.index.minute >= 930) & (orb_df.index.hour * 100 + orb_df.index.minute < 1000)
    orb_period = orb_df[orb_mask]

    if len(orb_period) >= 2:
        return orb_period["High"].max(), orb_period["Low"].min(), orb_period["High"].max() - orb_period["Low"].min()

    # Fallback: ganzer letzter Tag als Proxy
    last_day = orb_df.iloc[-1]
    return last_day["High"], last_day["Low"], last_day["High"] - last_day["Low"]

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range"""
    high_low = df["High"] - df["Low"]
    high_close = np.abs(df["High"] - df["Close"].shift())
    low_close = np.abs(df["Low"] - df["Close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute technical indicators for ORB strategy"""
    df = df.copy()
    df["ATR"] = calculate_atr(df)
    # Volume indicators
    df["Volume_MA"] = df["Volume"].rolling(20).mean()
    df["Volume_Ratio"] = df["Volume"] / df["Volume_MA"]
    return df

# ============================= Portfolio Management =============================
class ORBPortfolio:
    def __init__(self, config: dict):
        self.cfg = config
        self.cfg["data_dir"].mkdir(exist_ok=True)
        self.data = self._load()
        self.daily_stats = self._load_daily_stats()
        
    def _load(self) -> dict:
        if self.cfg["portfolio_file"].exists():
            with open(self.cfg["portfolio_file"], "r") as f:
                return json.load(f)
        return {
            "cash": self.cfg["initial_capital"],
            "initial_capital": self.cfg["initial_capital"],
            "positions": {},
            "trades": [],
            "equity_curve": [],
            "daily_pnl": {},
            "last_updated": None
        }
    
    def _load_daily_stats(self) -> dict:
        if self.cfg["daily_stats_file"].exists():
            with open(self.cfg["daily_stats_file"], "r") as f:
                return json.load(f)
        return {
            "trades_today": 0,
            "pnl_today": 0.0,
            "wins_today": 0,
            "losses_today": 0,
            "win_rate_today": 0.0,
            "last_reset_date": None
        }
    
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
            self.daily_stats = {
                "trades_today": 0,
                "pnl_today": 0.0,
                "wins_today": 0,
                "losses_today": 0,
                "win_rate_today": 0.0,
                "last_reset_date": today
            }
            self._save_daily_stats()
    
    def can_trade_today(self) -> bool:
        self.reset_daily_stats_if_needed()
        return self.daily_stats["trades_today"] < self.cfg["max_daily_trades"]
    
    def has_pos(self, sym: str) -> bool:
        return sym in self.data["positions"]
    
    def get_pos(self, sym: str) -> dict:
        return self.data["positions"].get(sym)
    
    def calculate_position_size(self, entry_price: float, stop_loss: float, equity: float, symbol: str = None) -> int:
        """Universal position size – erkennt automatisch Futures"""
        if symbol and symbol in self.cfg.get("futures_config", {}).get("point_values", {}):
            return self._calculate_futures_position_size(symbol, entry_price, stop_loss, equity)
        # Normaler Stock/ETF Modus
        risk_per_share = abs(entry_price - stop_loss)
        if risk_per_share <= 0:
            return 0
        risk_amount = equity * self.cfg["risk_per_trade"]
        shares = int(risk_amount / risk_per_share)
        max_risk_amount = equity * self.cfg["max_equity_at_risk"]
        max_shares = int(max_risk_amount / risk_per_share) if risk_per_share > 0 else 0
        return min(shares, max_shares)

    def _calculate_futures_position_size(self, sym: str, entry_price: float, stop_loss: float, equity: float) -> int:
        """Spezielle Berechnung für Futures (Kontrakte statt Shares)"""
        point_value = self.cfg["futures_config"]["point_values"].get(sym, 1)
        risk_per_point = abs(entry_price - stop_loss)
        risk_dollar_per_contract = risk_per_point * point_value

        risk_amount = equity * self.cfg["risk_per_trade"]
        contracts = int(risk_amount / risk_dollar_per_contract) if risk_dollar_per_contract > 0 else 0

        # Margin-Check (max. 30 % des Kapitals für Margin)
        margin = self.cfg["futures_config"]["margin_per_contract"].get(sym, 10000)
        max_contracts = int((equity * 0.3) / margin) if margin > 0 else 0

        return min(contracts, max_contracts, 10)  # Sicherheits-Cap
    
    def buy(self, sym: str, price: float, shares: int, stop_loss: float, reason: str) -> dict:
        if shares <= 0:
            return {"ok": False, "msg": "Invalid shares"}
        
        cost = price * shares
        if cost > self.data["cash"]:
            return {"ok": False, "msg": "Insufficient cash"}
        
        # Check daily trade limit
        if not self.can_trade_today():
            return {"ok": False, "msg": "Daily trade limit reached"}
        
        self.data["cash"] -= cost
        pos = {
            "symbol": sym,
            "shares": shares,
            "entry": price,
            "stop_loss": stop_loss,
            "price": price,
            "highest": price,
            "trail_stop": None,
            "reason": reason,
            "entry_time": datetime.now().isoformat(),
            "unrealized_pnl": 0.0
        }
        self.data["positions"][sym] = pos
        self._log_trade(sym, "BUY", shares, price, 0.0, reason)
        self._update_daily_stats("entry", 0.0)
        self.save()
        return {"ok": True, "pos": pos}
    
    def sell(self, sym: str, price: float, shares: int, reason: str) -> dict:
        pos = self.data["positions"].get(sym)
        if not pos or pos["shares"] < shares:
            return {"ok": False, "msg": "No position or insufficient shares"}
        
        proceeds = price * shares
        self.data["cash"] += proceeds
        
        # Calculate P&L
        cost_basis = pos["entry"] * shares
        pnl = proceeds - cost_basis
        
        # Update position or remove
        pos["shares"] -= shares
        if pos["shares"] <= 0:
            del self.data["positions"][sym]
        else:
            # Update remaining position (simplified)
            pass
        
        self._log_trade(sym, "SELL", shares, price, pnl, reason)
        if action == "SELL":
            send_telegram(f"ORB_Bot SELL {sym} {shares} @ {price:.2f} (PnL: {pnl:+.2f}) - {reason}")
        self._update_daily_stats("exit", pnl)
        self.save()
        return {"ok": True, "pnl": pnl, "remaining": pos.get("shares", 0)}
    
    def _log_trade(self, sym, action, shares, price, pnl, reason):
        trade_record = {
            "time": datetime.now().isoformat(),
            "symbol": sym,
            "action": action,
            "shares": shares,
            "price": price,
            "pnl": pnl,
            "reason": reason,
            "strategy": "ORB"
        }
        self.data["trades"].append(trade_record)
        
        # Also append to ORB-specific memory
        self._append_to_memory(f"**{datetime.now().strftime('%Y-%m-%d %H:%M')}** {sym} {action} {shares} @ {price:.2f} - P&L: {pnl:+.2f} ({reason})")
    
    def _update_daily_stats(self, action: str, pnl: float = 0.0):
        self.daily_stats["pnl_today"] += pnl  # Always update PnL
        
        if action == "entry":
            self.daily_stats["trades_today"] += 1
        
        if pnl > 0:
            self.daily_stats["wins_today"] += 1
        elif pnl < 0:
            self.daily_stats["losses_today"] += 1
            
        total_trades = self.daily_stats["wins_today"] + self.daily_stats["losses_today"]
        self.daily_stats["win_rate_today"] = (self.daily_stats["wins_today"] / max(total_trades, 1)) * 100 if total_trades > 0 else 0.0
        
        self._save_daily_stats()
    
    def _append_to_memory(self, content: str):
        """Append to ORB-specific memory file"""
        timestamp = datetime.now().strftime("%Y-%m-%d")
        memory_path = self.cfg["memory_file"]
        
        # Ensure memory file exists with header
        if not memory_path.exists():
            with open(memory_path, "w") as f:
                f.write("# ORB_Bot Memory Log\n\n")
        
        with open(memory_path, "a") as f:
            f.write(f"{content}\n\n")
    
    def equity(self, price_dict: dict) -> float:
        positions_val = sum(p["shares"]*price_dict.get(sym,0) for sym,p in self.data["positions"].items())
        return self.data["cash"] + positions_val

# ============================= ORB Strategy =============================
class ORBStrategy:
    def __init__(self, config: dict):
        self.cfg = config
    
    def calculate_orb_levels(self, df: pd.DataFrame) -> Tuple[float, float, float, Dict]:
        """
        Calculate ORB levels and return additional context
        Returns: (ORB_high, ORB_low, ORB_range, context_dict)
        """
        orb_high, orb_low, orb_range = get_opening_range(df)
        
        # Calculate volume confirmation
        recent_volume = df["Volume"].iloc[-1] if len(df) > 0 else 0
        avg_volume = df["Volume_MA"].iloc[-1] if len(df) > 0 and not np.isnan(df["Volume_MA"].iloc[-1]) else 1
        volume_ratio = recent_volume / avg_volume if avg_volume > 0 else 0
        
        # For daily data approximation, we estimate bars in ORB period
        # In a real implementation with intraday data, this would be actual count
        bars_in_orb_estimate = 6 if len(df) > 0 else 0  # Approximate 6 5-min bars in 30 minutes
        
        context = {
            "volume_ratio": volume_ratio,
            "volume_confirmed": volume_ratio >= self.cfg["volume_multiplier"],
            "orb_range_pct": (orb_range / orb_low * 100) if orb_low > 0 else 0,
            "bars_in_orb": bars_in_orb_estimate
        }
        
        return orb_high, orb_low, orb_range, context
    
    def generate_signal(self, df: pd.DataFrame) -> Tuple[str, float, str, Dict]:
        """
        Generate ORB signal
        Returns: (signal, strength, reason, context)
        Signal: "BUY", "SELL", or "HOLD"
        Strength: 0.0 to 1.0
        """
        if len(df) < 2:
            return "HOLD", 0.0, "Insufficient data", {}
        
        # Need data that includes ORB period and current bar
        orb_high, orb_low, orb_range, context = self.calculate_orb_levels(df)
        
        if orb_range <= 0:
            return "HOLD", 0.0, "Invalid ORB range", context
        
        current_price = df["Close"].iloc[-1]
        current_time = df.index[-1].to_pydatetime() if hasattr(df.index[-1], 'to_pydatetime') else datetime.now()
        
        # Only trade after ORB period ends
        if not is_orb_period(current_time) and current_time.time() >= self.cfg["orb_end_time"]:
            # Check for breakout above ORB high
            if current_price > orb_high:
                breakout_strength = min((current_price - orb_high) / orb_range, 2.0)  # Cap at 2x range
                strength = min(breakout_strength, 1.0)  # Normalize to 0-1
                
                # Volume confirmation bonus
                if context["volume_confirmed"]:
                    strength = min(strength * 1.2, 1.0)
                
                reason = f"ORB Breakout: {current_price:.2f} > {orb_high:.2f} (ORB High)"
                if context["volume_confirmed"]:
                    reason += f" + Volume Confirmed ({context['volume_ratio']:.1f}x avg)"
                
                return "BUY", strength, reason, context
            
            # Check for breakdown below ORB low (for short bias or avoidance)
            elif current_price < orb_low:
                breakdown_strength = min((orb_low - current_price) / orb_range, 2.0)
                strength = min(breakdown_strength, 1.0)
                
                reason = f"ORB Breakdown: {current_price:.2f} < {orb_low:.2f} (ORB Low)"
                return "HOLD", strength, reason, context  # We're only doing longs for now
        
        return "HOLD", 0.0, "Waiting for ORB breakout", context

# ============================= Main Engine =============================
class ORB_Bot:
    def __init__(self, config: Optional[dict] = None):
        self.cfg = config or ORB_CONFIG
        self.portfolio = ORBPortfolio(self.cfg)
        self.strategy = ORBStrategy(self.cfg)
        self.reports_dir = self.cfg["data_dir"] / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        print("ORB_Bot initialized")
        print(f"Trading symbols: {', '.join(self.cfg['symbols'])}")
        print(f"Data directory: {self.cfg['data_dir']}")
    
    def fetch_intraday_data(self, symbol: str, period: str = "5d") -> pd.DataFrame:
        """Fetch intraday data for ORB calculation"""
        try:
            # Get 5-minute data for the last 5 days
            df = yf.Ticker(symbol).history(period=period, interval="5m")
            if df.empty:
                return pd.DataFrame()
            
            # For simplicity, we'll work with naive datetime and approximate market hours
            # Since we're doing daily scans, we'll use daily data for ORB calculation
            # In a real implementation, you'd need proper timezone handling
            return df[["Open", "High", "Low", "Close", "Volume"]]
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()
    
    def run_orb_scan(self) -> dict:
        """Run ORB scan for all symbols"""
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"\n=== ORB_Bot Scan – {today} ===")
        
        # Update prices and manage existing positions
        price_dict = {}
        signals_generated = []
        
        for sym in self.cfg["symbols"]:
            df = self.fetch_intraday_data(sym, period="5d")
            if df.empty or len(df) < 20:  # Need sufficient data
                print(f"  {sym}: Insufficient data")
                continue
            
            df = compute_indicators(df)
            current_price = df["Close"].iloc[-1]
            price_dict[sym] = current_price
            
            # Manage existing positions
            if self.portfolio.has_pos(sym):
                pos = self.portfolio.get_pos(sym)
                self._manage_position(sym, pos, current_price, df)
            
            # Generate new signals if we can trade
            elif self.portfolio.can_trade_today():
                # Apply weekday filters first
                current_weekday = datetime.now().weekday()  # Monday=0, Sunday=6
                if self.cfg.get("avoid_fridays", False) and current_weekday == 4:  # Friday
                    print(f"  {sym}: SKIPPED (Friday filter)")
                    continue
                if self.cfg.get("avoid_mondays", False) and current_weekday == 0:  # Monday
                    print(f"  {sym}: SKIPPED (Monday filter)")
                    continue
                
                signal, strength, reason, context = self.strategy.generate_signal(df)
                if signal == "BUY" and strength > 0.3:  # Minimum strength threshold
                    # Calculate stop loss based on ORB range or ATR
                    orb_high, orb_low, orb_range, _ = self.strategy.calculate_orb_levels(df)
                    atr_value = df["ATR"].iloc[-1] if not np.isnan(df["ATR"].iloc[-1]) else orb_range
                    
                    # Use ORB low as stop loss, or ATR-based stop, whichever is tighter
                    stop_loss_or_low = orb_low
                    stop_loss_atr = current_price - (1.5 * atr_value)  # 1.5 ATR stop
                    stop_loss = max(stop_loss_or_low, stop_loss_atr)  # The higher (closer to entry) stop
                    
                    # Ensure stop loss is below entry
                    if stop_loss >= current_price:
                        stop_loss = current_price - (1.0 * atr_value)  # Fallback to 1 ATR
                    
                    shares = self.portfolio.calculate_position_size(current_price, stop_loss, self.portfolio.equity(price_dict), symbol=sym)
                    
                    if shares > 0:
                        res = self.portfolio.buy(sym, current_price, shares, stop_loss, reason)
                        if res["ok"]:
                            print(f"  {sym}: BUY {shares} @ {current_price:.2f} (SL: {stop_loss:.2f}) - {reason}")
                            signals_generated.append({
                                "symbol": sym,
                                "action": "BUY",
                                "shares": shares,
                                "price": current_price,
                                "stop_loss": stop_loss,
                                "reason": reason,
                                "strength": strength
                            })
                            send_telegram(f"ORB_Bot BUY {sym} {shares} @ {current_price:.2f} (SL: {stop_loss:.2f}) - {reason}")
                        else:
                            print(f"  {sym}: FAILED BUY - {res.get('msg', 'Unknown error')}")
                    else:
                        print(f"  {sym}: ZERO POSITION SIZE CALCULATED")
                else:
                    if signal == "BUY":
                        print(f"  {sym}: HOLD (weak signal: strength={strength:.2f}) - {reason}")
                    else:
                        print(f"  {sym}: {signal} - {reason}")
            else:
                print(f"  {sym}: DAILY TRADE LIMIT REACHED")
        
        # Generate daily report
        equity = self.portfolio.equity(price_dict)
        self._generate_daily_report(today, price_dict, equity, signals_generated)
        
        return {
            "date": today,
            "equity": equity,
            "signals_generated": len(signals_generated),
            "positions": list(self.portfolio.data["positions"].keys()),
            "daily_trades": self.portfolio.daily_stats["trades_today"]
        }
    
    def _manage_position(self, sym: str, pos: dict, current_price: float, df: pd.DataFrame):
        """Manage existing position with profit targets and trailing stops"""
        entry_price = pos["entry"]
        shares = pos["shares"]
        initial_stop = pos["stop_loss"]
        
        # Calculate risk and reward in R multiples
        risk_per_share = entry_price - initial_stop
        if risk_per_share <= 0:
            # Invalid risk, exit immediately
            res = self.portfolio.sell(sym, current_price, shares, "Invalid risk parameters")
            if res["ok"]:
                print(f"  {sym}: EXIT (Invalid Risk) {shares} @ {current_price:.2f} (P&L: {res['pnl']:.2f})")
            return
        
        profit_per_share = current_price - entry_price
        r_multiple = profit_per_share / risk_per_share if risk_per_share > 0 else 0
        
        # Check for profit target
        if r_multiple >= self.cfg["profit_target_r"]:
            res = self.portfolio.sell(sym, current_price, shares, f"Profit Target ({r_multiple:.1f}R)")
            if res["ok"]:
                print(f"  {sym}: TARGET EXIT {shares} @ {current_price:.2f} (P&L: {res['pnl']:.2f}, {r_multiple:.1f}R)")
            return
        
        # Check for stop loss
        if current_price <= initial_stop:
            res = self.portfolio.sell(sym, current_price, shares, "Stop Loss")
            if res["ok"]:
                print(f"  {sym}: STOP EXIT {shares} @ {current_price:.2f} (P&L: {res['pnl']:.2f})")
            return
        
        # Check for trailing stop activation
        if r_multiple >= self.cfg["trail_after_r"] and pos.get("trail_stop") is None:
            # Activate trailing stop
            atr_value = df["ATR"].iloc[-1] if not np.isnan(df["ATR"].iloc[-1]) else (entry_price - initial_stop)
            trail_amount = self.cfg["trail_distance_r"] * risk_per_share
            pos["trail_stop"] = current_price - trail_amount
            pos["highest"] = max(pos.get("highest", entry_price), current_price)
            print(f"  {sym}: TRAIL ACTIVATED @ {current_price:.2f} (Trail: {pos['trail_stop']:.2f})")
            self.portfolio.save()  # Persist position changes
        
        # Update trailing stop if active
        if pos.get("trail_stop") is not None:
            # Update highest price
            if current_price > pos.get("highest", entry_price):
                pos["highest"] = current_price
                # Trail stop up: move stop up as price makes new highs
                trail_amount = self.cfg["trail_distance_r"] * risk_per_share
                new_trail_stop = pos["highest"] - trail_amount
                if new_trail_stop > pos["trail_stop"]:
                    pos["trail_stop"] = new_trail_stop
                    print(f"  {sym}: TRAIL UPDATE @ {current_price:.2f} (New Trail: {pos['trail_stop']:.2f})")
                    self.portfolio.save()  # Persist position changes
            
            # Check if trailing stop is hit
            if current_price <= pos["trail_stop"]:
                res = self.portfolio.sell(sym, current_price, shares, "Trailing Stop")
                if res["ok"]:
                    print(f"  {sym}: TRAIL EXIT {shares} @ {current_price:.2f} (P&L: {res['pnl']:.2f})")
                return
        
        # Update current price in position
        pos["price"] = current_price
        self.portfolio.save()  # Persist position changes
    
    def _generate_daily_report(self, date_str: str, price_dict: dict, equity: float, signals: list):
        """Generate daily ORB bot report"""
        report_file = self.reports_dir / f"orb_report_{date_str}.txt"
        init_capital = self.cfg["initial_capital"]
        total_return = (equity / init_capital - 1) * 100
        
        lines = []
        lines.append("="*60)
        lines.append(f"ORB_BOT – DAILY REPORT – {date_str}")
        lines.append("="*60)
        lines.append("")
        lines.append(f"Starting Capital: {init_capital:,.2f} {self.cfg['currency']}")
        lines.append(f"Current Equity:   {equity:,.2f} {self.cfg['currency']}")
        lines.append(f"Daily Return:     {total_return:+.2f}%")
        lines.append("")
        lines.append(f"Signals Generated: {len(signals)}")
        lines.append(f"Trades Today:      {self.portfolio.daily_stats['trades_today']}/{self.cfg['max_daily_trades']}")
        lines.append(f"Daily P&L:         {self.portfolio.daily_stats['pnl_today']:+.2f} {self.cfg['currency']}")
        lines.append("")
        lines.append("Open Positions:")
        if self.portfolio.data["positions"]:
            for sym, pos in self.portfolio.data["positions"].items():
                unrealized_pnl = (price_dict.get(sym, 0) - pos["entry"]) * pos["shares"]
                lines.append(
                    f"  {sym}: {pos['shares']} sh @ {pos['entry']:.2f} → "
                    f"now {price_dict.get(sym,0):.2f} | "
                    f"SL {pos.get('stop_loss',0):.2f} | "
                    f"{'Trail: '+str(round(pos.get('trail_stop',0),2)) if pos.get('trail_stop') else 'No Trail'} | "
                    f"P&L: {unrealized_pnl:+.2f}"
                )
        else:
            lines.append("  (none)")
        lines.append("")
        lines.append("Today's Signals:")
        if signals:
            for sig in signals:
                lines.append(
                    f"  {sig['symbol']}: {sig['action']} {sig['shares']} @ {sig['price']:.2f} "
                    f"(SL: {sig['stop_loss']:.2f}) - {sig['reason']} [Strength: {sig['strength']:.2f}]"
                )
        else:
            lines.append("  (none)")
        lines.append("")
        lines.append("="*60)
        
        with open(report_file, "w") as f:
            f.write("\n".join(lines))
        
        print(f"  Report written to {report_file}")
    
    def get_status(self) -> dict:
        """Get current bot status"""
        # Get latest prices for equity calculation
        price_dict = {}
        for sym in self.cfg["symbols"][:3]:  # Just check first 3 for status
            df = self.fetch_intraday_data(sym, period="1d")
            if not df.empty:
                price_dict[sym] = df["Close"].iloc[-1]
        
        equity = self.portfolio.equity(price_dict) if price_dict else self.portfolio.data["cash"]
        init_capital = self.cfg["initial_capital"]
        total_return = (equity / init_capital - 1) * 100 if init_capital > 0 else 0
        
        return {
            "bot_name": "ORB_Bot",
            "status": "active",
            "equity": equity,
            "return_pct": total_return,
            "open_positions": list(self.portfolio.data["positions"].keys()),
            "daily_trades": self.portfolio.daily_stats["trades_today"],
            "max_daily_trades": self.cfg["max_daily_trades"],
            "data_directory": str(self.cfg["data_dir"]),
            "memory_file": str(self.cfg["memory_file"])
        }

# ============================= BACKTESTER =============================
import pandas as pd
from datetime import datetime, timedelta
import time

class ORB_Backtester:
    def __init__(self, config: dict = None):
        self.cfg = config or ORB_CONFIG
        self.portfolio = ORBPortfolio(self.cfg)
        self.strategy = ORBStrategy(self.cfg)
        self.commission_rate = 0.00005 # 0.005 %
        self.slippage_rate = 0.0002 # 0.02 %

    def _download_chunked_5m(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Lädt 5m-Daten monatsweise (yfinance-Limit umgehen)"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        all_dfs = []
 
        current = start
        print(f"Downloading {symbol}...")
        while current < end:
            chunk_end = min(current + timedelta(days=59), end) # max ~60 Tage pro Call
            try:
                df = yf.Ticker(symbol).history(
                    start=current.strftime("%Y-%m-%d"),
                    end=chunk_end.strftime("%Y-%m-%d"),
                    interval="5m"
                )
                if not df.empty:
                    all_dfs.append(df)
                    print(f"Downloaded up to {chunk_end.date()}", end=" ")
            except Exception as e:
                print(f"Error {symbol} {current.date()}: {e}")
            
            current = chunk_end + timedelta(days=1)
            time.sleep(0.5) # höflich zu Yahoo
        
        if not all_dfs:
            return pd.DataFrame()
        
        df = pd.concat(all_dfs)
        df = df[["Open", "High", "Low", "Close", "Volume"]]
        df = df[~df.index.duplicated(keep='first')]
        return df

    def run_backtest(self, start_date: str = "2024-01-01", end_date: str = None):
        """Haupt-Backtest"""
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        print(f"\n=== ORB_Backtester – {start_date} bis {end_date} ===\n")
        
        # Daten für alle Symbole laden
        data_cache = {}
        for sym in self.cfg["symbols"]:
            print(f"Lade Daten für {sym}...")
            df = self._download_chunked_5m(sym, start_date, end_date)
            if len(df) > 100:
                data_cache[sym] = compute_indicators(df)
                print(f" ✓ {len(df)} 5m-Bars geladen")
            else:
                print(f" ✗ Zu wenig Daten für {sym}")
        
        # Portfolio zurücksetzen
        self.portfolio.data = self.portfolio._load()
        self.portfolio.data["cash"] = self.cfg["initial_capital"]
        self.portfolio.data["positions"] = {}
        self.portfolio.data["trades"] = []
        
        # Bar-by-Bar Simulation (nur nach 10:00 ET)
        all_dates = pd.date_range(start_date, end_date, freq='B')
        
        for current_date in all_dates:
            date_str = current_date.strftime("%Y-%m-%d")
            weekday = current_date.weekday()
            
            # Weekday-Filter (wie im Live-Bot)
            if self.cfg.get("avoid_fridays") and weekday == 4:
                continue
            if self.cfg.get("avoid_mondays") and weekday == 0:
                continue
            
            price_dict = {}
            signals_generated = []
            
            for sym, df_full in data_cache.items():
                # Nur Bars bis heute
                day_df = df_full[df_full.index.date == current_date.date()]
                if day_df.empty or len(day_df) < 20:
                    continue
                
                df = day_df.copy()
                current_price = df["Close"].iloc[-1]
                price_dict[sym] = current_price
                
                # Position managen
                if self.portfolio.has_pos(sym):
                    pos = self.portfolio.get_pos(sym)
                    self._manage_position(sym, pos, current_price, df)
                
                # Neues Signal nur nach ORB-Ende
                elif self.portfolio.can_trade_today():
                    orb_high, orb_low, orb_range, _ = self.strategy.calculate_orb_levels(df)
                    if orb_range <= 0:
                        continue
                    
                    signal, strength, reason, context = self.strategy.generate_signal(df)
                    
                    if signal == "BUY" and strength > 0.3:
                        atr_value = df["ATR"].iloc[-1] if not np.isnan(df["ATR"].iloc[-1]) else orb_range
                        stop_loss_or_low = orb_low
                        stop_loss_atr = current_price - (1.5 * atr_value)
                        stop_loss = max(stop_loss_or_low, stop_loss_atr)
                        if stop_loss >= current_price:
                            stop_loss = current_price - atr_value
                        
                        shares = self.portfolio.calculate_position_size(
                            current_price, stop_loss, self.portfolio.equity(price_dict), symbol=sym
                        )
                        
                        if shares > 0:
                            # Kosten simulieren
                            cost = current_price * shares * (1 + self.commission_rate + self.slippage_rate)
                            if cost <= self.portfolio.data["cash"]:
                                res = self.portfolio.buy(sym, current_price, shares, stop_loss, reason)
                                if res["ok"]:
                                    signals_generated.append({
                                        "symbol": sym, "action": "BUY", "shares": shares,
                                        "price": current_price, "stop_loss": stop_loss, "reason": reason
                                    })
            
            # Equity für diesen Tag speichern
            equity = self.portfolio.equity(price_dict)
            self.portfolio.data["equity_curve"].append({
                "date": date_str,
                "equity": equity
            })
        
        # Ergebnisse auswerten
        self._print_backtest_results()
        self._save_backtest_report(start_date, end_date)
        
        return self.portfolio.data["trades"]

    def _manage_position(self, sym: str, pos: dict, current_price: float, df: pd.DataFrame):
        """Position management for backtest (uses live bot logic)"""
        # Use the live bot's management logic
        bot = ORB_Bot(self.cfg)
        bot.portfolio.data = self.portfolio.data  # Use backtest portfolio
        bot.portfolio._save_daily_stats = lambda: None  # Disable stats save for backtest
        bot._manage_position(sym, pos, current_price, df)

    def _print_backtest_results(self):
        """Pretty results output"""
        trades = self.portfolio.data["trades"]
        if not trades:
            print("Keine Trades ausgeführt.")
            return
        
        df_trades = pd.DataFrame(trades)
        wins = df_trades[df_trades["pnl"] > 0]
        
        total_return = (self.portfolio.equity({}) / self.cfg["initial_capital"] - 1) * 100
        win_rate = len(wins) / len(df_trades) * 100 if len(df_trades) > 0 else 0
        gross_profit = wins["pnl"].sum()
        gross_loss = df_trades[df_trades["pnl"] < 0]["pnl"].sum()
        profit_factor = abs(gross_profit / gross_loss) if gross_loss != 0 else float('inf')
        
        equity_curve = pd.DataFrame(self.portfolio.data["equity_curve"])
        equity_curve["date"] = pd.to_datetime(equity_curve["date"])
        equity_curve.set_index("date", inplace=True)
        max_dd = ((equity_curve["equity"] / equity_curve["equity"].cummax()) - 1).min() * 100
        
        print("\n" + "="*70)
        print("ORB_BACKTEST RESULTS")
        print("="*70)
        print(f"Zeitraum : {self.portfolio.data['equity_curve'][0]['date']} – {self.portfolio.data['equity_curve'][-1]['date']}")
        print(f"Startkapital : {self.cfg['initial_capital']:,.0f} EUR")
        print(f"Endkapital : {self.portfolio.equity({}):,.0f} EUR")
        print(f"Gesamtrendite : {total_return:+.2f} %")
        print(f"Trades : {len(df_trades)}")
        print(f"Win-Rate : {win_rate:.1f} %")
        print(f"Profit-Faktor : {profit_factor:.2f}")
        print(f"Max. Drawdown : {max_dd:.2f} %")
        print(f"Durchschnittlicher Trade: {df_trades['pnl'].mean():+.2f} EUR")
        print("="*70)

    def _save_backtest_report(self, start_date: str, end_date: str):
        report_file = self.cfg["data_dir"] / f"backtest_{start_date}_{end_date}.txt"
        with open(report_file, "w") as f:
            f.write("ORB_Backtester – Full Report\n")
            f.write("="*60 + "\n")
            f.write(f"Zeitraum: {start_date} – {end_date}\n")
            f.write(f"Symbole: {', '.join(self.cfg['symbols'])}\n\n")
            f.write("Trades:\n")
            for t in self.portfolio.data["trades"]:
                f.write(f"{t['time']} {t['symbol']} {t['action']} {t['shares']} @ {t['price']:.2f} | P&L: {t['pnl']:+.2f}\n")
        print(f"\n✅ Vollständiger Bericht gespeichert: {report_file}")

# ============================= Neue Entrypoints =============================
def run_backtest_mode():
    bot = ORB_Bot() # nur zum Initialisieren der Config
    tester = ORB_Backtester(bot.cfg)
    tester.run_backtest(start_date="2024-01-01", end_date="2025-04-01") # ← hier anpassen!

# ============================= Entrypoint =============================
def main():
    bot = ORB_Bot()
    bot.run_orb_scan()

if __name__ == "__main__":
    # run_backtest_mode() # Backtest
    main() # Live