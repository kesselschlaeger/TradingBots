#!/usr/bin/env python3
"""
Botti Trader – Redesign v5.0
Trend‑following core with optional ML overlay, ATR‑based risk,
dynamic trailing stops, enhanced pair‑trading, and expanded universe.
Universe (16 assets):
    SPY, QQQ, GLD, XLE, AAPL, TSLA, NVDA, JNJ,
    META, MU, CRWD, GOOGL, HOOD, PLTR, AMD, RKLB
Features:
    - SMA 20/50 Golden/Death Cross (trend filter SMA20>SMA50)
    - ADX > 20 filter for trend strength
    - Volatility‑adjusted position sizing (ATR based)
    - Partial profit at +20% (50% of position)
    - Initial stop‑loss 2*ATR
    - Dynamic trailing stop = 2.5*ATR from high
    - Pair trading (SPY/QQQ) with Kalman‑filter mean & adaptive lookback
    - Optional ML overlay (placeholder: logistic regression probability >0.6)
    - Portfolio heat limit (max 80% equity at risk)
    - Comprehensive reporting & trade log
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yfinance as yf
import pandas as pd
import numpy as np
import joblib

# ============================= Configuration =============================
CONFIG = {
    # --- Universe -------------------------------------------------------
    "symbols": [
        "SPY", "QQQ", "GLD", "XLE", "AAPL", "TSLA", "NVDA", "JNJ",
        "META", "MU", "CRWD", "GOOGL", "HOOD", "PLTR", "AMD", "RKLB"
    ],
    # --- Core trend -----------------------------------------------------
    "sma_short": 20,
    "sma_long": 50,
    # --- Risk / sizing --------------------------------------------------
    "atr_period": 14,
    "risk_per_trade": 0.02,          # 2% of equity per trade (vol‑adjusted)
    "max_equity_at_risk": 0.80,      # portfolio heat limit
    # --- Trade management -----------------------------------------------
    "partial_profit_pct": 0.25,      # +25% → sell 50%
    "initial_sl_atr_mult": 2.5,      # stop‑loss = entry – mult*ATR
    "trailing_atr_mult": 3.0,        # trailing = high – mult*ATR
    # --- Trend filter ---------------------------------------------------
    "adx_period": 14,
    "adx_threshold": 20,
    # --- Pair trading ---------------------------------------------------
    "pair": ("SPY", "QQQ"),
    "pair_lookback_base": 20,
    "pair_z_entry": 2.0,
    "pair_z_exit": 0.5,
    # --- ML overlay ---------------------------------------------------
    "use_ml": False,                 # set True when model is trained
    "ml_prob_threshold": 0.6,
    # --- Misc -----------------------------------------------------------
    "currency": "EUR",
    "initial_capital": 10000.0,
    "data_dir": Path(__file__).parent / "trading_data",
    "portfolio_file": Path(__file__).parent / "trading_data" / "portfolio.json",
}


# ============================= Helper Functions =============================
def atr(df: pd.DataFrame, period: int) -> pd.Series:
    high_low = df["High"] - df["Low"]
    high_close = np.abs(df["High"] - df["Close"].shift())
    low_close = np.abs(df["Low"] - df["Close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def adx(df: pd.DataFrame, period: int) -> pd.Series:
    # Directional Movement
    up = df["High"].diff()
    down = df["Low"].diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0)
    minus_dm = np.where((down > up) & (down > 0), -down, 0)
    # Ensure same index as df
    plus_dm = pd.Series(plus_dm, index=df.index)
    minus_dm = pd.Series(minus_dm, index=df.index)
    tr = atr(df, period)
    plus_di = 100 * (plus_dm.rolling(period, min_periods=1).mean() / tr)
    minus_di = 100 * (minus_dm.rolling(period, min_periods=1).mean() / tr)
    dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx_val = dx.rolling(period, min_periods=1).mean()
    return adx_val

def compute_indicators(df: pd.DataFrame, cfg: dict = None) -> pd.DataFrame:
    if cfg is None:
        cfg = CONFIG
    df = df.copy()
    df["SMA20"] = df["Close"].rolling(window=cfg["sma_short"]).mean()
    df["SMA50"] = df["Close"].rolling(window=cfg["sma_long"]).mean()
    df["ATR"] = atr(df, cfg["atr_period"])
    df["ADX"] = adx(df, cfg["adx_period"])
    # RSI (14)
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))
    return df

def kalman_estimate(series: pd.Series) -> Tuple[float, float]:
    """Very simple Kalman filter: returns (mean, variance)"""
    # Initialize
    x_est = series.iloc[0]
    p_est = 1.0
    Q = 1e-5   # process variance
    R = 0.01   # measurement variance
    for z in series.iloc[1:]:
        # prediction
        x_pred = x_est
        p_pred = p_est + Q
        # update
        k = p_pred / (p_pred + R)
        x_est = x_pred + k * (z - x_pred)
        p_est = (1 - k) * p_pred
    return float(x_est), float(p_est)

# ============================= Portfolio =============================
class Portfolio:
    def __init__(self, config: dict):
        self.cfg = config
        self.cfg["data_dir"].mkdir(exist_ok=True)
        self.data = self._load()
    def _load(self) -> dict:
        if self.cfg["portfolio_file"].exists():
            with open(self.cfg["portfolio_file"], "r") as f:
                data = json.load(f)
                # backfill missing keys
                for sym, pos in data.get("positions", {}).items():
                    pos.setdefault("highest", pos["entry"])
                    pos.setdefault("stop", pos["entry"] * (1 - self.cfg["initial_sl_atr_mult"] * pos.get("atr_at_entry", 0)))
                    pos.setdefault("trailing", pos["highest"] - self.cfg["trailing_atr_mult"] * pos.get("atr_at_entry", 0))
                    pos.setdefault("partial_sold", False)
                return data
        return {
            "cash": self.cfg["initial_capital"],
            "initial_capital": self.cfg["initial_capital"],
            "positions": {},
            "trades": [],
            "equity_curve": [],
        }
    def save(self, price_dict: dict = None, update_equity: bool = True):
        if update_equity:
            positions_val = sum(p["shares"] * price_dict.get(p["symbol"], p["price"]) for p in self.data["positions"].values()) if price_dict else sum(p["shares"]*p["price"] for p in self.data["positions"].values())
            self.data["equity_curve"].append({
                "date": datetime.now().isoformat(),
                "cash": self.data["cash"],
                "positions_value": positions_val,
            })
        self.data["last_updated"] = datetime.now().isoformat()
        with open(self.cfg["portfolio_file"], "w") as f:
            json.dump(self.data, f, indent=2)
    # position helpers
    def has_pos(self, sym: str) -> bool:
        return sym in self.data["positions"]
    def get_pos(self, sym: str) -> dict:
        return self.data["positions"].get(sym)
    # buy
    def buy(self, sym: str, price: float, shares: int, atr_val: float, reason: str) -> dict:
        cost = price * shares
        if cost > self.data["cash"]:
            return {"ok": False, "msg": "not enough cash"}
        self.data["cash"] -= cost
        pos = {
            "symbol": sym,
            "shares": shares,
            "entry": price,
            "atr_at_entry": atr_val,
            "highest": price,
            "stop": price - self.cfg["initial_sl_atr_mult"] * atr_val,
            "trailing": price - self.cfg["trailing_atr_mult"] * atr_val,
            "partial_sold": False,
            "reason": reason,
            "price": price,
        }
        self.data["positions"][sym] = pos
        self._log_trade(sym, "BUY", shares, price, 0.0, reason)
        self.save(update_equity=False)
        return {"ok": True, "pos": pos}
    # sell
    def sell(self, sym: str, price: float, shares: int, reason: str) -> dict:
        pos = self.data["positions"].get(sym)
        if not pos or pos["shares"] < shares:
            return {"ok": False, "msg": "no position"}
        proceeds = price * shares
        self.data["cash"] += proceeds
        # pnl
        cost_basis = pos["entry"] * shares
        pnl = proceeds - cost_basis
        pos["shares"] -= shares
        if pos["shares"] <= 0:
            del self.data["positions"][sym]
        else:
            # adjust highest/trailing if needed (keep same)
            pass
        self._log_trade(sym, "SELL", shares, price, pnl, reason)
        self.save(update_equity=False)
        return {"ok": True, "pnl": pnl, "remaining": pos.get("shares", 0)}
    def _log_trade(self, sym, action, shares, price, pnl, reason):
        self.data["trades"].append({
            "time": datetime.now().isoformat(),
            "symbol": sym,
            "action": action,
            "shares": shares,
            "price": price,
            "pnl": pnl,
            "reason": reason,
        })
    # equity
    def equity(self, price_dict: dict) -> float:
        positions_val = sum(p["shares"]*price_dict.get(sym,0) for sym,p in self.data["positions"].items())
        return self.data["cash"] + positions_val

# ============================= Strategy Core =============================
class Strategy:
    def __init__(self, config: dict):
        self.cfg = config
    def signal_row(self, df: pd.DataFrame) -> Tuple[str, str]:
        """Return (signal, reason) for the latest row."""
        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) >= 2 else None
        if prev is None:
            return "HOLD", "not enough data"
        # Check crossovers BEFORE any directional filter
        golden = prev["SMA20"] <= prev["SMA50"] and last["SMA20"] > last["SMA50"]
        death  = prev["SMA20"] >= prev["SMA50"] and last["SMA20"] < last["SMA50"]
        # Death Cross: SMA20 drops below SMA50 – always a SELL signal
        if death:
            return "SELL", "Death Cross"
        # Uptrend filter and ADX only apply to BUY signals
        if not (last["SMA20"] > last["SMA50"]):
            return "HOLD", "SMA20 <= SMA50 (no uptrend)"
        if last["ADX"] < self.cfg["adx_threshold"]:
            return "HOLD", f"ADX {last['ADX']:.1f} < {self.cfg['adx_threshold']}"
        if golden:
            return "BUY", "Golden Cross"
        return "HOLD", ""
    def pair_signal(self, df: pd.DataFrame) -> Tuple[str, float, str]:
        """Return (signal, zscore, reason) for SPY/QQQ pair."""
        sym1, sym2 = self.cfg["pair"]
        if sym1 not in df.columns or sym2 not in df.columns:
            return "HOLD", 0.0, "missing pair data"
        spread = df[sym2] - df[sym1]  # QQQ - SPY
        # adaptive lookback based on recent volatility of spread
        vol = spread.rolling(10).std().iloc[-1]
        base = self.cfg["pair_lookback_base"]
        lookback = int(base * (1 + vol / (spread.mean() + 1e-9)))
        lookback = max(10, min(50, lookback))
        ma = spread.rolling(lookback).mean()
        sd = spread.rolling(lookback).std().replace(0, np.nan)
        z = (spread - ma) / sd
        z = z.fillna(0)
        # Kalman mean (optional smoothing)
        mu, _ = kalman_estimate(spread.tail(50))
        # z using Kalman mean
        z_kal = (spread.iloc[-1] - mu) / (sd.iloc[-1] + 1e-9)
        # decide
        if z_kal > self.cfg["pair_z_entry"]:
            return "SHORT_QQQ_LONG_SPY", float(z_kal), f"Pair Z {z_kal:.2f} > {self.cfg['pair_z_entry']}"
        if z_kal < -self.cfg["pair_z_entry"]:
            return "LONG_QQQ_SHORT_SPY", float(z_kal), f"Pair Z {z_kal:.2f} < -{self.cfg['pair_z_entry']}"
        if abs(z_kal) < self.cfg["pair_z_exit"]:
            return "FLAT", float(z_kal), f"Pair Z {z_kal:.2f} within exit band"
        return "HOLD", float(z_kal), ""
    def ml_prob(self, features: dict, ml_model=None, ml_scaler=None) -> float:
        if ml_model is None or ml_scaler is None:
            return 0.5
        # order: sma_diff, adx, atr_pct, rsi (match run_daily feats)
        X = np.array([[features['sma_diff'], features['adx'], features['atr_pct'], features['rsi']]])
        X_scaled = ml_scaler.transform(X)
        prob = ml_model.predict_proba(X_scaled)[0, 1]  # probability of class 1
        return float(prob)

# ============================= Main Engine =============================
class Trader:
    def __init__(self, config: Optional[dict] = None):
        self.cfg = config or CONFIG
        self.portfolio = Portfolio(self.cfg)
        # If starting fresh, seed NVDA position on 2026-03-23
        if not self.portfolio.data["positions"] and self.portfolio.data["cash"] == self.cfg["initial_capital"]:
            try:
                nvda_df = yf.Ticker("NVDA").history(start="2025-12-01", end="2026-03-24")
                if not nvda_df.empty:
                    price = float(nvda_df["Close"].iloc[-1])
                    shares = 6
                    cost = price * shares
                    if cost <= self.portfolio.data["cash"]:
                        self.portfolio.data["cash"] -= cost
                        df_seed = compute_indicators(nvda_df, self.cfg)
                        atr_val = df_seed["ATR"].iloc[-1] if not np.isnan(df_seed["ATR"].iloc[-1]) else price * 0.02
                        pos = {
                            "symbol": "NVDA",
                            "shares": shares,
                            "entry": price,
                            "atr_at_entry": atr_val,
                            "highest": price,
                            "stop": price - self.cfg["initial_sl_atr_mult"] * atr_val,
                            "trailing": price - self.cfg["trailing_atr_mult"] * atr_val,
                            "partial_sold": False,
                            "reason": "Seed position",
                            "price": price,
                        }
                        self.portfolio.data["positions"]["NVDA"] = pos
                        self.portfolio._log_trade("NVDA", "BUY", shares, price, 0.0, "Seed position")
                        self.portfolio.save()
            except Exception as e:
                # If seed fails, just continue with empty portfolio
                pass
        self.strategy = Strategy(self.cfg)
        self.reports_dir = self.cfg["data_dir"] / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        # Load ML model if enabled
        self.ml_model = None
        self.ml_scaler = None
        if self.cfg.get("use_ml", False):
            model_path = Path(__file__).parent / "ml_model" / "logistic_model.pkl"
            scaler_path = Path(__file__).parent / "ml_model" / "scaler.pkl"
            if model_path.exists() and scaler_path.exists():
                try:
                    self.ml_model = joblib.load(model_path)
                    self.ml_scaler = joblib.load(scaler_path)
                    print("ML model loaded.")
                except Exception as e:
                    print(f"Failed to load ML model: {e}")
                    self.ml_model = None
                    self.ml_scaler = None
            else:
                print("ML model files not found, ML overlay disabled.")
    # -----------------------------------------------------------------
    def fetch_data(self, symbol: str, period: str = "400d") -> pd.DataFrame:
        df = yf.Ticker(symbol).history(period=period)
        if df.empty:
            return pd.DataFrame()
        return df[["Open","High","Low","Close","Volume"]]
    def fetch_pair(self) -> pd.DataFrame:
        sym1, sym2 = self.cfg["pair"]
        df = yf.download([sym1, sym2], period="400d")
        if isinstance(df.columns, pd.MultiIndex):
            df = df["Close"]
        else:
            df = df[[sym1, sym2]]
        return df
    # -----------------------------------------------------------------
    def run_daily(self) -> dict:
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"\n=== Botti Trader v5.0 – {today} ===")
        # 1) update prices & manage exits
        price_dict = {}
        sym_to_df = {}
        current_risk = 0.0
        for sym in self.cfg["symbols"]:
            df = self.fetch_data(sym, period="100d")
            if df.empty:
                continue
            df = compute_indicators(df, self.cfg)
            sym_to_df[sym] = df
            price = df["Close"].iloc[-1]
            price_dict[sym] = price
            atr_val = df["ATR"].iloc[-1] if not np.isnan(df["ATR"].iloc[-1]) else 0.0
            # manage existing positions
            if self.portfolio.has_pos(sym):
                pos = self.portfolio.get_pos(sym)
                # update price early
                pos["price"] = price
                # update current risk
                risk_this = pos["shares"] * (pos["entry"] - pos["stop"])
                current_risk += max(risk_this, 0.0)
                # update highest & trailing
                if price > pos["highest"]:
                    pos["highest"] = price
                    new_trail = price - self.cfg["trailing_atr_mult"] * pos["atr_at_entry"]
                    if new_trail > pos["trailing"]:
                        pos["trailing"] = new_trail
                # check exits
                exited = False
                # partial profit
                if not pos["partial_sold"] and price >= pos["entry"] * (1 + self.cfg["partial_profit_pct"]):
                    shares_to_sell = pos["shares"] // 2
                    if shares_to_sell > 0:
                        res = self.portfolio.sell(sym, price, shares_to_sell, "Partial Profit")
                        if res["ok"]:
                            print(f"  {sym}: PARTIAL SELL {shares_to_sell} @ {price:.2f} (PNL {res['pnl']:.2f})")
                            pos["partial_sold"] = True
                            exited = True
                # trailing stop
                if not exited and price <= pos["trailing"]:
                    res = self.portfolio.sell(sym, price, pos["shares"], "Trailing Stop")
                    if res["ok"]:
                        print(f"  {sym}: TRAILING STOP SELL {pos['shares']} @ {price:.2f} (PNL {res['pnl']:.2f})")
                        exited = True
                # stop loss
                if not exited and price <= pos["stop"]:
                    res = self.portfolio.sell(sym, price, pos["shares"], "Stop-Loss")
                    if res["ok"]:
                        print(f"  {sym}: STOP-LOSS SELL {pos['shares']} @ {price:.2f} (PNL {res['pnl']:.2f})")
                        exited = True
        # 2) generate new signals / Death Cross exits
        for sym in self.cfg["symbols"]:
            df = sym_to_df.get(sym)
            if df is None or df.empty or len(df) < self.cfg["sma_long"] + 5:
                continue
            signal, reason = self.strategy.signal_row(df)
            # Death Cross exit for existing positions
            if signal == "SELL" and self.portfolio.has_pos(sym):
                pos = self.portfolio.get_pos(sym)
                price = price_dict.get(sym, df["Close"].iloc[-1])
                res = self.portfolio.sell(sym, price, pos["shares"], reason)
                if res["ok"]:
                    print(f"  {sym}: SELL {pos['shares']} @ {price:.2f} ({reason})")
                continue
            if self.portfolio.has_pos(sym):
                continue  # already have position, no new entry needed
            if signal == "BUY":
                # check portfolio heat
                equity = self.portfolio.equity(price_dict)
                if current_risk > equity * self.cfg["max_equity_at_risk"]:
                    print(f"  {sym}: SKIPPED BUY - Portfolio heat limit reached ({current_risk/equity*100:.1f}% > {self.cfg['max_equity_at_risk']*100:.0f}%)")
                    continue
                # volatility‑adjusted size
                atr_val = df["ATR"].iloc[-1]
                risk_per_share = self.cfg["initial_sl_atr_mult"] * atr_val  # approximate $ risk if SL hit
                equity = self.portfolio.equity(price_dict)
                max_risk = equity * self.cfg["risk_per_trade"]
                shares_by_risk = int(max_risk / risk_per_share) if risk_per_share > 0 else 0
                # also respect max position % of cash
                cash = self.portfolio.data["cash"]
                max_cash = cash * 0.4  # 40% of cash per position
                shares_by_cash = int(max_cash / df["Close"].iloc[-1])
                shares = min(shares_by_risk, shares_by_cash)
                if shares <= 0:
                    continue
                # ML overlay
                if self.cfg["use_ml"]:
                    feats = {
                        "sma_diff": (df["SMA20"].iloc[-1] - df["SMA50"].iloc[-1]) / df["Close"].iloc[-1],
                        "rsi": df["RSI"].iloc[-1],
                        "adx": df["ADX"].iloc[-1],
                        "atr_pct": atr_val / df["Close"].iloc[-1],
                    }
                    prob = self.strategy.ml_prob(feats, self.ml_model, self.ml_scaler)
                    if prob < self.cfg["ml_prob_threshold"]:
                        print(f"  {sym}: ML filter reject (prob={prob:.2f})")
                        continue
                price = df["Close"].iloc[-1]
                res = self.portfolio.buy(sym, price, shares, atr_val, reason)
                if res["ok"]:
                    print(f"  {sym}: BUY {shares} @ {price:.2f} ({reason})")
                    current_risk += shares * self.cfg["initial_sl_atr_mult"] * atr_val
        # 3) Pair trading signal (informational, not executed here)
        pair_df = self.fetch_pair()
        if not pair_df.empty:
            pair_signal, z, reason = self.strategy.pair_signal(pair_df)
            print(f"  Pair {self.cfg['pair'][0]}/{self.cfg['pair'][1]}: {pair_signal} (Z={z:.2f}) – {reason}")
        # 4) equity & reporting
        equity = self.portfolio.equity(price_dict)
        self.portfolio.save(price_dict)
        init = self.cfg["initial_capital"]
        ret = (equity / init - 1) * 100
        print(f"  Equity: {equity:,.2f} {self.cfg['currency']} (Return {ret:+.2f}%)")
        self._generate_report(today, price_dict, equity)
        return {
            "date": today,
            "equity": equity,
            "return_pct": ret,
            "positions": list(self.portfolio.data["positions"].keys()),
            "trades_today": len([t for t in self.portfolio.data["trades"] if t["time"].startswith(today)]),
        }
    # -----------------------------------------------------------------
    def _generate_report(self, date_str: str, price_dict: dict, equity: float):
        report_file = self.reports_dir / f"report_{date_str}.txt"
        lines = []
        lines.append("="*60)
        lines.append(f"BOTTI TRADER v5.0 – DAILY REPORT – {date_str}")
        lines.append("="*60)
        lines.append("")
        lines.append(f"Starting Capital: {self.cfg['initial_capital']:,.2f} {self.cfg['currency']}")
        lines.append(f"Current Equity:   {equity:,.2f} {self.cfg['currency']}")
        lines.append(f"Return:           {(equity/self.cfg['initial_capital']-1)*100:+.2f}%")
        lines.append("")
        lines.append("Open Positions:")
        if self.portfolio.data["positions"]:
            for sym, pos in self.portfolio.data["positions"].items():
                lines.append(
                    f"  {sym}: {pos['shares']} sh @ {pos['entry']:.2f} → "
                    f"now {price_dict.get(sym,0):.2f} | "
                    f"SL {pos['stop']:.2f} | Trail {pos['trailing']:.2f} | "
                    f"{'Partial' if pos['partial_sold'] else 'Full'}"
                )
        else:
            lines.append("  (none)")
        lines.append("")
        lines.append("Recent Trades (last 10):")
        recent = self.portfolio.data["trades"][-10:]
        if recent:
            for t in recent:
                lines.append(
                    f"  {t['time'][:16]} {t['symbol']} {t['action']} {t['shares']} @ {t['price']:.2f} "
                    f"P&L {t['pnl']:+.2f} ({t['reason']})"
                )
        else:
            lines.append("  (none)")
        lines.append("")
        lines.append("="*60)
        with open(report_file, "w") as f:
            f.write("\n".join(lines))
        print(f"  Report written to {report_file}")

# ============================= Entrypoint =============================
def main():
    trader = Trader()
    trader.run_daily()

if __name__ == "__main__":
    main()
