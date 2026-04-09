#!/usr/bin/env python3
"""
one_bar_breakout.py – 50-Bar High/Low Momentum Strategy
Basierend auf dem Video-Konzept: Kauf bei 50-Bar-High-Close, Verkauf am nächsten Open.

Architektur: Variante A (Eigenständiges Skript)
Warum? Die Time-Exit Logik (Market-on-Open) unterscheidet sich fundamental 
vom Preis-Exit (Stop/Target) des ORB-Bots.
"""

import os
import argparse
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
from tabulate import tabulate # Für saubere Reports: pip install tabulate

try:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest, GetOrdersRequest
    from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    ALPACA_AVAILABLE = True
except ImportError:
    ALPACA_AVAILABLE = False

## für eine lokale Ausführung ohne OpenClaw-Umgebung können die
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ET = pytz.timezone("America/New_York")

# --- Konfiguration ---
CONFIG = {
    #"symbols": ["SPY", "QQQ", "IWM", "TSLA", "NVDA", "AAPL", "AMD"],
    "symbols": [
        "SPY", "QQQ", "IWM", "DIA",
        "NVDA", "TSLA", "AMD", "AVGO",
        "AAPL", "MSFT", "META", "AMZN",
        "PLTR", "GOOGL", "NFLX",
    ],
    "lookback": 50,
    "allow_shorts": True,
    "risk_pct": 0.15,          # 15% Equity pro Trade (da sehr kurze Haltedauer)
    "slippage_bps": 5,         # 5 Basispunkte Slippage
    "commission": 0.0,         # Alpaca ist i.d.R. kommissionsfrei (außer ECN-Fees)
    "alpaca_paper": True,
}

# ==========================================
# STRATEGIE-KERN
# ==========================================
def apply_strategy_logic(df: pd.DataFrame, lookback: int):
    """
    Berechnet Signale basierend auf dem gestrigen Close im Vergleich
    zu den vorangegangenen 50 Highs/Lows.
    """
    # Wichtig: Wir schauen uns das High/Low der *vorherigen* N Bars an
    df['n_high'] = df['high'].shift(1).rolling(window=lookback).max()
    df['n_low']  = df['low'].shift(1).rolling(window=lookback).min()
    
    # Signale (Boolean)
    df['long_signal']  = df['close'] > df['n_high']
    df['short_signal'] = df['close'] < df['n_low']
    return df

# ==========================================
# BACKTESTER
# ==========================================
def run_backtest(symbols, start_date, end_date):
    client = StockHistoricalDataClient(os.getenv("APCA_API_KEY_ID"), os.getenv("APCA_API_SECRET_KEY"))
    
    all_trades = []
    
    for sym in symbols:
        print(f"Lade Daten für {sym}...")
        req = StockBarsRequest(
            symbol_or_symbols=sym,
            timeframe=TimeFrame.Day,
            start=datetime.strptime(start_date, "%Y-%m-%d"),
            end=datetime.strptime(end_date, "%Y-%m-%d"),
            feed="iex"
        )
        bars = client.get_stock_bars(req).df
        if bars.empty: continue
        
        df = bars.reset_index()
        df = apply_strategy_logic(df, CONFIG["lookback"])
        
        # Simulation
        for i in range(CONFIG["lookback"], len(df) - 1):
            row = df.iloc[i]
            next_day = df.iloc[i+1]
            
            signal = None
            if row['long_signal']: signal = "LONG"
            elif row['short_signal'] and CONFIG["allow_shorts"]: signal = "SHORT"
            
            if signal:
                entry_price = row['close']
                exit_price = next_day['open']
                
                # Slippage Simulation
                slip = entry_price * (CONFIG["slippage_bps"] / 10000)
                if signal == "LONG":
                    pnl_pct = (exit_price - (entry_price + slip)) / (entry_price + slip)
                else:
                    pnl_pct = ((entry_price - slip) - exit_price) / (entry_price - slip)
                
                all_trades.append({
                    "Symbol": sym,
                    "Date": row['timestamp'].date(),
                    "Side": signal,
                    "Entry": entry_price,
                    "Exit": exit_price,
                    "PnL%": round(pnl_pct * 100, 3)
                })

    # Report
    if not all_trades:
        print("Keine Trades generiert.")
        return
        
    tdf = pd.DataFrame(all_trades)
    win_rate = len(tdf[tdf['PnL%'] > 0]) / len(tdf)
    avg_trade = tdf['PnL%'].mean()
    
    print("\n--- BACKTEST REPORT: ONE-BAR BREAKOUT ---")
    print(tabulate(tdf.tail(10), headers='keys', tablefmt='psql'))
    print(f"\nTotal Trades: {len(tdf)}")
    print(f"Win Rate:     {win_rate:.2%}")
    print(f"Avg Trade:    {avg_trade:.3f}%")
    print(f"Profit Factor: {abs(tdf[tdf['PnL%']>0]['PnL%'].sum() / tdf[tdf['PnL%']<0]['PnL%'].sum()):.2f}")

# ==========================================
# LIVE EXECUTION (ALPACA)
# ==========================================
class OneBarLive:
    def __init__(self):
        key = os.getenv("APCA_API_KEY_ID")
        sec = os.getenv("APCA_API_SECRET_KEY")
        self.trading = TradingClient(key, sec, paper=CONFIG["alpaca_paper"])
        self.data = StockHistoricalDataClient(key, sec)

    def execute_entries(self):
        """Wird um 15:58 ET aufgerufen (kurz vor Close)"""
        print(f"[{datetime.now()}] Starte Entry-Scan...")
        account = self.trading.get_account()
        equity = float(account.equity)
        
        for sym in CONFIG["symbols"]:
            # Hole letzte 60 Bars (um 50 sicher zu haben)
            req = StockBarsRequest(symbol_or_symbols=sym, timeframe=TimeFrame.Day, 
                                   start=datetime.now()-timedelta(days=100))
            bars = self.data.get_stock_bars(req).df.loc[sym]
            df = apply_strategy_logic(bars, CONFIG["lookback"])
            
            last_row = df.iloc[-1]
            side = None
            if last_row['long_signal']: side = OrderSide.BUY
            elif last_row['short_signal'] and CONFIG["allow_shorts"]: side = OrderSide.SELL
            
            if side:
                qty = int((equity * CONFIG["risk_pct"]) / last_row['close'])
                if qty > 0:
                    order = MarketOrderRequest(symbol=sym, qty=qty, side=side, time_in_force=TimeInForce.DAY)
                    self.trading.submit_order(order)
                    print(f"ORDER GESENDET: {side} {qty} {sym}")

    def execute_exits(self):
        """Wird um 09:30 ET aufgerufen (Market Open)"""
        print(f"[{datetime.now()}] Schließe alle Overnight-Positionen...")
        positions = self.trading.get_all_positions()
        for pos in positions:
            # Wir schließen nur, was in unserer Symbol-Liste ist
            if pos.symbol in CONFIG["symbols"]:
                self.trading.close_position(pos.symbol)
                print(f"EXIT: {pos.symbol} geschlossen.")

# ==========================================
# CLI
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="One-Bar Breakout Bot & Backtester")
    
    # Pflicht-Modus
    parser.add_argument("--mode", choices=["backtest", "entry", "exit"], required=True,
                        help="backtest: Simulation | entry: Kauf vor Close | exit: Verkauf bei Open")
    
    # Optionale Datums-Argumente für den Backtest
    parser.add_argument("--start", type=str, default="2024-01-01", 
                        help="Startdatum für Backtest (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=datetime.now().strftime("%Y-%m-%d"), 
                        help="Enddatum für Backtest (YYYY-MM-DD)")
    
    args = parser.parse_args()

    if args.mode == "backtest":
        # Wir nutzen die Symbole aus der CONFIG oben im Skript
        print(f"Starte Backtest von {args.start} bis {args.end}...")
        run_backtest(CONFIG["symbols"], args.start, args.end)
        
    elif args.mode == "entry":
        bot = OneBarLive()
        bot.execute_entries()
        
    elif args.mode == "exit":
        bot = OneBarLive()
        bot.execute_exits()