#!/usr/bin/env python3
"""
one_bar_breakout.py – 50-Bar High/Low Momentum Strategy
Basierend auf dem Video-Konzept: Kauf bei 50-Bar-High-Close, Verkauf am nächsten Open.

Architektur: Variante A (Eigenständiges Skript)
Warum? Die Time-Exit Logik (Market-on-Open) unterscheidet sich fundamental 
vom Preis-Exit (Stop/Target) des ORB-Bots.


Aufruf Backtest: python OneBarBreakoutGemini.py --mode backtest --start 2024-01-01 --end 2026-04-08
Aufruf Live Entry: python OneBarBreakoutGemini.py --mode entry
Aufruf Live Exit:  python OneBarBreakoutGemini.py --mode exit
"""

import os
import argparse
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
from pathlib import Path
from tabulate import tabulate # Für saubere Reports: pip install tabulate

# Matplotlib (optional)
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.dates import DateFormatter
    MPL_AVAILABLE = True
except ImportError:
    MPL_AVAILABLE = False

# openpyxl für Excel-Export
try:
    import openpyxl  # noqa
    XLSX_AVAILABLE = True
except ImportError:
    XLSX_AVAILABLE = False

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
        #"SPY", "QQQ", "IWM", "DIA", "NVDA", "TSLA", "AMD", "AVGO", "AAPL", "MSFT", "META", "AMZN", "PLTR", "GOOGL", "NFLX"
        
"""        
        "SOXL", #winrate 100
        "SMCI", #winrate 83
        "RKLB", #winrate 100
        "TQQQ", ##winrate 96
        "INTC",
        "HOOD", #--winrate100
        "MSTR",
        "SNDK", #--winrate100
        "MU",
        "WBD", #--winrate100
        "AVGO" #--winrate100
"""        
        # vom Universal Selector bestimmt
        "ORCL","NOW","TSM"

            ],
    "lookback": 50,
    "allow_shorts": True,
    "initial_capital": 10_000.0,
    "risk_pct": 0.15,          # 15% Equity pro Trade (da sehr kurze Haltedauer)
    "slippage_bps": 5,         # 5 Basispunkte Slippage
    "commission": 0.00005,         # Claude hat etwas angenommen Alpaca ist i.d.R. kommissionsfrei (außer ECN-Fees)
    "max_daily_trades": 3,     # Max. Trades pro Tag
    "alpaca_paper": True,
    "alpaca_data_feed": "iex",   # IEX für kostenlose Accounts; SIP nur mit Abo
    # ── Kelly Sizing (optional) ────────────────────────────────────────
    "use_kelly_sizing": False,           # Kelly-basiertes Sizing aktivieren
    "kelly_fraction": 0.50,             # Half-Kelly für Sicherheit
    "kelly_lookback_trades": 50,        # Trades für Rolling Win-Rate
    "kelly_min_trades": 20,             # Minimum Trades before using Kelly
    "kelly_payoff_ratio": 1.0,          # Geschätztes Win/Loss-Verhältnis
}

# ==========================================
# STRATEGIE-KERN
# ==========================================
def compute_rolling_win_rate(trades: list, lookback: int = 50, min_trades: int = 20):
    """
    Berechne Win-Rate der letzten `lookback` abgeschlossenen Trades.
    
    trades: Liste von Dicts mit "PnL_USD" Key
    lookback: max. Anzahl Trades für Rolling-Fenster
    min_trades: Mindestanzahl bevor eine Win-Rate berechnet wird
    Rückgabe: Win-Rate [0,1] oder None wenn zu wenig Daten
    """
    if not trades:
        return None
    closed = [t for t in trades if t.get("PnL_USD", 0) != 0]
    if len(closed) < min_trades:
        return None
    recent = closed[-lookback:]  # nimmt die letzten N (oder alle wenn < lookback)
    wins = sum(1 for t in recent if t["PnL_USD"] > 0)
    return wins / len(recent)

def calculate_position_size_kelly(equity: float, price: float, cfg: dict, rolling_win_rate):
    """
    Position Sizing mit Kelly-Formel (oder Fixed-Fraction als Fallback).
    
    Kelly: f* = (p × b - q) / b, wobei:
      p = Win-Probability, b = Payoff-Ratio, q = 1-p
    """
    if price <= 0 or equity <= 0:
        return 0
    
    if cfg.get("use_kelly_sizing", False) and rolling_win_rate is not None:
        # rolling_win_rate ist nur != None wenn genug Trades vorhanden
        b = float(cfg.get("kelly_payoff_ratio", 1.0))
        p = float(np.clip(rolling_win_rate, 0.05, 0.95))
        q = 1.0 - p
        kelly_full = max(0.0, (p * b - q) / b)
        kelly_fraction = float(cfg.get("kelly_fraction", 0.50))
        position_pct = kelly_full * kelly_fraction
        # Cap auf risk_pct als Sicherheitsnetz
        max_pct = float(cfg.get("risk_pct", 0.15))
        position_pct = min(position_pct, max_pct)
    else:
        # Fallback: Fixed Percent (auch wenn Kelly an, aber zu wenig Trades)
        position_pct = float(cfg.get("risk_pct", 0.15))
    
    notional = equity * position_pct
    shares = int(notional / price)
    return max(0, shares)

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
    initial_capital = float(CONFIG.get("initial_capital", 10_000.0))
    current_equity = initial_capital
    cash = initial_capital
    risk_pct = float(CONFIG.get("risk_pct", 0.15))

    # Equity-Tracking pro Tag
    equity_by_date: dict = {}  # date_str → equity
    max_daily = int(CONFIG.get("max_daily_trades", 3))

    for sym in symbols:
        print(f"Lade Daten für {sym}...")
        # Lookback-Puffer: 50 Bars + 100 Kalendertage Extra
        lookback_days = CONFIG["lookback"] * 2
        try:
            fetch_start = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=lookback_days))
        except ValueError:
            fetch_start = datetime.strptime(start_date, "%Y-%m-%d")

        req = StockBarsRequest(
            symbol_or_symbols=sym,
            timeframe=TimeFrame.Day,
            start=fetch_start,
            end=datetime.strptime(end_date, "%Y-%m-%d"),
            feed="iex"
        )
        try:
            bars = client.get_stock_bars(req).df
            if bars.empty: continue
            
            df = bars.reset_index()
            df = apply_strategy_logic(df, CONFIG["lookback"])
            
            for i in range(CONFIG["lookback"], len(df) - 1):
                row = df.iloc[i]
                next_day = df.iloc[i+1]

                # Nur Trades im gewünschten Zeitraum
                trade_date = row['timestamp']
                if hasattr(trade_date, 'date'):
                    trade_date_str = trade_date.strftime("%Y-%m-%d")
                else:
                    trade_date_str = str(trade_date)[:10]
                if trade_date_str < start_date:
                    continue
                
                # Tagesgrenze prüfen: zähle Trades für diesen Tag
                trades_today = len([t for t in all_trades if t["Trade_Date"] == trade_date_str])
                if trades_today >= max_daily:
                    continue
                
                signal = None
                if row['long_signal']: signal = "LONG"
                elif row['short_signal'] and CONFIG["allow_shorts"]: signal = "SHORT"
                
                if signal:
                    entry_price = row['close']
                    exit_price = next_day['open']
                    
                    # Positionsgröße mit Kelly (oder Fixed Percent)
                    rolling_wr = compute_rolling_win_rate(
                        all_trades,
                        lookback=int(CONFIG.get("kelly_lookback_trades", 50)),
                        min_trades=int(CONFIG.get("kelly_min_trades", 20)))
                    qty = calculate_position_size_kelly(current_equity, entry_price, CONFIG, rolling_wr)
                    if qty == 0: continue

                    # Slippage & PnL Berechnung
                    slip = entry_price * (CONFIG["slippage_bps"] / 10000)
                    if signal == "LONG":
                        net_entry = entry_price + slip
                        net_exit = exit_price - slip
                    else:
                        net_entry = entry_price - slip
                        net_exit = exit_price + slip
                    
                    pnl_pct = (net_exit - net_entry) / net_entry if signal == "LONG" else (net_entry - net_exit) / net_entry
                    pnl_usd = pnl_pct * (qty * entry_price)

                    current_equity += pnl_usd
                    
                    all_trades.append({
                        "Trade_Timestamp": row['timestamp'],
                        "Trade_Date": trade_date_str,
                        "Symbol": sym,
                        "Side": signal,
                        "Qty": qty,
                        "Entry_Price": round(net_entry, 2),
                        "Exit_Date": next_day['timestamp'].strftime("%Y-%m-%d"),
                        "Exit_Price": round(net_exit, 2),
                        "PnL_USD": round(pnl_usd, 2),
                        "PnL_Percent": round(pnl_pct * 100, 3),
                        "Duration": "Overnight",
                    })

                    # Equity-Snapshot
                    equity_by_date[trade_date_str] = round(current_equity, 2)

        except Exception as e:
            print(f"Fehler bei {sym}: {e}")

    if not all_trades:
        print("Keine Trades generiert.")
        return

    # DataFrame erstellen & Sortieren
    trades_df = pd.DataFrame(all_trades)
    trades_df["Trade_Timestamp"] = pd.to_datetime(trades_df["Trade_Timestamp"], errors="coerce")
    trades_df = trades_df.sort_values(by=["Trade_Timestamp", "Symbol"], kind="mergesort").reset_index(drop=True)

    # Equity_After immer chronologisch aus realisiertem PnL ableiten.
    # So ist die Spalte konsistent mit der Export-Reihenfolge und reproduzierbar.
    trades_df["Equity_After"] = (initial_capital + trades_df["PnL_USD"].cumsum()).round(2)

    # ── Equity-Kurve aufbauen ─────────────────────────────────────────────
    # Chronologisch nach Trade-Datum die Equity kumulieren
    trades_sorted = trades_df.sort_values(["Trade_Timestamp", "Symbol"], kind="mergesort")
    eq_values = [initial_capital]
    eq_dates  = [pd.Timestamp(start_date)]
    running_eq = initial_capital
    for _, t in trades_sorted.iterrows():
        running_eq += t["PnL_USD"]
        eq_values.append(running_eq)
        eq_dates.append(pd.Timestamp(t["Trade_Date"]))
    eq_series = pd.Series(eq_values, index=pd.DatetimeIndex(eq_dates))
    # Duplikate: letzten Wert pro Tag behalten
    eq_series = eq_series[~eq_series.index.duplicated(keep="last")].sort_index()

    # ── Metriken berechnen ────────────────────────────────────────────────
    final_equity = float(eq_series.iloc[-1])
    total_ret    = (final_equity - initial_capital) / initial_capital
    n_days       = max((eq_series.index[-1] - eq_series.index[0]).days, 1)
    n_years      = n_days / 365.25
    cagr         = (1 + total_ret) ** (1 / max(n_years, 1e-6)) - 1
    returns      = eq_series.pct_change().dropna()
    sharpe       = (returns.mean() / (returns.std() + 1e-9)) * np.sqrt(252)
    neg_ret      = returns[returns < 0]
    sortino      = ((returns.mean() / (neg_ret.std() + 1e-9)) * np.sqrt(252)
                    if len(neg_ret) > 0 else float('nan'))
    rolling_max  = eq_series.cummax()
    dd           = (eq_series - rolling_max) / (rolling_max + 1e-9)
    max_dd       = float(dd.min())
    calmar       = cagr / (abs(max_dd) + 1e-9)

    # Trade-Statistiken
    wins       = trades_df[trades_df['PnL_USD'] > 0]
    losses     = trades_df[trades_df['PnL_USD'] <= 0]
    n_trades   = len(trades_df)
    win_rate   = len(wins) / (n_trades + 1e-9)
    pf         = wins['PnL_USD'].sum() / (abs(losses['PnL_USD'].sum()) + 1e-9)
    avg_pnl    = float(trades_df['PnL_USD'].mean())
    avg_win    = float(wins['PnL_USD'].mean()) if not wins.empty else 0.0
    avg_loss   = float(losses['PnL_USD'].mean()) if not losses.empty else 0.0

    # Streaks
    def _max_streak(arr):
        max_s = cur_s = 0
        for v in arr:
            cur_s = cur_s + 1 if v == 1 else 0
            max_s = max(max_s, cur_s)
        return max_s

    win_flags  = (trades_sorted['PnL_USD'] > 0).astype(int).tolist()
    loss_flags = (trades_sorted['PnL_USD'] <= 0).astype(int).tolist()
    max_win_streak  = _max_streak(win_flags)
    max_loss_streak = _max_streak(loss_flags)

    # ══════════════════════════════════════════════════════════════════════
    # REPORT AUSGABE (analog one_bar_backtest.py)
    # ══════════════════════════════════════════════════════════════════════
    sep  = "─" * 65
    sep2 = "═" * 65

    print(f"\n{sep2}")
    print(f"  BACKTEST REPORT – One-Bar-Breakout Gemini ({CONFIG['lookback']}-Bar High/Low)")
    print(f"  Zeitraum: {start_date} → {end_date}")
    print(f"  Symbole:  {', '.join(symbols)}")
    print(sep2)

    # Metriken-Tabelle
    print(f"\n  {'Metrik':<28} {'Wert':>12}")
    sep_short = "─" * 42
    print(f"  {sep_short}")
    metrics = [
        ("Gesamtrendite (%)",       f"{total_ret * 100:>+.2f}"),
        ("CAGR (%)",                f"{cagr * 100:>+.2f}"),
        ("Sharpe Ratio",            f"{sharpe:>.3f}"),
        ("Sortino Ratio",           f"{sortino:>.3f}" if not np.isnan(sortino) else "n/a"),
        ("Max Drawdown (%)",        f"{max_dd * 100:>.2f}"),
        ("Calmar Ratio",            f"{calmar:>.3f}"),
        ("Win-Rate (%)",            f"{win_rate * 100:>.2f}"),
        ("Profit Factor",           f"{pf:>.3f}"),
        ("Trades gesamt",           f"{n_trades:>d}"),
        ("Ø PnL / Trade ($)",       f"{avg_pnl:>+.2f}"),
        ("Ø Win ($)",               f"{avg_win:>+.2f}"),
        ("Ø Loss ($)",              f"{avg_loss:>+.2f}"),
        ("Endkapital ($)",          f"{final_equity:>,.2f}"),
    ]
    for label, val_str in metrics:
        print(f"  {label:<28} {val_str:>12}")
    print(f"  {sep_short}")

    # Jahresverteilung
    trades_df["_year"] = pd.to_datetime(trades_df["Trade_Date"]).dt.year
    years = trades_df["_year"].value_counts().sort_index()
    if not years.empty:
        print(f"\n  Jahresverteilung:")
        for yr, cnt in years.items():
            sub   = trades_df[trades_df["_year"] == yr]
            wr    = (sub["PnL_USD"] > 0).mean() * 100
            p_sum = sub["PnL_USD"].sum()
            print(f"    {yr}: {cnt:>4} Trades  "
                  f"WR={wr:>5.1f}%  PnL={p_sum:>+10.2f}")

    # Signal-Verteilung
    sig_counts = trades_df["Side"].value_counts()
    print(f"\n  Signal-Verteilung:")
    for sig, cnt in sig_counts.items():
        sub_sig = trades_df[trades_df["Side"] == sig]
        wr_sig  = (sub_sig["PnL_USD"] > 0).mean() * 100
        pnl_sig = sub_sig["PnL_USD"].sum()
        print(f"    {sig:<10} {cnt:>4} Trades  WR={wr_sig:>5.1f}%  PnL={pnl_sig:>+10.2f}")

    # Symbol-Verteilung
    sym_counts = trades_df["Symbol"].value_counts().sort_index()
    print(f"\n  Symbol-Verteilung:")
    for sym_name, cnt in sym_counts.items():
        sub_sym = trades_df[trades_df["Symbol"] == sym_name]
        wr_sym  = (sub_sym["PnL_USD"] > 0).mean() * 100
        pnl_sym = sub_sym["PnL_USD"].sum()
        print(f"    {sym_name:<8} {cnt:>4} Trades  WR={wr_sym:>5.1f}%  PnL={pnl_sym:>+10.2f}")

    print(f"\n{sep}")
    print(f"  Max Win-Streak:     {max_win_streak:>5}")
    print(f"  Max Loss-Streak:    {max_loss_streak:>5}")
    print(sep2)

    # ── Excel/CSV Export ──────────────────────────────────────────────────
    output_dir = Path(__file__).parent / "obb_trading_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Temp-Spalte entfernen
    export_df = trades_df.drop(columns=["_year"], errors="ignore")

    # Excel kann keine timezone-aware Datetimes speichern.
    if "Trade_Timestamp" in export_df.columns:
        ts = pd.to_datetime(export_df["Trade_Timestamp"], errors="coerce")
        try:
            if getattr(ts.dt, "tz", None) is not None:
                ts = ts.dt.tz_localize(None)
        except Exception:
            pass
        export_df["Trade_Timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")

    xlsx_path = output_dir / f"gemini_trades_{timestamp}.xlsx"
    if XLSX_AVAILABLE:
        try:
            export_df.to_excel(xlsx_path, index=False, engine="openpyxl")
            print(f"  Trade-Liste: {xlsx_path}")
        except Exception as e:
            print(f"  [ERROR] Excel-Export fehlgeschlagen: {e}")
            print("  [HINWEIS] Kein CSV-Fallback aktiv (gewuenscht: nur Excel).")
    else:
        print("  [ERROR] openpyxl nicht installiert – kein Export geschrieben (gewuenscht: nur Excel).")
        print("  [HINWEIS] Installation: pip install openpyxl")

    # ── Equity-Kurve Plot ─────────────────────────────────────────────────
    if MPL_AVAILABLE and len(eq_series) > 1:
        fig, axes = plt.subplots(2, 1, figsize=(15, 8), gridspec_kw={"height_ratios": [3, 1]})

        # Equity
        ax_eq = axes[0]
        ax_eq.plot(eq_series.index, eq_series.values,
                   label=f"Gemini OBB ({CONFIG['lookback']}-Bar Breakout)",
                   linewidth=2, color="#4CAF50")
        ax_eq.axhline(y=initial_capital, color="gray", linestyle="--", alpha=0.5, label="Startkapital")
        ax_eq.set_title(f"Equity Curve – OBB Gemini  |  {start_date} → {end_date}", fontsize=13)
        ax_eq.set_ylabel("Equity ($)")
        ax_eq.legend(fontsize=10)
        ax_eq.grid(True, alpha=0.3)
        ax_eq.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))

        # Drawdown
        ax_dd = axes[1]
        dd_pct = dd * 100
        ax_dd.fill_between(dd_pct.index, dd_pct.values, 0,
                           alpha=0.4, color="#F44336", label="Drawdown")
        ax_dd.set_title("Drawdown")
        ax_dd.set_ylabel("Drawdown (%)")
        ax_dd.legend(fontsize=10)
        ax_dd.grid(True, alpha=0.3)
        ax_dd.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))

        fig.autofmt_xdate(rotation=30, ha="right")
        plt.tight_layout()
        plot_path = output_dir / f"gemini_equity_{timestamp}.png"
        fig.savefig(plot_path, dpi=120, bbox_inches="tight")
        plt.close(fig)
        print(f"  Plot: {plot_path}")

# ==========================================
# LIVE EXECUTION (ALPACA)
# ==========================================
class OneBarLive:
    def __init__(self):
        key = os.getenv("APCA_API_KEY_ID")
        sec = os.getenv("APCA_API_SECRET_KEY")
        self.trading = TradingClient(key, sec, paper=CONFIG["alpaca_paper"])
        self.data = StockHistoricalDataClient(key, sec)
        self.trades_history = self._load_trades_history()

    def _load_trades_history(self):
        """Lade bisherige Trades aus Excel-Exports für Kelly-Berechnung."""
        data_dir = Path(__file__).parent / "obb_trading_data"
        all_trades = []
        
        # Suche neueste gemini_trades_*.xlsx
        if data_dir.exists():
            xlsx_files = sorted(data_dir.glob("gemini_trades_*.xlsx"), reverse=True)
            
            if xlsx_files:
                try:
                    df = pd.read_excel(xlsx_files[0])
                    all_trades = df.to_dict("records")
                except Exception as e:
                    print(f"Fehler beim Laden XLSX: {e}")
        
        return all_trades

    def execute_entries(self):
        """Wird um 15:58 ET aufgerufen (kurz vor Close)"""
        print(f"[{datetime.now()}] Starte Entry-Scan...")
        try:
            account = self.trading.get_account()
            equity = float(account.equity)
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (401, 403):
                print("[ERROR] Alpaca Auth fehlgeschlagen (401/403). Prüfe APCA_API_KEY_ID/APCA_API_SECRET_KEY und Paper/Live-Mode.")
            else:
                print(f"[ERROR] Account konnte nicht geladen werden: {e}")
            return
        
        # Lade aktuelle Win-Rate aus Historical Trades
        rolling_wr = compute_rolling_win_rate(
            self.trades_history,
            lookback=int(CONFIG.get("kelly_lookback_trades", 50)),
            min_trades=int(CONFIG.get("kelly_min_trades", 20)))
        
        for sym in CONFIG["symbols"]:
            # Hole letzte 60 Bars (um 50 sicher zu haben)
            req = StockBarsRequest(
                symbol_or_symbols=sym,
                timeframe=TimeFrame.Day,
                start=datetime.now()-timedelta(days=100),
                feed=CONFIG.get("alpaca_data_feed", "iex")
            )

            try:
                bars_df = self.data.get_stock_bars(req).df
            except Exception as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status in (401, 403):
                    # Einmaliger Fallback: falls Feed nicht erlaubt, auf IEX zurückfallen
                    if CONFIG.get("alpaca_data_feed", "iex").lower() != "iex":
                        print(f"[WARN] {sym}: Feed '{CONFIG.get('alpaca_data_feed')}' nicht erlaubt. Fallback auf IEX.")
                        try:
                            req_fallback = StockBarsRequest(
                                symbol_or_symbols=sym,
                                timeframe=TimeFrame.Day,
                                start=datetime.now()-timedelta(days=100),
                                feed="iex"
                            )
                            bars_df = self.data.get_stock_bars(req_fallback).df
                        except Exception as e2:
                            print(f"[ERROR] {sym}: Alpaca Datenabruf fehlgeschlagen (auch mit IEX): {e2}")
                            continue
                    else:
                        print(f"[ERROR] {sym}: Alpaca Auth/Permission-Fehler beim Datenabruf (401/403): {e}")
                        continue
                else:
                    print(f"[WARN] {sym}: Datenabruf fehlgeschlagen: {e}")
                    continue

            if bars_df is None or bars_df.empty:
                print(f"[WARN] {sym}: Keine Tagesdaten erhalten.")
                continue

            try:
                bars = bars_df.loc[sym]
            except Exception:
                print(f"[WARN] {sym}: Symbol nicht im Daten-Response enthalten.")
                continue

            if bars is None or len(bars) < CONFIG["lookback"] + 1:
                print(f"[WARN] {sym}: Zu wenige Bars für Lookback={CONFIG['lookback']}.")
                continue

            df = apply_strategy_logic(bars, CONFIG["lookback"])
            
            last_row = df.iloc[-1]
            side = None
            if last_row['long_signal']: side = OrderSide.BUY
            elif last_row['short_signal'] and CONFIG["allow_shorts"]: side = OrderSide.SELL
            
            if side:
                # Nutze Kelly Sizing mit historischer Win-Rate
                qty = calculate_position_size_kelly(equity, last_row['close'], CONFIG, rolling_wr)
                if qty > 0:
                    order = MarketOrderRequest(symbol=sym, qty=qty, side=side, time_in_force=TimeInForce.DAY)
                    self.trading.submit_order(order)
                    wr_str = f" (WR={rolling_wr*100:.1f}%)" if rolling_wr else ""
                    print(f"ORDER GESENDET: {side} {qty} {sym}{wr_str}")

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
# ==========================================
# KORRIGIERTER CLI ENTRY POINT
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
    parser.add_argument("--no-shorts", action="store_true",
                        help="Short-Signale deaktivieren (nur Longs)")
    
    args = parser.parse_args()

    if args.no_shorts:
        CONFIG["allow_shorts"] = False

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