"""Diagnose-Skript: ICT OB-Detection Trichteranalyse.

Lädt historische IBKR-Bars (MIDPOINT, useRTH=False) für ein Futures-Symbol,
resampled auf 4H und zählt, wie viele Kandidaten jede Filterstufe überleben:

  Stufe 1 – Displacement:   |body| > displacement_mult × ATR(atr_period)
  Stufe 2 – + FVG:          3-Kerzen-Imbalance im ±1-Bar-Umfeld
  Stufe 3 – + Sweep:        Sweep eines prior Swing-Low/High
  Stufe 4 – + Price-in-OB:  aktueller Preis ≤ OB-Zone ±50 %

Verwendung:
    python tools/diagnose_ict_ob.py \
        --config configs/ict_ob_futures_live.yaml \
        [--symbol NQ] [--days 60] \
        [--disp-range 0.8,1.0,1.2,1.4,1.6,1.8,2.0]
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

from core.indicators import atr, detect_order_blocks, resample_ohlcv, swing_highs, swing_lows


# ── Stufen-Trichter (ohne externe Strategie-Klasse) ─────────────────────────

def _fvg_set(df: pd.DataFrame) -> set[int]:
    highs = df["High"].values
    lows = df["Low"].values
    fvgs: set[int] = set()
    for i in range(1, len(df) - 1):
        if lows[i + 1] > highs[i - 1]:
            fvgs.add(i)
        if highs[i + 1] < lows[i - 1]:
            fvgs.add(i)
    return fvgs


def _funnel(
    df: pd.DataFrame,
    atr_period: int,
    displacement_mult: float,
    swing_lookback: int,
    current_price: float,
) -> dict:
    """Gibt Counts pro Filterstufe zurück."""
    n = len(df)
    min_req = max(swing_lookback * 2 + 3, atr_period + 2)
    if n < min_req:
        return {"bars_4h": n, "displacement": 0, "fvg": 0, "sweep": 0, "price_in_ob": 0}

    atr_vals = atr(df, atr_period)
    highs = df["High"].values
    lows = df["Low"].values
    opens = df["Open"].values
    closes = df["Close"].values

    sw_hi = swing_highs(df["High"], swing_lookback)
    sw_lo = swing_lows(df["Low"], swing_lookback)
    swing_high_levels = [(i, highs[i]) for i in range(n) if sw_hi.iloc[i]]
    swing_low_levels = [(i, lows[i]) for i in range(n) if sw_lo.iloc[i]]
    fvgs = _fvg_set(df)

    cnt_displacement = cnt_fvg = cnt_sweep = cnt_price = 0
    obs: list[dict] = []

    for i in range(min_req, n):
        atr_val = atr_vals.iloc[i]
        if pd.isna(atr_val) or atr_val <= 0:
            continue
        body = closes[i] - opens[i]
        if abs(body) <= displacement_mult * atr_val:
            continue
        cnt_displacement += 1

        has_fvg = i in fvgs or (i - 1) in fvgs or (i + 1 < n and (i + 1) in fvgs)
        if not has_fvg:
            continue
        cnt_fvg += 1

        search_start = max(0, i - swing_lookback * 4)
        if body > 0:
            last_sl = next(((si, sl) for si, sl in reversed(swing_low_levels) if si < i), None)
            if last_sl is None:
                continue
            swept = any(lows[j] < last_sl[1] for j in range(last_sl[0] + 1, i + 1))
            if not swept:
                continue
            ob_idx = next((k for k in range(i - 1, max(search_start - 1, -1), -1)
                           if closes[k] < opens[k]), None)
            if ob_idx is None:
                continue
            ob = {"type": "bullish",
                  "high": float(opens[ob_idx]),
                  "low": float(closes[ob_idx]),
                  "idx": ob_idx}
        else:
            last_sh = next(((si, sh) for si, sh in reversed(swing_high_levels) if si < i), None)
            if last_sh is None:
                continue
            swept = any(highs[j] > last_sh[1] for j in range(last_sh[0] + 1, i + 1))
            if not swept:
                continue
            ob_idx = next((k for k in range(i - 1, max(search_start - 1, -1), -1)
                           if closes[k] > opens[k]), None)
            if ob_idx is None:
                continue
            ob = {"type": "bearish",
                  "high": float(closes[ob_idx]),
                  "low": float(opens[ob_idx]),
                  "idx": ob_idx}

        cnt_sweep += 1
        obs.append(ob)

    # Stufe 4: valid_obs (nicht invalidiert + Preis in Zone)
    closes_all = closes
    for ob in obs:
        ob_high, ob_low = ob["high"], ob["low"]
        ob_range = ob_high - ob_low
        if ob_range <= 0:
            continue
        invalidated = False
        for j in range(ob["idx"] + 1, n):
            if ob["type"] == "bullish" and closes_all[j] < ob_low:
                invalidated = True
                break
            if ob["type"] == "bearish" and closes_all[j] > ob_high:
                invalidated = True
                break
        if invalidated:
            continue
        buf = ob_range * 0.5
        if ob_low - buf <= current_price <= ob_high + buf:
            cnt_price += 1

    return {
        "bars_4h": n,
        "displacement": cnt_displacement,
        "fvg": cnt_fvg,
        "sweep": cnt_sweep,
        "price_in_ob": cnt_price,
    }


# ── Daten laden ──────────────────────────────────────────────────────────────

# yfinance-Ticker-Mapping für Futures (Symbol → yf-Ticker)
_YF_FUTURES_MAP = {
    "NQ": "NQ=F",
    "ES": "ES=F",
    "YM": "YM=F",
    "RTY": "RTY=F",
    "MNQ": "MNQ=F",
    "MES": "MES=F",
    "CL":  "CL=F",
    "GC":  "GC=F",
}


def _load_yfinance(symbol: str, days: int) -> pd.DataFrame:
    """Lädt 5M-Bars via yfinance (max. 60 Tage, kein IBKR nötig)."""
    try:
        import yfinance as yf
    except ImportError:
        print("[FEHLER] yfinance nicht installiert: pip install yfinance")
        return pd.DataFrame()

    ticker = _YF_FUTURES_MAP.get(symbol.upper(), symbol)
    # yfinance liefert 5m nur für ≤60 Tage; bei mehr: auf 60 kappen
    actual_days = min(days, 59)
    if actual_days < days:
        print(f"[yfinance] 5M-Limit: kürze auf {actual_days} Tage")

    print(f"[yfinance] Lade {ticker} ({actual_days}d, 5m) …")
    try:
        df = yf.download(
            ticker,
            period=f"{actual_days}d",
            interval="5m",
            auto_adjust=True,
            progress=False,
        )
    except Exception as e:
        print(f"[FEHLER] yfinance-Download fehlgeschlagen: {e}")
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # yfinance kann MultiIndex-Columns liefern
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.rename(columns={
        "Open": "Open", "High": "High", "Low": "Low",
        "Close": "Close", "Volume": "Volume",
    })
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in df.columns:
            df[col] = 0.0

    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    return df[["Open", "High", "Low", "Close", "Volume"]].sort_index()


async def _load_ibkr(
    symbol: str,
    days: int,
    params: dict,
    host: str = "192.168.188.93",
    port: int = 4004,
    client_id: int = 101,
    timeframe: str = "5Min",
) -> pd.DataFrame:
    from data.providers.ibkr_provider import IBKRDataProvider
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    print(f"  host={host}  port={port}  client_id={client_id}  tf={timeframe}")
    provider = IBKRDataProvider(
        host=host,
        port=port,
        client_id=client_id,
        asset_class="futures",
        contract_cfg=params,
        use_rth=False,
    )
    try:
        df = await provider.get_bars(symbol, start, end, timeframe)
        await provider.close()
        return df
    except Exception as e:
        print(f"[IBKR] Verbindung fehlgeschlagen: {e}")
        return pd.DataFrame()


# ── Haupt-Ausgabe ─────────────────────────────────────────────────────────

def _print_table(rows: list[dict], symbol: str, days: int) -> None:
    print(f"\n{'=' * 70}")
    print(f"ICT OB Trichteranalyse – {symbol}  |  {days} Tage  |  4H-Zeitebene")
    print(f"{'=' * 70}")
    header = f"{'disp_mult':>10} {'bars_4h':>8} {'disp':>6} {'fvg':>6} "
    header += f"{'sweep':>6} {'price_ob':>9} {'disp%':>7} {'fvg%':>7}"
    print(header)
    print("-" * 70)
    for r in rows:
        b = r["bars_4h"]
        d = r["displacement"]
        f = r["fvg"]
        s = r["sweep"]
        p = r["price_in_ob"]
        disp_pct = f"{d / b * 100:.1f}" if b else "-"
        fvg_pct = f"{f / b * 100:.1f}" if b else "-"
        print(
            f"{r['disp_mult']:>10.2f} {b:>8} {d:>6} {f:>6} "
            f"{s:>6} {p:>9} {disp_pct:>7} {fvg_pct:>7}"
        )
    print(f"{'=' * 70}")
    print("disp%: Displacement-Kerzen / alle 4H-Kerzen")
    print("fvg%: + FVG-Filterrate")
    print("Empfehlung: disp_mult so wählen, dass disp% ≥ 2–3 %")


def _print_single(r: dict, disp_mult: float) -> None:
    b = r["bars_4h"]
    print(f"\n{'─' * 50}")
    print(f"  4H-Bars gesamt:       {b}")
    print(f"  Displacement >{disp_mult}×ATR: {r['displacement']}")
    print(f"  + FVG vorhanden:      {r['fvg']}")
    print(f"  + Swing Sweep:        {r['sweep']}  ← vollständige OBs")
    print(f"  + Preis in OB-Zone:   {r['price_in_ob']}  ← signal-fähig (heute)")
    if b:
        pct = r["displacement"] / b * 100
        print(f"\n  Displacement-Rate:    {pct:.1f} %  {'⚠ zu niedrig' if pct < 2 else '✓ ok'}")
    print(f"{'─' * 50}")


# ── CLI ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="ICT OB 4H-Trichteranalyse für Futures",
    )
    parser.add_argument("--config", "-c", default="configs/ict_ob_futures_live.yaml",
                        help="Config-Pfad (für Contract-Parameter)")
    parser.add_argument("--symbol", default=None,
                        help="Futures-Symbol (Default aus Config)")
    parser.add_argument("--days", type=int, default=60,
                        help="Lookback in Tagen (Default: 60)")
    parser.add_argument("--atr-period", type=int, default=None,
                        help="ATR-Periode (Default aus Config)")
    parser.add_argument("--swing-lookback", type=int, default=None,
                        help="Swing-Lookback 4H (Default aus Config)")
    parser.add_argument(
        "--disp-range",
        default=None,
        help="Komma-getrennte Liste von displacement_mult-Werten (z.B. '0.8,1.0,1.2,1.4,1.6,1.8')",
    )
    parser.add_argument("--csv", default=None,
                        help="Statt IBKR eine lokale CSV-Datei (5M OHLCV) verwenden")
    args = parser.parse_args()

    # Config laden über zentralen Loader (YAML+base.yaml+.env-Override) –
    # identisch zu main.py _build_data_provider, kein Hardcoding.
    params: dict = {}
    cfg_symbol = "NQ"
    cfg_atr = 14
    cfg_swing = 3
    cfg_disp = 1.25
    ibkr_host = "127.0.0.1"
    ibkr_port = 4002
    ibkr_client_id = 1
    ibkr_timeframe = "5Min"

    cfg_path = Path(args.config)
    if cfg_path.exists():
        try:
            import sys as _sys
            _repo_root = str(Path(__file__).parent.parent)
            if _repo_root not in _sys.path:
                _sys.path.insert(0, _repo_root)
            from core.config import load_config
            app_cfg = load_config(cfg_path)
            strat_params = app_cfg.strategy.params or {}
            params = dict(strat_params)
            cfg_symbol = (app_cfg.strategy.symbols or [cfg_symbol])[0]
            cfg_atr = int(strat_params.get("atr_period", cfg_atr))
            cfg_swing = int(strat_params.get("swing_lookback_4h", cfg_swing))
            cfg_disp = float(strat_params.get("displacement_mult", cfg_disp))
            ibkr_host = str(app_cfg.broker.ibkr_host or "127.0.0.1")
            ibkr_port = int(app_cfg.broker.ibkr_port)
            ibkr_client_id = int(app_cfg.broker.ibkr_client_id)
            ibkr_timeframe = str(app_cfg.data.timeframe)
            print(f"[Config geladen] {cfg_path}  (host={ibkr_host}:{ibkr_port})")
        except Exception as e:
            print(f"[Warnung] Config nicht geladen: {e}")
    else:
        print(f"[Warnung] Config-Datei nicht gefunden: {cfg_path} – verwende Defaults")

    symbol = (args.symbol or cfg_symbol).upper()
    atr_period = args.atr_period or cfg_atr
    swing_lookback = args.swing_lookback or cfg_swing
    # +100 auf client_id genau wie main.py _build_data_provider
    data_client_id = ibkr_client_id + 100

    # Displacement-Multiplikatoren für Sweep
    if args.disp_range:
        try:
            disp_mults = [float(x.strip()) for x in args.disp_range.split(",")]
        except ValueError:
            print("Ungültiger --disp-range")
            sys.exit(1)
    else:
        disp_mults = None  # nur config-Wert

    # Daten laden: CSV → IBKR → yfinance (Fallback-Kette)
    if args.csv:
        print(f"[CSV] Lade {args.csv}")
        df_5m = pd.read_csv(args.csv, index_col=0, parse_dates=True)
        df_5m.columns = [c.strip().title() for c in df_5m.columns]
        if df_5m.index.tz is None:
            df_5m.index = df_5m.index.tz_localize("UTC")
    else:
        print(f"[IBKR] Lade {symbol} ({args.days}d, tf={ibkr_timeframe}, MIDPOINT) …")
        df_5m = await _load_ibkr(
            symbol, args.days, params,
            host=ibkr_host, port=ibkr_port,
            client_id=data_client_id,
            timeframe=ibkr_timeframe,
        )
        if df_5m is None or df_5m.empty:
            print("[IBKR] Keine Daten – versuche yfinance als Fallback …")
            df_5m = _load_yfinance(symbol, args.days)

    if df_5m is None or df_5m.empty:
        print("[FEHLER] Keine 5M-Bars weder via IBKR noch yfinance. Abbruch.")
        sys.exit(1)

    print(f"[Daten] {len(df_5m)} 5M-Bars geladen  "
          f"({df_5m.index[0]}  –  {df_5m.index[-1]})")

    # 4H resamplen
    df_4h = resample_ohlcv(df_5m, "4H")
    current_price = float(df_5m["Close"].iloc[-1])
    print(f"[4H]   {len(df_4h)} 4H-Bars  |  aktueller Preis: {current_price:.2f}")

    # ATR-Statistik
    atr_vals = atr(df_4h, atr_period)
    valid_atr = atr_vals.dropna()
    if not valid_atr.empty:
        print(f"[ATR-{atr_period}] median={valid_atr.median():.1f}  "
              f"min={valid_atr.min():.1f}  max={valid_atr.max():.1f}  "
              f"(aktuell: {valid_atr.iloc[-1]:.1f})")
        atr_now = float(valid_atr.iloc[-1])
        print(f"  → displacement_mult {cfg_disp:.1f}×: "
              f"Body-Minimum = {cfg_disp * atr_now:.0f} Pkt")

    print()

    if disp_mults:
        # Sweep über Multiplikatoren
        rows = []
        for dm in disp_mults:
            r = _funnel(df_4h, atr_period, dm, swing_lookback, current_price)
            r["disp_mult"] = dm
            rows.append(r)
        _print_table(rows, symbol, args.days)

        # Hinweis für den konfigurierten Wert
        configured_row = next((r for r in rows if abs(r["disp_mult"] - cfg_disp) < 0.01), None)
        if configured_row:
            print(f"\nKonfigurierter Wert ({cfg_disp:.1f}):")
            _print_single(configured_row, cfg_disp)
    else:
        # Nur konfigurierten Wert
        r = _funnel(df_4h, atr_period, cfg_disp, swing_lookback, current_price)
        _print_single(r, cfg_disp)

    # Einzelanalyse mit vollständigem detect_order_blocks
    print(f"\n[detect_order_blocks] konfiguriert "
          f"(disp_mult={cfg_disp}, atr={atr_period}, swing={swing_lookback})")
    obs_full = detect_order_blocks(df_4h, atr_period, cfg_disp, swing_lookback)
    print(f"  Rohe OBs detektiert:    {len(obs_full)}")
    if obs_full:
        from strategy.ict_ob import IctOrderBlockStrategy
        valid = IctOrderBlockStrategy._filter_valid_obs(obs_full, df_4h, current_price)
        print(f"  Nach valid_obs-Filter:  {len(valid)}")
        if valid:
            for v in valid[-3:]:
                print(f"    {v['type']:8s}  OB=[{v['low']:.2f}–{v['high']:.2f}]"
                      f"  ts={v['timestamp'].date() if hasattr(v['timestamp'], 'date') else v['timestamp']}")
        else:
            print("  → Kein OB überlebt valid_obs (invalidiert oder Preis außerhalb)")
    print()


if __name__ == "__main__":
    asyncio.run(main())
