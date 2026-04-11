#!/usr/bin/env python3
"""
obb_universe_selector.py – Dynamisches, regime-adaptives OBB-Universum

Zweischichtiges Scoring:
  Schicht 1: Discovery-Score (Langfrist-Tauglichkeit aus OBBAutoDiscoveryScanners)
  Schicht 2: Rolling OBB Performance Score (echte Strategie-Edge der letzten N Tage)

Kombinierter Final Score → harte Filter → Diversifikationsschutz → Top 15-30 Symbole.

Aufruf:
  python obb_universe_selector.py                   # Standard (CSV vom Scanner laden)
  python obb_universe_selector.py --fresh            # Scanner-CSV frisch erzeugen (ruft Scanner auf)
  python obb_universe_selector.py --top 20           # Max. 20 Symbole im Universum
  python obb_universe_selector.py --min-trades 6     # Rolling Min-Trades Override

Integration:
  from obb_universe_selector import load_current_universe
  symbols = load_current_universe()                  # List[str]
  universe_df = load_current_universe(as_dataframe=True)  # pd.DataFrame
"""

import json
import logging
import argparse
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========================== KONFIGURATION ==========================

BASE_DIR = Path(__file__).parent
DISCOVERY_CSV = BASE_DIR / "obb_discovery_pro" / "obb_pro_full.csv"
OUTPUT_DIR = BASE_DIR / "obb_universe"

# Rolling OBB Backtest Parameter
ROLLING_WINDOW_DAYS = 60          # Handelstage für Rolling-Simulation
OBB_LOOKBACK = 50                 # 50-Bar High/Low (identisch mit Trader)
DATA_FETCH_DAYS = 252             # Kalendertage an Daten laden (genug für 60 Handelstage + Lookback)
MAX_WORKERS = 15                  # Threads für parallele Berechnung

# Scoring-Gewichte
WEIGHT_DISCOVERY = 0.40
WEIGHT_ROLLING = 0.60

# Harte Filter
FILTER_MIN_ROLLING_TRADES = 8
FILTER_MIN_ROLLING_WINRATE = 57.0
FILTER_MIN_AVG_RANGE = 3.0
FILTER_MIN_AVG_VOLUME_M = 5.0
FILTER_MAX_DRAWDOWN_30D = -35.0   # > -35 (also -30 ist ok, -40 wird rausgefiltert)
FILTER_MIN_VOLUME_SPIKE = 1.4

# Diversifikation: Max Symbole pro Korrelationsgruppe
MAX_PER_GROUP = 2

# Universum-Größe
DEFAULT_TOP_N = 30

# ========================== KORRELATIONS-GRUPPEN ==========================

CORRELATION_GROUPS = {
    "index_etfs":    {"SPY", "QQQ", "IWM", "DIA", "VOO", "VTI", "RSP"},
    "leveraged":     {"TQQQ", "SOXL", "TECL", "FNGU", "BULZ", "SPXL", "UPRO", "TNA", "LABU"},
    "mega_tech":     {"AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NFLX"},
    "semi_ai":       {"NVDA", "AMD", "AVGO", "SMCI", "ARM", "TSM", "ASML", "MU", "INTC",
                      "MRVL", "QCOM", "SNPS", "CDNS", "AMAT", "LRCX", "KLAC", "SNDK"},
    "meme_hype":     {"TSLA", "PLTR", "HOOD", "RKLB", "GME", "AMC", "BBBY"},
    "crypto_proxy":  {"COIN", "MSTR", "MARA", "RIOT", "CLSK", "BITF", "HUT"},
    "biotech":       {"XBI", "LABU", "MRNA", "BNTX", "CRSP", "NTLA"},
    "high_beta":     {"CVNA", "UPST", "APP", "SOFI", "AFRM", "LCID", "RIVN"},
    "crwd_cyber":    {"CRWD", "PANW", "FTNT", "ZS", "S", "NET"},
}


def _get_group(symbol: str) -> str:
    """Gibt die Korrelationsgruppe eines Symbols zurück."""
    for group_name, members in CORRELATION_GROUPS.items():
        if symbol in members:
            return group_name
    return "other"


# ========================== LOGGING ==========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("OBB_Universe")


# ========================== MARKTREGIME ==========================

def _as_float_scalar(value, default: float = 0.0) -> float:
    """Extrahiert robust einen Float aus Scalar/Series/DataFrame-Zellen."""
    try:
        if isinstance(value, pd.Series):
            if value.empty:
                return default
            return float(value.iloc[-1])
        return float(value)
    except Exception:
        return default

def get_market_regime() -> tuple[float, str]:
    """VIX-Level und SPY-Trend (bull/bear/neutral) ermitteln."""
    try:
        vix_data = yf.download("^VIX", period="5d", progress=False)
        vix_close = vix_data["Close"]
        if isinstance(vix_close, pd.DataFrame):
            vix_close = vix_close.iloc[:, 0]
        vix = _as_float_scalar(vix_close.iloc[-1], default=20.0)
    except Exception:
        vix = 20.0

    try:
        spy = yf.download("SPY", period="60d", progress=False)
        spy_close = spy["Close"]
        if isinstance(spy_close, pd.DataFrame):
            spy_close = spy_close.iloc[:, 0]
        sma20 = spy_close.rolling(20).mean().iloc[-1]
        last_close = _as_float_scalar(spy_close.iloc[-1], default=np.nan)
        sma20_val = _as_float_scalar(sma20, default=np.nan)
        trend = "bull" if (not np.isnan(last_close) and not np.isnan(sma20_val) and last_close > sma20_val) else "bear"
    except Exception:
        trend = "neutral"

    return vix, trend


# ========================== ROLLING OBB SIMULATION ==========================

def compute_rolling_obb_score(symbol: str, rolling_days: int = ROLLING_WINDOW_DAYS,
                               lookback: int = OBB_LOOKBACK) -> dict | None:
    """
    Simuliert die echte OBB-Logik auf den letzten `rolling_days` Handelstagen:
      - 50-Bar High/Low → Long bei Close > n-High, Short bei Close < n-Low
      - Exit am nächsten Open
    Berechnet: rolling_winrate, rolling_profit_factor, rolling_trades, avg_return_per_trade.
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=f"{DATA_FETCH_DAYS}d", auto_adjust=True)

        if df.empty or len(df) < lookback + rolling_days:
            return None

        # MultiIndex-Fix (yfinance gibt manchmal MultiIndex zurück)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.droplevel(0, axis=1)

        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()

        # OBB-Signale berechnen (identisch mit OneBarBreakoutGemini)
        df["n_high"] = df["High"].shift(1).rolling(window=lookback).max()
        df["n_low"] = df["Low"].shift(1).rolling(window=lookback).min()
        df["long_signal"] = df["Close"] > df["n_high"]
        df["short_signal"] = df["Close"] < df["n_low"]

        # Nur die letzten rolling_days Handelstage auswerten
        df_recent = df.iloc[-(rolling_days + 1):].copy()

        trades = []
        for i in range(len(df_recent) - 1):
            row = df_recent.iloc[i]
            next_row = df_recent.iloc[i + 1]

            if row["long_signal"]:
                entry = row["Close"]
                exit_price = next_row["Open"]
                ret = (exit_price - entry) / entry
                trades.append({"side": "LONG", "return": ret})
            elif row["short_signal"]:
                entry = row["Close"]
                exit_price = next_row["Open"]
                ret = (entry - exit_price) / entry
                trades.append({"side": "SHORT", "return": ret})

        if len(trades) == 0:
            return {"symbol": symbol, "rolling_trades": 0, "rolling_winrate": 0,
                    "rolling_profit_factor": 0, "avg_return_per_trade": 0,
                    "rolling_obb_score": 0}

        returns = [t["return"] for t in trades]
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r <= 0]

        n_trades = len(trades)
        winrate = len(wins) / n_trades * 100
        avg_return = float(np.mean(returns)) * 100  # in Prozent

        gross_profit = float(sum(wins)) if wins else 0.0
        gross_loss = abs(float(sum(losses))) if losses else 0.0
        if gross_loss <= 1e-12:
            profit_factor = 5.0 if gross_profit > 0 else 1.0
        else:
            profit_factor = gross_profit / gross_loss

        # Rolling OBB Score: Kombination aus Winrate, Profit Factor und Trades
        # Normiert: winrate/100 * profit_factor * log(trades+1) * avg_return_factor
        avg_ret_factor = max(0.1, 1.0 + avg_return / 2.0)  # avg_return in % → Bonus
        rolling_score = (winrate / 100) * min(profit_factor, 5.0) * np.log1p(n_trades) * avg_ret_factor

        return {
            "symbol": symbol,
            "rolling_trades": n_trades,
            "rolling_winrate": round(winrate, 1),
            "rolling_profit_factor": round(profit_factor, 2),
            "avg_return_per_trade": round(avg_return, 3),
            "rolling_obb_score": round(rolling_score, 3),
        }

    except Exception as e:
        log.warning(f"{symbol}: Rolling-OBB-Fehler: {e}")
        return None


# ========================== DISCOVERY-DATEN LADEN ==========================

def load_discovery_data(csv_path: Path = DISCOVERY_CSV) -> pd.DataFrame:
    """Lädt die Discovery-Scanner CSV mit allen Metriken."""
    if not csv_path.exists():
        log.error(f"Discovery-CSV nicht gefunden: {csv_path}")
        log.error("Bitte zuerst OBBAutoDiscoveryScanners.py ausführen oder --fresh verwenden.")
        return pd.DataFrame()

    df = pd.read_csv(csv_path)
    log.info(f"Discovery-Daten geladen: {len(df)} Assets aus {csv_path.name}")
    return df


# ========================== SCORING & FILTER ==========================

def normalize_series(s: pd.Series) -> pd.Series:
    """Min-Max-Normalisierung auf [0, 1]."""
    smin, smax = s.min(), s.max()
    if smax - smin < 1e-9:
        return pd.Series(0.5, index=s.index)
    return (s - smin) / (smax - smin)


def _apply_hard_filters(df: pd.DataFrame,
                        min_rolling_trades: int,
                        min_rolling_winrate: float,
                        min_avg_range: float,
                        min_avg_volume_m: float,
                        max_drawdown_30d: float,
                        min_volume_spike: float) -> pd.DataFrame:
    """Wendet die Filterregeln auf ein DataFrame an."""
    out = df[df["rolling_trades"] >= min_rolling_trades].copy()
    out = out[out["rolling_winrate"] >= min_rolling_winrate].copy()
    out = out[out["Avg_Range_%"] >= min_avg_range].copy()
    out = out[out["Avg_Volume_M"] >= min_avg_volume_m].copy()
    out = out[out["Drawdown_30d_%"] > max_drawdown_30d].copy()
    out = out[
        (out["Volume_Spike"] >= min_volume_spike) |
        (out["Hype_Anomaly"] == "Yes")
    ].copy()
    return out


def build_universe(discovery_df: pd.DataFrame, vix: float, spy_trend: str,
                   top_n: int = DEFAULT_TOP_N,
                   min_rolling_trades: int = FILTER_MIN_ROLLING_TRADES,
                   rolling_days: int = ROLLING_WINDOW_DAYS) -> pd.DataFrame:
    """
    Hauptfunktion: Berechnet Rolling OBB Score für alle Assets,
    kombiniert mit Discovery Score, filtert und diversifiziert.
    """
    if discovery_df.empty:
        return pd.DataFrame()

    symbols = discovery_df["Symbol"].tolist()
    log.info(f"Berechne Rolling OBB Scores für {len(symbols)} Symbole (parallel, {MAX_WORKERS} Workers)...")

    # ── Schicht 2: Rolling OBB Score parallel berechnen ──
    rolling_results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(compute_rolling_obb_score, sym, rolling_days=rolling_days): sym
            for sym in symbols
        }
        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            if done_count % 50 == 0:
                log.info(f"  ... {done_count}/{len(symbols)} berechnet")
            result = future.result()
            if result:
                rolling_results[result["symbol"]] = result

    log.info(f"Rolling Scores berechnet für {len(rolling_results)}/{len(symbols)} Symbole")

    # Merge Discovery + Rolling
    rolling_df = pd.DataFrame(rolling_results.values())
    if rolling_df.empty:
        log.error("Keine Rolling-Ergebnisse verfügbar!")
        return pd.DataFrame()

    rolling_df = rolling_df.rename(columns={"symbol": "Symbol"})
    merged = discovery_df.merge(rolling_df, on="Symbol", how="inner")

    if merged.empty:
        log.error("Merge ergab 0 Symbole!")
        return pd.DataFrame()

    log.info(f"Merge: {len(merged)} Symbole mit beiden Scores")

    # ── Harte Filter anwenden ──
    before_filter = len(merged)

    merged_base = merged.copy()

    strict_filtered = _apply_hard_filters(
        merged_base,
        min_rolling_trades=min_rolling_trades,
        min_rolling_winrate=FILTER_MIN_ROLLING_WINRATE,
        min_avg_range=FILTER_MIN_AVG_RANGE,
        min_avg_volume_m=FILTER_MIN_AVG_VOLUME_M,
        max_drawdown_30d=FILTER_MAX_DRAWDOWN_30D,
        min_volume_spike=FILTER_MIN_VOLUME_SPIKE,
    )
    merged = strict_filtered
    log.info(f"Nach harten Filtern: {len(merged)} von {before_filter} Symbolen übrig")

    if merged.empty:
        # Fallback: Regeln leicht lockern, falls das Universum sonst leer bleibt.
        # So bleibt der Selector robust in ruhigen/ungewöhnlichen Marktphasen.
        log.warning("Harte Filter ergaben 0 Symbole. Aktiviere sanften Fallback.")

        fallback_levels = [
            {
                "name": "Fallback A (Volume-Spike leicht gelockert)",
                "min_rolling_trades": max(6, min_rolling_trades),
                "min_rolling_winrate": 56.0,
                "min_avg_range": FILTER_MIN_AVG_RANGE,
                "min_avg_volume_m": FILTER_MIN_AVG_VOLUME_M,
                "max_drawdown_30d": FILTER_MAX_DRAWDOWN_30D,
                "min_volume_spike": 1.1,
            },
            {
                "name": "Fallback B (Winrate/Trades moderat gelockert)",
                "min_rolling_trades": 6,
                "min_rolling_winrate": 54.0,
                "min_avg_range": 2.8,
                "min_avg_volume_m": 4.0,
                "max_drawdown_30d": -40.0,
                "min_volume_spike": 1.0,
            },
        ]

        for level in fallback_levels:
            candidate = _apply_hard_filters(
                merged_base,
                min_rolling_trades=level["min_rolling_trades"],
                min_rolling_winrate=level["min_rolling_winrate"],
                min_avg_range=level["min_avg_range"],
                min_avg_volume_m=level["min_avg_volume_m"],
                max_drawdown_30d=level["max_drawdown_30d"],
                min_volume_spike=level["min_volume_spike"],
            )
            if not candidate.empty:
                merged = candidate
                log.warning(f"{level['name']} aktiv: {len(merged)} Symbole")
                break

        if merged.empty:
            log.error("Auch Fallback lieferte 0 Symbole. Kein Universum erzeugbar.")
            return pd.DataFrame()

    # ── Normalisierung und Final Score ──
    merged["discovery_norm"] = normalize_series(merged["Score"])
    merged["rolling_norm"] = normalize_series(merged["rolling_obb_score"])

    merged["Final_Score"] = (
        WEIGHT_DISCOVERY * merged["discovery_norm"] +
        WEIGHT_ROLLING * merged["rolling_norm"]
    )

    # Regime-abhängiger Bonus für Hype/Squeeze im Bull-Markt
    if spy_trend == "bull":
        hype_bonus = (merged["Hype_Anomaly"] == "Yes").astype(float) * 0.05
        squeeze_bonus = (merged["Short_Squeeze_Candidate"] == "Yes").astype(float) * 0.03
        merged["Final_Score"] += hype_bonus + squeeze_bonus

    # VIX-Regime: Im hohen VIX-Umfeld Leveraged leicht abstrafen
    if vix > 30:
        is_leveraged = merged["Symbol"].isin(CORRELATION_GROUPS.get("leveraged", set()))
        merged.loc[is_leveraged, "Final_Score"] *= 0.85

    merged = merged.sort_values("Final_Score", ascending=False).reset_index(drop=True)

    # ── Diversifikationsfilter (Independence Guard) ──
    merged["Corr_Group"] = merged["Symbol"].apply(_get_group)
    group_counts: dict[str, int] = {}
    keep_idx = []

    for idx, row in merged.iterrows():
        group = row["Corr_Group"]
        current = group_counts.get(group, 0)
        if current < MAX_PER_GROUP:
            keep_idx.append(idx)
            group_counts[group] = current + 1

    diversified = merged.loc[keep_idx].head(top_n).copy()
    diversified["Rank"] = range(1, len(diversified) + 1)

    removed_by_div = len(merged) - len(diversified)
    if removed_by_div > 0:
        log.info(f"Diversifikation: {removed_by_div} Symbole wegen Gruppen-Limit entfernt")

    log.info(f"Finales Universum: {len(diversified)} Symbole")

    return diversified


# ========================== AUSGABE ==========================

def save_outputs(df: pd.DataFrame, vix: float, spy_trend: str) -> None:
    """Speichert JSON, TXT und HTML Dashboard."""
    OUTPUT_DIR.mkdir(exist_ok=True)

    if df.empty:
        log.warning("Leeres Universum – keine Ausgabe erzeugt.")
        return

    # ── JSON (mit allen Metriken) ──
    json_path = OUTPUT_DIR / "current_obb_universe.json"
    records = df.to_dict(orient="records")
    payload = {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market_regime": {"vix": vix, "spy_trend": spy_trend},
        "universe_size": len(df),
        "symbols": df["Symbol"].tolist(),
        "details": records,
    }
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log.info(f"JSON  → {json_path}")

    # ── TXT (einfache Symbol-Liste) ──
    txt_path = OUTPUT_DIR / "current_obb_universe.txt"
    txt_path.write_text("\n".join(df["Symbol"].tolist()), encoding="utf-8")
    log.info(f"TXT   → {txt_path}")

    # ── HTML Dashboard ──
    html_path = OUTPUT_DIR / "obb_universe_dashboard.html"
    _generate_dashboard(df, vix, spy_trend, html_path)
    log.info(f"HTML  → {html_path}")


def _generate_dashboard(df: pd.DataFrame, vix: float, spy_trend: str, path: Path) -> None:
    """Interaktives HTML-Dashboard mit DataTables und Spalten-Filtern."""

    # Spalten für die Anzeige auswählen
    display_cols = [
        "Rank", "Symbol", "Category", "Corr_Group", "Final_Score",
        "Score", "rolling_obb_score", "rolling_winrate", "rolling_profit_factor",
        "rolling_trades", "avg_return_per_trade",
        "Best_Lookback", "Winrate_Long_%", "Avg_Range_%", "Avg_Volume_M",
        "Volume_Spike", "Drawdown_30d_%", "Hype_Anomaly", "Short_Squeeze_Candidate",
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    display_df = df[available_cols].copy()

    # Runden für bessere Lesbarkeit
    for col in ["Final_Score", "rolling_obb_score", "avg_return_per_trade"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].round(3)

    table_html = display_df.to_html(
        classes="table table-striped table-hover", index=False, table_id="universeTable"
    )

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    col_descriptions = {
        "Rank": "Rang im finalen Universum nach Final_Score.",
        "Symbol": "Ticker-Symbol.",
        "Category": "Thematische Kategorie aus dem Discovery-Scanner.",
        "Corr_Group": "Korrelationsgruppe für Diversifikation. Max 2 Symbole pro Gruppe.",
        "Final_Score": f"Gewichteter Score: {WEIGHT_DISCOVERY*100:.0f}% Discovery + {WEIGHT_ROLLING*100:.0f}% Rolling OBB.",
        "Score": "Discovery-Score (Langfrist-Tauglichkeit für Breakouts).",
        "rolling_obb_score": "Rolling OBB Performance Score der letzten ~60 Handelstage.",
        "rolling_winrate": "Win-Rate (%) der simulierten OBB-Trades im Rolling-Fenster.",
        "rolling_profit_factor": "Profit Factor (Brutto-Gewinn / Brutto-Verlust) im Rolling-Fenster.",
        "rolling_trades": "Anzahl OBB-Trades im Rolling-Fenster. Minimum: 8.",
        "avg_return_per_trade": "Durchschnittlicher Return pro Trade (%) im Rolling-Fenster.",
        "Best_Lookback": "Optimaler Lookback aus dem Discovery-Scanner.",
        "Winrate_Long_%": "Long-Winrate über den gesamten Datensatz (Discovery).",
        "Avg_Range_%": "Durchschnittliche Daily Range in %.",
        "Avg_Volume_M": "Durchschnittliches Volumen in Mio. Stück.",
        "Volume_Spike": "Aktuelles Volumen relativ zum 20-Tage-Durchschnitt.",
        "Drawdown_30d_%": "30-Tage Drawdown vom Hoch in %.",
        "Hype_Anomaly": "Volume Spike > 2.8 UND Avg Range > 5.5%.",
        "Short_Squeeze_Candidate": "Drawdown < -25% UND Volume Spike > 2.0.",
    }

    col_desc_js = json.dumps(col_descriptions, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <title>OBB Universe Selector – {ts}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdn.datatables.net/2.0.8/css/dataTables.bootstrap5.min.css" rel="stylesheet">
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdn.datatables.net/2.0.8/js/dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/2.0.8/js/dataTables.bootstrap5.min.js"></script>
    <style>
        body {{ padding: 25px; background: #f8f9fa; font-family: 'Segoe UI', sans-serif; }}
        .table th {{ position: sticky; top: 0; background: #198754 !important; color: #fff !important;
                     z-index: 10; font-weight: bold; }}
        .table th select {{ color: #212529 !important; background: #fff !important;
                            border: 1px solid #adb5bd; }}
        .regime-card {{ padding: 15px 20px; border-radius: 8px; display: inline-block;
                        margin-right: 15px; font-weight: 600; }}
        .regime-bull {{ background: #d4edda; color: #155724; }}
        .regime-bear {{ background: #f8d7da; color: #721c24; }}
        .regime-neutral {{ background: #fff3cd; color: #856404; }}
        .col-info {{ display: inline-block; margin-left: 4px; cursor: help; color: #fff;
                     background: rgba(255,255,255,0.3); border-radius: 50%; width: 15px; height: 15px;
                     font-size: 10px; font-weight: bold; line-height: 15px; text-align: center;
                     border: 1px solid rgba(255,255,255,0.5); vertical-align: middle; }}
        .stats-box {{ background: #fff; border: 1px solid #dee2e6; border-radius: 8px;
                      padding: 12px 18px; display: inline-block; margin: 5px; }}
    </style>
</head>
<body>
<div class="container-fluid">
    <h1 class="mb-3">🎯 OBB Universe Selector</h1>
    <p class="text-muted mb-2">Dynamisches, regime-adaptives Universum – {ts}</p>

    <div class="mb-4">
        <span class="regime-card regime-{'bull' if spy_trend == 'bull' else ('bear' if spy_trend == 'bear' else 'neutral')}">
            SPY Trend: {spy_trend.upper()}
        </span>
        <span class="regime-card" style="background:#e2e3e5; color:#383d41;">
            VIX: {vix:.1f}
        </span>
        <span class="stats-box">Universum: <strong>{len(df)}</strong> Symbole</span>
        <span class="stats-box">Gewichtung: {WEIGHT_DISCOVERY*100:.0f}% Discovery / {WEIGHT_ROLLING*100:.0f}% Rolling</span>
    </div>

    <div class="mb-3">
        <strong>Top 5:</strong>
        {', '.join(df['Symbol'].head(5).tolist())}
    </div>

    {table_html}
</div>

<script>
    var COL_DESCRIPTIONS = {col_desc_js};

    $(document).ready(function() {{
        $('#universeTable').DataTable({{
            "pageLength": 30,
            "order": [[0, "asc"]],
            "responsive": true,
            "language": {{
                "search": "Suchen:",
                "lengthMenu": "Zeige _MENU_ Einträge",
                "info": "Zeige _START_ bis _END_ von _TOTAL_ Symbolen"
            }},
            "initComplete": function () {{
                var api = this.api();
                api.columns().every(function () {{
                    var column = this;
                    var header = $(column.header());
                    var title = header.text().trim();

                    header.empty();
                    var desc = COL_DESCRIPTIONS[title] || '';
                    var infoBtn = desc
                        ? ' <span class="col-info" data-bs-toggle="popover" data-bs-trigger="hover focus" data-bs-placement="bottom" data-bs-content="' + desc.replace(/"/g, '&quot;') + '">?</span>'
                        : '';
                    header.append('<div style="font-weight:bold;margin-bottom:4px;white-space:nowrap;">' + title + infoBtn + '</div>');

                    var select = $('<select class="form-select form-select-sm" style="color:#212529 !important;background:#fff !important;"><option value="">Alle</option></select>')
                        .appendTo(header)
                        .on('change', function () {{
                            var val = $.fn.dataTable.util.escapeRegex($(this).val());
                            column.search(val ? '^' + val + '$' : '', true, false).draw();
                        }});

                    column.data().unique().sort().each(function (d) {{
                        select.append('<option value="' + d + '">' + d + '</option>');
                    }});
                }});
            }}
        }});

        $('[data-bs-toggle="popover"]').each(function() {{
            new bootstrap.Popover(this, {{ sanitize: false }});
        }});
    }});
</script>
</body>
</html>"""

    path.write_text(html, encoding="utf-8")


# ========================== INTEGRATION API ==========================

def load_current_universe(as_dataframe: bool = False,
                          json_path: Path | None = None):
    """
    Lädt das aktuelle OBB-Universum aus der JSON-Datei.

    Args:
        as_dataframe: True → pd.DataFrame, False → List[str] (nur Symbole)
        json_path: Optionaler Pfad zur JSON. Default: obb_universe/current_obb_universe.json

    Returns:
        List[str] oder pd.DataFrame

    Beispiel:
        from obb_universe_selector import load_current_universe
        symbols = load_current_universe()  # ["SOXL", "SMCI", ...]
    """
    if json_path is None:
        json_path = OUTPUT_DIR / "current_obb_universe.json"

    if not json_path.exists():
        log.warning(f"Universe-JSON nicht gefunden: {json_path}")
        log.warning("Bitte zuerst obb_universe_selector.py ausführen.")
        return pd.DataFrame() if as_dataframe else []

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if as_dataframe:
        return pd.DataFrame(data.get("details", []))

    return data.get("symbols", [])


def get_universe_metadata(json_path: Path | None = None) -> dict:
    """Gibt die Metadaten des aktuellen Universums zurück (Zeitstempel, Regime, Größe)."""
    if json_path is None:
        json_path = OUTPUT_DIR / "current_obb_universe.json"

    if not json_path.exists():
        return {}

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return {
        "generated": data.get("generated"),
        "market_regime": data.get("market_regime"),
        "universe_size": data.get("universe_size"),
    }


# ========================== MAIN ==========================

def main():
    parser = argparse.ArgumentParser(description="OBB Universe Selector – Dynamisches Universum")
    parser.add_argument("--fresh", action="store_true",
                        help="Scanner-CSV frisch erzeugen (ruft OBBAutoDiscoveryScanners auf)")
    parser.add_argument("--top", type=int, default=DEFAULT_TOP_N,
                        help=f"Max. Symbole im Universum (default: {DEFAULT_TOP_N})")
    parser.add_argument("--min-trades", type=int, default=FILTER_MIN_ROLLING_TRADES,
                        help=f"Min. Rolling Trades (default: {FILTER_MIN_ROLLING_TRADES})")
    parser.add_argument("--rolling-days", type=int, default=ROLLING_WINDOW_DAYS,
                        help=f"Rolling-Fenster in Handelstagen (default: {ROLLING_WINDOW_DAYS})")
    args = parser.parse_args()

    print()
    print("=" * 70)
    print("  OBB UNIVERSE SELECTOR – Regime-adaptives EV-optimiertes Universum")
    print("=" * 70)

    # ── Marktregime ──
    vix, spy_trend = get_market_regime()
    log.info(f"Marktregime → VIX: {vix:.1f} | SPY Trend: {spy_trend.upper()}")

    # ── Discovery-Daten laden ──
    if args.fresh:
        log.info("Starte Discovery-Scanner frisch...")
        try:
            from OBBAutoDiscoveryScanners import main as run_scanner
            run_scanner()
        except ImportError:
            log.error("OBBAutoDiscoveryScanners.py nicht importierbar. "
                      "Bitte manuell ausführen.")
            return
        except Exception as e:
            log.error(f"Scanner-Fehler: {e}")
            return

    discovery_df = load_discovery_data()
    if discovery_df.empty:
        return

    # ── Universum bauen ──
    universe_df = build_universe(
        discovery_df, vix, spy_trend,
        top_n=args.top,
        min_rolling_trades=args.min_trades,
        rolling_days=args.rolling_days,
    )

    if universe_df.empty:
        log.error("Kein Universum erzeugt! Prüfe Filter und Daten.")
        return

    # ── Ausgaben ──
    save_outputs(universe_df, vix, spy_trend)

    # ── Konsolen-Summary ──
    print()
    print("─" * 70)
    print(f"  HEUTIGES OBB-UNIVERSUM: {len(universe_df)} Symbole")
    print(f"  Markt: VIX={vix:.1f} | SPY={spy_trend.upper()}")
    print(f"  Gewichtung: {WEIGHT_DISCOVERY*100:.0f}% Discovery + {WEIGHT_ROLLING*100:.0f}% Rolling OBB")
    print("─" * 70)

    top5 = universe_df.head(5)
    print(f"\n  Top 5: {', '.join(top5['Symbol'].tolist())}")

    # Kompakte Tabelle der Top-10
    display_cols = ["Rank", "Symbol", "Corr_Group", "Final_Score",
                    "rolling_winrate", "rolling_profit_factor", "rolling_trades"]
    available = [c for c in display_cols if c in universe_df.columns]
    print(f"\n{universe_df.head(10)[available].to_string(index=False)}")

    # Gruppen-Verteilung
    group_dist = universe_df["Corr_Group"].value_counts()
    print(f"\n  Gruppen-Verteilung:")
    for group, count in group_dist.items():
        syms = universe_df[universe_df["Corr_Group"] == group]["Symbol"].tolist()
        print(f"    {group}: {count} → {', '.join(syms)}")

    print()
    print(f"  Ausgabe: {OUTPUT_DIR}/")
    print("=" * 70)


if __name__ == "__main__":
    main()
