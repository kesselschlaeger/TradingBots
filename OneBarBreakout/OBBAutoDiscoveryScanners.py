#!/usr/bin/env python3
"""
OBB Auto Discovery PRO – Ultimate Version mit Spalten-Filter-Dropdowns
Dynamisches Universum: NASDAQ-100 + S&P500 + Russell 2000 + volatile ETFs
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from tqdm import tqdm

# ========================== KONFIGURATION ==========================
LOOKBACK_DAYS = 504
MAX_WORKERS = 20
MIN_VOLUME = 5_000_000
MIN_RANGE_PCT = 2.0
LOOKBACK_CANDIDATES = [20, 30, 50, 80, 100]

# ========================== SCORING ==========================
def get_market_regime():
    try:
        vix = yf.download("^VIX", period="5d", progress=False)["Close"].iloc[-1]
        spy = yf.download("SPY", period="60d", progress=False)
        trend = "bull" if spy["Close"].iloc[-1] > spy["Close"].rolling(20).mean().iloc[-1] else "bear"
        return float(vix), trend
    except:
        return 20.0, "neutral"


def score_asset(symbol: str, vix: float, spy_trend: str) -> dict | None:
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=f"{LOOKBACK_DAYS}d", auto_adjust=True)

        if df.empty or len(df) < 200:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df = df.droplevel(0, axis=1)

        df = df[['Open', 'High', 'Low', 'Close', 'Volume']].copy()

        df["Range_Pct"] = (df["High"] - df["Low"]) / df["Close"] * 100
        avg_range = float(df["Range_Pct"].mean())
        avg_volume = float(df["Volume"].mean())
        vol_20 = float(df["Volume"].rolling(20).mean().iloc[-1])
        volume_spike = df["Volume"].iloc[-1] / vol_20 if vol_20 > 0 else 1.0

        drawdown_30d = (df["Close"].iloc[-1] / df["Close"].rolling(30).max().iloc[-1] - 1) * 100

        # Auto-Optimierung Lookback
        best_score = -np.inf
        best_lb = 50
        best_winrate = 0
        for lb in LOOKBACK_CANDIDATES:
            if len(df) < lb + 10: continue
            df_temp = df.copy()
            df_temp["HH"] = df_temp["High"].rolling(lb).max().shift(1)
            long_ret = df_temp[df_temp["Close"] > df_temp["HH"]]["Close"].pct_change().dropna()
            winrate = (long_ret > 0).mean() * 100 if len(long_ret) > 0 else 0
            score_lb = winrate * (avg_range / 5)
            if score_lb > best_score:
                best_score = score_lb
                best_lb = lb
                best_winrate = winrate

        vol_weight = 1.4 if vix > 25 else 1.0
        momentum_weight = 1.5 if spy_trend == "bull" else 0.7
        final_score = (best_winrate / 50) * avg_range * np.log10(avg_volume / 1e6) * vol_weight * momentum_weight

        cat = "Other"
        if any(x in symbol for x in ["NVDA","AMD","AVGO","SMCI","ARM","TSM"]): cat = "AI_Chip"
        elif any(x in symbol for x in ["TSLA","RKLB","PLTR","HOOD"]): cat = "Meme_Hype"
        elif symbol in ["TQQQ","SOXL","TECL","FNGU","BULZ"]: cat = "Leveraged"
        elif any(x in symbol for x in ["COIN","MSTR","MARA"]): cat = "Crypto_Proxy"

        is_hype = volume_spike > 2.8 and avg_range > 5.5
        is_squeeze = drawdown_30d < -25 and volume_spike > 2.0

        return {
            "Symbol": symbol,
            "Category": cat,
            "Best_Lookback": best_lb,
            "Winrate_Long_%": round(best_winrate, 1),
            "Avg_Range_%": round(avg_range, 2),
            "Avg_Volume_M": round(avg_volume / 1_000_000, 1),
            "Volume_Spike": round(volume_spike, 2),
            "Drawdown_30d_%": round(drawdown_30d, 1),
            "Score": round(final_score, 3),
            "Hype_Anomaly": "Yes" if is_hype else "No",
            "Short_Squeeze_Candidate": "Yes" if is_squeeze else "No",
        }
    except Exception as exc:
        print(f"[WARN] {symbol}: {exc}")
        return None


# ========================== DYNAMISCHES UNIVERSUM ==========================
def fetch_html(url: str) -> str | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    try:
        request = Request(url, headers=headers)
        with urlopen(request, timeout=20) as response:
            return response.read().decode("utf-8", errors="ignore")
    except Exception:
        return None


def load_symbols_from_web_tables(name: str, url: str) -> set[str]:
    html = fetch_html(url)
    if not html:
        print(f"[WARN] {name}: Seite konnte nicht geladen werden")
        return set()

    try:
        tables = pd.read_html(StringIO(html))
    except Exception as exc:
        print(f"[WARN] {name}: Tabellen konnten nicht geparst werden: {exc}")
        return set()

    for table in tables:
        normalized_columns = {str(col).strip().lower(): col for col in table.columns}
        for candidate in ["symbol", "ticker", "ticker symbol"]:
            if candidate not in normalized_columns:
                continue

            raw_symbols = table[normalized_columns[candidate]].astype(str).str.strip()
            symbols = {
                symbol.replace(".", "-")
                for symbol in raw_symbols
                if symbol and symbol != "nan" and symbol.upper() == symbol
            }
            if symbols:
                print(f"→ {name}: {len(symbols)} Symbole geladen")
                return symbols

    print(f"[WARN] {name}: Keine Symbol-Spalte gefunden")
    return set()


def build_dynamic_universe() -> list:
    """Baut ein großes dynamisches Universum aus Index-Mitgliedern plus volatilen ETFs."""
    print("Baue dynamisches Universum auf...")
    universe = set()

    universe.update(load_symbols_from_web_tables("S&P 500", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"))
    universe.update(load_symbols_from_web_tables("NASDAQ-100", "https://en.wikipedia.org/wiki/Nasdaq-100"))

    # ETFs selbst als handelbare Assets aufnehmen.
    # ETF-Holdings via yfinance Ticker.info['holdings'] sind oft leer oder nicht mehr verfügbar.
    etfs = ["QQQ", "SPY", "IWM", "ARKK", "SOXX", "SMH", "XBI", "XLK", "TQQQ", "SOXL"]
    universe.update(etfs)

    # Manuelle hochvolatile / beliebte Assets hinzufügen
    extras = [
        "NVDA","TSLA","AMD","SMCI","ARM","PLTR","HOOD","COIN","MSTR","RKLB",
        "CVNA","UPST","APP","MU","ASML","TSM","CRWD","AVGO","META","AMZN"
    ]
    universe.update(extras)

    final_universe = sorted(list(universe))
    print(f"→ Dynamisches Universum enthält {len(final_universe)} Assets")
    return final_universe


# ========================== MAIN + DASHBOARD ==========================
def main():
    print(f"\n=== OBB Auto Discovery PRO – ULTIMATE VERSION mit Spalten-Filtern ===\n")

    vix, spy_trend = get_market_regime()
    print(f"Marktregime → VIX: {vix:.1f} | SPY Trend: {spy_trend.upper()}\n")

    universe = build_dynamic_universe()

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(score_asset, sym, vix, spy_trend) for sym in universe]
        for future in tqdm(as_completed(futures), total=len(universe), desc="Scanning"):
            res = future.result()
            if res:
                results.append(res)

    df = pd.DataFrame(results)
    if df.empty or "Score" not in df.columns:
        print("\n⚠ Keine verwertbaren Ergebnisse gefunden.")
        print("   Prüfe die WARN-Meldungen oben. Häufige Ursachen: fehlende Markt-Daten, API-Limits oder Exceptions im Scoring.")
        return
    df = df.sort_values("Score", ascending=False).reset_index(drop=True)

        # ====================== INTERAKTIVES DASHBOARD ======================
    output_dir = Path("obb_discovery_pro")
    output_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""
    <html>
    <head>
        <title>OBB Auto Discovery PRO – {datetime.now().strftime('%Y-%m-%d')}</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <link href="https://cdn.datatables.net/2.0.8/css/dataTables.bootstrap5.min.css" rel="stylesheet">
        <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
        <script src="https://cdn.datatables.net/2.0.8/js/dataTables.min.js"></script>
        <script src="https://cdn.datatables.net/2.0.8/js/dataTables.bootstrap5.min.js"></script>
        <style>
            body {{ padding: 25px; background: #f8f9fa; }}
            .table th {{ position: sticky; top: 0; background: #cfe2ff !important; color: #003087 !important; z-index: 10; font-weight: bold; }}
            .table th select {{ color: #212529 !important; background: #fff !important; border: 1px solid #adb5bd; }}
            .filter-row th {{ background: #e9ecef; font-weight: normal; padding: 4px 8px; }}
            .hype {{ background: #fff3cd !important; }}
            .squeeze {{ background: #d4edda !important; }}
            .nav-tabs .nav-link {{ font-weight: 600; }}
            .col-info {{ display: inline-block; margin-left: 5px; cursor: help; color: #0d6efd;
                         background: #fff; border-radius: 50%; width: 16px; height: 16px;
                         font-size: 10px; font-weight: bold; line-height: 16px; text-align: center;
                         border: 1px solid #0d6efd; vertical-align: middle; }}
        </style>
    </head>
    <body>
    <div class="container-fluid">
        <h1 class="mb-3">🚀 OBB Auto Discovery PRO</h1>
        <p class="lead"><strong>VIX:</strong> {vix:.1f} | <strong>SPY Trend:</strong> {spy_trend.upper()} | {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>

        <ul class="nav nav-tabs mb-4" id="mainTabs">
            <li class="nav-item"><a class="nav-link active" data-bs-toggle="tab" href="#top">Top Assets</a></li>
            <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#hype">Hype &amp; Anomaly</a></li>
            <li class="nav-item"><a class="nav-link" data-bs-toggle="tab" href="#squeeze">Short-Squeeze Candidates</a></li>
        </ul>

        <div class="tab-content">
            <!-- TOP ASSETS -->
            <div class="tab-pane fade show active" id="top">
                {df.to_html(classes="table table-striped table-hover", index=False, table_id="mainTable")}
            </div>
            <!-- HYPE -->
            <div class="tab-pane fade" id="hype">
                {df[df["Hype_Anomaly"] == "Yes"].to_html(classes="table table-warning", index=False, table_id="hypeTable")}
            </div>
            <!-- SHORT SQUEEZE -->
            <div class="tab-pane fade" id="squeeze">
                {df[df["Short_Squeeze_Candidate"] == "Yes"].to_html(classes="table table-success", index=False, table_id="squeezeTable")}
            </div>
        </div>
    </div>

    <script>
        // Beschreibungen der Kennzahlen für Tooltip-Overlays
        var COL_DESCRIPTIONS = {{
            "Symbol":                   "Ticker-Symbol des Assets an der Börse (z.B. NVDA, TSLA).",
            "Category":                 "Thematische Gruppe: AI_Chip, Meme_Hype, Leveraged ETF, Crypto_Proxy oder Other.",
            "Best_Lookback":            "Optimaler Rückblick-Zeitraum (in Handelstagen), bei dem der Breakout-Score am höchsten war. Getestet: 20, 30, 50, 80, 100 Tage.",
            "Winrate_Long_%":           "Anteil der Tage (in %), an denen der Kurs nach einem Breakout über das Lookback-Hoch gestiegen ist. Werte über 55 % gelten als gut.",
            "Avg_Range_%":              "Durchschnittliche tägliche Schwankungsbreite (High - Low) in Prozent des Schlusskurses. Höhere Werte = mehr potenzielle Bewegung pro Trade.",
            "Avg_Volume_M":             "Durchschnittliches tägliches Handelsvolumen in Millionen Stück. Liquiditätsindikator – niedrige Werte erhöhen Slippage-Risiko.",
            "Volume_Spike":             "Heutiges Volumen relativ zum 20-Tage-Durchschnitt. Werte > 2 deuten auf erhöhtes Interesse/News hin. Basis für Hype-Erkennung.",
            "Drawdown_30d_%":           "Kursrückgang des Assets vom 30-Tage-Hoch bis heute in Prozent. Stark negative Werte können auf Erholung (Short-Squeeze) oder Trendbruch hinweisen.",
            "Score":                    "Gesamtbewertung des Assets. Berechnet aus: Winrate × Avg_Range × log(Volumen) × VIX-Gewicht × Markttrend-Gewicht. Höher = trading-attraktiver.",
            "Hype_Anomaly":             "'Yes' wenn Volume_Spike > 2,8 UND Avg_Range > 5,5 %. Signalisiert ungewöhnliche Aktivität – potenzielle Momentum-Chance oder Manipulation.",
            "Short_Squeeze_Candidate":  "'Yes' wenn Drawdown_30d < -25 % UND Volume_Spike > 2,0. Deutet auf starken Verkaufsdruck mit plötzlichem Volumenanstieg – klassisches Squeeze-Setup."
        }};

        $(document).ready(function() {{
            // DataTable mit sauberen Filtern (Überschriften bleiben sichtbar!)
            function initTable(tableId) {{
                return $('#' + tableId).DataTable({{
                    "pageLength": 15,
                    "order": [[7, "desc"]],           // Score absteigend
                    "responsive": true,
                    "language": {{ 
                        "search": "Suchen:", 
                        "lengthMenu": "Zeige _MENU_ Einträge",
                        "info": "Zeige _START_ bis _END_ von _TOTAL_ Einträgen"
                    }},
                    "initComplete": function () {{
                        var api = this.api();
                        api.columns().every(function () {{
                            var column = this;
                            var header = $(column.header());
                            var title = header.text().trim();

                            // Überschrift leeren und als sichtbaren Label neu setzen,
                            // damit der Spaltenname nicht vom Select verdeckt wird
                            header.empty();
                            var desc = COL_DESCRIPTIONS[title] || '';
                            var infoBtn = desc
                                ? ' <span class="col-info" data-bs-toggle="popover" data-bs-trigger="hover focus" data-bs-placement="bottom" data-bs-content="' + desc.replace(/"/g, '&quot;') + '">?</span>'
                                : '';
                            header.append('<div style="font-weight:bold;margin-bottom:4px;color:#003087;white-space:nowrap;">' + title + infoBtn + '</div>');

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
            }}

            var mainTable = initTable("mainTable");
            var hypeTable = initTable("hypeTable");
            var squeezeTable = initTable("squeezeTable");

            // Bootstrap Popovers aktivieren (nach DataTable-Init)
            $('[data-bs-toggle="popover"]').each(function() {{
                new bootstrap.Popover(this, {{ sanitize: false }});
            }});

            // Tab-Wechsel → Tabelle neu zeichnen (verhindert Layout-Probleme)
            $('#mainTabs a').on('shown.bs.tab', function (e) {{
                mainTable.draw(false);
                hypeTable.draw(false);
                squeezeTable.draw(false);
            }});
        }});
    </script>
    </body>
    </html>
    """

    html_path = output_dir / "obb_pro_dashboard.html"
    html_path.write_text(html, encoding="utf-8")

    csv_path = output_dir / "obb_pro_full.csv"
    df.to_csv(csv_path, index=False)

    print(f"\n✅ ULTIMATE SCANNER MIT DYNAMISCHEM UNIVERSUM FERTIG!")
    print(f"   → Interaktives Dashboard: {html_path}")
    print(f"   → CSV: {csv_path}")
    print(f"   → Gefundene Assets: {len(df)}")

    print(f"\nTop 10:")
    print(df.head(10)[["Symbol", "Category", "Best_Lookback", "Winrate_Long_%", "Avg_Range_%", "Score", "Hype_Anomaly", "Short_Squeeze_Candidate"]].to_string(index=False))


if __name__ == "__main__":
    main()