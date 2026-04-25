# ICT Order Block MTF Confluence

ICT Order Block ist eine regelbasierte SMC/ICT-Strategie (Smart Money Concepts) auf
**5-Min-Bars mit Multi-Timeframe-Confluence** (4H / 1H / 15M / 5M). Die Strategie
identifiziert institutionelle Order-Block-Zonen, in denen Smart-Money-Positionen aufgebaut
wurden, und tradet die Reaktion beim erneuten Preis-Retest dieser Zonen.

Die Kernidee: Institutionen platzieren große Orders in definierten Preiszonen (Order Blocks),
bevor eine impulsive Bewegung (Displacement) startet. Kehrt der Preis in diese Zone zurück,
reagieren Institutionen erneut — der Confluenz-Check auf 4 Timeframes filtert falsche Zonen heraus.

Registriert als `"ict_ob_mtf"`. Unterstützt drei Asset-Klassen: **equity | futures | crypto**.

---

## Funktionsprinzip — Multi-Timeframe-Confluence

Die Strategie prüft vier Timeframes in Kaskade. Alle Bedingungen müssen erfüllt sein
(Confluence-Score ≥ 0.75, d.h. mindestens 3 von 4 ausgerichtet):

| Timeframe | Aufgabe | Bedingung |
|---|---|---|
| **4H** | Bias + Order-Block-Zone | Valider OB (Sweep + Displacement + FVG), Preis in der Zone |
| **1H** | Struktur-Bestätigung | BOS / CHOCH / Higher Low (bullish) oder Lower High (bearish) |
| **15M** | Imbalance-Nachweis | Fair Value Gap innerhalb oder am Rand des 4H-OBs |
| **5M** | Präziser Entry | Reaktionskerze (Hammer/Engulfing) oder Close > 50% des OB |

Alle Timeframes werden **live aus 5M-Bars resampled** — es ist kein separater Daten-Feed
für 4H, 1H oder 15M nötig.

---

## Asset-Klassen

ICT OB unterstützt drei Asset-Klassen über den Config-Parameter `asset_class`:

| Aspekt | `equity` | `futures` | `crypto` |
|---|---|---|---|
| **Trading-Session** | 09:30–16:00 ET | CME Globex Mo 18:00–Fr 17:00 ET | 24/7 |
| **Entry-Cutoff** | `entry_cutoff_time` (Default 15:00) | `futures_entry_cutoff` (Default 15:45) | keiner |
| **Trend-Referenz** | SPY via `spy_df_asof()` | ES (oder Config-Override) | deaktiviert |
| **Gap-Filter** | aktiv (max 3%) | deaktiviert | deaktiviert |
| **Sizing** | R-basiert (`position_size`) | Kontrakt-Anzahl aus Punktrisiko | R-basiert (fraktional) |
| **Konfigs** | `ict_ob_live.yaml` | `ict_ob_futures_live.yaml` | `ict_ob_crypto_live.yaml` |

---

## Benötigte Daten

| Quelle | Timeframe | Wozu |
|---|---|---|
| **Handelssymbol** (z.B. NVDA, NQ, BTC) | 5-Min | Primär-Feed; 4H/1H/15M werden intern resampled |
| **SPY** (equity) / **ES** (futures) | 5-Min | Trend-Filter-Referenz |
| **VIX** (equity/futures) | Snapshot | Positions-Sizing-Halbierung bei VIX > 30 |

Für Live: `provider: ibkr`, `timeframe: "5Min"`, `lookback_days: 60` (genug Bars für 4H-Aggregation).
Für Backtest: `provider: yfinance`, `timeframe: "5Min"`.

**Mindest-Warmup:**
- Equity: `min_bars: 250` — ausreichend für 4H-Resampling (ca. 8 Handelstage à 30 Bars)
- Futures/Crypto: `min_bars: 800` — mehr 5M-Bars wegen der längeren/durchgehenden Sessions

---

## Handelszeiten / Tagesablauf

### Equity (US-Aktien)

| Uhrzeit ET | Event |
|---|---|
| 09:30 | Session-Start — Signal-Scan beginnt auf jedem 5M-Bar |
| bis 15:00 | Entry-Cutoff — kein neuer Entry nach 15:00 ET |
| 15:27 | EOD-Flatten via TradeManager (konfigurierbar) |
| 16:00 | Session-Ende |

### Futures (CME Globex)

| Uhrzeit ET | Event |
|---|---|
| Mo 18:00 | Globex-Session-Start (Wochenöffnung) |
| Fr 17:00 | Globex-Session-Ende (Wochenschluss) |
| Sa / So | Immer geschlossen |
| Mo vor 18:00 | Geschlossen (kein Pre-Globex-Trade) |
| täglich bis 15:45 | Entry-Cutoff (`futures_entry_cutoff`) |

### Crypto (24/7)

Kein Session-Gate. `crypto_session_open: null`, `crypto_entry_cutoff: null` — der Bot
tradet rund um die Uhr. Entry-Cutoff kann optional konfiguriert werden.

---

## Filter-Stack

| Filter | Parameter | Wirkung |
|---|---|---|
| **Confluence-Score** | `min_confluence_score: 0.75` | Mind. 75% der 4 TF-Bedingungen erfüllt |
| **Trend-Filter** | `trend_ema_period: 20` | Equity: SPY > EMA20; Futures: ES > EMA20 |
| **Gap-Filter** | `max_gap_pct: 0.03` | Nur Equity: blockiert bei Overnight-Gap > 3% |
| **VIX-Regime** | `vix_threshold: 30.0` | Positionsgröße halbiert bei VIX > 30 |
| **MIT Independence Guard** | `mit_correlation_groups` | Optional: max. 1 Position pro Korrelationsgruppe |
| **Entry-Cutoff** | `entry_cutoff_time` | Asset-class-spezifisch (s. Tabelle oben) |

---

## Entry / Stop / Target

```
Long:
  entry  = Close des 5M-Reaktions-Bars
  stop   = OB-Low − stop_ob_mult × OB-Range  (Default: 0.75 × OB-Range)
  target = entry + profit_target_r × (entry − stop)  (Default: 2R)

Short:
  entry  = Close des 5M-Reaktions-Bars
  stop   = OB-High + stop_ob_mult × OB-Range
  target = entry − profit_target_r × (entry − stop)
```

**Futures-Sizing:**
```
contracts = equity × risk_pct / (stop_distance_points × futures_point_value)
Minimum: 1 Kontrakt
```

**FUTURES_POINT_VALUES** (Fallback, überschreibbar via `futures_point_value`):

| Symbol | $/Punkt |
|---|---|
| NQ | $20 |
| ES | $50 |
| YM | $5 |
| RTY | $50 |
| MNQ | $2 |
| MES | $5 |

---

## Entry-Modi

| Modus | Verhalten |
|---|---|
| `aggressive` | Entry beim ersten Kontakt mit der OB-Zone |
| `standard` | Entry bei Reaktionskerze oder Close > 50% des OB (Default) |
| `conservative` | Entry nur bei vollständiger Engulfing-Bestätigung |

---

## Konfiguration

```yaml
# Equity (NVDA, AMD, AVGO)
strategy:
  name: ict_ob_mtf
  params:
    asset_class: equity
    displacement_mult: 1.2
    ob_entry_mode: standard
    min_confluence_score: 0.75
    stop_ob_mult: 0.75
    profit_target_r: 2.0
    risk_per_trade: 0.005
    use_trend_filter: true
    entry_cutoff_time: "15:00"
```

```bash
# Equity
python main.py paper --config configs/ict_ob_live.yaml
# Futures (NQ)
python main.py paper --config configs/ict_ob_futures_live.yaml
# Crypto (BTC, ETH)
python main.py paper --config configs/ict_ob_crypto_live.yaml
```

---

## Implementierung

```
strategy/ict_ob.py
  IctOrderBlockStrategy._generate_signals(bar)
    ├─ _is_trading_session(now)         → asset-class-spezifisches Session-Gate
    ├─ _entry_cutoff_ok(now)            → asset-class-spezifischer Cutoff
    ├─ resample_ohlcv(df_5m, "4H")      → 4H-OBs via detect_order_blocks()
    ├─ resample_ohlcv(df_5m, "1H")      → 1H-Struktur via detect_structure_break()
    ├─ resample_ohlcv(df_5m, "15M")     → 15M-FVG via fair_value_gaps()
    ├─ _is_bullish/bearish_reaction()   → 5M-Entry-Bestätigung
    ├─ Confluence-Score (0..1)
    ├─ _resolve_trend(bar)              → SPY / ES / neutral
    ├─ _gap_check_for_asset(df_5m)      → equity-only
    └─ _effective_risk_qty(equity, ...) → asset-class-spezifisches Sizing

execution/contract_factory.py
  build_contract(symbol, asset_class, cfg)
    → Stock (equity) | Future (CME, kein lastTradeDate) | Crypto (PAXOS)
```

!!! note "IBKR Crypto-Fallback"
    Bei älteren `ib_insync`-Versionen ohne `Crypto`-Klasse fällt `build_contract`
    auf `Stock(symbol, 'PAXOS', currency)` zurück — graceful, kein Crash.
