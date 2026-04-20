# Botti-Strategie

Trend Following + Mean Reversion auf Daily-Bars. Migriert aus `Trading_Bot/trader_v6.py`.

## Überblick

| Eigenschaft | Wert |
|---|---|
| Timeframe | `1D` (Daily-Bars) |
| Signal-Typen | `botti_trend` (Trend-Following), `botti_mr` (Mean Reversion) |
| Optionaler Filter | Multi-Timeframe-Proxy (Daily-Bars als Intraday-Proxy) |
| Stop-Loss | ATR-basiert (`initial_sl_atr_mult × ATR`) |
| Target | ATR-basiert (`trailing_atr_mult × ATR`) |
| Registriert als | `"botti"` |

---

## Signal-Typen

### `botti_trend` — Trend-Following

Mehrere Einstiegsmuster, alle verlangen Uptrend (`SMA20 > SMA30`, `ADX > threshold`):

| Muster | Bedingung |
|---|---|
| **Golden Cross** | `SMA20` kreuzt `SMA30` von unten |
| **Fast Cross** | `EMA9` kreuzt `EMA21` (konfigurierbarer Typ: EMA oder SMA) |
| **Early Golden Cross** | `SMA20` nähert sich `SMA30` auf `< 2%` + `RSI > 55` + `MACD↑` |
| **Pullback-Entry** | Preis pullback zur `EMA20` nach einem Cross (Low berührt EMA, bullisher Close) |

Bestätigungsfilter (alle drei müssen erfüllt sein, sofern aktiviert):
- **RSI-Filter**: `50 ≤ RSI ≤ 70`
- **MACD-Filter**: MACD-Histogramm > 0
- **Volume-Filter**: `Volume > Volume-SMA(20)`

### `botti_mr` — Mean Reversion

Einstieg, wenn Preis unter das untere Bollinger-Band fällt und stark überverkauft ist:

- `Close ≤ BB_lower(20, 2σ)`
- `RSI < mr_rsi_max` (Standard: 35)
- `Volume > Volume-SMA(20)`

Target: `entry × (1 + mr_profit_target_pct)`. Kein MTF-Filter (bewusst ausgenommen, s.u.).

### `SELL` — Death Cross

`SMA20` kreuzt `SMA30` von oben → sofortiges Exit-Signal (`direction=0`).

---

## Multi-Timeframe-Filter (MTF)

**Gilt ausschließlich für `botti_trend`-Signale**, nicht für Mean Reversion.

### Konzept

Daily entscheidet die Richtung; ein zweiter Check stellt sicher, dass der Einstiegszeitpunkt einem sauberen Intraday-Setup entspricht.

### Backtest: Daily-Proxy

Da im Backtest keine echten Intraday-Bars vorliegen, simuliert der Proxy den Intraday-Check auf Basis des Daily-Bars:

| Bedingung | Daily-Proxy |
|---|---|
| **Pullback** (wenn `pullback_entry: true`) | `Bar-Low ≤ EMA20 × (1 + mtf_pullback_proximity)` |
| **Momentum** | `RSI(14) > lower_rsi_min` |
| **Trigger** (einer reicht) | MACD-Hist > 0 UND steigend **ODER** `Close > max(High der letzten N Bars)` |

Alle drei Bedingungen müssen erfüllt sein. Look-ahead-frei: nutzt nur den abgeschlossenen Daily-Bar.

### Live: Phase 2 (geplant)

Echte Intraday-Bars (15Min/5Min via IBKR oder Alpaca) als Bestätigung. Architektur:

```
Scheduler (09:15 ET): Daily-Scan → BUY-Kandidaten in Watchlist
Intraday-Poll (15Min): intraday_entry_confirmation(df_15min) → Order bei Match
EOD:                   Watchlist leeren
```

---

## Schutzfilter

| Filter | Parameter | Wirkung |
|---|---|---|
| **Drawdown-Breaker** | `max_drawdown_pct: 0.15` | Keine neuen Trades wenn DD > 15% |
| **VIX-Regime** | `vix_high_threshold: 30` | Positionsgröße halbiert bei VIX > 30 |
| **Sector-Cluster-Guard** | `max_per_sector: 2` | Max. 2 offene Positionen pro Sektor |
| **ADX-Filter** | `adx_threshold: 15` | Kein Trend-Signal bei schwachem Trend |

---

## Konfiguration

### Vollständige Parameter-Referenz

```yaml
strategy:
  name: botti
  params:
    # Trend-Signale
    sma_short: 20
    sma_long: 30
    use_fast_cross: true
    fast_cross_type: EMA          # EMA oder SMA
    fast_cross_short: 9
    fast_cross_long: 21
    use_early_golden_cross: true
    early_gc_proximity_pct: 0.02  # SMA20 max. 2% unter SMA30
    early_gc_rsi_min: 55
    use_pullback_entry_daily: true
    pullback_daily_lookback: 15   # Cross muss in letzten 15 Bars liegen
    pullback_daily_ema: 20
    pullback_daily_proximity: 0.015
    pullback_daily_rsi_min: 50

    # Bestätigungsfilter
    use_rsi_filter: true
    rsi_buy_min: 50
    rsi_buy_max: 70
    use_volume_filter: true
    volume_sma_period: 20
    use_macd_filter: true
    macd_fast: 12
    macd_slow: 26
    macd_signal_period: 9

    # Mean Reversion
    use_mean_reversion: true
    bb_period: 20
    bb_std: 2.0
    mr_rsi_max: 35
    mr_profit_target_pct: 0.05

    # Risk / Sizing
    atr_period: 14
    risk_per_trade: 0.02
    max_equity_at_risk: 0.80
    initial_sl_atr_mult: 2.5
    trailing_atr_mult: 3.0

    # Schutzfilter
    adx_period: 14
    adx_threshold: 15
    vix_high_threshold: 30
    vix_size_reduction: 0.5
    max_drawdown_pct: 0.15
    max_per_sector: 2
    sector_groups:
      tech_semi: [NVDA, AMD, MU]
      tech_large: [AAPL, META, GOOGL]
      tech_growth: [CRWD, PLTR, HOOD, RKLB]
      etf_broad: [SPY, QQQ]
      defensive: [JNJ, GLD]
      energy: [XLE]
      ev_space: [TSLA]

    # Multi-Timeframe-Filter
    use_multi_timeframe: true     # false = Baseline ohne MTF
    lower_ema_period: 20
    lower_rsi_min: 50
    pullback_entry: true          # false = nur Momentum-Setup ohne Pullback-Bedingung
    mtf_pullback_proximity: 0.015 # Low max. 1.5% über EMA (Proxy-Toleranz)
    mtf_breakout_lookback: 5      # Breakout über Hoch der letzten N Bars
```

---

## Backtest

### Baseline vs. MTF (yfinance, 16 Symbole, 1 Jahr)

| Metrik | Ohne MTF | Mit MTF | Δ |
|---|---:|---:|---|
| Total Return | +14.25% | +12.58% | −1.67pp |
| Max Drawdown | 13.59% | **11.45%** | **−2.14pp** |
| Sharpe | 1.25 | **1.33** | **+0.08** |
| Trades | 75 | 64 | −11 (−15%) |
| Win Rate | 68.0% | **70.3%** | **+2.3pp** |
| Profit Factor | 1.46 | **1.54** | **+0.08** |
| Expectancy/Trade | $57.13 | **$59.18** | **+$2.05** |

**Interpretation:** Der MTF-Filter reduziert die Tradeanzahl um 15%, verbessert aber
alle risikoadjustierten Metriken. Weniger, aber präzisere Einstiege — klassisches MTA-Muster.

### Quick-Start Backtest

```bash
# Mit MTF-Filter (empfohlen)
PYTHONIOENCODING=utf-8 python main.py backtest --config configs/botti_backtest_mtf.yaml

# Baseline ohne MTF
PYTHONIOENCODING=utf-8 python main.py backtest --config configs/botti_backtest_nomtf.yaml
```

### WFO für MTF-Parameter

```yaml
# configs/botti_backtest_mtf.yaml – wfo-Block hinzufügen:
wfo:
  is_days: 180
  oos_days: 60
  step_days: 30
  metric: sharpe
  min_trades_is: 10
  n_workers: 0
  param_grid:
    lower_rsi_min: [40, 50, 55]
    mtf_pullback_proximity: [0.010, 0.015, 0.020]
    mtf_breakout_lookback: [3, 5, 7]
```

```bash
python main.py wfo --config configs/botti_backtest_mtf.yaml
```

---

## Implementierung

```
strategy/botti.py
  BottiStrategy._generate_signals(bar)
    ├─ _classify_signal(df, last, prev, cfg)  → signal_type, reason
    ├─ [BUY + use_multi_timeframe] → _daily_mtf_proxy(df, cfg)
    ├─ [BUY_MR] → _make_mr_signal()           ← MTF wird bewusst übersprungen
    └─ [SELL]   → _make_exit_signal()

  Hilfsfunktionen (reine Funktionen, kein I/O):
    _compute_botti_indicators(df, cfg)
    _confirmation_filters(last, cfg)
    _early_golden_cross(last, prev, cfg)
    _pullback_signal(df, cfg)
    _daily_mtf_proxy(df, cfg)                 ← neu (Phase 1)
```

!!! note "Phase 2 (geplant)"
    `_daily_mtf_proxy` wird für Live-Betrieb durch `intraday_entry_confirmation(df_intraday, cfg)`
    ersetzt, das echte 15Min-Bars auswertet. Der Daily-Proxy bleibt für den Backtest erhalten.
