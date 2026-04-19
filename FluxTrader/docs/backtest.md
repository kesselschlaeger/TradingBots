# Backtest

Der Backtest nutzt exakt dieselbe Strategy-Klasse wie der Live-Runner –
kein separater Backtest-Code, kein Drift.

## Architektur

```mermaid
graph LR
    YF[YFinanceDataProvider] -->|dict[sym, DataFrame]| E
    CTX[MarketContextService] --> E
    E[BarByBarEngine] -->|on_bar| STR[ORBStrategy / OBBStrategy]
    STR -->|Signal| E
    E -->|execute_signal| PA[PaperAdapter]
    PA -->|fills| TL[Trade-Log]
    E -->|check_bar_exit| TM[TradeManager]
    TM -->|exit| E
    TL --> RPT[Tearsheet-Report]
```

## Datenquellen

### yfinance (empfohlen für Backtest)

```yaml
data:
  provider: yfinance
  timeframe: "5Min"
  lookback_days: 60
```

!!! warning "yfinance Intraday-Limit"
    yfinance liefert Intraday-Bars nur für die letzten ~60 Tage.
    Für längere Backtests Daily-Bars nutzen (`timeframe: "1Day"`).

### Alpaca Historical Data

```yaml
data:
  provider: alpaca
  timeframe: "5Min"
  lookback_days: 365
```

Alpaca liefert bis zu 2 Jahre historische 1-Min-Bars (kostenpflichtig ab SIP).

## Backtest starten

```bash
python main.py backtest --config configs/orb_backtest.yaml
```

## Slippage & Commission

Configurable im Code (nicht in YAML):

```python
from backtest.engine import BacktestConfig
from backtest.slippage import SlippageModel, CommissionModel

cfg = BacktestConfig(
    initial_capital=100_000,
    slippage=SlippageModel(
        fixed_cents=0.01,    # 1 Cent pro Aktie
        percentage=0.0005,   # 5 Basispunkte
    ),
    commission=CommissionModel(
        per_share=0.005,     # $0.005 pro Aktie (Interactive Brokers-Style)
        minimum=1.0,         # Mindest-Commission $1
    ),
)
```

## Exit-Logik im Backtest

Da Alpaca/IBKR Bracket-Orders serverseitig verwalten, simuliert der `BarByBarEngine`
dies innerhalb jedes Bars:

```
Für jeden Bar:
  1. exit_next_open? → Schließe Position zum Open-Preis dieses Bars
  2. set_market_price(close)
  3. check_bar_exit(high, low, close):
     - Long:  low  <= current_stop → STOP (zum stop_price)
     - Long:  high >= target       → TARGET (zum target_price)
     - Short: high >= current_stop → STOP
     - Short: low  <= target       → TARGET
  4. should_eod_close? → Schließe zum Close-Preis
  5. Strategie aufrufen → ggf. neues Signal
```

!!! info "Pessimistisches Backtest-Assumption"
    Wenn im selben Bar sowohl SL als auch TP gerissen werden (was auf tiefer
    Timeframe-Auflösung unrealistisch ist), gewinnt der SL (worst-case).

## Tearsheet

```python
from backtest.report import build_tearsheet, format_tearsheet

ts = build_tearsheet(result.equity_curve, result.trades, initial_capital=100_000)
print(format_tearsheet(ts))
```

### Metriken

| Metrik | Formel |
|---|---|
| **Total Return** | `(final / initial - 1) × 100%` |
| **CAGR** | `(final/initial)^(365/span_days) - 1` |
| **Max Drawdown** | `max((peak - equity) / peak)` rolling |
| **Sharpe** | `√(ann) × mean(returns) / std(returns)` |
| **Sortino** | `√(ann) × mean(returns) / std(downside_returns)` |
| **Win Rate** | `count(pnl > 0) / count(all_trades)` |
| **Profit Factor** | `sum(wins) / abs(sum(losses))` |
| **Expectancy** | `win_rate × avg_win + (1-win_rate) × avg_loss` |

## Programmatischer Backtest

```python
import asyncio
from datetime import datetime, timedelta, timezone

from backtest.engine import BacktestConfig, BarByBarEngine
from backtest.report import build_tearsheet, format_tearsheet
from core.context import MarketContextService, set_context_service
from data.providers.yfinance_provider import YFinanceDataProvider
from execution.paper_adapter import PaperAdapter
from strategy.orb import ORBStrategy

async def main():
    ctx = MarketContextService(initial_capital=100_000)
    ctx.update_account(equity=100_000, cash=100_000, buying_power=400_000)
    set_context_service(ctx)

    strat = ORBStrategy({"use_mit_probabilistic_overlay": True}, context=ctx)
    paper = PaperAdapter(initial_cash=100_000)
    prov = YFinanceDataProvider()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)

    data = await prov.get_bars_bulk(["AAPL", "NVDA"], start, end, "5Min")
    spy = await prov.get_bars("SPY", start, end, "5Min")
    ctx.set_spy_df(spy)

    engine = BarByBarEngine(strat, paper, ctx, BacktestConfig())
    result = await engine.run(data=data, spy_df=spy)

    ts = build_tearsheet(result.equity_curve, result.trades, 100_000)
    print(format_tearsheet(ts))

asyncio.run(main())
```

---

## Walk-Forward-Optimierung (WFO)

WFO verhindert Overfitting: Parameter werden auf In-Sample-Daten (IS) optimiert
und sofort auf Out-of-Sample-Daten (OOS) validiert. Nur der kombinierte
OOS-Track zählt.

### Konzept

```
|── IS (120 Tage) ──|─ OOS (30 Tage) ─|
                    |── IS ──|─ OOS ─|           ← nächstes Fenster (Step: 20 Tage)
                             |── IS ──|─ OOS ─|  ← ...
```

### 1 · YAML-Config vorbereiten

Ins Config-YAML einen `wfo:`-Block einfügen:

```yaml
# configs/orb_backtest.yaml
data:
  provider: alpaca
  timeframe: "5Min"
  lookback_days: 470   # genug Daten für mehrere Fenster

wfo:
  is_days:       120
  oos_days:       30
  step_days:      20
  metric:         sharpe   # oder: cagr_pct, profit_factor
  min_trades_is:  20       # IS-Combos mit weniger Trades werden ignoriert
  n_workers:       0       # 0 = auto (CPU-1), 1 = sequentiell
  param_grid:
    profit_target_r:     [1.5, 2.0, 3.0]
    stop_loss_r:         [0.75, 1.0, 1.25]
    volume_multiplier:   [1.0, 1.3, 1.5]
    min_signal_strength: [0.15, 0.20, 0.30]
```

`param_grid`-Keys überschreiben `strategy.params` je Kombination.
Alle anderen Werte aus dem Config-Block bleiben erhalten.

### 2 · CLI-Aufruf

```bash
python main.py wfo --config configs/orb_backtest.yaml
```

Keine weiteren Flags nötig – alle WFO-Parameter kommen aus dem YAML.
`--help` funktioniert ohne laufenden Event-Loop.

### 3 · Programmatischer Aufruf

```python
from backtest.wfo import WalkForwardOptimizer, run_flux_backtest
from core.config import load_config

cfg = load_config("configs/orb_backtest.yaml")
wfo_raw = cfg.model_extra.get("wfo", {})

wfo = WalkForwardOptimizer(
    data_dict=data,          # dict[str, pd.DataFrame] – OHLCV je Symbol
    vix_series=vix,          # Optional[pd.Series] Tages-VIX
    base_cfg=cfg,
    param_grid=wfo_raw["param_grid"],
    backtest_func=run_flux_backtest,   # Standard-Adapter auf BarByBarEngine
    is_days=wfo_raw.get("is_days", 120),
    oos_days=wfo_raw.get("oos_days", 30),
    step_days=wfo_raw.get("step_days", 20),
    metric=wfo_raw.get("metric", "sharpe"),
    min_trades_is=wfo_raw.get("min_trades_is", 20),
    spy_df=spy_df,
)
windows = wfo.run()

# Ergebnisse auswerten
print(wfo.summary_frame().to_string(index=False))
print(wfo.stability_report())

combined = wfo.combined_oos_equity()
total_return = (combined.iloc[-1] / combined.iloc[0] - 1) * 100
print(f"Kombinierte OOS-Rendite: {total_return:+.2f}%")
```

### 4 · Eigene backtest_func (Custom-Engine)

`backtest_func` muss diese Signatur haben:

```python
def my_func(
    data:        dict[str, pd.DataFrame],
    vix:         pd.Series | None,
    cfg:         AppConfig,
    *,
    vix3m_series: pd.Series | None = None,
    spy_df:       pd.DataFrame | None = None,
    silent:       bool = True,
) -> dict:
    ...
    return {
        "sharpe":           float,
        "cagr_pct":         float,
        "max_drawdown_pct": float,
        "num_trades":       int,       # oder total_trades
        "equity_curve":     pd.Series, # optional – für combined_oos_equity
        "trades":           list,      # optional
    }
```

!!! warning "Pickelbarkeit bei n_workers > 1"
    Wenn `n_workers > 1` (Parallelisierung via `ProcessPoolExecutor`), muss
    `backtest_func` als Top-Level-Funktion importierbar sein – keine Lambdas
    oder Closures. `run_flux_backtest` aus `backtest.wfo` erfüllt das.
    Für Tests lokale Mock-Funktionen mit `n_workers=1` nutzen.

### 5 · Ergebnis-Objekte

| Attribut/Methode | Beschreibung |
|---|---|
| `wfo.windows` | `list[WFOWindow]` – alle Fenster |
| `wfo.summary_frame()` | DataFrame: IS/OOS-Zeiträume, Metriken je Fenster |
| `wfo.stability_report()` | DataFrame: Parameter-Häufigkeit, OOS-Sharpe/CAGR |
| `wfo.combined_oos_equity()` | Verkettete, normalisierte OOS-Equity-Kurve |
| `win.best_params` | Dict mit den besten IS-Parametern |
| `win.oos_metrics` | Dict mit OOS-Metriken (sharpe, cagr_pct, …) |

### 6 · Unit-Test-Pattern

```python
# Kein Netzwerk, keine echte Strategie – rein deterministisch
def _mock_backtest(data, vix, cfg, *, vix3m_series=None, spy_df=None, silent=True):
    foo = float(cfg.strategy.params.get("foo", 1.0))
    return {"sharpe": foo, "num_trades": 30, "cagr_pct": foo * 5.0,
            "max_drawdown_pct": 1.0}

wfo = WalkForwardOptimizer(
    data_dict={"SPY": daily_df_30_rows},
    vix_series=None,
    base_cfg=cfg,
    param_grid={"foo": [1.0, 2.0, 3.0]},
    backtest_func=_mock_backtest,
    is_days=10, oos_days=5, step_days=10,
    min_trades_is=1,
    n_workers=1,   # sequential – Mock ist nicht picklebar
)
assert wfo.estimated_window_count() == 2
windows = wfo.run()
assert len(windows) == 2
assert windows[0].best_params == {"foo": 3.0}
```

Fertige Tests: `tests/unit/test_wfo.py` (7 Tests, kein Netzwerk).

---

## SPY & VIX für den Backtest

```python
# SPY für Trend-Filter
spy_df = await prov.get_bars("SPY", start, end, "5Min")

# VIX für Regime-Filter (Daily, aus yfinance)
import yfinance as yf
vix = yf.download("^VIX", start=start, end=end, interval="1d")["Close"]
vix3m = yf.download("^VIX3M", start=start, end=end, interval="1d")["Close"]

result = await engine.run(data=data, spy_df=spy_df,
                           vix_series=vix, vix3m_series=vix3m)
```
